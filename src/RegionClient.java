/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import static org.jboss.netty.channel.ChannelHandler.Sharable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Stateful handler that manages a connection to a specific RegionServer.
 * <p>
 * This handler manages the RPC IDs, the serialization and de-serialization of
 * RPC requests and responses, and keeps track of the RPC in flights for which
 * a response is currently awaited, as well as temporarily buffered RPCs that
 * are awaiting to be sent to the network.
 * <p>
 * For more details on how RPCs are serialized and de-serialized, read the
 * unofficial HBase RPC documentation in the code of the {@link HBaseRpc} class.
 * <p>
 * This class needs careful synchronization.   It's a non-sharable handler,
 * meaning there is one instance of it per Netty {@link Channel} and each
 * instance is only used by one Netty IO thread at a time.  At the same time,
 * {@link HBaseClient} calls methods of this class from random threads at
 * random times.  The bottom line is that any data only used in the Netty IO
 * threads doesn't require synchronization, everything else does.
 * <p>
 * Acquiring the monitor on an object of this class will prevent it from
 * accepting write requests as well as buffering requests if the underlying
 * channel isn't connected.
 */
final class RegionClient extends ReplayingDecoder<VoidEnum> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionClient.class);

  /** Maps remote exception types to our corresponding types.  */
  private static final HashMap<String, HBaseException> REMOTE_EXCEPTION_TYPES;
  static {
    REMOTE_EXCEPTION_TYPES = new HashMap<String, HBaseException>();
    REMOTE_EXCEPTION_TYPES.put(NoSuchColumnFamilyException.REMOTE_CLASS,
                               new NoSuchColumnFamilyException(null, null));
    REMOTE_EXCEPTION_TYPES.put(NotServingRegionException.REMOTE_CLASS,
                               new NotServingRegionException(null, null));
    REMOTE_EXCEPTION_TYPES.put(UnknownScannerException.REMOTE_CLASS,
                               new UnknownScannerException(null, null));
    REMOTE_EXCEPTION_TYPES.put(UnknownRowLockException.REMOTE_CLASS,
                               new UnknownRowLockException(null, null));
  }

  /** The HBase client we belong to.  */
  private final HBaseClient hbase_client;

  /**
   * The channel we're connected to.
   * This will be {@code null} while we're not connected to the RegionServer.
   * This attribute is volatile because {@link #shutdown} may access it from a
   * different thread.
   */
  private volatile Channel chan;

  /**
   * Set to {@code true} once we've disconnected from the server.
   * This way, if any thread is still trying to use this client after it's
   * been removed from the caches in the {@link HBaseClient}, we will
   * immediately fail / reschedule its requests.
   * <p>
   * Manipulating this value requires synchronizing on `this'.
   */
  private boolean dead = false;

  /**
   * What RPC protocol version is this RegionServer using?.
   * -1 means unknown.  No synchronization is typically used to read / write
   * this value, as it's typically accessed after a volatile read or updated
   * before a volatile write on {@link #deferred_server_version}.
   */
  private byte server_version = -1;

  /**
   * If we're in the process of looking up the server version...
   * ... This will be non-null.
   */
  private volatile Deferred<Long> deferred_server_version;

  /**
   * Multi-put request in which we accumulate buffered edits.
   * Manipulating this reference requires synchronizing on `this'.
   */
  private MultiPutRequest edit_buffer;

  /**
   * RPCs we've been asked to serve while disconnected from the RegionServer.
   * This reference is lazily created.  Synchronize on `this' before using it.
   *
   * TODO(tsuna): Properly manage this buffer.  Right now it's unbounded, we
   * don't auto-reconnect anyway, etc.
   */
  private ArrayList<HBaseRpc> pending_rpcs;

  /**
   * Maps an RPC ID to the in-flight RPC that was given this ID.
   * RPCs can be sent out from any thread, so we need a concurrent map.
   */
  private final ConcurrentHashMap<Integer, HBaseRpc> rpcs_inflight =
    new ConcurrentHashMap<Integer, HBaseRpc>();

  /**
   * A monotonically increasing counter for RPC IDs.
   * RPCs can be sent out from any thread, so we need an atomic integer.
   * RPC IDs can be arbitrary.  So it's fine if this integer wraps around and
   * becomes negative.  They don't even have to start at 0, but we do it for
   * simplicity and ease of debugging.
   */
  private final AtomicInteger rpcid = new AtomicInteger(-1);

  private final TimerTask flush_timer = new TimerTask() {
    public void run(final Timeout timeout) {
      periodicFlush();
    }
    public String toString() {
      return "flush commits of " + RegionClient.this;
    }
  };

  /**
   * Semaphore used to rate-limit META lookups and prevent "META storms".
   * <p>
   * Once we have more than this number of concurrent META lookups, we'll
   * start to throttle ourselves slightly.
   * @see #acquireMetaLookupPermit
   */
  private final Semaphore meta_lookups = new Semaphore(100);

  /**
   * Constructor.
   * @param hbase_client The HBase client this instance belongs to.
   */
  public RegionClient(final HBaseClient hbase_client) {
    this.hbase_client = hbase_client;
  }

  /**
   * Tells whether or not this handler should be used.
   * <p>
   * This method is not synchronized.  You need to synchronize on this
   * instance if you need a memory visibility guarantee.  You may not need
   * this guarantee if you're OK with the RPC finding out that the connection
   * has been reset "the hard way" and you can retry the RPC.  In this case,
   * you can call this method as a hint.  After getting the initial exception
   * back, this thread is guaranteed to see this method return {@code false}
   * without synchronization needed.
   * @return {@code false} if this handler is known to have been disconnected
   * from the server and sending an RPC (via {@link #sendRpc} or any other
   * indirect mean such as {@link #getClosestRowBefore}) will fail immediately
   * by having the RPC's {@link Deferred} called back immediately with a
   * {@link ConnectionResetException}.  This typically means that you got a
   * stale reference (or that the reference to this instance is just about to
   * be invalidated) and that you shouldn't use this instance.
   */
  public boolean isAlive() {
    return !dead;
  }

  /** Periodically flushes buffered edits.  */
  private void periodicFlush() {
    if (chan != null || dead) {
      // If we're dead, we want to flush our edits (this will cause them to
      // get failed / retried).
      if (LOG.isDebugEnabled()) {
        LOG.debug("Periodic flush timer: flushing edits for " + this);
      }
      // Copy the edits to local variables and null out the buffers.
      MultiPutRequest edit_buffer;
      synchronized (this) {
        edit_buffer = this.edit_buffer;
        this.edit_buffer = null;
      }
      if (edit_buffer == null || edit_buffer.size() == 0) {
        return;  // Nothing to flush, let's stop periodic flushes for now.
      }
      doFlush(edit_buffer);
    }
  }

  /** Schedules the next periodic flush of buffered edits.  */
  private void scheduleNextPeriodicFlush() {
    final short interval = hbase_client.getFlushInterval();
    if (interval > 0) {
      // Since we often connect to many regions at the same time, we should
      // try to stagger the flushes to avoid flushing too many different
      // RegionClient concurrently.
      // To this end, we "randomly" adjust the time interval using the
      // system's time.  nanoTime uses the machine's most precise clock, but
      // often nanoseconds (the lowest bits) aren't available.  Most modern
      // machines will return microseconds so we can cheaply extract some
      // random adjustment from that.
      short adj = (short) (System.nanoTime() & 0xF0);
      if (interval < 3 * adj) {  // Is `adj' too large compared to `interval'?
        adj >>>= 2;  // Reduce the adjustment to not be too far off `interval'.
      }
      if ((adj & 0x10) == 0x10) {  // if some arbitrary bit is set...
        adj = (short) -adj;        // ... use a negative adjustment instead.
      }
      hbase_client.timer.newTimeout(flush_timer, interval + adj, MILLISECONDS);
    }
  }

  /**
   * Flushes to HBase any buffered client-side write operation.
   * <p>
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  public Deferred<Object> flush() {
    // Copy the edits to local variables and null out the buffers.
    MultiPutRequest edit_buffer;
    synchronized (this) {
      edit_buffer = this.edit_buffer;
      this.edit_buffer = null;
    }
    return doFlush(edit_buffer);
  }

  /**
   * Flushes the given multi-puts.
   * <p>
   * Typically this method should be called after atomically getting the
   * buffers that we need to flush.
   * @param edit_buffer Edits to flush, can be {@code null}.
   * @return A {@link Deferred}, whose callback chain will be invoked the
   * given edits have been flushed.  If the argument is {@code null}, returns
   * a {@link Deferred} already called back.
   */
  private Deferred<Object> doFlush(final MultiPutRequest edit_buffer) {
    // Note: the argument are shadowing the attribute of the same name.
    // This is intentional.
    if (edit_buffer == null) {  // Nothing to do.
      return Deferred.fromResult(null);
    }
    final Deferred<Object> d = edit_buffer.getDeferred();
    sendRpc(edit_buffer);
    return d;
  }

  /**
   * Attempts to gracefully terminate the connection to this RegionServer.
   */
  public Deferred<Object> shutdown() {
    final class RetryShutdown<T> implements Callback<Deferred<Object>, T> {
      private final int nrpcs;
      RetryShutdown(final int nrpcs) {
        this.nrpcs = nrpcs;
      }
      public Deferred<Object> call(final T ignored) {
        return shutdown();
      }
      public String toString() {
        return "wait until " + nrpcs + " RPCs complete";
      }
    };

    // First, check whether we have RPCs in flight.  If we do, we need to wait
    // until they complete.
    {
      final ArrayList<Deferred<Object>> inflight =
        new ArrayList<Deferred<Object>>();
      for (final HBaseRpc rpc : rpcs_inflight.values()) {
        inflight.add(rpc.getDeferred());
      }
      final int size = inflight.size();
      if (size > 0) {
        return Deferred.group(inflight)
          .addCallbackDeferring(new RetryShutdown<ArrayList<Object>>(size));
      }
      // Then check whether have buffered edits.  If we do, flush them.
      // Copy the edits to local variables and null out the buffers.
      MultiPutRequest edit_buffer;
      synchronized (this) {
        edit_buffer = this.edit_buffer;
        this.edit_buffer = null;
      }
      if (edit_buffer != null && edit_buffer.size() != 0) {
        return doFlush(edit_buffer)
          .addCallbackDeferring(new RetryShutdown<Object>(1));
      }
    }

    synchronized (this) {
      if (pending_rpcs != null && !pending_rpcs.isEmpty()) {
        final ArrayList<Deferred<Object>> pending =
          new ArrayList<Deferred<Object>>(pending_rpcs.size());
        for (final HBaseRpc rpc : pending_rpcs) {
          pending.add(rpc.getDeferred());
        }
        return Deferred.group(pending).addCallbackDeferring(
          new RetryShutdown<ArrayList<Object>>(pending.size()));
      }
    }

    final Channel chancopy = chan;     // Make a copy as ...
    if (chancopy == null) {
      return Deferred.fromResult(null);
    }
    LOG.debug("Shutdown requested, chan={}", chancopy);
    if (chancopy.isConnected()) {
      Channels.disconnect(chancopy);   // ... this is going to set it to null.
      // At this point, all in-flight RPCs are going to be failed.
    }
    if (chancopy.isBound()) {
      Channels.unbind(chancopy);
    }
    // It's OK to call close() on a Channel if it's already closed.
    final ChannelFuture future = Channels.close(chancopy);

    // Now wrap the ChannelFuture in a Deferred.
    final Deferred<Object> d = new Deferred<Object>();
    // Opportunistically check if it's already completed successfully.
    if (future.isSuccess()) {
      d.callback(null);
    } else {
      // If we get here, either the future failed (yeah, that sounds weird)
      // or the future hasn't completed yet (heh).
      future.addListener(new ChannelFutureListener() {
        public void operationComplete(final ChannelFuture future) {
          if (future.isSuccess()) {
            d.callback(null);
          }
          final Throwable t = future.getCause();
          if (t instanceof Exception) {
            d.callback(t);
          } else {
            // Wrap the Throwable because Deferred doesn't handle Throwables,
            // it only uses Exception.
            d.callback(new NonRecoverableException("Failed to shutdown: "
                                                   + RegionClient.this, t));
          }
        }
      });
    }
    return d;
  }

  private static final byte[] GET_PROTOCOL_VERSION = new byte[] {
    'g', 'e', 't',
    'P', 'r', 'o', 't', 'o', 'c', 'o', 'l',
    'V', 'e', 'r', 's', 'i', 'o', 'n'
  };

  final static class GetProtocolVersionRequest extends HBaseRpc {

    GetProtocolVersionRequest() {
      super(GET_PROTOCOL_VERSION);
    }

    ChannelBuffer serialize(final byte unused_server_version) {
    /** Pre-serialized form for this RPC, which is always the same.  */
      // num param + type 1 + string length + string + type 2 + long
      final ChannelBuffer buf = newBuffer(4 + 1 + 1 + 44 + 1 + 8);
      buf.writeInt(2);  // Number of parameters.
      // 1st param.
      writeHBaseString(buf, "org.apache.hadoop.hbase.ipc.HRegionInterface");
      writeHBaseLong(buf, 24);  // 2nd param.
      return buf;
    }
  };

  /**
   * Returns a new {@link GetProtocolVersionRequest} ready to go.
   * The RPC returned already has the right callback set.
   */
  @SuppressWarnings("unchecked")
  private GetProtocolVersionRequest getProtocolVersionRequest() {
    final GetProtocolVersionRequest rpc = new GetProtocolVersionRequest();
    final Deferred/*<Long>*/ version = rpc.getDeferred();
    deferred_server_version = version;  // Volatile write.
    version.addCallback(got_protocol_version);
    return rpc;
  }

  /**
   * Asks the server which RPC protocol version it's running.
   * @return a Deferred {@link Long}.
   */
  @SuppressWarnings("unchecked")
  public Deferred<Long> getProtocolVersion() {
    Deferred<Long> version = deferred_server_version;  // Volatile read.
    if (server_version != -1) {
      return Deferred.fromResult((long) server_version);
    }
    // Non-atomic check-then-update is OK here.  In case of a race and a
    // thread overwrites this reference by creating another lookup, we just
    // pay for an unnecessary network round-trip, not a big deal.
    if (version != null) {
      return version;
    }

    final GetProtocolVersionRequest rpc = getProtocolVersionRequest();
    sendRpc(rpc);
    return (Deferred) rpc.getDeferred();
  }

  /** Singleton callback to handle responses of getProtocolVersion RPCs.  */
  private final Callback<Long, Object> got_protocol_version =
    new Callback<Long, Object>() {
      public Long call(final Object response) {
        if (!(response instanceof Long)) {
          throw new InvalidResponseException(Long.class, response);
        }
        final Long version = (Long) response;
        final long v = version;
        if (v < 0 || v > Byte.MAX_VALUE) {
          throw new InvalidResponseException("getProtocolVersion returned a "
            + (v < 0 ? "negative" : "too large") + " value", version);
        }
        final byte prev_version = server_version;
        server_version = (byte) v;
        deferred_server_version = null;  // Volatile write.
        if (prev_version == -1) {   // We're 1st to get the version.
          if (LOG.isDebugEnabled()) {
            LOG.debug(chan + " uses RPC protocol version " + server_version);
          }
        } else if (prev_version != server_version) {
          LOG.error("WTF?  We previously found that " + chan + " uses RPC"
                    + " protocol version " + prev_version + " but now the "
                    + " server claims to be using version " + server_version);
        }
        return (Long) response;
      }
      public String toString() {
        return "type getProtocolVersion response";
      }
    };

  private static final byte[] GET_CLOSEST_ROW_BEFORE = new byte[] {
    'g', 'e', 't',
    'C', 'l', 'o', 's', 'e', 's', 't',
    'R', 'o', 'w',
    'B', 'e', 'f', 'o', 'r', 'e'
  };

  /**
   * Attempts to acquire a permit before performing a META lookup.
   * <p>
   * This method should be called prior to starting a META lookup with
   * {@link #getClosestRowBefore}.  Whenever it's called we try to acquire a
   * permit from the {@link #meta_lookups} semaphore.  Because we promised a
   * non-blocking API, we actually only wait for a handful of milliseconds to
   * get a permit.  If you don't get one, you can proceed anyway without one.
   * This strategy doesn't incur too much overhead seems rather effective to
   * avoid "META storms".  For instance, if a thread creates 100k requests for
   * slightly different rows that all miss our local META cache, then we'd
   * kick off 100k META lookups.  The very small pause introduced by the
   * acquisition of a permit is typically effective enough to let in-flight
   * META lookups complete and throttle the application a bit, which will
   * cause subsequent requests to hit our META cache.
   * <p>
   * If this method returns {@code true}, you <b>must</b> make sure you call
   * {@link #releaseMetaLookupPermit} once you're done with META.
   * @return {@code true} if a permit was acquired, {@code false} otherwise.
   */
  boolean acquireMetaLookupPermit() {
    try {
      // With such a low timeout, the JVM may chose to spin-wait instead of
      // de-scheduling the thread (and causing context switches and whatnot).
      return meta_lookups.tryAcquire(5, MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();  // Make this someone else's problem.
      return false;
    }
  }

  /**
   * Releases a META lookup permit that was acquired.
   * @see #acquireMetaLookupPermit
   */
  void releaseMetaLookupPermit() {
    meta_lookups.release();
  }

  /**
   * Finds the highest row that's less than or equal to the given row.
   * @param region The region in which to search.
   * @param tabl The table to which the key belongs.
   * @param row The row to search.
   * @param family The family to get.
   * @return A Deferred {@link ArrayList} of {@link KeyValue}.  The list is
   * guaranteed to be non-{@code null} but may be empty.
   */
  public
    Deferred<ArrayList<KeyValue>> getClosestRowBefore(final RegionInfo region,
                                                      final byte[] tabl,
                                                      final byte[] row,
                                                      final byte[] family) {
    final class GetClosestRowBefore extends HBaseRpc {
      GetClosestRowBefore() {
        super(GET_CLOSEST_ROW_BEFORE, tabl, row);
      }

      @Override
      ChannelBuffer serialize(final byte unused_server_version) {
        // region.length and row.length will use at most a 3-byte VLong.
        // This is because VLong wastes 1 byte of meta-data + 2 bytes of
        // payload.  HBase's own KeyValue code uses a short to store the row
        // length.  Finally, family.length cannot be on more than 1 byte,
        // HBase's own KeyValue code uses a byte to store the family length.
        final byte[] region_name = region.name();
        final ChannelBuffer buf = newBuffer(4      // num param
          + 1 + 2 + region_name.length             // 3 times 1 byte for the
          + 1 + 4 + row.length                     //   parm type + VLong
          + 1 + 1 + family.length);                //             + array
        buf.writeInt(3);  // Number of parameters.
        writeHBaseByteArray(buf, region_name);     // 1st param.
        writeHBaseByteArray(buf, row);             // 2nd param.
        writeHBaseByteArray(buf, family);          // 3rd param.
        return buf;
      }

    };

    final HBaseRpc rpc = new GetClosestRowBefore();
    rpc.setRegion(region);
    final Deferred<ArrayList<KeyValue>> d = rpc.getDeferred()
      .addCallback(got_closest_row_before);
    sendRpc(rpc);
    return d;
  }

  /** Singleton callback to handle responses of getClosestRowBefore RPCs.  */
  private static final Callback<ArrayList<KeyValue>, Object>
    got_closest_row_before =
      new Callback<ArrayList<KeyValue>, Object>() {
        public ArrayList<KeyValue> call(final Object response) {
          if (response == null) {  // No result.
            return new ArrayList<KeyValue>(0);
          } else if (response instanceof ArrayList) {
            @SuppressWarnings("unchecked")
            final ArrayList<KeyValue> row = (ArrayList<KeyValue>) response;
            return row;
          } else {
            throw new InvalidResponseException(ArrayList.class, response);
          }
        }
        public String toString() {
          return "type getClosestRowBefore response";
        }
      };

  /**
   * Buffers the given edit and possibly flushes the buffer if needed.
   * <p>
   * If the edit buffer grows beyond a certain size, we will flush it
   * even though the flush interval specified by the client hasn't
   * elapsed yet.
   * @param request An edit to sent to HBase.
   */
  private void bufferEdit(final PutRequest request) {
    MultiPutRequest multiput;
    boolean schedule_flush = false;

    synchronized (this) {
      if (edit_buffer == null) {
        edit_buffer = new MultiPutRequest();
        addMultiPutCallbacks(edit_buffer);
        schedule_flush = true;
      }
      multiput = edit_buffer;
      // Unfortunately we have to hold the monitor on `this' while we do
      // this entire atomic dance.
      multiput.add(request);
      if (multiput.size() < 1024) {  // XXX Don't hardcode.
        multiput = null;   // We're going to buffer this edit for now.
      } else {
        // Execute the edits buffered so far.  But first we must clear
        // the reference to the buffer we're about to send to HBase.
        edit_buffer = new MultiPutRequest();
        addMultiPutCallbacks(edit_buffer);
      }
    }

    if (schedule_flush) {
      scheduleNextPeriodicFlush();
    } else if (multiput != null) {
      sendRpc(multiput);
    }
  }

  /**
   * Creates callbacks to handle a multi-put and adds them to the request.
   * @param request The request for which we must handle the response.
   */
  private void addMultiPutCallbacks(final MultiPutRequest request) {
    final class MultiPutCallback implements Callback<Object, Object> {
      public Object call(final Object resp) {
        if (!(resp instanceof MultiPutResponse)) {
          if (resp instanceof PutRequest) {  // Single-edit multi-put?
            return null;  // Yes, nothing to do.  See multiPutToSinglePut.
          }
          throw new InvalidResponseException(MultiPutResponse.class, resp);
        }
        final MultiPutResponse response = (MultiPutResponse) resp;
        final Bytes.ByteMap<Integer> failures = response.failures();
        if (failures.isEmpty()) {
          for (final PutRequest edit : request.edits()) {
            edit.callback(null);  // Success.
          }
          return null;  // Yay, success!
        }
        // TODO(tsuna): Wondering whether logging this at the WARN level
        // won't be too spammy / distracting.  Maybe we can tune this down to
        // DEBUG once the pretty bad bug HBASE-2898 is fixed.
        LOG.warn("Some edits failed for " + failures
                 + ", hopefully it's just due to a region split.");
        for (final PutRequest edit : request.handlePartialFailure(failures)) {
          retryEdit(edit, null);
        }
        return null;  // We're retrying, so let's call it a success for now.
      }
      public String toString() {
        return "multiPut response";
      }
    };
    final class MultiPutErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (!(e instanceof RecoverableException)) {
          for (final PutRequest edit : request.edits()) {
            edit.callback(e);
          }
          return e;  // Can't recover from this error, let it propagate.
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Multi-put request failed, retrying each of the "
                    + request.size() + " edits individually.", e);
        }
        for (final PutRequest edit : request.edits()) {
          retryEdit(edit, (RecoverableException) e);
        }
        return null;  // We're retrying, so let's call it a success for now.
      }
      public String toString() {
        return "multiPut errback";
      }
    };
    request.getDeferred().addCallbacks(new MultiPutCallback(),
                                       new MultiPutErrback());
  }

  /**
   * Retries an edit that failed with a recoverable error.
   * @param edit The edit that failed.
   * @param e The recoverable error that caused the edit to fail, if known.
   * Can be {@code null}.
   * @param The deferred result of the new attempt to send this edit.
   */
  private Deferred<Object> retryEdit(final PutRequest edit,
                                     final RecoverableException e) {
    if (HBaseClient.cannotRetryRequest(edit)) {
      return HBaseClient.tooManyAttempts(edit, e);
    }
    edit.setBufferable(false);
    return hbase_client.sendRpcToRegion(edit);
  }

  /**
   * Creates callbacks to handle a single-put and adds them to the request.
   * @param edit The edit for which we must handle the response.
   */
  private void addSingleEditCallbacks(final PutRequest edit) {
    // There's no callback to add on a single put request, because
    // the remote method returns `void', so we only need an errback.
    final class PutErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (!(e instanceof RecoverableException)) {
          return e;  // Can't recover from this error, let it propagate.
        }
        return retryEdit(edit, (RecoverableException) e);
      }
      public String toString() {
        return "put errback";
      }
    };
    edit.getDeferred().addErrback(new PutErrback());
  }

  /**
   * Sends an RPC out to the wire, or queues it if we're disconnected.
   * <p>
   * <b>IMPORTANT</b>: Make sure you've got a reference to the Deferred of this
   * RPC ({@link HBaseRpc#getDeferred}) before you call this method.  Otherwise
   * there's a race condition if the RPC completes before you get a chance to
   * call {@link HBaseRpc#getDeferred} (since presumably you'll need to either
   * return that Deferred or attach a callback to it).
   */
  void sendRpc(HBaseRpc rpc) {
    if (chan != null) {
      if (rpc instanceof PutRequest) {
        final PutRequest edit = (PutRequest) rpc;
        if (edit.canBuffer() && hbase_client.getFlushInterval() > 0) {
          bufferEdit(edit);
          return;
        }
        addSingleEditCallbacks(edit);
      } else if (rpc instanceof MultiPutRequest) {
        // Transform single-edit multi-put into single-put.
        final MultiPutRequest multiput = (MultiPutRequest) rpc;
        if (multiput.size() == 1) {
          rpc = multiPutToSinglePut(multiput);
        }
      }
      final ChannelBuffer serialized = encode(rpc);
      if (serialized == null) {  // Error during encoding.
        return;  // Stop here.  RPC has been failed already.
      }
      final Channel chan = this.chan;  // Volatile read.
      if (chan != null) {  // Double check if we disconnected during encode().
        Channels.write(chan, serialized);
        return;
      }  // else: continue to the "we're disconnected" code path below.
    }

    boolean dead;  // Shadows this.dead;
    synchronized (this) {
      if (!(dead = this.dead)) {
        if (pending_rpcs == null) {
          pending_rpcs = new ArrayList<HBaseRpc>();
        }
        pending_rpcs.add(rpc);
      }
    }
    if (dead) {
      if (rpc.getRegion() == null) {  // Can't retry.
        rpc.callback(new ConnectionResetException(null));
      } else {
        hbase_client.sendRpcToRegion(rpc);  // Re-schedule the RPC.
      }
      return;
    }
    LOG.debug("RPC queued: {}", rpc);
  }

  /**
   * Transforms the given single-edit multi-put into a regular single-put.
   * @param multiput The single-edit multi-put to transform.
   */
  private PutRequest multiPutToSinglePut(final MultiPutRequest multiput) {
    final PutRequest edit = multiput.edits().get(0);
    addSingleEditCallbacks(edit);
    // Once the single-edit is done, we still need to make sure we're
    // going to run the callback chain of the MultiPutRequest.
    final class Multi2SingleCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
        // If there was a problem, let the MultiPutRequest know.
        // Otherwise, give the PutRequest in argument to the MultiPutRequest
        // callback.  This is kind of a kludge: the MultiPutRequest callback
        // will understand that this means that this single-edit was already
        // successfully executed on its own.
        multiput.callback(arg instanceof Exception ? arg : edit);
        return arg;
      }
    }
    edit.getDeferred().addBoth(new Multi2SingleCB());
    return edit;
  }

  // -------------------------------------- //
  // Managing the life-cycle of the channel //
  // -------------------------------------- //

  @Override
  public void channelConnected(final ChannelHandlerContext ctx,
                               final ChannelStateEvent e) {
    chan = e.getChannel();
    ArrayList<HBaseRpc> rpcs;
    synchronized (this) {
      rpcs = pending_rpcs;
      pending_rpcs = null;
    }
    if (rpcs != null) {
      for (final HBaseRpc rpc : rpcs) {
        if (chan != null) {
          LOG.debug("Executing RPC queued: {}", rpc);
          final ChannelBuffer serialized = encode(rpc);
          if (serialized != null) {
            Channels.write(chan, serialized);
          }
        }
      }
    }
  }

  @Override
  public void channelDisconnected(final ChannelHandlerContext ctx,
                                  final ChannelStateEvent e) throws Exception {
    chan = null;
    super.channelDisconnected(ctx, e);  // Let the ReplayingDecoder cleanup.
    cleanup(e.getChannel());
  }

  @Override
  public void channelClosed(final ChannelHandlerContext ctx,
                            final ChannelStateEvent e) {
    chan = null;
    // No need to call super.channelClosed() because we already called
    // super.channelDisconnected().  If we get here without getting a
    // DISCONNECTED event, then we were never connected in the first place so
    // the ReplayingDecoder has nothing to cleanup.
    cleanup(e.getChannel());
  }

  /**
   * Cleans up any outstanding or lingering RPC (used when shutting down).
   * <p>
   * All RPCs in flight will fail with a {@link ConnectionResetException} and
   * all edits buffered will be re-scheduled.
   */
  private void cleanup(final Channel chan) {
    final ConnectionResetException exception =
      new ConnectionResetException(chan);
    failOrRetryRpcs(rpcs_inflight.values(), exception);
    rpcs_inflight.clear();

    ArrayList<HBaseRpc> rpcs;
    MultiPutRequest multiput;
    synchronized (this) {
      dead = true;
      rpcs = pending_rpcs;
      pending_rpcs = null;
      multiput = edit_buffer;
      edit_buffer = null;
    }
    if (rpcs != null) {
      failOrRetryRpcs(rpcs, exception);
    }
    if (multiput != null) {
      multiput.callback(exception);  // Make it fail.
    }
  }

  /**
   * Fail all RPCs in a collection or attempt to reschedule them if possible.
   * @param rpcs A possibly empty but non-{@code null} collection of RPCs.
   * @param exception The exception with which to fail RPCs that can't be
   * retried.
   */
  private void failOrRetryRpcs(final Collection<HBaseRpc> rpcs,
                               final ConnectionResetException exception) {
    for (final HBaseRpc rpc : rpcs) {
      final RegionInfo region = rpc.getRegion();
      if (region == null) {  // Can't retry, dunno where this RPC should go.
        rpc.callback(exception);
      } else {
        final NotServingRegionException nsre =
          new NotServingRegionException("Connection reset: "
                                        + exception.getMessage(), rpc);
        // Re-schedule the RPC by (ab)using the NSRE handling mechanism.
        hbase_client.handleNSRE(rpc, region.name(), nsre);
      }
    }
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx,
                             final ChannelEvent e) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent event) {
    final Throwable e = event.getCause();
    final Channel c = event.getChannel();
    final SocketAddress remote = c.getRemoteAddress();

    if (e instanceof RejectedExecutionException) {
      LOG.warn("RPC rejected by the executor,"
               + " ignore this if we're shutting down", e);
    } else {
      LOG.error("Unexpected exception from downstream.", e);
    }
    // TODO(tsuna): Do we really want to do that all the time?
    Channels.close(c);
  }

  // ------------------------------- //
  // Low-level encoding and decoding //
  // ------------------------------- //

  /**
   * Callback that retries the given RPC and returns its argument unchanged.
   */
  final class RetryRpc<T> implements Callback<T, T> {
    private final HBaseRpc rpc;

    RetryRpc(final HBaseRpc rpc) {
      this.rpc = rpc;
    }

    public T call(final T arg) {
      sendRpc(rpc);
      return arg;
    }
  }

  /**
   * Encodes an RPC and sends it downstream (to the wire).
   * <p>
   * This method can be called from any thread so it needs to be thread-safe.
   * @param rpc The RPC to send downstream.
   * @return The buffer to write to the channel or {@code null} if there was
   * an error and there's nothing to write.
   */
  private ChannelBuffer encode(final HBaseRpc rpc) {
    if (!rpc.hasDeferred()) {
      throw new AssertionError("Should never happen!  rpc=" + rpc);
    }
    if (rpc.versionSensitive() && server_version == -1) {
      getProtocolVersion().addBoth(new RetryRpc<Long>(rpc));
      return null;
    }

    // TODO(tsuna): Add rate-limiting here.  We don't want to send more than
    // N QPS to a given region server.
    // TODO(tsuna): Check the size() of rpcs_inflight.  We don't want to have
    // more than M RPCs in flight at the same time, and we may be overwhelming
    // the server if we do.

    final int rpcid = this.rpcid.incrementAndGet();
    ChannelBuffer payload;
    try {
      payload = rpc.serialize(server_version);
      // We assume that payload has enough bytes at the beginning for us to
      // "fill in the blanks" and put the RPC header.  This is accounted for
      // automatically by HBaseRpc#newBuffer.  If someone creates their own
      // buffer without this extra space at the beginning, we're going to
      // corrupt the RPC at this point.
      final byte[] method = rpc.method();
      // The first int is the size of the message, excluding the 4 bytes
      // needed for the size itself, hence the `-4'.
      payload.setInt(0, payload.readableBytes() - 4); // 4 bytes
      payload.setInt(4, rpcid);                       // 4 bytes
      payload.setShort(8, method.length);             // 2 bytes
      payload.setBytes(10, method);                   // method.length bytes
    } catch (Exception e) {
      LOG.error("Uncaught exception while serializing RPC: " + rpc, e);
      rpc.callback(e);  // Make the RPC fail with the exception.
      return null;
    }

    // TODO(tsuna): This is the right place to implement message coalescing.
    // If the payload buffer is small (like way less than 1400 bytes), we may
    // want to wait a handful of milliseconds to see if more RPCs come in for
    // this region server, and then send them all in one go in order to send
    // fewer, bigger TCP packets, and make better use of the network.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending RPC #" + rpcid + ", payload=" + payload + ' '
                + Bytes.pretty(payload));
    }
    {
      final HBaseRpc oldrpc = rpcs_inflight.put(rpcid, rpc);
      if (oldrpc != null) {
        final String wtf = "WTF?  There was already an RPC in flight with"
          + " rpcid=" + rpcid + ": " + oldrpc
          + ".  This happened when sending out: " + rpc;
        LOG.error(wtf);
        // Make it fail.  This isn't an expected failure mode.
        oldrpc.callback(new NonRecoverableException(wtf));
      }
    }
    return payload;
  }

  /**
   * Decodes the response of an RPC and triggers its {@link Deferred}.
   * <p>
   * This method is only invoked by Netty from a Netty IO thread and will not
   * be invoked on the same instance concurrently, so it only needs to be
   * careful when accessing shared state, but certain synchronization
   * constraints may be relaxed.  Basically, anything that is only touched
   * from a Netty IO thread doesn't require synchronization.
   * @param ctx Unused.
   * @param chan The channel on which the response came.
   * @param buf The buffer containing the raw RPC response.
   * @return {@code null}, always.
   */
  @Override
  protected Object decode(final ChannelHandlerContext ctx,
                          final Channel chan,
                          final ChannelBuffer buf,
                          final VoidEnum unused) {
    final int rdx = buf.readerIndex();
    final long start = System.nanoTime();
    LOG.debug("------------------>> ENTERING DECODE >>------------------");
    final int rpcid = buf.readInt();
    final Object decoded = deserialize(buf, rpcid);
    final HBaseRpc rpc = rpcs_inflight.remove(rpcid);
    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcid=" + rpcid
                + ", response size=" + (buf.readerIndex() - rdx) + " bytes"
                + ", " + actualReadableBytes() + " readable bytes left"
                + ", rpc=" + rpc);
    }

    if (rpc == null) {
      final String msg = "Invalid rpcid: " + rpcid + " found in "
        + buf + '=' + Bytes.pretty(buf);
      LOG.error(msg);
      // The problem here is that we don't know which Deferred corresponds to
      // this RPC, since we don't have a valid ID.  So we're hopeless, we'll
      // never be able to recover because responses are not framed, we don't
      // know where the next response will start...  We have to give up here
      // and throw this outside of our Netty handler, so Netty will call our
      // exception handler where we'll close this channel, which will cause
      // all RPCs in flight to be failed.
      throw new NonRecoverableException(msg);
    } else if (decoded instanceof NotServingRegionException
               && rpc.getRegion() != null) {
      // We only handle NSREs for RPCs targeted at a specific region, because
      // if we don't know which region caused the NSRE (e.g. during multiPut)
      // we can't do anything about it.
      hbase_client.handleNSRE(rpc, rpc.getRegion().name(),
                              (NotServingRegionException) decoded);
      return null;
    }

    try {
      rpc.callback(decoded);
    } catch (Exception e) {
      LOG.error("Unexpected exception while handling RPC #" + rpcid
                + ", rpc=" + rpc + ", buf=" + Bytes.pretty(buf), e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("------------------<< LEAVING  DECODE <<------------------"
                + " time elapsed: " + ((System.nanoTime() - start) / 1000) + "us");
    }
    return null;  // Stop processing here.  The Deferred does everything else.
  }

  /**
   * De-serializes an RPC response.
   * @param buf The buffer from which to de-serialize the response.
   * @param rpcid The ID of the RPC for which we're de-serializing the
   * response.
   * @return The de-serialized RPC response (which can be {@code null}
   * or an exception).
   */
  private Object deserialize(final ChannelBuffer buf, final int rpcid) {
    // The 1st byte of the payload tells us whether the request failed.
    if (buf.readByte() != 0x00) {  // 0x00 means no error.
      // In case of failures, the rest of the response is just 2
      // Hadoop-encoded strings.  The first is the class name of the
      // exception, the 2nd is the message and stack trace.
      final String type = HBaseRpc.readHadoopString(buf);
      final String msg = HBaseRpc.readHadoopString(buf);
      final HBaseException exc = REMOTE_EXCEPTION_TYPES.get(type);
      if (exc != null) {
        return exc.make(msg, rpcs_inflight.get(rpcid));
      } else {
        return new RemoteException(type, msg);
      }
    }
    try {
      return deserializeObject(buf);
    } catch (IllegalArgumentException e) {  // The RPC didn't look good to us.
      return new InvalidResponseException(e.getMessage(), e);
    }
  }

  /**
   * De-serializes a "Writable" serialized by
   * {@code HbaseObjectWritable#writeObject}.
   * @return The de-serialized object (which can be {@code null}).
   */
  private static Object deserializeObject(final ChannelBuffer buf) {
    switch (buf.readByte()) {  // Read the type of the response.
      case  1:  // Boolean
        return buf.readByte() != 0x00;
      case  6:  // Long
        return buf.readLong();
      case 14:  // Writable
        return deserializeObject(buf);  // Recursively de-serialize it.
      case 17:  // NullInstance
        buf.readByte();  // Consume the (useless) type of the "null".
        return null;
      case 37:  // Result
        buf.readByte();  // Read the type again.  See HBASE-2877.
        return parseResult(buf);
      case 38:  // Result[]
        return parseResults(buf);
      case 58:  // MultiPutResponse
        buf.readByte();  // Read the type again.  See HBASE-2877.
        return MultiPutResponse.fromBuffer(buf);
    }
    throw new NonRecoverableException("Couldn't de-serialize "
                                      + Bytes.pretty(buf));
  }

  /**
   * De-serializes an {@code hbase.client.Result} object.
   * @param buf The buffer that contains a serialized {@code Result}.
   * @return The result parsed into a list of {@link KeyValue} objects.
   */
  private static ArrayList<KeyValue> parseResult(final ChannelBuffer buf) {
    final int length = buf.readInt();
    HBaseRpc.checkArrayLength(buf, length);
    // Immediately try to "fault" if `length' bytes aren't available.
    buf.markReaderIndex();
    buf.skipBytes(length);
    buf.resetReaderIndex();
    //LOG.debug("total Result response length={}", length);

    // Pre-compute how many KVs we have so the array below will be rightsized.
    int num_kv = 0;
    {
      int bytes_read = 0;
      while (bytes_read < length) {
        final int kv_length = buf.readInt();
        HBaseRpc.checkArrayLength(buf, kv_length);
        num_kv++;
        bytes_read += kv_length + 4;
        buf.skipBytes(kv_length);
      }
      buf.resetReaderIndex();
    }

    final ArrayList<KeyValue> results = new ArrayList<KeyValue>(num_kv);
    KeyValue kv = null;
    for (int i = 0; i < num_kv; i++) {
      final int kv_length = buf.readInt();  // Previous loop checked it's >0.
      // Now read a KeyValue that spans over kv_length bytes.
      kv = KeyValue.fromBuffer(buf, kv);
      final int key_length = (2 + kv.key().length + 1 + kv.family().length
                              + kv.qualifier().length + 8 + 1);   // XXX DEBUG
      if (key_length + kv.value().length + 4 + 4 != kv_length) {
        badResponse("kv_length=" + kv_length
                    + " doesn't match key_length + value_length ("
                    + key_length + " + " + kv.value().length + ") in " + buf
                    + '=' + Bytes.pretty(buf));
      }
      results.add(kv);
    }
    return results;
  }

  /**
   * De-serializes an {@code hbase.client.Result} array.
   * @param buf The buffer that contains a serialized {@code Result[]}.
   * @return The results parsed into a list of rows, where each row itself is
   * a list of {@link KeyValue} objects.
   */
  private static
    ArrayList<ArrayList<KeyValue>> parseResults(final ChannelBuffer buf) {
    final byte version = buf.readByte();
    if (version != 0x01) {
      LOG.warn("Received unsupported Result[] version: " + version);
      // Keep going anyway, just in case the new version is backwards
      // compatible somehow.  If it's not, we'll just fail later.
    }
    final int nresults = buf.readInt();
    if (nresults < 0) {
      badResponse("Negative number of results=" + nresults + " found in "
                  + buf + '=' + Bytes.pretty(buf));
    } else if (nresults == 0) {
      return null;
    }
    final int length = buf.readInt();  // Guaranteed > 0 as we have > 0 Result.
    HBaseRpc.checkNonEmptyArrayLength(buf, length);
    // Immediately try to "fault" if `length' bytes aren't available.
    buf.markReaderIndex();
    buf.skipBytes(length);
    buf.resetReaderIndex();
    //LOG.debug("total Result[] response length={}", length);
    //LOG.debug("Result[] "+nresults+" buf="+buf+'='+Bytes.pretty(buf));

    final ArrayList<ArrayList<KeyValue>> results =
      new ArrayList<ArrayList<KeyValue>>(nresults);
    int bytes_read = 0;
    for (int i = 0; i < nresults; i++) {
      final int num_kv = buf.readInt();
      //LOG.debug("num_kv="+num_kv);
      bytes_read += 4;
      if (num_kv < 0) {
        badResponse("Negative number of KeyValues=" + num_kv + " for Result["
                    + i + "] found in " + buf + '=' + Bytes.pretty(buf));
      } else if (nresults == 0) {
        continue;
      }
      final ArrayList<KeyValue> result = new ArrayList<KeyValue>(num_kv);
      KeyValue kv = null;
      for (int j = 0; j < num_kv; j++) {
        final int kv_length = buf.readInt();
        HBaseRpc.checkNonEmptyArrayLength(buf, kv_length);
        //LOG.debug("kv_length="+kv_length);
        kv = KeyValue.fromBuffer(buf, kv);
        result.add(kv);
        bytes_read += 4 + kv_length;
      }
      results.add(result);
    }
    if (length != bytes_read) {   // Sanity check.
      badResponse("Result[" + nresults + "] was supposed to be " + length
                  + " bytes, but we only read " + bytes_read + " bytes from "
                  + buf + '=' + Bytes.pretty(buf));
    }
    return results;
  }

  /** Throws an exception with the given error message.  */
  private static void badResponse(final String errmsg) {
    LOG.error(errmsg);
    throw new InvalidResponseException(errmsg, null);
  }

  /**
   * Decodes the response of an RPC and triggers its {@link Deferred}.
   * <p>
   * This method is used by {@link FrameDecoder} when the channel gets
   * disconnected.  The buffer for that channel is passed to this method in
   * case there's anything left in it.
   * @param ctx Unused.
   * @param chan The channel on which the response came.
   * @param buf The buffer containing the raw RPC response.
   * @return {@code null}, always.
   */
  @Override
  protected Object decodeLast(final ChannelHandlerContext ctx,
                              final Channel chan,
                              final ChannelBuffer buf,
                              final VoidEnum unused) {
    // When we disconnect, decodeLast is called instead of decode.
    // We simply check whether there's any data left in the buffer, in which
    // case we attempt to process it.  But if there's no data left, then we
    // don't even bother calling decode() as it'll complain that the buffer
    // doesn't contain enough data, which unnecessarily pollutes the logs.
    if (buf.readable()) {
      try {
        return decode(ctx, chan, buf, unused);
      } finally {
        if (buf.readable()) {
          LOG.error("After decoding the last message on " + chan
                    + ", there was still some undecoded bytes in the channel's"
                    + " buffer (which are going to be lost): "
                    + buf + '=' + Bytes.pretty(buf));
        }
      }
    } else {
      return null;
    }
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(13 + 10 + 6 + 64 + 16 + 1
                                                + 9 + 2 + 17 + 2 + 1);
    buf.append("RegionClient@")           // =13
      .append(hashCode())                 // ~10
      .append("(chan=")                   // = 6
      .append(chan)                       // ~64 (up to 66 when using IPv4)
      .append(", #pending_rpcs=");        // =16
    int npending_rpcs;
    int nedits;
    synchronized (this) {
      npending_rpcs = pending_rpcs == null ? 0 : pending_rpcs.size();
      nedits = edit_buffer == null ? 0 : edit_buffer.size();
    }
    buf.append(npending_rpcs)             // = 1
      .append(", #edits=")                // = 9
      .append(nedits);                    // ~ 2
    buf.append(", #rpcs_inflight=")       // =17
      .append(rpcs_inflight.size())       // ~ 2
      .append(')');                       // = 1
    return buf.toString();
  }

  /**
   * Handler that sends the Hadoop RPC "hello" header before the first RPC.
   * <p>
   * This handler will simply prepend the "hello" header before the first RPC
   * and will then remove itself from the pipeline, so all subsequent messages
   * going through the pipeline won't need to go through this handler.
   */
  @Sharable
  final static class SayHelloFirstRpc implements ChannelDownstreamHandler {

    /** Singleton instance.  */
    public final static SayHelloFirstRpc INSTANCE = new SayHelloFirstRpc();

    /** The header to send.  */
    private static final byte[] HELLO_HEADER;
    static {
      // CDH3 b3 includes a temporary patch that is non-backwards compatible
      // and results in clients getting disconnected as soon as they send the
      // header, because the HBase RPC protocol provides no mechanism to send
      // an error message back to the client during the initial "hello" stage
      // of a connection.
      if (System.getProperty("org.hbase.async.cdh3b3") != null) {
        final byte[] user = Bytes.UTF8(System.getProperty("user.name", "hbaseasync"));
        HELLO_HEADER = new byte[4 + 1 + 4 + 4 + user.length];
        headerCDH3b3(user);
      } else {
        HELLO_HEADER = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
        normalHeader();
      }
    }

    /** Common part of the hello header: magic + version.  */
    private static ChannelBuffer commonHeader() {
      final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(HELLO_HEADER);
      buf.clear();  // Set the writerIndex to 0.

      // Magic header.  See HBaseClient#writeHeader
      // "hrpc" followed by the version (3).
      // See HBaseServer#HEADER and HBaseServer#CURRENT_VERSION.
      buf.writeBytes(new byte[] { 'h', 'r', 'p', 'c', 3 });  // 4 + 1
      return buf;
    }

    /** Hello header for HBase 0.90.0 and earlier.  */
    private static void normalHeader() {
      final ChannelBuffer buf = commonHeader();

      // Serialized UserGroupInformation to say who we are.
      // We're not nice so we're not gonna say who we are and we'll just send
      // `null' (hadoop.io.ObjectWritable$NullInstance).
      // First, we need the size of the whole damn UserGroupInformation thing.
      // We skip 4 bytes now and will set it to the actual size at the end.
      buf.writerIndex(buf.writerIndex() + 4);                // 4
      // Write the class name of the object.
      // See hadoop.io.ObjectWritable#writeObject
      // See hadoop.io.UTF8#writeString
      // String length as a short followed by UTF-8 string.
      String klass = "org.apache.hadoop.io.Writable";
      buf.writeShort(klass.length());                        // 2
      buf.writeBytes(Bytes.ISO88591(klass));                 // 29
      klass = "org.apache.hadoop.io.ObjectWritable$NullInstance";
      buf.writeShort(klass.length());                        // 2
      buf.writeBytes(Bytes.ISO88591(klass));                 // 48
      klass = "org.apache.hadoop.security.UserGroupInformation";
      buf.writeShort(klass.length());                        // 2
      buf.writeBytes(Bytes.ISO88591(klass));                 // 47

      // Now set the length of the whole damn thing.
      // -4 because the length itself isn't part of the payload.
      // -5 because the "hrpc" + version isn't part of the payload.
      buf.setInt(5, buf.writerIndex() - 4 - 5);
    }

    /** CDH3b3-specific header for Hadoop "security".  */
    private static void headerCDH3b3(final byte[] user) {
      // Our username.
      final ChannelBuffer buf = commonHeader();

      // Length of the encoded string (useless).
      buf.writeInt(4 + user.length);  // 4
      // String as encoded by `WritableUtils.writeString'.
      buf.writeInt(user.length);      // 4
      buf.writeBytes(user);           // length bytes
    }

    private SayHelloFirstRpc() {  // Singleton, can't instantiate from outside.
    }

    /**
     * Handles messages going downstream (to the wire).
     * @param ctx The context of the channel handler.
     * @param event The event traveling downstream.
     */
    @Override
    public void handleDownstream(final ChannelHandlerContext ctx,
                                 ChannelEvent event) {
      if (event instanceof MessageEvent) {
        final MessageEvent me = (MessageEvent) event;
        final ChannelBuffer payload = (ChannelBuffer) me.getMessage();
        final ChannelBuffer header = ChannelBuffers.wrappedBuffer(HELLO_HEADER);

        // Piggyback a version request in the 1st packet, after the payload
        // we were trying to send.  This way we'll have the version handy
        // pretty quickly.  Since it's most likely going to fit in the same
        // packet we send out, it adds ~zero overhead.  But don't piggyback
        // a version request if the payload is already a version request.
        ChannelBuffer buf;
        if (!isVersionRequest(payload)) {
          final RegionClient client = ctx.getPipeline().get(RegionClient.class);
          final ChannelBuffer version =
            client.encode(client.getProtocolVersionRequest());
          buf = ChannelBuffers.wrappedBuffer(header, payload, version);
        } else {
          buf = ChannelBuffers.wrappedBuffer(header, payload);
        }
        // We're going to send the header, so let's remove ourselves from the
        // pipeline.
        try {
          ctx.getPipeline().remove(this);
          event = new DownstreamMessageEvent(ctx.getChannel(), me.getFuture(),
                                             buf, me.getRemoteAddress());
        } catch (NoSuchElementException e) {
          // There was a race with another thread, and we lost the race: the
          // other thread sent the handshake and remove ourselves from the
          // pipeline already (we got the NoSuchElementException because we
          // tried remove ourselves again).  In this case, it's fine, we can
          // just keep going and pass the event downstream, unchanged.
        }
      }
      ctx.sendDownstream(event);
    }

    /** Inspects the payload and returns true if it's a version RPC.  */
    private static boolean isVersionRequest(final ChannelBuffer payload) {
      final int length = GET_PROTOCOL_VERSION.length;
      // Header = 4+4+2, followed by method name.
      if (payload.readableBytes() < 4 + 4 + 2 + length) {
        return false;  // Too short to be a version request.
      }
      for (int i = 0; i < length; i++) {
        if (payload.getByte(4 + 4 + 2 + i) != GET_PROTOCOL_VERSION[i]) {
          return false;
        }
      }
      // No other RPC has a name that starts with "getProtocolVersion"
      // so this must be a version request.
      return true;
    }

  }

}
