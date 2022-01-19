/*
 * Copyright (C) 2010-2018  The Async HBase Authors.  All rights reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.protobuf.CodedOutputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.generated.ClientPB;
import org.hbase.async.generated.HBasePB;
import org.hbase.async.generated.RPCPB;

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
    REMOTE_EXCEPTION_TYPES.put(RegionMovedException.REMOTE_CLASS,
                               new RegionMovedException(null, null));
    REMOTE_EXCEPTION_TYPES.put(RegionOpeningException.REMOTE_CLASS,
                               new RegionOpeningException(null, null));
    REMOTE_EXCEPTION_TYPES.put(RegionServerAbortedException.REMOTE_CLASS,
                               new RegionServerAbortedException(null, null));
    REMOTE_EXCEPTION_TYPES.put(RegionServerStoppedException.REMOTE_CLASS,
                               new RegionServerStoppedException(null, null));
    REMOTE_EXCEPTION_TYPES.put(RegionTooBusyException.REMOTE_CLASS,
                               new RegionTooBusyException(null, null));
    REMOTE_EXCEPTION_TYPES.put(ServerNotRunningYetException.REMOTE_CLASS,
                               new ServerNotRunningYetException(null, null));
    REMOTE_EXCEPTION_TYPES.put(UnknownScannerException.REMOTE_CLASS,
                               new UnknownScannerException(null, null));
    REMOTE_EXCEPTION_TYPES.put(UnknownRowLockException.REMOTE_CLASS,
                               new UnknownRowLockException(null, null));
    REMOTE_EXCEPTION_TYPES.put(VersionMismatchException.REMOTE_CLASS,
                               new VersionMismatchException(null, null));
    REMOTE_EXCEPTION_TYPES.put(CallQueueTooBigException.REMOTE_CLASS,
                               new CallQueueTooBigException(null, null));
    REMOTE_EXCEPTION_TYPES.put(UnknownProtocolException.REMOTE_CLASS,
                               new UnknownProtocolException(null, null));
  }

  /** We don't know the RPC protocol version of the server yet.  */
  private static final byte SERVER_VERSION_UNKNWON = 0;

  /** Protocol version we pretend to use for HBase 0.90 and before.  */
  static final byte SERVER_VERSION_090_AND_BEFORE = 24;

  /** We know at that the server runs 0.92 to 0.94.  */
  static final byte SERVER_VERSION_092_OR_ABOVE = 29;

  /**
   * This is a made-up value for HBase 0.95 and above.
   * As of 0.95 there is no longer a protocol version, because everything
   * switched to Protocol Buffers.  Internally we use this made-up value
   * to refer to the post-protobuf era.
   */
  static final byte SERVER_VERSION_095_OR_ABOVE = 95;

  /** The HBase client we belong to.  */
  private final HBaseClient hbase_client;

  /** Whether or not to check the channel write status before sending RPCs */
  private final boolean check_write_status;
  
  /**
   * The channel we're connected to.
   * This will be {@code null} while we're not connected to the RegionServer.
   * This attribute is volatile because {@link #shutdown} may access it from a
   * different thread, and because while we connect various user threads will
   * test whether it's {@code null}.  Once we're connected and we know what
   * protocol version the server speaks, we'll set this reference.
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
   * No synchronization is typically used to read this value.
   * It is written only once by {@link ProtocolVersionCB}.
   */
  private byte server_version = SERVER_VERSION_UNKNWON;

  /**
   * RPCs being batched together for efficiency.
   * Manipulating this reference requires synchronizing on `this'.
   */
  private MultiAction batched_rpcs;

  /**
   * RPCs we've been asked to serve while disconnected from the RegionServer.
   * This reference is lazily created.  Synchronize on `this' before using it.
   *
   * Invariants:
   *   If pending_rpcs != null      =>  !pending_rpcs.isEmpty()
   *   If pending_rpcs != null      =>  rpcs_inflight.isEmpty()
   *   If pending_rpcs == null      =>  batched_rpcs == null
   *   If !rpcs_inflight.isEmpty()  =>  pending_rpcs == null
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

  /** A counter of how many RPCs were actually sent over the TCP socket though
   * they may not have made it all the way */
  private final AtomicInteger rpcs_sent = new AtomicInteger();
  
  /** Number of RPCs failed due to timeout */
  private final AtomicInteger rpcs_timedout = new AtomicInteger();
  
  /** The number of responses received from HBase that were timed out, meaning
   * we got the results late. */
  private final AtomicInteger rpc_response_timedout = new AtomicInteger();
  
  /** The number of responses received from HBase that didn't match an RPC that
   * we sent. This means the ID was greater than our RPC ID counter */
  private final AtomicInteger rpc_response_unknown = new AtomicInteger();
  
  /** Number of RPCs that were blocked due to the channel in a non-writable state */
  private final AtomicInteger writes_blocked = new AtomicInteger();
  
  /** Number of RPCs failed due to exceeding the inflight limit */
  private final AtomicInteger inflight_breached = new AtomicInteger();
  
  /** Number of RPCs failed due to exceeding the pending limit */
  private final AtomicInteger pending_breached = new AtomicInteger();
  
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

  /** A class used for authentication and/or encryption/decryption of packets. */
  private SecureRpcHelper secure_rpc_helper;
  
  /** Maximum number of inflight RPCs. If 0, unlimited */
  private int inflight_limit;
  
  /** Maximum number of RPCs queued while waiting to connect. If 0, unlimited */
  private int pending_limit;
  
  /** Number of batchable RPCs allowed in a single batch before it's sent off */
  private int batch_size;
  
  /**
   * Constructor.
   * @param hbase_client The HBase client this instance belongs to.
   */
  public RegionClient(final HBaseClient hbase_client) {
    this.hbase_client = hbase_client;
    check_write_status = hbase_client.getConfig().getBoolean(
            "hbase.region_client.check_channel_write_status");
    inflight_limit = hbase_client.getConfig().getInt(
        "hbase.region_client.inflight_limit");
    pending_limit = hbase_client.getConfig().getInt(
        "hbase.region_client.pending_limit");
    batch_size = hbase_client.getConfig().getInt("hbase.rpcs.batch.size");
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

  /**
   * @return The remote address this client is connected to
   * @since 1.7
   */
  public String getRemoteAddress() {
    return chan != null ? chan.getRemoteAddress().toString() : null;
  }
  
  /**
   * Return statistics about this particular region client
   * @return A RegionClientStats object with an immutable copy of the stats
   * @since 1.7
   */
  public RegionClientStats stats() {
    synchronized (this) {
      return new RegionClientStats(
        rpcs_sent.get(),
        rpcs_inflight != null ? rpcs_inflight.size() : 0,
        pending_rpcs != null ? pending_rpcs.size() : 0,
        rpcid.get(),
        dead,
        chan != null ? chan.getRemoteAddress().toString() : "",
        batched_rpcs != null ? batched_rpcs.size() : 0,
        rpcs_timedout.get(),
        writes_blocked.get(),
        rpc_response_timedout.get(),
        rpc_response_unknown.get(),
        inflight_breached.get(),
        pending_breached.get()
      );
    }
  }
  
  /** Periodically flushes buffered RPCs.  */
  private void periodicFlush() {
    if (chan != null || dead) {
      // If we're dead, we want to flush our RPCs (this will cause them to
      // get failed / retried).
      if (LOG.isDebugEnabled()) {
        LOG.debug("Periodic flush timer: flushing RPCs for " + this);
      }
      // Copy the batch to a local variable and null it out.
      final MultiAction batched_rpcs;
      synchronized (this) {
        batched_rpcs = this.batched_rpcs;
        this.batched_rpcs = null;
      }
      if (batched_rpcs != null && batched_rpcs.size() != 0) {
        final Deferred<Object> d = batched_rpcs.getDeferred();
        sendRpc(batched_rpcs);
      }
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
        if (adj < interval) {
          adj = (short) -adj;      // ... use a negative adjustment instead.
        } else {
          adj = (short) (interval / -2);
        }
      }
      hbase_client.newTimeout(flush_timer, interval + adj);
    }
  }

  /**
   * Flushes to HBase any buffered client-side write operation.
   * <p>
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  Deferred<Object> flush() {
    // Copy the batch to a local variable and null it out.
    final MultiAction batched_rpcs;
    final ArrayList<Deferred<Object>> pending;
    synchronized (this) {
      batched_rpcs = this.batched_rpcs;
      this.batched_rpcs = null;
      pending = getPendingRpcs();
    }

    if (pending != null && !pending.isEmpty()) {
      @SuppressWarnings("unchecked")
      final Deferred<Object> wait = (Deferred) Deferred.group(pending);
      // We can return here because if we found pending RPCs it's guaranteed
      // that batched_rpcs was null.
      return wait;
    }

    if (batched_rpcs == null || batched_rpcs.size() == 0) {
      return Deferred.fromResult(null);
    }
    final Deferred<Object> d = batched_rpcs.getDeferred();
    sendRpc(batched_rpcs);
    return d;
  }

  /**
   * Introduces a sync point for all outstanding RPCs.
   * <p>
   * All RPCs known to this {@code RegionClient}, whether they are buffered,
   * in flight, pending, etc., will get grouped together in the Deferred
   * returned, thereby introducing a sync point past which we can guarantee
   * that all RPCs have completed (successfully or not).  This is similar
   * to the {@code sync(2)} Linux system call.
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  Deferred<Object> sync() {
    flush();

    ArrayList<Deferred<Object>> rpcs = getInflightRpcs();  // Never null.
    // There are only two cases to handle here thanks to the invariant that
    // says that if we inflight isn't empty, then pending is null.
    if (rpcs.isEmpty()) {
      rpcs = getPendingRpcs();
    }

    if (rpcs == null) {
      return Deferred.fromResult(null);
    }
    @SuppressWarnings("unchecked")
    final Deferred<Object> sync = (Deferred) Deferred.group(rpcs);
    return sync;
  }

  /**
   * Returns a possibly empty list of all the RPCs that are in-flight.
   */
  private ArrayList<Deferred<Object>> getInflightRpcs() {
    final ArrayList<Deferred<Object>> inflight =
      new ArrayList<Deferred<Object>>();
    for (final HBaseRpc rpc : rpcs_inflight.values()) {
      inflight.add(rpc.getDeferred());
    }
    return inflight;
  }

  /**
   * Returns a possibly {@code null} list of all RPCs that are pending.
   * <p>
   * Pending RPCs are those that are scheduled to be sent as soon as we
   * are connected to the RegionServer and have done version negotiation.
   */
  private ArrayList<Deferred<Object>> getPendingRpcs() {
    synchronized (this) {
      if (pending_rpcs != null) {
        final ArrayList<Deferred<Object>> pending =
          new ArrayList<Deferred<Object>>(pending_rpcs.size());
        for (final HBaseRpc rpc : pending_rpcs) {
          pending.add(rpc.getDeferred());
        }
        return pending;
      }
    }
    return null;
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
      final ArrayList<Deferred<Object>> inflight = getInflightRpcs();
      final int size = inflight.size();
      if (size > 0) {
        return Deferred.group(inflight)
          .addCallbackDeferring(new RetryShutdown<ArrayList<Object>>(size));
      }
      // Then check whether have batched RPCs.  If we do, flush them.
      // Copy the batch to a local variable and null it out.
      final MultiAction batched_rpcs;
      synchronized (this) {
        batched_rpcs = this.batched_rpcs;
        this.batched_rpcs = null;
      }
      if (batched_rpcs != null && batched_rpcs.size() != 0) {
        final Deferred<Object> d = batched_rpcs.getDeferred();
        sendRpc(batched_rpcs);
        return d.addCallbackDeferring(new RetryShutdown<Object>(1));
      }
    }

    {
      final ArrayList<Deferred<Object>> pending = getPendingRpcs();
      if (pending != null) {
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
            return;
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

  /**
   * RPC used to discover the exact protocol version spoken by the server.
   * Not exposed as part of the public API, and only used when talking to
   * HBase 0.94.x and earlier.
   */
  private final static class GetProtocolVersionRequest extends HBaseRpc {

    @Override
    byte[] method(final byte unused_server_version) {
      return GET_PROTOCOL_VERSION;
    }

    ChannelBuffer serialize(final byte server_version) {
    /** Pre-serialized form for this RPC, which is always the same.  */
      // num param + type 1 + string length + string + type 2 + long
      final ChannelBuffer buf = newBuffer(server_version,
                                          4 + 1 + 1 + 44 + 1 + 8);
      buf.writeInt(2);  // Number of parameters.
      // 1st param.
      writeHBaseString(buf, "org.apache.hadoop.hbase.ipc.HRegionInterface");
      // 2nd param: what protocol version to speak.  If we don't know what the
      // server expects, try an old version first (0.90 and before).
      // Otherwise tell the server we speak the same version as it does.
      writeHBaseLong(buf, server_version == SERVER_VERSION_UNKNWON
                     ?  SERVER_VERSION_090_AND_BEFORE : server_version);
      return buf;
    }

    @Override
    Object deserialize(final ChannelBuffer buf, final int cell_size) {
      throw new AssertionError("Should never be here.");
    }

  };

  /** Callback to handle responses of getProtocolVersion RPCs.  */
  private final class ProtocolVersionCB implements Callback<Long, Object> {

    /** Channel connected to the server for which we're getting the version.  */
    private final Channel chan;

    public ProtocolVersionCB(final Channel chan) {
      this.chan = chan;
    }

    /**
     * Handles the response of {@link #helloRpc}.
     * @return null when it re-tried for a version mismatch. A version
     * object if it was successful.
     * @throws an exception If there was an error that we cannot retry. It
     * includes ServerNotRunningYetExceptio, and NotServingRegionException.
     */
    public Long call(final Object response) throws Exception {
      if (response instanceof VersionMismatchException) {
        if (server_version == SERVER_VERSION_UNKNWON) {
          // If we get here, it's because we tried to handshake with a server
          // running HBase 0.92 or above, but using a pre-0.92 handshake.  So
          // we know we have to handshake differently.
          server_version = SERVER_VERSION_092_OR_ABOVE;
          helloRpc(chan, header092());
        } else {
          // We get here if the server refused our 0.92-style handshake.  This
          // must be a future version of HBase that broke compatibility again,
          // and we don't know how to talk to it, so give up here.
          throw (VersionMismatchException) response;
        }
        return null;
      } else if (!(response instanceof Long)) {
        if (response instanceof Exception) {  // If it's already an exception,
          throw (Exception) response;         // just re-throw it as-is.
        }
        throw new InvalidResponseException(Long.class, response);
      }
      final Long version = (Long) response;
      final long v = version;
      if (v <= 0 || v > Byte.MAX_VALUE) {
        throw new InvalidResponseException("getProtocolVersion returned a "
          + (v <= 0 ? "negative" : "too large") + " value", version);
      }
      becomeReady(chan, (byte) v);
      return version;
    }

    public String toString() {
      return "handle getProtocolVersion response on " + chan;
    }

  }

  void becomeReady(final Channel chan, final byte server_version) {
    this.server_version = server_version;
    // The following line will make this client no longer queue incoming
    // RPCs, as we're now ready to communicate with the server.
    this.chan = chan;  // Volatile write.
    sendQueuedRpcs();
  }

  private static final byte[] GET_CLOSEST_ROW_BEFORE = new byte[] {
    'g', 'e', 't',
    'C', 'l', 'o', 's', 'e', 's', 't',
    'R', 'o', 'w',
    'B', 'e', 'f', 'o', 'r', 'e'
  };

  /** Piece of protobuf to specify the family during META lookups.  */
  private static final ClientPB.Column FAM_INFO = ClientPB.Column.newBuilder()
          .setFamily(Bytes.wrap(HBaseClient.INFO))
          .build();

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
        super(tabl, row);
      }

      @Override
      byte[] method(final byte server_version) {
        return server_version >= SERVER_VERSION_095_OR_ABOVE
          ? GetRequest.GGET : GET_CLOSEST_ROW_BEFORE;
      }

      @Override
      Object deserialize(final ChannelBuffer buf, final int cell_size) {
        assert cell_size == 0 : "cell_size=" + cell_size;
        final ClientPB.GetResponse resp =
          readProtobuf(buf, ClientPB.GetResponse.PARSER);
        return GetRequest.extractResponse(resp, buf, cell_size);
      }

      @Override
      ChannelBuffer serialize(final byte server_version) {
        if (server_version < SERVER_VERSION_095_OR_ABOVE) {
          return serializeOld(server_version);
        }
        final ClientPB.Get getpb = ClientPB.Get.newBuilder()
          .setRow(Bytes.wrap(row))
          .addColumn(FAM_INFO)  // Fetch from one family only.
          .setClosestRowBefore(true)
          .build();
        final ClientPB.GetRequest get = ClientPB.GetRequest.newBuilder()
          .setRegion(region.toProtobuf())
          .setGet(getpb)
          .build();
        return toChannelBuffer(GetRequest.GGET, get);
      }

      private ChannelBuffer serializeOld(final byte server_version) {
        // region.length and row.length will use at most a 3-byte VLong.
        // This is because VLong wastes 1 byte of meta-data + 2 bytes of
        // payload.  HBase's own KeyValue code uses a short to store the row
        // length.  Finally, family.length cannot be on more than 1 byte,
        // HBase's own KeyValue code uses a byte to store the family length.
        final byte[] region_name = region.name();
        final ChannelBuffer buf = newBuffer(server_version,
          + 4                                      // num param
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
  private void bufferEdit(final BatchableRpc request) {
    MultiAction batch;
    boolean schedule_flush = false;

    synchronized (this) {
      if (batched_rpcs == null) {
        batched_rpcs = new MultiAction();
        addMultiActionCallbacks(batched_rpcs);
        schedule_flush = true;
      }
      batch = batched_rpcs;
      // Unfortunately we have to hold the monitor on `this' while we do
      // this entire atomic dance.
      batch.add(request);
      if (batch.size() < batch_size) {
        batch = null;  // We're going to buffer this edit for now.
      } else {
        // Execute the edits buffered so far.  But first we must clear
        // the reference to the buffer we're about to send to HBase.
        batched_rpcs = new MultiAction();
        addMultiActionCallbacks(batched_rpcs);
      }
    }

    if (schedule_flush) {
      scheduleNextPeriodicFlush();
    } else if (batch != null) {
      sendRpc(batch);
    }
  }

  /**
   * Creates callbacks to handle a multi-put and adds them to the request.
   * @param request The request for which we must handle the response.
   */
  private void addMultiActionCallbacks(final MultiAction request) {
    final class MultiActionCallback implements Callback<Object, Object> {
      public Object call(final Object resp) {
        if (!(resp instanceof MultiAction.Response)) {
          if (resp instanceof BatchableRpc) {  // Single-RPC multi-action?
            return null;  // Yes, nothing to do.  See multiActionToSingleAction.
          } else if (resp instanceof Exception) {
            return handleException((Exception) resp);
          }
          throw new InvalidResponseException(MultiAction.Response.class, resp);
        }
        final MultiAction.Response response = (MultiAction.Response) resp;
        final ArrayList<BatchableRpc> batch = request.batch();
        final int n = batch.size();
        for (int i = 0; i < n; i++) {
          final BatchableRpc rpc = batch.get(i);
          final Object r = response.result(i);
          if (r instanceof RecoverableException) {
            if (r instanceof NotServingRegionException) {
              // We need to do NSRE handling here too, as the response might
              // have come back successful, but only some parts of the batch
              // could have encountered an NSRE.
              hbase_client.handleNSRE(rpc, rpc.getRegion().name(),
                                      (NotServingRegionException) r);
            } else {
              retryEdit(rpc, (RecoverableException) r);
            }
          } else {
            rpc.callback(r);
          }
        }
        // We're successful.  If there was a problem, the exception was
        // delivered to the specific RPCs that failed, and they will be
        // responsible for retrying.
        return null;
      }

      private Object handleException(final Exception e) {
        if (!(e instanceof RecoverableException)) {

          if (e instanceof HBaseException){
            HBaseException ex = (HBaseException)e;
            for (final BatchableRpc rpc : request.batch()) {
              rpc.callback(ex.make(ex, rpc));
            }
          } else{
            for (final BatchableRpc rpc : request.batch()) {
              rpc.callback(e);
            }
          }

          return e;  // Can't recover from this error, let it propagate.
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Multi-action request failed, retrying each of the "
                    + request.size() + " RPCs individually.", e);
        }
        for (final BatchableRpc rpc : request.batch()) {
          retryEdit(rpc, (RecoverableException) e);
        }
        return null;  // We're retrying, so let's call it a success for now.
      }

      public String toString() {
        return "multi-action response";
      }
    };
    request.getDeferred().addBoth(new MultiActionCallback());
  }

  /**
   * Retries an edit that failed with a recoverable error.
   * @param rpc The RPC that failed.
   * @param e The recoverable error that caused the edit to fail, if known.
   * Can be {@code null}.
   * @param The deferred result of the new attempt to send this edit.
   */
  private Deferred<Object> retryEdit(final BatchableRpc rpc,
                                     final RecoverableException e) {
    if (hbase_client.cannotRetryRequest(rpc)) {
      return HBaseClient.tooManyAttempts(rpc, e);
    }
    // This RPC has already been delayed because of a failure,
    // so make sure we don't buffer it again.
    rpc.setBufferable(false);
    return hbase_client.sendRpcToRegion(rpc);
  }

  /**
   * Creates callbacks to handle a single-put and adds them to the request.
   * @param edit The edit for which we must handle the response.
   */
  private void addSingleEditCallbacks(final BatchableRpc edit) {
    // There's no callback to add on a single put request, because
    // the remote method returns `void', so we only need an errback.
    final class SingleEditErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (!(e instanceof RecoverableException)) {
          return e;  // Can't recover from this error, let it propagate.
        }
        return retryEdit(edit, (RecoverableException) e);
      }
      public String toString() {
        return "single-edit errback";
      }
    };
    edit.getDeferred().addErrback(new SingleEditErrback());
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
      // Now {@link GetRequest} is also a {@link BatchableRpc}, we don't want to retry 
      // the get request once it fails.
      if (rpc instanceof BatchableRpc 
          && !(rpc instanceof GetRequest)
          && (server_version >= SERVER_VERSION_092_OR_ABOVE  // Before 0.92,
              || rpc instanceof PutRequest)) {  // we could only batch "put".
        final BatchableRpc edit = (BatchableRpc) rpc;
        if (edit.canBuffer() && hbase_client.getFlushInterval() > 0) {
          bufferEdit(edit);
          return;
        }
        addSingleEditCallbacks(edit);
      } else if (rpc instanceof MultiAction) {
        // Transform single-edit multi-put into single-put.
        final MultiAction batch = (MultiAction) rpc;
        if (batch.size() == 1) {
          rpc = multiActionToSingleAction(batch);
        } else {
          hbase_client.num_multi_rpcs.increment();
        }
      }
      final ChannelBuffer serialized = encode(rpc);
      if (serialized == null) {  // Error during encoding.
        return;  // Stop here.  RPC has been failed already.
      }
      final Channel chan = this.chan;  // Volatile read.
      if (chan != null) {  // Double check if we disconnected during encode().
        // if our channel isn't able to write, we want to properly queue and
        // retry the RPC later or fail it immediately so we don't fill up the
        // channel's buffer.
        if (check_write_status && !chan.isWritable()) {
          rpc.callback(new PleaseThrottleException("Region client [" + this + 
              " ] channel is not writeable.", null, rpc, rpc.getDeferred()));
          removeRpc(rpc, false);
          writes_blocked.incrementAndGet();
          return;
        }
        
        rpc.enqueueTimeout(this);
        Channels.write(chan, serialized);
        rpcs_sent.incrementAndGet();
        return;
      }  // else: continue to the "we're disconnected" code path below.
    }

    boolean tryagain = false;
    boolean dead;  // Shadows this.dead;
    synchronized (this) {
      dead = this.dead;
      // Check if we got connected while entering this synchronized block.
      if (chan != null) {
        tryagain = true;
      } else if (!dead) {
        if (pending_rpcs == null) {
          pending_rpcs = new ArrayList<HBaseRpc>();
        }
        if (pending_limit > 0 && pending_rpcs.size() >= pending_limit) {
          rpc.callback(new PleaseThrottleException(
              "Exceeded the pending RPC limit", null, rpc, rpc.getDeferred()));
          pending_breached.incrementAndGet();
          return;
        }
        pending_rpcs.add(rpc);
      }
    }
    if (dead) {
      if (rpc.getRegion() == null  // Can't retry, dunno where it should go.
          || rpc.failfast()) {
        rpc.callback(new ConnectionResetException(null));
      } else {
        hbase_client.sendRpcToRegion(rpc);  // Re-schedule the RPC.
      }
      return;
    } else if (tryagain) {
      // This recursion will not lead to a loop because we only get here if we
      // connected while entering the synchronized block above. So when trying
      // a second time,  we will either succeed to send the RPC if we're still
      // connected, or fail through to the code below if we got disconnected
      // in the mean time.
      sendRpc(rpc);
      return;
    }
    LOG.debug("RPC queued: {}", rpc);
  }

  /**
   * Transforms the given single-edit multi-put into a regular single-put.
   * @param multiput The single-edit multi-put to transform.
   */
  private BatchableRpc multiActionToSingleAction(final MultiAction batch) {
    final BatchableRpc rpc = batch.batch().get(0);
    addSingleEditCallbacks(rpc);
    // Once the single-edit is done, we still need to make sure we're
    // going to run the callback chain of the MultiAction.
    final class Multi2SingleCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
        // If there was a problem, let the MultiAction know.
        // Otherwise, give the HBaseRpc in argument to the MultiAction
        // callback.  This is kind of a kludge: the MultiAction callback
        // will understand that this means that this single-RPC was already
        // successfully executed on its own.
        batch.callback(arg instanceof Exception ? arg : rpc);
        return arg;
      }
    }
    rpc.getDeferred().addBoth(new Multi2SingleCB());
    return rpc;
  }

  // -------------------------------------- //
  // Managing the life-cycle of the channel //
  // -------------------------------------- //

  @Override
  public void channelConnected(final ChannelHandlerContext ctx,
                               final ChannelStateEvent e) {
    final Channel chan = e.getChannel();
    final ChannelBuffer header;
    
    if (hbase_client.getConfig().getBoolean("hbase.security.auth.enable") && 
        hbase_client.getConfig().hasProperty("hbase.security.auth.94")) {
      secure_rpc_helper = new SecureRpcHelper94(hbase_client, this, 
          chan.getRemoteAddress());
      secure_rpc_helper.sendHello(chan);
      LOG.info("Initialized security helper: " + secure_rpc_helper + 
          " for region client: " + this);
    } else {
      if (!hbase_client.has_root || hbase_client.split_meta) {
        if (hbase_client.getConfig().getBoolean("hbase.security.auth.enable")) {
          secure_rpc_helper = new SecureRpcHelper96(hbase_client, this, 
              chan.getRemoteAddress());
          secure_rpc_helper.sendHello(chan);
          LOG.info("Initialized security helper: " + secure_rpc_helper + 
              " for region client: " + this);
          return;
        }
        header = header095();
        Channels.write(chan, header);
        becomeReady(chan, SERVER_VERSION_095_OR_ABOVE);
        return;
      } else if (System.getProperty("org.hbase.async.cdh3b3") != null) {
        header = headerCDH3b3();
      } else {
        header = header090();
      }
      helloRpc(chan, header);
    }
  }

  /**
   * Sends the queued RPCs to the server, once we're connected to it.
   * This gets called after {@link #channelConnected}, once we were able to
   * handshake with the server and find out which version it's running.
   * @see #helloRpc
   */
  private void sendQueuedRpcs() {
    ArrayList<HBaseRpc> rpcs;
    synchronized (this) {
      rpcs = pending_rpcs;
      pending_rpcs = null;
    }
    if (rpcs != null) {
      for (final HBaseRpc rpc : rpcs) {
        LOG.debug("Executing RPC queued: {}", rpc);
        sendRpc(rpc);
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

    final ArrayList<HBaseRpc> rpcs;
    final MultiAction batch;
    synchronized (this) {
      dead = true;
      rpcs = pending_rpcs;
      pending_rpcs = null;
      batch = batched_rpcs;
      batched_rpcs = null;
    }
    if (rpcs != null) {
      failOrRetryRpcs(rpcs, exception);
    }
    if (batch != null) {
      batch.callback(exception);  // Make it fail.
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
      if (region == null  // Can't retry, dunno where this RPC should go.
          || rpc.failfast()) {
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
      LOG.debug("handleUpstream {}", e);
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent event) {
    final Throwable e = event.getCause();
    final Channel c = event.getChannel();

    if (e instanceof RejectedExecutionException) {
      LOG.warn("RPC rejected by the executor,"
               + " ignore this if we're shutting down", e);
    } else {
      LOG.error("Unexpected exception from downstream on " + c, e);
    }
    if (c.isOpen()) {
      Channels.close(c);  // Will trigger channelClosed(), which will cleanup()
    } else {              // else: presumably a connection timeout.
      cleanup(c);         // => need to cleanup() from here directly.
    }
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

    // TODO(tsuna): Add rate-limiting here.  We don't want to send more than
    // N QPS to a given region server.
    // TODO(tsuna): Check the size() of rpcs_inflight.  We don't want to have
    // more than M RPCs in flight at the same time, and we may be overwhelming
    // the server if we do.

    rpc.rpc_id = this.rpcid.incrementAndGet();
    ChannelBuffer payload;
    try {
      payload = rpc.serialize(server_version);
      // We assume that payload has enough bytes at the beginning for us to
      // "fill in the blanks" and put the RPC header.  This is accounted for
      // automatically by HBaseRpc#newBuffer.  If someone creates their own
      // buffer without this extra space at the beginning, we're going to
      // corrupt the RPC at this point.
      final byte[] method = rpc.method(server_version);
      if (server_version >= SERVER_VERSION_095_OR_ABOVE) {
        final RPCPB.RequestHeader header = RPCPB.RequestHeader.newBuilder()
          .setCallId(rpc.rpc_id)                        // 1 + 1-to-5 bytes (vint)
          .setMethodNameBytes(Bytes.wrap(method))  // 1 + 1 + N bytes
          .setRequestParam(true)                   // 1 + 1 bytes
          .build();
        final int pblen = header.getSerializedSize();
        // In HBaseRpc.newBuffer() we reserved 19 bytes for the RPC header
        // (without counting the leading 4 bytes for the overall size).
        // Here the size is variable due to the nature of the protobuf
        // encoding, but the expected absolute maximum size is 17 bytes
        // if we ignore the method name.  So we have to offset the header
        // by 2 to 13 bytes typically.  Note that the "-1" is for the varint
        // that's at the beginning of the header that indicates how long the
        // header itself is.
        final int offset = 19 + method.length - pblen - 1;
        assert offset >= 0 : ("RPC header too big (" + pblen + " bytes): "
                              + header);
        // Skip the few extraneous bytes we over-allocated for the header.
        payload.readerIndex(offset);
        // The first int is the size of the message, excluding the 4 bytes
        // needed for the size itself, hence the `-4'.
        payload.setInt(offset, payload.readableBytes() - 4); // 4 bytes
        try {
          final CodedOutputStream output =
            CodedOutputStream.newInstance(payload.array(), 4 + offset,
                                          1 + pblen);
          output.writeRawByte(pblen);  // varint but always on 1 byte here.
          header.writeTo(output);
          output.checkNoSpaceLeft();
        } catch (IOException e) {
          throw new RuntimeException("Should never happen", e);
        }
      } else if (server_version >= SERVER_VERSION_092_OR_ABOVE) {
        // The first int is the size of the message, excluding the 4 bytes
        // needed for the size itself, hence the `-4'.
        payload.setInt(0, payload.readableBytes() - 4); // 4 bytes
        payload.setInt(4, rpc.rpc_id);                  // 4 bytes
        // RPC version (org.apache.hadoop.hbase.ipc.Invocation.RPC_VERSION).
        payload.setByte(8, 1);                          // 4 bytes
        payload.setShort(9, method.length);             // 2 bytes
        payload.setBytes(11, method);                   // method.length bytes
        // Client version.  We always pretend to run the same version as the
        // server we're talking to.
        payload.setLong(11 + method.length, server_version);
        // Finger print of the method (also called "clientMethodsHash").
        // This field is unused, so we never set it, which is why the next
        // line is commented out.  It doesn't matter what value it has.
        //payload.setInt(11 + method.length + 8, 0);
      } else {  // Serialize for versions 0.90 and before.
        // The first int is the size of the message, excluding the 4 bytes
        // needed for the size itself, hence the `-4'.
        payload.setInt(0, payload.readableBytes() - 4); // 4 bytes
        payload.setInt(4, rpc.rpc_id);                  // 4 bytes
        payload.setShort(8, method.length);             // 2 bytes
        payload.setBytes(10, method);                   // method.length bytes
      }
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
      LOG.debug(chan + " Sending RPC #" + rpcid + ", payload=" + payload + ' '
                + Bytes.pretty(payload));
    }
    {
      if (inflight_limit > 0 && rpcs_inflight.size() >= inflight_limit) {
        rpc.callback(new PleaseThrottleException(
            "Exceeded the inflight RPC limit", null, rpc, rpc.getDeferred()));
        inflight_breached.incrementAndGet();
        return null;
      }
      final HBaseRpc oldrpc = rpcs_inflight.put(rpc.rpc_id, rpc);
      if (oldrpc != null) {
        final String wtf = "WTF?  There was already an RPC in flight with"
          + " rpcid=" + rpcid + ": " + oldrpc
          + ".  This happened when sending out: " + rpc;
        LOG.error(wtf);
        // Make it fail.  This isn't an expected failure mode.
        oldrpc.callback(new NonRecoverableException(wtf));
      }
    }
    if (secure_rpc_helper != null) {
      payload = secure_rpc_helper.wrap(payload);
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
                          final ChannelBuffer channel_buffer,
                          final VoidEnum unused) {
    ChannelBuffer buf = channel_buffer;
    final long start = System.nanoTime();
    final int rdx = buf.readerIndex();
    LOG.debug("------------------>> ENTERING DECODE >>------------------");
    final int rpcid;
    final RPCPB.ResponseHeader header;
    
    if (secure_rpc_helper != null) {
      buf = secure_rpc_helper.handleResponse(buf, chan);
      if (buf == null) {
        // everything in the buffer was part of the security handshake so we're
        // done here.
        return null;
      }
    }
    
    final int size;
    if (server_version >= SERVER_VERSION_095_OR_ABOVE) {
      size = buf.readInt();
      ensureReadable(buf, size);
      HBaseRpc.checkArrayLength(buf, size);
      header = HBaseRpc.readProtobuf(buf, RPCPB.ResponseHeader.PARSER);
      if (!header.hasCallId()) {
        final String msg = "RPC response (size: " + size + ") doesn't"
          + " have a call ID: " + header + ", buf=" + Bytes.pretty(buf);
        throw new NonRecoverableException(msg);
      }
      rpcid = header.getCallId();
    } else {  // HBase 0.94 and before.
      size = 0;
      header = null;  // No protobuf back then.
      rpcid = buf.readInt();
    }

    final HBaseRpc rpc = rpcs_inflight.get(rpcid);
    if (rpc == null) {
      // make sure to consume the RPC body so that we can decode the next
      // RPC in the buffer if there is one. Do this before incrementing counters
      // as with pre-protobuf RPCs we may throw replays while consuming.
      if (server_version >= SERVER_VERSION_095_OR_ABOVE) {
        buf.readerIndex(rdx + size + 4);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipped timed out RPC ID " + rpcid + " of " + size + 
              " bytes on " + this);
        }
      } else {
        consumeTimedoutNonPBufRPC(buf, rdx, rpcid);
      }
      
      // TODO - account for the rpcid overflow
      if (rpcid > -1 && rpcid <= this.rpcid.get()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received a response for rpcid: " + rpcid + 
              " that is no longer in our inflight map on region client " + this + 
              ". It may have been evicted. buf=" + Bytes.pretty(buf));
        }
        rpc_response_timedout.incrementAndGet();
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received rpcid: " + rpcid + " that doesn't seem to be a "
              + "valid ID on region client " + this + ". This packet may have "
              + "been corrupted. buf=" + Bytes.pretty(buf));
        }
        rpc_response_unknown.incrementAndGet();
      }
      return null;
    }
    
    // TODO - if the RPC doesn't match we could search the map for the proper
    // RPC. For now though, something went really pear shaped so we should
    // toss an exception.
    assert rpc.rpc_id == rpcid;

    final Object decoded;
    try {
      if (server_version >= SERVER_VERSION_095_OR_ABOVE) {
        if (header.hasException()) {
          decoded = decodeException(rpc, header.getException());
        } else {
          final int cell_size;
          {
            final RPCPB.CellBlockMeta cellblock = header.getCellBlockMeta();
            if (cellblock == null) {
              cell_size = 0;
            } else {
              cell_size = cellblock.getLength();
              HBaseRpc.checkArrayLength(buf, cell_size);
            }
          }
          decoded = rpc.deserialize(buf, cell_size);
        }
      } else {  // HBase 0.94 and before.
        decoded = deserialize(buf, rpc);
      }
    } catch (RuntimeException e) {
      final String msg = "Uncaught error during de-serialization of " + rpc
        + ", rpcid=" + rpcid;
      LOG.error(msg);
      if (!(e instanceof HBaseException)) {
        e = new NonRecoverableException(msg, e);
      }
      rpc.callback(e);
      removeRpc(rpc, false);
      throw e;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcid=" + rpcid
                + ", response size=" + (buf.readerIndex() - rdx) + " bytes"
                + ", " + actualReadableBytes() + " readable bytes left"
                + ", rpc=" + rpc);
    }
    removeRpc(rpc, false);

    if (decoded instanceof NotServingRegionException
        && rpc.getRegion() != null) {
      // We only handle NSREs for RPCs targeted at a specific region, because
      // if we don't know which region caused the NSRE (e.g. during multiPut)
      // we can't do anything about it.
      hbase_client.handleNSRE(rpc, rpc.getRegion().name(),
                              (RecoverableException) decoded);
      return null;
    } else if (decoded instanceof RecoverableException && 
        // RSSE could pop on a multi action in which case we want to pass it
        // on to the multi action callback handler.
        !(decoded instanceof RegionServerStoppedException && 
            rpc instanceof MultiAction)) {
      // retry a recoverable RPC that doesn't conform to the NSRE path
      if (hbase_client.cannotRetryRequest(rpc)) {
        return HBaseClient.tooManyAttempts(rpc, (RecoverableException) decoded);
      }
      
      final class RetryTimer implements TimerTask {
        public void run(final Timeout timeout) {
          if (isAlive()) {
            rpc.attempt++;
            sendRpc(rpc);
          } else {
            if (rpc instanceof MultiAction) {
              ((MultiAction) rpc).callback(decoded);
            } else {
              hbase_client.sendRpcToRegion(rpc);
            }
          }
        }
        @Override
        public String toString() {
          return "RPC Recoverable Retry Timer Task: " + rpc;
        }
      }
      
      if (rpc.timeout_handle != null) {
        rpc.timeout_handle.cancel();
        rpc.timeout_handle = null;
      }
      
      hbase_client.newTimeout(new RetryTimer(), rpc.getRetryDelay());
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
   * Ensures that at least a {@code nbytes} are readable from the given buffer.
   * If there aren't enough bytes in the buffer this will raise an exception
   * and cause the {@link ReplayingDecoder} to undo whatever we did thus far
   * so we can wait until we read more from the socket.
   * @param buf Buffer to check.
   * @param nbytes Number of bytes desired.
   */
  private static void ensureReadable(final ChannelBuffer buf, final int nbytes) {
    buf.markReaderIndex();
    buf.skipBytes(nbytes);
    buf.resetReaderIndex();
  }

  /**
   * De-serializes an RPC response.
   * @param buf The buffer from which to de-serialize the response.
   * @param rpc The RPC for which we're de-serializing the response.
   * @return The de-serialized RPC response (which can be {@code null}
   * or an exception).
   */
  private Object deserialize(final ChannelBuffer buf, final HBaseRpc rpc) {
    // The 1st byte of the payload contains flags:
    //   0x00  Old style success (prior 0.92).
    //   0x01  RPC failed with an exception.
    //   0x02  New style success (0.92 and above).
    final int flags;
    if (secure_rpc_helper != null) {
      //0.94-security uses an int for the flag section
      flags = buf.readInt();
    } else {
      flags = buf.readByte();
    }
    if ((flags & HBaseRpc.RPC_FRAMED) != 0) {
      // Total size of the response, including the RPC ID (4 bytes) and flags
      // (1 byte) that we've already read, including the 4 bytes used by
      // the length itself, and including the 4 bytes used for the RPC status.
      final int length = buf.readInt() - 4 - 1 - 4 - 4;
      final int status = buf.readInt();  // Unused right now.
      try {
        HBaseRpc.checkArrayLength(buf, length);
        // Make sure we have that many bytes readable.
        // This will have to change to be able to do streaming RPCs where we
        // deserialize parts of the response as it comes off the wire.
        ensureReadable(buf, length);
      } catch (IllegalArgumentException e) {
        LOG.error("WTF?  RPC #" + rpcid + ": ", e);
      }
    } else {
      LOG.info("RPC wasn't framed: " + rpc);
    }

    if ((flags & HBaseRpc.RPC_ERROR) != 0) {
      return deserializeException(buf, rpc);
    }
    try {
      return deserializeObject(buf, rpc);
    } catch (IllegalArgumentException e) {  // The RPC didn't look good to us.
      return new InvalidResponseException(e.getMessage(), e);
    }
  }

  /**
   * Consumes a pre-protobuf RPC from HBase that has been timed out. 
   * @param buf The channel buffer to read from
   * @param rdx The original reader index used if the response is framed
   * @param rpcid The RPC ID used for logging
   */
  private void consumeTimedoutNonPBufRPC(final ChannelBuffer buf, final int rdx, 
      final int rpcid) {
    final int flags;
    if (secure_rpc_helper != null) {
      flags = buf.readInt();
    } else {
      flags = buf.readByte();
    }
    
    if ((flags & HBaseRpc.RPC_FRAMED) != 0) {
      final int length = buf.readInt() - 4 - 1 - 4 - 4;
      buf.readInt();  // Unused right now.
      try {
        ensureReadable(buf, length);
      } catch (IllegalArgumentException e) {
        LOG.error("WTF?  RPC #" + rpcid + ": ", e);
      }
      // this is easy as we have the length to skip
      buf.readerIndex(rdx + length + 4 + 1 + 4 + 4);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipped timed out RPC ID " + rpcid + " of " + length + 
            " bytes on " + this);
      }
      return;
    }

    if ((flags & HBaseRpc.RPC_ERROR) != 0) {
      final String type = HBaseRpc.readHadoopString(buf);
      final String msg = HBaseRpc.readHadoopString(buf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipped timed out RPC ID " + rpcid + 
            " with an exception response of type: " + type + 
            " and message: " + msg + " on client " + this);
      }
      return;
    }
    
    try {
      consumeNonFramedTimedoutNonPBufRPC(buf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipped timed out RPC ID " + rpcid + " on client " + this);
      }
    } catch (IllegalArgumentException e) {  // The RPC didn't look good to us.
      LOG.error("Failure parsing timedout exception", 
          new InvalidResponseException(e.getMessage(), e));
    }
  }
  
  /**
   * Consumes a pre-0.92 HBase RPC response that has been timedout. 
   * Since we no longer have the original RPC we just keep reading.
   * @param buf The channel buffer to read from.
   */
  private static void consumeNonFramedTimedoutNonPBufRPC(final ChannelBuffer buf) {
    int length = 0;
    switch (buf.readByte()) {  // Read the type of the response.
    case  1:  // Boolean
      buf.readByte();
      return;
    case  6:  // Long
      buf.readLong();
      return;
    case 14:  // Writable
      consumeNonFramedTimedoutNonPBufRPC(buf);  // Recursively de-serialize it.
    case 17:  // NullInstance
      buf.readByte();  // Consume the (useless) type of the "null".
      return;
    case 37:  // Result
      buf.readByte();  // Read the type again.  See HBASE-2877.
      length = buf.readInt();
      HBaseRpc.checkArrayLength(buf, length);
      buf.readerIndex(buf.readerIndex() + length);
      return;
    case 38:  // Result[]
      length = buf.readInt();
      HBaseRpc.checkArrayLength(buf, length);
      buf.readerIndex(buf.readerIndex() + length);
      return;
    case 58:  // MultiPutResponse
      // Fall through
    case 67:  // MultiResponse
      int type = buf.readByte(); // sub type, either multi-put or multi
      // multi-put
      int nregions = buf.readInt();
      HBaseRpc.checkNonEmptyArrayLength(buf, nregions);
      for (int i = 0; i < nregions; i++) {
        HBaseRpc.readByteArray(buf); // region_name
        if (type == 58) {
          buf.readInt(); // index of the first failed edit
        } else {
          final int nkeys = buf.readInt();
          HBaseRpc.checkNonEmptyArrayLength(buf, nkeys);
          for (int j = 0; j < nkeys; j++) {
            buf.readInt();
            boolean error = buf.readByte() != 0x00;
            if (error) {
              HBaseRpc.readHadoopString(buf);
              HBaseRpc.readHadoopString(buf);
            } else {
              // recurse to consume the embedded response
              consumeNonFramedTimedoutNonPBufRPC(buf);
            }
          }
        }
      }
      return;
    }
  }
  
  /**
   * De-serializes an exception from HBase 0.94 and before.
   * @param buf The buffer to read from.
   * @param request The RPC that caused this exception.
   */
  static HBaseException deserializeException(final ChannelBuffer buf,
                                             final HBaseRpc request) {
    // In case of failures, the rest of the response is just 2
    // Hadoop-encoded strings.  The first is the class name of the
    // exception, the 2nd is the message and stack trace.
    final String type = HBaseRpc.readHadoopString(buf);
    final String msg = HBaseRpc.readHadoopString(buf);
    return makeException(request, type, msg);
  }

  /**
   * Creates an appropriate {@link HBaseException} for the given type.
   * When we de-serialize an exception from the wire, we're given a string as
   * a class name, which we use here to map to an appropriate subclass of
   * {@link HBaseException}, for which we create an instance that we return.
   * @param request The RPC in response of which the exception was received.
   * @param type The fully qualified class name of the exception type from
   * HBase's own code.
   * @param msg Some arbitrary additional string that accompanies the
   * exception, typically carrying a stringified stack trace.
   */
  private static final HBaseException makeException(final HBaseRpc request,
                                                    final String type,
                                                    final String msg) {
    final HBaseException exc = REMOTE_EXCEPTION_TYPES.get(type);
    if (exc != null) {
      return exc.make(msg, request);
    } else {
      return new RemoteException(type, msg);
    }
  }

  /**
   * Decodes an exception from HBase 0.95 and up.
   * @param e the exception protobuf obtained from the RPC response header
   * containing the exception.
   */
  private
    static HBaseException decodeException(final HBaseRpc request,
                                          final RPCPB.ExceptionResponse e) {
    final String type;
    if (e.hasExceptionClassName()) {
      type = e.getExceptionClassName();
    } else {
      type = "(missing exception type)";  // Shouldn't happen.
    }
    return makeException(request, type, e.getStackTrace());
  }

  /**
   * Decodes an exception from HBase 0.95 and up {@link HBasePB.NameBytesPair}.
   * @param pair A pair whose name is the exception type, and whose value is
   * the stringified stack trace.
   */
  static HBaseException decodeExceptionPair(final HBaseRpc request,
                                            final HBasePB.NameBytesPair pair) {
    final String stacktrace;
    if (pair.hasValue()) {
      stacktrace = pair.getValue().toStringUtf8();
    } else {
      stacktrace = "(missing server-side stack trace)";  // Shouldn't happen.
    }
    return makeException(request, pair.getName(), stacktrace);
  }

  /**
   * De-serializes a "Writable" serialized by
   * {@code HbaseObjectWritable#writeObject}.
   * @return The de-serialized object (which can be {@code null}).
   */
  @SuppressWarnings("fallthrough")
  static Object deserializeObject(final ChannelBuffer buf,
                                  final HBaseRpc request) {
    switch (buf.readByte()) {  // Read the type of the response.
      case  1:  // Boolean
        return buf.readByte() != 0x00;
      case  6:  // Long
        return buf.readLong();
      case 14:  // Writable
        return deserializeObject(buf, request);  // Recursively de-serialize it.
      case 17:  // NullInstance
        buf.readByte();  // Consume the (useless) type of the "null".
        return null;
      case 37:  // Result
        buf.readByte();  // Read the type again.  See HBASE-2877.
        return parseResult(buf);
      case 38:  // Result[]
        return parseResults(buf);
      case 58:  // MultiPutResponse
        // Fall through
      case 67:  // MultiResponse
        // Don't read the type again, responseFromBuffer() will need it.
        return ((MultiAction) request).responseFromBuffer(buf);
    }
    throw new NonRecoverableException("Couldn't de-serialize "
                                      + Bytes.pretty(buf));
  }

  /**
   * Pre-computes how many KVs we have so we can rightsized arrays.
   * This assumes that what's coming next in the buffer is a sequence of
   * KeyValues, each of which is prefixed by its length on 32 bits.
   * @param buf The buffer to peek into.
   * @param length The total size of all the KeyValues that follow.
   */
  static int numberOfKeyValuesAhead(final ChannelBuffer buf, int length) {
    // Immediately try to "fault" if `length' bytes aren't available.
    ensureReadable(buf, length);
    int num_kv = 0;
    int offset = buf.readerIndex();
    length += offset;
    while (offset < length) {
      final int kv_length = buf.getInt(offset);
      HBaseRpc.checkArrayLength(buf, kv_length);
      num_kv++;
      offset += kv_length + 4;
    }
    if (offset != length) {
      final int index = buf.readerIndex();
      badResponse("We wanted read " + (length - index)
                  + " bytes but we read " + (offset - index)
                  + " from " + buf + '=' + Bytes.pretty(buf));
    }
    return num_kv;
  }

  /**
   * De-serializes an {@code hbase.client.Result} object.
   * @param buf The buffer that contains a serialized {@code Result}.
   * @return The result parsed into a list of {@link KeyValue} objects.
   */
  private static ArrayList<KeyValue> parseResult(final ChannelBuffer buf) {
    final int length = buf.readInt();
    HBaseRpc.checkArrayLength(buf, length);
    //LOG.debug("total Result response length={}", length);

    final int num_kv = numberOfKeyValuesAhead(buf, length);

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
    ensureReadable(buf, length);
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

  /**
   * Package private method to allow the timeout timer or other methods 
   * to remove an RPCfrom the inflight map. Note that the RPC ID has to be set 
   * in the RPC for this to work. And if, somehow, we pop a different RPC from 
   * the map then we'll make sure to callback the popped RPC
   * @param rpc The RPC to remove from the map
   * @param timedout Whether or not the RPC timedout
   * @return The RPC that was removed
   */
  HBaseRpc removeRpc(final HBaseRpc rpc, final boolean timedout) {
    final HBaseRpc old_rpc = rpcs_inflight.remove(rpc.rpc_id);
    if (old_rpc != rpc) {
      LOG.error("Removed the wrong RPC " + old_rpc + 
          " when we meant to remove " + rpc);
      if (old_rpc != null) {
        old_rpc.callback(new NonRecoverableException(
            "Removed the wrong RPC from client " + this));
      }
    }
    if (timedout) {
      rpcs_timedout.incrementAndGet();
    }
    return old_rpc;
  }
  
  /** @return Package private method to fetch the associated HBase client */
  HBaseClient getHBaseClient() {
    return hbase_client;
  }
  
  public String toString() {
    final StringBuilder buf = new StringBuilder(13 + 10 + 6 + 64 + 16 + 1
                                                + 9 + 2 + 17 + 2 + 1);
    buf.append("RegionClient@")           // =13
      .append(hashCode())                 // ~10
      .append("(chan=")                   // = 6
      .append(chan)                       // ~64 (up to 66 when using IPv4)
      .append(", #pending_rpcs=");        // =16
    
    // avoid synchronization
    ArrayList<HBaseRpc> pending_rpcs = this.pending_rpcs;
    MultiAction batched_rpcs = this.batched_rpcs;
    int npending_rpcs = pending_rpcs == null ? 0 : pending_rpcs.size();
    int nedits = batched_rpcs == null ? 0 : batched_rpcs.size();
    
    buf.append(npending_rpcs)             // = 1
      .append(", #batched=")              // = 9
      .append(nedits);                    // ~ 2
    buf.append(", #rpcs_inflight=")       // =17
      .append(rpcs_inflight.size())       // ~ 2
      .append(')');                       // = 1
    return buf.toString();
  }

  // -------------------------------------------- //
  // Dealing with the handshake at the beginning. //
  // -------------------------------------------- //

  /** Initial part of the header until 0.94.  */
  private final static byte[] HRPC3 = new byte[] { 'h', 'r', 'p', 'c', 3 };

  /** Authentication method (for HBase 0.95 and up). */
  private final static byte SIMPLE_AUTH = (byte) 0x50;

  /** Initial part of the header for 0.95 and up.  */
  private final static byte[] HBASE = new byte[] { 'H', 'B', 'a', 's',
                                                   0,     // RPC version.
                                                   SIMPLE_AUTH,
                                                   };

  /** Common part of the hello header: magic + version.  */
  private ChannelBuffer commonHeader(final byte[] buf, final byte[] hrpc) {
    final ChannelBuffer header = ChannelBuffers.wrappedBuffer(buf);
    header.clear();  // Set the writerIndex to 0.

    // Magic header.  See HBaseClient#writeHeader
    // "hrpc" followed by the version.
    // See HBaseServer#HEADER and HBaseServer#CURRENT_VERSION.
    header.writeBytes(hrpc);  // for 0.94 and earlier: 4 + 1. For 0.95+: 4 + 2
    return header;
  }

  /** Hello header for HBase 0.95 and later.  */
  private ChannelBuffer header095() {
    final RPCPB.UserInformation user = RPCPB.UserInformation.newBuilder()
      .setEffectiveUser(System.getProperty("user.name", "asynchbase"))
      .build();
    final RPCPB.ConnectionHeader pb = RPCPB.ConnectionHeader.newBuilder()
      .setUserInfo(user)
      .setServiceName("ClientService")
      .setCellBlockCodecClass("org.apache.hadoop.hbase.codec.KeyValueCodec")
      .build();
    final int pblen = pb.getSerializedSize();
    final byte[] buf = new byte[HBASE.length + 4 + pblen];
    final ChannelBuffer header = commonHeader(buf, HBASE);
    header.writeInt(pblen);  // 4 bytes
    try {
      final CodedOutputStream output =
        CodedOutputStream.newInstance(buf, HBASE.length + 4, pblen);
      pb.writeTo(output);
      output.checkNoSpaceLeft();
    } catch (IOException e) {
      throw new RuntimeException("Should never happen", e);
    }
    // We wrote to the underlying buffer but Netty didn't see the writes,
    // so move the write index forward.
    header.writerIndex(buf.length);
    return header;
  }

  /** Hello header for HBase 0.92 to 0.94.  */
  private ChannelBuffer header092() {
    final byte[] buf = new byte[4 + 1 + 4 + 1 + 44];
    final ChannelBuffer header = commonHeader(buf, HRPC3);

    // Serialized ipc.ConnectionHeader
    // We skip 4 bytes now and will set it to the actual size at the end.
    header.writerIndex(header.writerIndex() + 4);  // 4
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    header.writeByte(klass.length());              // 1
    header.writeBytes(Bytes.ISO88591(klass));      // 44

    // Now set the length of the whole damn thing.
    // -4 because the length itself isn't part of the payload.
    // -5 because the "hrpc" + version isn't part of the payload.
    header.setInt(5, header.writerIndex() - 4 - 5);
    return header;
  }

  /** Hello header for HBase 0.90 and earlier.  */
  private ChannelBuffer header090() {
    final byte[] buf = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
    final ChannelBuffer header = commonHeader(buf, HRPC3);

    // Serialized UserGroupInformation to say who we are.
    // We're not nice so we're not gonna say who we are and we'll just send
    // `null' (hadoop.io.ObjectWritable$NullInstance).
    // First, we need the size of the whole damn UserGroupInformation thing.
    // We skip 4 bytes now and will set it to the actual size at the end.
    header.writerIndex(header.writerIndex() + 4);             // 4
    // Write the class name of the object.
    // See hadoop.io.ObjectWritable#writeObject
    // See hadoop.io.UTF8#writeString
    // String length as a short followed by UTF-8 string.
    String klass = "org.apache.hadoop.io.Writable";
    header.writeShort(klass.length());                        // 2
    header.writeBytes(Bytes.ISO88591(klass));                 // 29
    klass = "org.apache.hadoop.io.ObjectWritable$NullInstance";
    header.writeShort(klass.length());                        // 2
    header.writeBytes(Bytes.ISO88591(klass));                 // 48
    klass = "org.apache.hadoop.security.UserGroupInformation";
    header.writeShort(klass.length());                        // 2
    header.writeBytes(Bytes.ISO88591(klass));                 // 47

    // Now set the length of the whole damn thing.
    // -4 because the length itself isn't part of the payload.
    // -5 because the "hrpc" + version isn't part of the payload.
    header.setInt(5, header.writerIndex() - 4 - 5);
    return header;
  }

  /** CDH3b3-specific header for Hadoop "security".  */
  private ChannelBuffer headerCDH3b3() {
    // CDH3 b3 includes a temporary patch that is non-backwards compatible
    // and results in clients getting disconnected as soon as they send the
    // header, because the HBase RPC protocol provides no mechanism to send
    // an error message back to the client during the initial "hello" stage
    // of a connection.
    final byte[] user = Bytes.UTF8(System.getProperty("user.name", "asynchbase"));
    final byte[] buf = new byte[4 + 1 + 4 + 4 + user.length];
    final ChannelBuffer header = commonHeader(buf, HRPC3);

    // Length of the encoded string (useless).
    header.writeInt(4 + user.length);  // 4
    // String as encoded by `WritableUtils.writeString'.
    header.writeInt(user.length);      // 4
    header.writeBytes(user);           // length bytes
    return header;
  }

  /**
   * Sends the "hello" message needed when opening a new connection.
   * This message is immediately followed by a {@link GetProtocolVersionRequest}
   * so we can learn what version the server is running to be able to talk to
   * it the way it expects.
   * @param chan The channel connected to the server we need to handshake.
   * @param header The header to use for the handshake.
   */
  private void helloRpc(final Channel chan, final ChannelBuffer header) {
    LOG.debug("helloRpc for the channel: {}", chan);
    Callback<Object, Exception> errorback = new Callback<Object, Exception>() {

      @Override
      public Object call(final Exception e) throws Exception {
        LOG.info("helloRpc failed. Closing the channel:" + chan, e);
        Channels.close(chan);
        return e;
      }
    };
    final GetProtocolVersionRequest rpc = new GetProtocolVersionRequest();
    rpc.getDeferred().addBoth(new ProtocolVersionCB(chan))
                     .addErrback(errorback);
    Channels.write(chan, ChannelBuffers.wrappedBuffer(header, encode(rpc)));
  }

  /**
   * Fetch the protocol version from the region server
   * @param chan The channel to use for communications
   */
  void sendVersion(final Channel chan) {
    final GetProtocolVersionRequest rpc = new GetProtocolVersionRequest();
    rpc.getDeferred().addBoth(new ProtocolVersionCB(chan));
    Channels.write(chan, encode(rpc));
  }
}
