/*
 * Copyright 2010 StumbleUpon, Inc.
 * This file is part of Async HBase.
 * Async HBase is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hbase.async;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
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
 */
final class RegionClient extends ReplayingDecoder<VoidEnum>
  implements ChannelDownstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RegionClient.class);

  /** Maps remote exception types to our corresponding types.  */
  private static final HashMap<String, HBaseException> REMOTE_EXCEPTION_TYPES;
  static {
    REMOTE_EXCEPTION_TYPES = new HashMap<String, HBaseException>();
    REMOTE_EXCEPTION_TYPES.put(NoSuchColumnFamilyException.REMOTE_CLASS,
                               new NoSuchColumnFamilyException(null));
    REMOTE_EXCEPTION_TYPES.put(NotServingRegionException.REMOTE_CLASS,
                               new NotServingRegionException(null));
    REMOTE_EXCEPTION_TYPES.put(UnknownScannerException.REMOTE_CLASS,
                               new UnknownScannerException(null));
    REMOTE_EXCEPTION_TYPES.put(UnknownRowLockException.REMOTE_CLASS,
                               new UnknownRowLockException(null));
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
   * Multi-put request in which we accumulate edits before sending them.
   * Because whether or not we write to the WAL is a property on an entire
   * MultiPut, not on a per-edit basis, we have to buffer edits that want
   * durability separately from edits that don't want durability.
   *
   * Manipulating this reference requires synchronizing on `this'.
   */
  private MultiPutRequest mput_not_durable;

  /**
   * Multi-put request in which we accumulate edits that need to be durable.
   * Manipulating this reference requires synchronizing on `this'.
   */
  private MultiPutRequest mput_durable;

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
  private AtomicInteger rpcid = new AtomicInteger(-1);

  private final TimerTask flush_timer = new TimerTask() {
    public void run(final Timeout timeout) {
      periodicFlush();
    }
    public String toString() {
      return "flush commits of " + RegionClient.this;
    }
  };

  /**
   * Constructor.
   * @param hbase_client The HBase client this instance belongs to.
   */
  public RegionClient(final HBaseClient hbase_client) {
    this.hbase_client = hbase_client;
  }

  /** Periodically flushes buffered edits.  */
  private void periodicFlush() {
    if (chan != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Periodic flush timer: flushing edits for " + this);
      }
      flush();
    }

    // Schedule the next periodic flush.
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
    MultiPutRequest mput_not_durable;
    MultiPutRequest mput_durable;
    synchronized (this) {
      mput_not_durable = this.mput_not_durable;
      this.mput_not_durable = null;
      mput_durable = this.mput_durable;
      this.mput_durable = null;
    }
    if (mput_not_durable == null && mput_durable == null) {  // Nothing to do.
      return Deferred.fromResult(null);
    } else if (mput_not_durable == null || mput_durable == null) {  // Either.
      final MultiPutRequest multiput =
        mput_durable == null ? mput_not_durable : mput_durable;
      final Deferred<Object> d = multiput.getDeferred();
      sendRpc(multiput);
      return d;
    }

    // If we get here, we have to flush both durable and non-durable edits.
    // Flush the non-durable ones first, they'll be easier on HBase.
    @SuppressWarnings("unchecked")
    final Deferred<Object> d =
    (Deferred) Deferred.group(mput_not_durable.getDeferred(),
                              mput_durable.getDeferred());
    sendRpc(mput_not_durable);
    sendRpc(mput_durable);
    return d;
  }

  /**
   * Attempts to gracefully terminate the connection to this RegionServer.
   */
  public Deferred<Object> shutdown() {
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
        return Deferred.group(inflight).addCallbackDeferring(
          new Callback<Deferred<Object>, ArrayList<Object>>() {
            public Deferred<Object> call(final ArrayList<Object> arg) {
              return shutdown();
            }
            public String toString() {
              return "wait until " + size + " RPCs complete";
            }
          });
      }
    }

    synchronized (this) {
      if (pending_rpcs != null && !pending_rpcs.isEmpty()) {
        LOG.error("Shutdown requested on " + this
                  + " while there are pending RPCs: " + pending_rpcs);
        final NonRecoverableException shuttingdown =
          new NonRecoverableException("shutting down");
        for (final HBaseRpc rpc : pending_rpcs) {
          rpc.popDeferred().callback(shuttingdown);
        }
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

  /**
   * Asks the server which RPC protocol version it's running.
   * @return a Deferred {@link Long}.
   */
  public Deferred<Long> getProtocolVersion() {
    final HBaseRpc rpc = new HBaseRpc(Bytes.ISO88591("getProtocolVersion")) {
      ChannelBuffer serialize() {
        // num param + type 1 + string length + string + type 2 + long
        final ChannelBuffer buf = newBuffer(4 + 1 + 1 + 44 + 1 + 8);
        buf.writeInt(2);  // Number of parameters.
        // 1st param.
        writeHBaseString(buf, "org.apache.hadoop.hbase.ipc.HRegionInterface");
        // 2nd param.
        writeHBaseLong(buf, 24);
        return buf;
      }
    };

    final Deferred<Long> d = rpc.getDeferred()
      .addCallback(got_protocol_version);
    sendRpc(rpc);
    return d;
  }

  /** Singleton callback to handle responses of getProtocolVersion RPCs.  */
  private final Callback<Long, Object> got_protocol_version =
    new Callback<Long, Object>() {
      public Long call(final Object response) {
        if (response instanceof Long) {
          return (Long) response;
        } else {
          throw new InvalidResponseException(Long.class, response);
        }
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
   * Finds the highest row that's less than or equal to the given row.
   * @param region_name The name of the region in which to search.
   * @param row The row to search.
   * @param family The family to get.
   * @return A Deferred {@link ArrayList} of {@link KeyValue}.  The list is
   * guaranteed to be non-{@code null} but may be empty.
   */
  public
    Deferred<ArrayList<KeyValue>> getClosestRowBefore(final byte[] region_name,
                                                      final byte[] row,
                                                      final byte[] family) {
    final HBaseRpc rpc = new HBaseRpc(GET_CLOSEST_ROW_BEFORE) {
      ChannelBuffer serialize() {
        // region.length and row.length will use at most a 3-byte VLong.
        // This is because VLong wastes 1 byte of meta-data + 2 bytes of
        // payload.  HBase's own KeyValue code uses a short to store the row
        // length.  Finally, family.length cannot be on more than 1 byte,
        // HBase's own KeyValue code uses a byte to store the family length.
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
   * Executes one or more "put" request on HBase.
   * @param requests A non-empty list of edits that are guaranteed to be sent
   * out together at the same time, in the same single RPC.
   * If any of the edits specifies an explicit row lock, then we expect all
   * edits in the list have the <b>same</b> explicit row lock.
   * @param region_name The name of the region all those edits belongs to.
   * @param durable If {@code true}, the success of this RPC guarantees that
   * HBase has stored the edits in a durable fashion.  See {@link PutRequest}.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
   */
  public Deferred<Object> put(final List<PutRequest> requests,
                              final byte[] region_name,
                              final boolean durable) {
    MultiPutRequest multiput;
    if (requests.get(0).lockid() != RowLock.NO_LOCK  // Explicit row lock?
        || hbase_client.getFlushInterval() == 0) {   // No buffering allowed.
      // Don't buffer the edits, flush them immediately.  If the first edit
      // specifies has an explicit row lock, we assume that they all have the
      // same explicit row lock.
      multiput = new MultiPutRequest(durable, requests.get(0).lockid());
      multiput.add(requests, region_name);
      final Deferred<Object> d = addPutCallbacks(multiput);
      sendRpc(multiput);
      return d;
    }

    // Note that we need just a single Deferred for all of the PutRequests.
    final Deferred<Object> d = new Deferred<Object>();

    synchronized (this) {
      if (durable) {
        if (mput_durable == null) {
          mput_durable = new MultiPutRequest(true);
          addPutCallbacks(mput_durable);
        }
        multiput = mput_durable;
      } else {
        if (mput_not_durable == null) {
          mput_not_durable = new MultiPutRequest(false);
          addPutCallbacks(mput_not_durable);
        }
        multiput = mput_not_durable;
      }
      // Unfortunately we have to hold the monitor on `this' while we do
      // this entire atomic dance.
      multiput.add(requests, region_name);
      // Make `d' run when the MultiPutRequest completes.
      multiput.getDeferred().chain(d);
      if (chan == null || multiput.size() < 127 - 1) {  // XXX Don't hardcode.
        return d;   // The edits got buffered, stop here.
      }
      // Execute the edits buffered so far.  But first we must clear
      // the reference to the buffer we're about to send to HBase.
      if (durable) {
        mput_durable = null;
      } else {
        mput_not_durable = null;
      }
    }

    sendRpc(multiput);
    return d;
  }

  /**
   * Creates a new callback to handle the response to a multi-put request.
   * @param request The request for which we must handle the response.  We need
   * it in case we need to retry some of the edits due to a partial failure
   * where not all the edits in the batch have been applied.
   */
  private Deferred<Object> addPutCallbacks(final MultiPutRequest request) {
    return request.getDeferred().addCallbacks(new Callback<Object, Object>() {
      public Object call(final Object resp) {
        if (!(resp instanceof MultiPutResponse)) {
          throw new InvalidResponseException(MultiPutResponse.class, resp);
        }
        final MultiPutResponse response = (MultiPutResponse) resp;
        final Bytes.ByteMap<Integer> failures = response.failures();
        if (failures.isEmpty()) {
          return null;  // Yay, success!
        }
        final MultiPutRequest retry = MultiPutRequest.retry(request, failures);
        // TODO(tsuna): Wondering whether logging this at the WARN level
        // won't be too spammy / distracting.  Maybe we can tune this down to
        // DEBUG once the pretty bad bug HBASE-2898 is fixed.
        LOG.warn("Some edits failed for " + failures
                 + ", hopefully it's just due to a region split.");
        // Retry the failed edits.  They should go to the same region server.
        // If they weren't supposed to go to the same region server, we would
        // have gotten an NSRE and we wouldn't be here.
        sendRpc(retry);
        return addPutCallbacks(retry);
      }
      public String toString() {
        return "type multiPut response";
      }
    },
    // Now the errback (2nd argument to `addCallbacks').
    new Callback<Object, Exception>() {
      public Object call(final Exception e) {
        if (e instanceof NotServingRegionException) {
          // This is the worst case.  See HBASE-2898 for some more background.
          // Basically, one or more of the edits attempted to go to a region
          // that is no longer being served by this region server, but we don't
          // know which.  So the only thing we can do is to invalidate our
          // cache for all the regions involved in the multiPut and then re-try
          // all the edits.
          for (final byte[] region_name : request.regions()) {
            hbase_client.invalidateRegionCache(region_name);
          }
          final ArrayList<PutRequest> edits = request.toPuts();
          hbase_client.put(edits);
        }
        return e;  // Let the error propagate.
      }
      public String toString() {
        return "multiPut errback";
      }
    });
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
  void sendRpc(final HBaseRpc rpc) {
    if (chan != null) {
      Channels.write(chan, rpc);
    } else {
      synchronized (this) {
        if (pending_rpcs == null) {
          pending_rpcs = new ArrayList<HBaseRpc>();
        }
        pending_rpcs.add(rpc);
      }
      LOG.debug("RPC queued: {}", rpc);
    }
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
          Channels.write(chan, rpc);
        }
      }
    }
    periodicFlush();
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
    final HBaseException exception = new ConnectionResetException(chan);
    failOrRetryRpcs(rpcs_inflight.values(), exception);
    rpcs_inflight.clear();

    ArrayList<HBaseRpc> rpcs;
    synchronized (this) {
      rpcs = pending_rpcs;
      pending_rpcs = null;
    }
    if (rpcs != null) {
      failOrRetryRpcs(rpcs, exception);
    }
  }

  /**
   * Fail all RPCs in a collection or attempt to reschedule them if possible.
   * @param rpcs A possibly empty but non-{@code null} collection of RPCs.
   * @param exception The exception with which to fail RPCs that can't be
   * retried.
   */
  private void failOrRetryRpcs(final Collection<HBaseRpc> rpcs,
                               final HBaseException exception) {
    for (final HBaseRpc rpc : rpcs) {
      final RegionInfo region = rpc.getRegion();
      if (region == null) {  // Can't retry, dunno where this RPC should go.
        rpc.popDeferred().callback(exception);
      } else {
        hbase_client.sendRpcToRegion(rpc);  // Re-schedule the RPC.
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

  /**
   * Handles messages going downstream (to the wire).
   * <p>
   * This method can be called from any thread so it needs to be thread-safe.
   * @param ctx The context of the channel handler.
   * @param event The event traveling downstream.
   */
  @Override
  public void handleDownstream(final ChannelHandlerContext ctx,
                               ChannelEvent event) {
    if (event instanceof MessageEvent) {
      final MessageEvent me = (MessageEvent) event;
      final ChannelBuffer buf = encode((HBaseRpc) me.getMessage());
      if (buf == null) {
        return;
      }
      event = new DownstreamMessageEvent(ctx.getChannel(), me.getFuture(),
                                         buf, me.getRemoteAddress());
    }
    ctx.sendDownstream(event);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent event) {
    final Throwable e = event.getCause();
    final Channel c = event.getChannel();
    final SocketAddress remote = c.getRemoteAddress();

    if (e instanceof ConnectException) {
      // Maybe the remote side refused our connection (because it's down).
      // Let's try to reconnect see if it works out.
      if (remote != null) {
        LOG.warn("Couldn't connect to " + remote + ", retrying...", e);
        c.connect(remote);   // XXX don't retry indefinitely.
        return;
      }  // if remote is null, we don't know who to reconnect to...
    } else if (e instanceof RejectedExecutionException) {
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
   * Encodes an RPC and sends it downstream (to the wire).
   * <p>
   * This method can be called from any thread so it needs to be thread-safe.
   * @param rpc The RPC to send downstream.
   * @return The buffer to write to the channel or {@code null} if there was
   * an error and there's nothing to write.
   */
  private ChannelBuffer encode(final HBaseRpc rpc) {
    // TODO(tsuna): Add rate-limiting here.  We don't want to send more than
    // N QPS to a given region server.
    // TODO(tsuna): Check the size() of rpcs_inflight.  We don't want to have
    // more than M RPCs in flight at the same time, and we may be overwhelming
    // the server if we do.

    final int rpcid = this.rpcid.incrementAndGet();
    ChannelBuffer payload;
    try {
      payload = rpc.serialize();
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
      rpc.popDeferred().callback(e);  // Make the RPC fail with the exception.
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
        oldrpc.popDeferred().callback(new NonRecoverableException(wtf));
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
    final Object decoded = deserialize(buf);
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
    } else if (decoded instanceof NotServingRegionException) {
      // If this RPC was targeted at a particular region and we got an NSRE,
      // it means this RegionServer no longer hosts that region.  So we have
      // to invalidate our region cache for this particular region and we can
      // try to restart the RPC (which will probably cause a META lookup to
      // find where that region is now).
      final RegionInfo region = rpc.getRegion();
      if (region != null && rpc.attempt <= 10) {  // XXX Don't hardcode.
        hbase_client.invalidateRegionCache(region);
        hbase_client.sendRpcToRegion(rpc);  // Restart the RPC.
        return null;
      }  // else: This RPC wasn't targeted at a particular region.
         //  -or- This RPC was already retried too many times.
         //   =>  We'll give the exception to the callback chain.
    }
    rpc.attempt = 0;  // In case this HBaseRpc instance gets re-used.

    try {
      // From the point where `popDeferred()' is invoked, the HBaseRpc object
      // is re-usable to re-send the same RPC.
      rpc.popDeferred().callback(decoded);
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
   * @return The de-serialized RPC response (which can be {@code null}
   * or an exception).
   */
  private static Object deserialize(final ChannelBuffer buf) {
    // The 1st byte of the payload tells us whether the request failed.
    if (buf.readByte() != 0x00) {  // 0x00 means no error.
      // In case of failures, the rest of the response is just 2
      // Hadoop-encoded strings.  The first is the class name of the
      // exception, the 2nd is the message and stack trace.
      final String type = HBaseRpc.readHadoopString(buf);
      final String msg = HBaseRpc.readHadoopString(buf);
      final HBaseException exc = REMOTE_EXCEPTION_TYPES.get(type);
      if (exc != null) {
        return exc.make(msg);
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
    final StringBuilder buf = new StringBuilder(13 + 10 + 6 + 64 + 16 + 1 + 17
                                                + 2 + 1);
    buf.append("RegionClient@")           // =13
      .append(hashCode())                 // ~10
      .append("(chan=")                   // = 6
      .append(chan)                       // ~64 (up to 66 when using IPv4)
      .append(", #pending_rpcs=");        // =16
    synchronized (this) {
      if (pending_rpcs == null) {
        buf.append('0');                  // = 1
      } else {                            // or
        buf.append(pending_rpcs.size());  // ~ 1
      }
    }
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
      HELLO_HEADER = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
      final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(HELLO_HEADER);
      buf.clear();  // Set the writerIndex to 0.

      // Magic header.  See HBaseClient#writeHeader
      // "hrpc" followed by the version (3).
      // See HBaseServer#HEADER and HBaseServer#CURRENT_VERSION.
      buf.writeBytes(new byte[] { 'h', 'r', 'p', 'c', 3 });  // 4 + 1
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
        final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(header, payload);
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

  }

}
