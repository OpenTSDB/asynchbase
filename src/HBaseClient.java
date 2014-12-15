/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.cache.LoadingCache;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.hbase.async.generated.ZooKeeperPB;

/**
 * A fully asynchronous, thread-safe, modern HBase client.
 * <p>
 * Unlike the traditional HBase client ({@code HTable}), this client should be
 * instantiated only once.  You can use it with any number of tables at the
 * same time.  The only case where you should have multiple instances is when
 * you want to use multiple different clusters at the same time.
 * <p>
 * If you play by the rules, this client is (in theory {@code :D}) completely
 * thread-safe.  Read the documentation carefully to know what the requirements
 * are for this guarantee to apply.
 * <p>
 * This client is fully non-blocking, any blocking operation will return a
 * {@link Deferred} instance to which you can attach a {@link Callback} chain
 * that will execute when the asynchronous operation completes.
 *
 * <h1>Note regarding {@code HBaseRpc} instances passed to this class</h1>
 * Every {@link HBaseRpc} passed to a method of this class should not be
 * changed or re-used until the {@code Deferred} returned by that method
 * calls you back.  <strong>Changing or re-using any {@link HBaseRpc} for
 * an RPC in flight will lead to <em>unpredictable</em> results and voids
 * your warranty</strong>.
 *
 * <a name="#durability"></a>
 * <h1>Data Durability</h1>
 * Some methods or RPC types take a {@code durable} argument.  When an edit
 * requests to be durable, the success of the RPC guarantees that the edit is
 * safely and durably stored by HBase and won't be lost.  In case of server
 * failures, the edit won't be lost although it may become momentarily
 * unavailable.  Setting the {@code durable} argument to {@code false} makes
 * the operation complete faster (and puts a lot less strain on HBase), but
 * removes this durability guarantee.  In case of a server failure, the edit
 * may (or may not) be lost forever.  When in doubt, leave it to {@code true}
 * (or use the corresponding method that doesn't accept a {@code durable}
 * argument as it will default to {@code true}).  Setting it to {@code false}
 * is useful in cases where data-loss is acceptable, e.g. during batch imports
 * (where you can re-run the whole import in case of a failure), or when you
 * intend to do statistical analysis on the data (in which case some missing
 * data won't affect the results as long as the data loss caused by machine
 * failures preserves the distribution of your data, which depends on how
 * you're building your row keys and how you're using HBase, so be careful).
 * <p>
 * Bear in mind that this durability guarantee holds only once the RPC has
 * completed successfully.  Any edit temporarily buffered on the client side
 * or in-flight will be lost if the client itself crashes.  You can control
 * how much buffering is done by the client by using {@link #setFlushInterval}
 * and you can force-flush the buffered edits by calling {@link #flush}.  When
 * you're done using HBase, you <strong>must not</strong> just give up your
 * reference to your {@code HBaseClient}, you must shut it down gracefully by
 * calling {@link #shutdown}.  If you fail to do this, then all edits still
 * buffered by the client will be lost.
 * <p>
 * <b>NOTE</b>: This entire section assumes that you use a distributed file
 * system that provides HBase with the required durability semantics.  If
 * you use HDFS, make sure you have a version of HDFS that provides HBase
 * the necessary API and semantics to durability store its data.
 *
 * <h1>{@code throws} clauses</h1>
 * None of the asynchronous methods in this API are expected to throw an
 * exception.  But the {@link Deferred} object they return to you can carry an
 * exception that you should handle (using "errbacks", see the javadoc of
 * {@link Deferred}).  In order to be able to do proper asynchronous error
 * handling, you need to know what types of exceptions you're expected to face
 * in your errbacks.  In order to document that, the methods of this API use
 * javadoc's {@code @throws} to spell out the exception types you should
 * handle in your errback.  Asynchronous exceptions will be indicated as such
 * in the javadoc with "(deferred)".
 * <p>
 * For instance, if a method {@code foo} pretends to throw an
 * {@link UnknownScannerException} and returns a {@code Deferred<Whatever>},
 * then you should use the method like so:
 * <pre>
 *   HBaseClient client = ...;
 *   {@link Deferred}{@code <Whatever>} d = client.foo();
 *   d.addCallbacks(new {@link Callback}{@code <Whatever, SomethingElse>}() {
 *     SomethingElse call(Whatever arg) {
 *       LOG.info("Yay, RPC completed successfully!");
 *       return new SomethingElse(arg.getWhateverResult());
 *     }
 *     String toString() {
 *       return "handle foo response";
 *     }
 *   },
 *   new {@link Callback}{@code <Exception, Object>}() {
 *     Object call(Exception arg) {
 *       if (arg instanceof {@link UnknownScannerException}) {
 *         LOG.error("Oops, we used the wrong scanner?", arg);
 *         return otherAsyncOperation();  // returns a {@code Deferred<Blah>}
 *       }
 *       LOG.error("Sigh, the RPC failed and we don't know what to do", arg);
 *       return arg;  // Pass on the error to the next errback (if any);
 *     }
 *     String toString() {
 *       return "foo errback";
 *     }
 *   });
 * </pre>
 * This code calls {@code foo}, and upon successful completion transforms the
 * result from a {@code Whatever} to a {@code SomethingElse} (which will then
 * be given to the next callback in the chain, if any).  When there's a
 * failure, the errback is called instead and it attempts to handle a
 * particular type of exception by retrying the operation differently.
 */
public final class HBaseClient {
  /*
   * TODO(tsuna): Address the following.
   *
   * - Properly handle disconnects.
   *    - Attempt to reconnect a couple of times, see if it was a transient
   *      network blip.
   *    - If the -ROOT- region is unavailable when we start, we should
   *      put a watch in ZK instead of polling it every second.
   * - Handling RPC timeouts.
   * - Stats:
   *     - QPS per RPC type.
   *     - Latency histogram per RPC type (requires open-sourcing the SU Java
   *       stats classes that I wrote in a separate package).
   *     - Cache hit rate in the local META cache.
   *     - RPC errors and retries.
   *     - Typical batch size when flushing edits (is that useful?).
   * - Write unit tests and benchmarks!
   */

  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

  /**
   * An empty byte array you can use.  This can be useful for instance with
   * {@link Scanner#setStartKey} and {@link Scanner#setStopKey}.
   */
  public static final byte[] EMPTY_ARRAY = new byte[0];

  /** A byte array containing a single zero byte.  */
  private static final byte[] ZERO_ARRAY = new byte[] { 0 };

  protected static final byte[] ROOT = new byte[] { '-', 'R', 'O', 'O', 'T', '-' };
  protected static final byte[] ROOT_REGION = new byte[] { '-', 'R', 'O', 'O', 'T', '-', ',', ',', '0' };
  protected static final byte[] META = new byte[] { '.', 'M', 'E', 'T', 'A', '.' };
  protected static final byte[] INFO = new byte[] { 'i', 'n', 'f', 'o' };
  protected static final byte[] REGIONINFO = new byte[] { 'r', 'e', 'g', 'i', 'o', 'n', 'i', 'n', 'f', 'o' };
  protected static final byte[] SERVER = new byte[] { 's', 'e', 'r', 'v', 'e', 'r' };
  /** HBase 0.95 and up: .META. is now hbase:meta */
  protected static final byte[] HBASE96_META =
    new byte[] { 'h', 'b', 'a', 's', 'e', ':', 'm', 'e', 't', 'a' };
  /** New for HBase 0.95 and up: the name of META is fixed.  */
  protected static final byte[] META_REGION_NAME =
    new byte[] { 'h', 'b', 'a', 's', 'e', ':', 'm', 'e', 't', 'a', ',', ',', '1' };
  /** New for HBase 0.95 and up: the region info for META is fixed.  */
  protected static final RegionInfo META_REGION =
    new RegionInfo(HBASE96_META, META_REGION_NAME, EMPTY_ARRAY);

  /** How many times to retry an RPC before giving up */
  protected static int MAX_RETRY_ATTEMPTS = 10;
  
  /**
   * In HBase 0.95 and up, this magic number is found in a couple places.
   * It's used in the znode that points to the .META. region, to
   * indicate that the contents of the znode is a protocol buffer.
   * It's also used in the value of the KeyValue found in the .META. table
   * that contain a {@link RegionInfo}, to indicate that the value contents
   * is a protocol buffer.
   */
  static final int PBUF_MAGIC = 1346524486;  // 4 bytes: "PBUF"

  /**
   * Timer we use to handle all our timeouts.
   * TODO(tsuna): Get it through the ctor to share it with others.
   * TODO(tsuna): Make the tick duration configurable?
   */
  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  /** Up to how many milliseconds can we buffer an edit on the client side.  */
  private volatile short flush_interval = 1000;  // ms

  /**
   * How many different counters do we want to keep in memory for buffering.
   * Each entry requires storing the table name, row key, family name and
   * column qualifier, plus 4 small objects.
   *
   * Assuming an average table name of 10 bytes, average key of 20 bytes,
   * average family name of 10 bytes and average qualifier of 8 bytes, this
   * would require 65535 * (10 + 20 + 10 + 8 + 4 * 32) / 1024 / 1024 = 11MB
   * of RAM, which isn't too excessive for a default value.  Of course this
   * might bite people with large keys or qualifiers, but then it's normal
   * to expect they'd tune this value to cater to their unusual requirements.
   */
  private volatile int increment_buffer_size = 65535;

  /**
   * Factory through which we will create all its channels / sockets.
   */
  private final ClientSocketChannelFactory channel_factory;

  /** Watcher to keep track of the -ROOT- region in ZooKeeper.  */
  private final ZKClient zkclient;

  /**
   * The client currently connected to the -ROOT- region.
   * If this is {@code null} then we currently don't know where the -ROOT-
   * region is and we're waiting for a notification from ZooKeeper to tell
   * us where it is.
   * Note that with HBase 0.95, {@link #has_root} would be false, and this
   * would instead point to the .META. region.
   */
  private volatile RegionClient rootregion;

  /**
   * Whether or not there is a -ROOT- region.
   * When connecting to HBase 0.95 and up, this would be set to false, so we
   * would go straight to .META. instead.
   */
  volatile boolean has_root = true;

  /**
   * Maps {@code (table, start_key)} pairs to the {@link RegionInfo} that
   * serves this key range for this table.
   * <p>
   * The keys in this map are region names.
   * @see #createRegionSearchKey
   * Because it's a sorted map, we can efficiently find a region given an
   * arbitrary key.
   * @see #getRegion
   * <p>
   * This map and the next 2 maps contain the same data, but indexed
   * differently.  There is no consistency guarantee across the maps.
   * They are not updated all at the same time atomically.  This map
   * is always the first to be updated, because that's the map from
   * which all the lookups are done in the fast-path of the requests
   * that need to locate a region.  The second map to be updated is
   * {@link region2client}, because it comes second in the fast-path
   * of every requests that need to locate a region.  The third map
   * is only used to handle RegionServer disconnections gracefully.
   * <p>
   * Note: before using the {@link RegionInfo} you pull out of this map,
   * you <b>must</b> ensure that {@link RegionInfo#table} doesn't return
   * {@link #EMPTY_ARRAY}.  If it does, it means you got a special entry
   * used to indicate that this region is known to be unavailable right
   * now due to an NSRE.  You must not use this {@link RegionInfo} as
   * if it was a normal entry.
   * @see #handleNSRE
   */
  private final ConcurrentSkipListMap<byte[], RegionInfo> regions_cache =
    new ConcurrentSkipListMap<byte[], RegionInfo>(RegionInfo.REGION_NAME_CMP);

  /**
   * Maps a {@link RegionInfo} to the client currently connected to the
   * RegionServer that serves this region.
   * <p>
   * The opposite mapping is stored in {@link #client2regions}.
   * There's no consistency guarantee with that other map.
   * See the javadoc for {@link #regions_cache} regarding consistency.
   */
  private final ConcurrentHashMap<RegionInfo, RegionClient> region2client =
    new ConcurrentHashMap<RegionInfo, RegionClient>();

  /**
   * Maps a client connected to a RegionServer to the list of regions we know
   * it's serving so far.
   * <p>
   * The opposite mapping is stored in {@link #region2client}.
   * There's no consistency guarantee with that other map.
   * See the javadoc for {@link #regions_cache} regarding consistency.
   * <p>
   * Each list in the map is protected by its own monitor lock.
   */
  private final ConcurrentHashMap<RegionClient, ArrayList<RegionInfo>>
    client2regions = new ConcurrentHashMap<RegionClient, ArrayList<RegionInfo>>();

  /**
   * Cache that maps a RegionServer address ("ip:port") to the client
   * connected to it.
   * <p>
   * Access to this map must be synchronized by locking its monitor.
   * Lock ordering: when locking both this map and a RegionClient, the
   * RegionClient must always be locked first to avoid deadlocks.  Logging
   * the contents of this map (or calling toString) requires copying it first.
   * <p>
   * This isn't a {@link ConcurrentHashMap} because we don't use it frequently
   * (just when connecting to / disconnecting from RegionServers) and when we
   * add something to it, we want to do an atomic get-and-put, but
   * {@code putIfAbsent} isn't a good fit for us since it requires to create
   * an object that may be "wasted" in case another thread wins the insertion
   * race, and we don't want to create unnecessary connections.
   * <p>
   * Upon disconnection, clients are automatically removed from this map.
   * We don't use a {@code ChannelGroup} because a {@code ChannelGroup} does
   * the clean-up on the {@code channelClosed} event, which is actually the
   * 3rd and last event to be fired when a channel gets disconnected.  The
   * first one to get fired is, {@code channelDisconnected}.  This matters to
   * us because we want to purge disconnected clients from the cache as
   * quickly as possible after the disconnection, to avoid handing out clients
   * that are going to cause unnecessary errors.
   * @see RegionClientPipeline#handleDisconnect
   */
  private final HashMap<String, RegionClient> ip2client =
    new HashMap<String, RegionClient>();

  /**
   * Map of region name to list of pending RPCs for this region.
   * <p>
   * The array-list isn't expected to be empty, except during rare race
   * conditions.  When the list is non-empty, the first element in the
   * list should be a special "probe" RPC we build to detect when the
   * region NSRE'd is back online.
   * <p>
   * For more details on how this map is used, please refer to the
   * documentation of {@link #handleNSRE}.
   * <p>
   * Each list in the map is protected by its own monitor lock.
   */
  private final ConcurrentSkipListMap<byte[], ArrayList<HBaseRpc>> got_nsre =
    new ConcurrentSkipListMap<byte[], ArrayList<HBaseRpc>>(RegionInfo.REGION_NAME_CMP);

  /**
   * Buffer for atomic increment coalescing.
   * This buffer starts out null, and remains so until the first time we need
   * to buffer an increment.  Once lazily initialized, this buffer will never
   * become null again.
   * <p>
   * We do this so that we can lazily schedule the flush timer only if we ever
   * have buffered increments.  Applications without buffered increments don't
   * need to pay any memory for the buffer or any CPU time for a useless timer.
   * @see #setupIncrementCoalescing
   */
  private volatile LoadingCache<BufferedIncrement, BufferedIncrement.Amount> increment_buffer;

  // ------------------------ //
  // Client usage statistics. //
  // ------------------------ //

  /** Number of connections created by {@link #newClient}.  */
  private final Counter num_connections_created = new Counter();

  /** How many {@code -ROOT-} lookups were made.  */
  private final Counter root_lookups = new Counter();

  /** How many {@code .META.} lookups were made (with a permit).  */
  private final Counter meta_lookups_with_permit = new Counter();

  /** How many {@code .META.} lookups were made (without a permit).  */
  private final Counter meta_lookups_wo_permit = new Counter();

  /** Number of calls to {@link #flush}.  */
  private final Counter num_flushes = new Counter();

  /** Number of NSREs handled by {@link #handleNSRE}.  */
  private final Counter num_nsres = new Counter();

  /** Number of RPCs delayed by {@link #handleNSRE}.  */
  private final Counter num_nsre_rpcs = new Counter();

  /** Number of {@link MultiAction} sent to the network.  */
  final Counter num_multi_rpcs = new Counter();

  /** Number of calls to {@link #get}.  */
  private final Counter num_gets = new Counter();

  /** Number of calls to {@link #openScanner}.  */
  private final Counter num_scanners_opened = new Counter();

  /** Number of calls to {@link #scanNextRows}.  */
  private final Counter num_scans = new Counter();

  /** Number calls to {@link #put}.  */
  private final Counter num_puts = new Counter();

  /** Number calls to {@link #lockRow}.  */
  private final Counter num_row_locks = new Counter();

  /** Number calls to {@link #delete}.  */
  private final Counter num_deletes = new Counter();

  /** Number of {@link AtomicIncrementRequest} sent.  */
  private final Counter num_atomic_increments = new Counter();

  /**
   * Constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   */
  public HBaseClient(final String quorum_spec) {
    this(quorum_spec, "/hbase");
  }

  /**
   * Constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   */
  public HBaseClient(final String quorum_spec, final String base_path) {
    this(quorum_spec, base_path, defaultChannelFactory());
  }

  /** Creates a default channel factory in case we haven't been given one.  */
  private static NioClientSocketChannelFactory defaultChannelFactory() {
    final Executor executor = Executors.newCachedThreadPool();
    return new NioClientSocketChannelFactory(executor, executor);
  }

  /**
   * Constructor for advanced users with special needs.
   * <p>
   * <strong>NOTE:</strong> Only advanced users who really know what they're
   * doing should use this constructor.  Passing an inappropriate thread
   * pool, or blocking its threads will prevent this {@code HBaseClient}
   * from working properly or lead to poor performance.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   * @param executor The executor from which to obtain threads for NIO
   * operations.  It is <strong>strongly</strong> encouraged to use a
   * {@link Executors#newCachedThreadPool} or something equivalent unless
   * you're sure to understand how Netty creates and uses threads.
   * Using a fixed-size thread pool will not work the way you expect.
   * <p>
   * Note that calling {@link #shutdown} on this client will <b>NOT</b>
   * shut down the executor.
   * @see NioClientSocketChannelFactory
   * @since 1.2
   */
  public HBaseClient(final String quorum_spec, final String base_path,
                     final Executor executor) {
    this(quorum_spec, base_path, new CustomChannelFactory(executor));
  }

  /** A custom channel factory that doesn't shutdown its executor.  */
  private static final class CustomChannelFactory
    extends NioClientSocketChannelFactory {
      CustomChannelFactory(final Executor executor) {
        super(executor, executor);
      }
      @Override
      public void releaseExternalResources() {
        // Do nothing, we don't want to shut down the executor.
      }
  }

  /**
   * Constructor for advanced users with special needs.
   * <p>
   * Most users don't need to use this constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   * @param channel_factory A custom factory to use to create sockets.
   * <p>
   * Note that calling {@link #shutdown} on this client will also cause the
   * shutdown and release of the factory and its underlying thread pool.
   * @since 1.2
   */
  public HBaseClient(final String quorum_spec, final String base_path,
                     final ClientSocketChannelFactory channel_factory) {
    this.channel_factory = channel_factory;
    zkclient = new ZKClient(quorum_spec, base_path);
  }

  /**
   * Returns a snapshot of usage statistics for this client.
   * @since 1.3
   */
  public ClientStats stats() {
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> cache =
      increment_buffer;
    return new ClientStats(
      num_connections_created.get(),
      root_lookups.get(),
      meta_lookups_with_permit.get(),
      meta_lookups_wo_permit.get(),
      num_flushes.get(),
      num_nsres.get(),
      num_nsre_rpcs.get(),
      num_multi_rpcs.get(),
      num_gets.get(),
      num_scanners_opened.get(),
      num_scans.get(),
      num_puts.get(),
      num_row_locks.get(),
      num_deletes.get(),
      num_atomic_increments.get(),
      cache != null ? cache.stats() : BufferedIncrement.ZERO_STATS
    );
  }

  /**
   * Flushes to HBase any buffered client-side write operation.
   * <p>
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   * <p>
   * Note that this doesn't guarantee that <b>ALL</b> outstanding RPCs have
   * completed.  This doesn't introduce any sort of global sync point.  All
   * it does really is it sends any buffered RPCs to HBase.
   */
  public Deferred<Object> flush() {
    {
      // If some RPCs are waiting for -ROOT- to be discovered, we too must wait
      // because some of those RPCs could be edits that we must wait on.
      final Deferred<Object> d = zkclient.getDeferredRootIfBeingLookedUp();
      if (d != null) {
        LOG.debug("Flush needs to wait on {} to come back",
                  has_root ? "-ROOT-" : ".META.");
        final class RetryFlush implements Callback<Object, Object> {
          public Object call(final Object arg) {
            LOG.debug("Flush retrying after {} came back",
                      has_root ? "-ROOT-" : ".META.");
            return flush();
          }
          public String toString() {
            return "retry flush";
          }
        }
        return d.addBoth(new RetryFlush());
      }
    }

    num_flushes.increment();
    final boolean need_sync;
    {
      final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> buf =
        increment_buffer;  // Single volatile-read.
      if (buf != null && !buf.asMap().isEmpty()) {
        flushBufferedIncrements(buf);
        need_sync = true;
      } else {
        need_sync = false;
      }
    }
    final ArrayList<Deferred<Object>> d =
      new ArrayList<Deferred<Object>>(client2regions.size()
                                      + got_nsre.size() * 8);
    // Bear in mind that we're traversing a ConcurrentHashMap, so we may get
    // clients that have been removed from the map since we started iterating.
    for (final RegionClient client : client2regions.keySet()) {
      d.add(need_sync ? client.sync() : client.flush());
    }
    for (final ArrayList<HBaseRpc> nsred : got_nsre.values()) {
      synchronized (nsred) {
        for (final HBaseRpc rpc : nsred) {
          if (rpc instanceof HBaseRpc.IsEdit) {
            d.add(rpc.getDeferred());
          }
        }
      }
    }
    @SuppressWarnings("unchecked")
    final Deferred<Object> flushed = (Deferred) Deferred.group(d);
    return flushed;
  }

  /**
   * Sets the maximum time (in milliseconds) for which edits can be buffered.
   * <p>
   * This interval will be honored on a "best-effort" basis.  Edits can be
   * buffered for longer than that due to GC pauses, the resolution of the
   * underlying timer, thread scheduling at the OS level (particularly if the
   * OS is overloaded with concurrent requests for CPU time), any low-level
   * buffering in the TCP/IP stack of the OS, etc.
   * <p>
   * Setting a longer interval allows the code to batch requests more
   * efficiently but puts you at risk of greater data loss if the JVM
   * or machine was to fail.  It also entails that some edits will not
   * reach HBase until a longer period of time, which can be troublesome
   * if you have other applications that need to read the "latest" changes.
   * <p>
   * Setting this interval to 0 disables this feature.
   * <p>
   * The change is guaranteed to take effect at most after a full interval
   * has elapsed, <i>using the previous interval</i> (which is returned).
   * @param flush_interval A positive time interval in milliseconds.
   * @return The previous flush interval.
   * @throws IllegalArgumentException if {@code flush_interval < 0}.
   */
  public short setFlushInterval(final short flush_interval) {
    // Note: if we have buffered increments, they'll pick up the new flush
    // interval next time the current timer fires.
    if (flush_interval < 0) {
      throw new IllegalArgumentException("Negative: " + flush_interval);
    }
    final short prev = this.flush_interval;
    this.flush_interval = flush_interval;
    return prev;
  }

  /**
   * Changes the size of the increment buffer.
   * <p>
   * <b>NOTE:</b> because there is no way to resize the existing buffer,
   * this method will flush the existing buffer and create a new one.
   * This side effect might be unexpected but is unfortunately required.
   * <p>
   * This determines the maximum number of counters this client will keep
   * in-memory to allow increment coalescing through
   * {@link #bufferAtomicIncrement}.
   * <p>
   * The greater this number, the more memory will be used to buffer
   * increments, and the more efficient increment coalescing can be
   * if you have a high-throughput application with a large working
   * set of counters.
   * <p>
   * If your application has excessively large keys or qualifiers, you might
   * consider using a lower number in order to reduce memory usage.
   * @param increment_buffer_size The new size of the buffer.
   * @return The previous size of the buffer.
   * @throws IllegalArgumentException if {@code increment_buffer_size < 0}.
   * @since 1.3
   */
  public int setIncrementBufferSize(final int increment_buffer_size) {
    if (increment_buffer_size < 0) {
      throw new IllegalArgumentException("Negative: " + increment_buffer_size);
    }
    final int current = this.increment_buffer_size;
    if (current == increment_buffer_size) {
      return current;
    }
    this.increment_buffer_size = increment_buffer_size;
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> prev =
      increment_buffer;  // Volatile-read.
    if (prev != null) {  // Need to resize.
      makeIncrementBuffer();  // Volatile-write.
      flushBufferedIncrements(prev);
    }
    return current;
  }

  /**
   * Returns the timer used by this client.
   * <p>
   * All timeouts, retries and other things that need to "sleep
   * asynchronously" use this timer.  This method is provided so
   * that you can also schedule your own timeouts using this timer,
   * if you wish to share this client's timer instead of creating
   * your own.
   * <p>
   * The precision of this timer is implementation-defined but is
   * guaranteed to be no greater than 20ms.
   * @since 1.2
   */
  public Timer getTimer() {
    return timer;
  }

  /**
   * Schedules a new timeout.
   * @param task The task to execute when the timer times out.
   * @param timeout_ms The timeout, in milliseconds (strictly positive).
   */
  void newTimeout(final TimerTask task, final long timeout_ms) {
    try {
      timer.newTimeout(task, timeout_ms, MILLISECONDS);
    } catch (IllegalStateException e) {
      // This can happen if the timer fires just before shutdown()
      // is called from another thread, and due to how threads get
      // scheduled we tried to call newTimeout() after timer.stop().
      LOG.warn("Failed to schedule timer."
               + "  Ignore this if we're shutting down.", e);
    }
  }

  /**
   * Returns the maximum time (in milliseconds) for which edits can be buffered.
   * <p>
   * The default value is an unspecified and implementation dependant, but is
   * guaranteed to be non-zero.
   * <p>
   * A return value of 0 indicates that edits are sent directly to HBase
   * without being buffered.
   * @see #setFlushInterval
   */
  public short getFlushInterval() {
    return flush_interval;
  }

  /**
   * Returns the capacity of the increment buffer.
   * <p>
   * Note this returns the <em>capacity</em> of the buffer, not the number of
   * items currently in it.  There is currently no API to get the current
   * number of items in it.
   * @since 1.3
   */
  public int getIncrementBufferSize() {
    return increment_buffer_size;
  }

  /**
   * Performs a graceful shutdown of this instance.
   * <p>
   * <ul>
   *   <li>{@link #flush Flushes} all buffered edits.</li>
   *   <li>Completes all outstanding requests.</li>
   *   <li>Terminates all connections.</li>
   *   <li>Releases all other resources.</li>
   * </ul>
   * <strong>Not calling this method before losing the last reference to this
   * instance may result in data loss and other unwanted side effects</strong>
   * @return A {@link Deferred}, whose callback chain will be invoked once all
   * of the above have been done.  If this callback chain doesn't fail, then
   * the clean shutdown will be successful, and all the data will be safe on
   * the HBase side (provided that you use <a href="#durability">durable</a>
   * edits).  In case of a failure (the "errback" is invoked) you may want to
   * retry the shutdown to avoid losing data, depending on the nature of the
   * failure.  TODO(tsuna): Document possible / common failure scenarios.
   */
  public Deferred<Object> shutdown() {
    // This is part of step 3.  We need to execute this in its own thread
    // because Netty gets stuck in an infinite loop if you try to shut it
    // down from within a thread of its own thread pool.  They don't want
    // to fix this so as a workaround we always shut Netty's thread pool
    // down from another thread.
    final class ShutdownThread extends Thread {
      ShutdownThread() {
        super("HBaseClient@" + HBaseClient.super.hashCode() + " shutdown");
      }
      public void run() {
        // This terminates the Executor.
        channel_factory.releaseExternalResources();
      }
    };

    // 3. Release all other resources.
    final class ReleaseResourcesCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
        LOG.debug("Releasing all remaining resources");
        timer.stop();
        new ShutdownThread().start();
        return arg;
      }
      public String toString() {
        return "release resources callback";
      }
    }

    // 2. Terminate all connections.
    final class DisconnectCB implements Callback<Object, Object> {
      public Object call(final Object arg) {
        return disconnectEverything().addCallback(new ReleaseResourcesCB());
      }
      public String toString() {
        return "disconnect callback";
      }
    }

    // If some RPCs are waiting for -ROOT- to be discovered, we too must wait
    // because some of those RPCs could be edits that we must not lose.
    final Deferred<Object> d = zkclient.getDeferredRootIfBeingLookedUp();
    if (d != null) {
      LOG.debug("Shutdown needs to wait on {} to come back",
                has_root ? "-ROOT-" : ".META.");
      final class RetryShutdown implements Callback<Object, Object> {
        public Object call(final Object arg) {
          LOG.debug("Shutdown retrying after {} came back",
                    has_root ? "-ROOT-" : ".META.");
          return shutdown();
        }
        public String toString() {
          return "retry shutdown";
        }
      }
      return d.addBoth(new RetryShutdown());
    }

    // 1. Flush everything.
    return flush().addCallback(new DisconnectCB());
  }

  /**
   * Closes every socket, which will also flush all internal region caches.
   */
  private Deferred<Object> disconnectEverything() {
    HashMap<String, RegionClient> ip2client_copy;

    synchronized (ip2client) {
      // Make a local copy so we can shutdown every Region Server client
      // without hold the lock while we iterate over the data structure.
      ip2client_copy = new HashMap<String, RegionClient>(ip2client);
    }

    final ArrayList<Deferred<Object>> d =
      new ArrayList<Deferred<Object>>(ip2client_copy.values().size() + 1);
    // Shut down all client connections, clear cache.
    for (final RegionClient client : ip2client_copy.values()) {
      d.add(client.shutdown());
    }
    if (rootregion != null && rootregion.isAlive()) {
      // It's OK if we already did that in the loop above.
      d.add(rootregion.shutdown());
    }
    ip2client_copy = null;

    final int size = d.size();
    return Deferred.group(d).addCallback(
      new Callback<Object, ArrayList<Object>>() {
        public Object call(final ArrayList<Object> arg) {
          // Normally, now that we've shutdown() every client, all our caches should
          // be empty since each shutdown() generates a DISCONNECTED event, which
          // causes RegionClientPipeline to call removeClientFromCache().
          HashMap<String, RegionClient> logme = null;
          synchronized (ip2client) {
            if (!ip2client.isEmpty()) {
              logme = new HashMap<String, RegionClient>(ip2client);
            }
          }
          if (logme != null) {
            // Putting this logging statement inside the synchronized block
            // can lead to a deadlock, since HashMap.toString() is going to
            // call RegionClient.toString() on each entry, and this locks the
            // client briefly.  Other parts of the code lock clients first and
            // the ip2client HashMap second, so this can easily deadlock.
            LOG.error("Some clients are left in the client cache and haven't"
                      + " been cleaned up: " + logme);
            logme = null;
            return disconnectEverything();  // Try again.
          }
          zkclient.disconnectZK();
          return arg;
        }
        public String toString() {
          return "wait " + size + " RegionClient.shutdown()";
        }
      });
  }

  /**
   * Ensures that a given table/family pair really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * Both strings are assumed to use the platform's default charset.
   * @param table The name of the table you intend to use.
   * @param family The column family you intend to use in that table.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   * @throws NoSuchColumnFamilyException (deferred) if the family doesn't exist.
   */
  public Deferred<Object> ensureTableFamilyExists(final String table,
                                                  final String family) {
    return ensureTableFamilyExists(table.getBytes(), family.getBytes());
  }

  /**
   * Ensures that a given table/family pair really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * @param family The column family you intend to use in that table.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   * @throws NoSuchColumnFamilyException (deferred) if the family doesn't exist.
   */
  public Deferred<Object> ensureTableFamilyExists(final byte[] table,
                                                  final byte[] family) {
    // Just "fault in" the first region of the table.  Not the most optimal or
    // useful thing to do but gets the job done for now.  TODO(tsuna): Improve.
    final HBaseRpc dummy;
    if (family == EMPTY_ARRAY) {
      dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY));
    } else {
      dummy = GetRequest.exists(table, probeKey(ZERO_ARRAY), family);
    }
    @SuppressWarnings("unchecked")
    final Deferred<Object> d = (Deferred) sendRpcToRegion(dummy);
    return d;
  }

  /**
   * Ensures that a given table really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * The string is assumed to use the platform's default charset.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   */
  public Deferred<Object> ensureTableExists(final String table) {
    return ensureTableFamilyExists(table.getBytes(), EMPTY_ARRAY);
  }

  /**
   * Ensures that a given table really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   */
  public Deferred<Object> ensureTableExists(final byte[] table) {
    return ensureTableFamilyExists(table, EMPTY_ARRAY);
  }

  /**
   * Retrieves data from HBase.
   * @param request The {@code get} request.
   * @return A deferred list of key-values that matched the get request.
   */
  public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
    num_gets.increment();
    return sendRpcToRegion(request).addCallbacks(got, Callback.PASSTHROUGH);
  }

  /** Singleton callback to handle responses of "get" RPCs.  */
  private static final Callback<ArrayList<KeyValue>, Object> got =
    new Callback<ArrayList<KeyValue>, Object>() {
      public ArrayList<KeyValue> call(final Object response) {
        if (response instanceof ArrayList) {
          @SuppressWarnings("unchecked")
          final ArrayList<KeyValue> row = (ArrayList<KeyValue>) response;
          return row;
        } else {
          throw new InvalidResponseException(ArrayList.class, response);
        }
      }
      public String toString() {
        return "type get response";
      }
    };

  /**
   * Creates a new {@link Scanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * @return A new scanner for this table.
   */
  public Scanner newScanner(final byte[] table) {
    return new Scanner(this, table);
  }

  /**
   * Creates a new {@link Scanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return A new scanner for this table.
   */
  public Scanner newScanner(final String table) {
    return new Scanner(this, table.getBytes());
  }

  /**
   * Package-private access point for {@link Scanner}s to open themselves.
   * @param scanner The scanner to open.
   * @return A deferred scanner ID (long) if HBase 0.94 and before, or a
   * deferred {@link Scanner.Response} if HBase 0.95 and up.
   */
  Deferred<Object> openScanner(final Scanner scanner) {
    num_scanners_opened.increment();
    return sendRpcToRegion(scanner.getOpenRequest()).addCallbacks(
      scanner_opened,
      new Callback<Object, Object>() {
        public Object call(final Object error) {
          // Don't let the scanner think it's opened on this region.
          scanner.invalidate();
          return error;  // Let the error propagate.
        }
        public String toString() {
          return "openScanner errback";
        }
      });
  }

  /** Singleton callback to handle responses of "openScanner" RPCs.  */
  private static final Callback<Object, Object> scanner_opened =
    new Callback<Object, Object>() {
      public Object call(final Object response) {
        if (response instanceof Scanner.Response) {  // HBase 0.95 and up
          return (Scanner.Response) response;
        } else if (response instanceof Long) {
          // HBase 0.94 and before: we expect just a long (the scanner ID).
          return (Long) response;
        } else {
          throw new InvalidResponseException(Long.class, response);
        }
      }
      public String toString() {
        return "type openScanner response";
      }
    };

  /**
   * Returns the client currently known to hose the given region, or NULL.
   */
  private RegionClient clientFor(final RegionInfo region) {
    if (region == null) {
      return null;
    } else if (region == META_REGION || Bytes.equals(region.table(), ROOT)) {
      // HBase 0.95+: META_REGION (which is 0.95 specific) is our root.
      // HBase 0.94 and earlier: if we're looking for -ROOT-, stop here.
      return rootregion;
    }
    return region2client.get(region);
  }

  /**
   * Package-private access point for {@link Scanner}s to scan more rows.
   * @param scanner The scanner to use.
   * @param nrows The maximum number of rows to retrieve.
   * @return A deferred row.
   */
  Deferred<Object> scanNextRows(final Scanner scanner) {
    final RegionInfo region = scanner.currentRegion();
    final RegionClient client = clientFor(region);
    if (client == null) {
      // Oops, we no longer know anything about this client or region.  Our
      // cache was probably invalidated while the client was scanning.  This
      // means that we lost the connection to that RegionServer, so we have to
      // re-open this scanner if we wanna keep scanning.
      scanner.invalidate();        // Invalidate the scanner so that ...
      @SuppressWarnings("unchecked")
      final Deferred<Object> d = (Deferred) scanner.nextRows();
      return d;  // ... this will re-open it ______.^
    }
    num_scans.increment();
    final HBaseRpc next_request = scanner.getNextRowsRequest();
    final Deferred<Object> d = next_request.getDeferred();
    client.sendRpc(next_request);
    return d;
  }

  /**
   * Package-private access point for {@link Scanner}s to close themselves.
   * @param scanner The scanner to close.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
   */
  Deferred<Object> closeScanner(final Scanner scanner) {
    final RegionInfo region = scanner.currentRegion();
    final RegionClient client = clientFor(region);
    if (client == null) {
      // Oops, we no longer know anything about this client or region.  Our
      // cache was probably invalidated while the client was scanning.  So
      // we can't close this scanner properly.
      LOG.warn("Cannot close " + scanner + " properly, no connection open for "
               + Bytes.pretty(region == null ? null : region.name()));
      return Deferred.fromResult(null);
    }
    final HBaseRpc close_request = scanner.getCloseRequest();
    final Deferred<Object> d = close_request.getDeferred();
    client.sendRpc(close_request);
    return d;
  }

  /**
   * Atomically and durably increments a value in HBase.
   * <p>
   * This is equivalent to
   * {@link #atomicIncrement(AtomicIncrementRequest, boolean) atomicIncrement}
   * {@code (request, true)}
   * @param request The increment request.
   * @return The deferred {@code long} value that results from the increment.
   */
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request) {
    num_atomic_increments.increment();
    return sendRpcToRegion(request).addCallbacks(icv_done,
                                                 Callback.PASSTHROUGH);
  }

  /**
   * Buffers a durable atomic increment for coalescing.
   * <p>
   * This increment will be held in memory up to the amount of time allowed
   * by {@link #getFlushInterval} in order to allow the client to coalesce
   * increments.
   * <p>
   * Increment coalescing can dramatically reduce the number of RPCs and write
   * load on HBase if you tend to increment multiple times the same working
   * set of counters.  This is very common in user-facing serving systems that
   * use HBase counters to keep track of user actions.
   * <p>
   * If client-side buffering is disabled ({@link #getFlushInterval} returns
   * 0) then this function has the same effect as calling
   * {@link #atomicIncrement(AtomicIncrementRequest)} directly.
   * @param request The increment request.
   * @return The deferred {@code long} value that results from the increment.
   * @since 1.3
   * @since 1.4 This method works with negative increment values.
   */
  public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request) {
    final long value = request.getAmount();
    if (!BufferedIncrement.Amount.checkOverflow(value)  // Value too large.
        || flush_interval == 0) {           // Client-side buffer disabled.
      return atomicIncrement(request);
    }

    final BufferedIncrement incr =
      new BufferedIncrement(request.table(), request.key(), request.family(),
                            request.qualifier());

    do {
      BufferedIncrement.Amount amount;
      // Semi-evil: the very first time we get here, `increment_buffer' will
      // still be null (we don't initialize it in our constructor) so we catch
      // the NPE that ensues to allocate the buffer and kick off a timer to
      // regularly flush it.
      try {
        amount = increment_buffer.getUnchecked(incr);
      } catch (NullPointerException e) {
        setupIncrementCoalescing();
        amount = increment_buffer.getUnchecked(incr);
      }
      if (amount.update(value)) {
        final Deferred<Long> deferred = new Deferred<Long>();
        amount.deferred.chain(deferred);
        return deferred;
      }
      // else: Loop again to retry.
      increment_buffer.refresh(incr);
    } while (true);
  }

  /**
   * Called the first time we get a buffered increment.
   * Lazily creates the increment buffer and sets up a timer to regularly
   * flush buffered increments.
   */
  private synchronized void setupIncrementCoalescing() {
    // If multiple threads attempt to setup coalescing at the same time, the
    // first one to get here will make `increment_buffer' non-null, and thus
    // subsequent ones will return immediately.  This is important to avoid
    // creating more than one FlushBufferedIncrementsTimer below.
    if (increment_buffer != null) {
      return;
    }
    makeIncrementBuffer();  // Volatile-write.

    // Start periodic buffered increment flushes.
    final class FlushBufferedIncrementsTimer implements TimerTask {
      public void run(final Timeout timeout) {
        try {
          flushBufferedIncrements(increment_buffer);
        } finally {
          final short interval = flush_interval; // Volatile-read.
          // Even if we paused or disabled the client side buffer by calling
          // setFlushInterval(0), we will continue to schedule this timer
          // forever instead of pausing it.  Pausing it is troublesome because
          // we don't keep a reference to this timer, so we can't cancel it or
          // tell if it's running or not.  So let's just KISS and assume that
          // if we need the timer once, we'll need it forever.  If it's truly
          // not needed anymore, we'll just cause a bit of extra work to the
          // timer thread every 100ms, no big deal.
          newTimeout(this, interval > 0 ? interval : 100);
        }
      }
    }
    final short interval = flush_interval; // Volatile-read.
    // Handle the extremely unlikely yet possible racy case where:
    //   flush_interval was > 0
    //   A buffered increment came in
    //   It was the first one ever so we landed here
    //   Meanwhile setFlushInterval(0) to disable buffering
    // In which case we just flush whatever we have in 1ms.
    timer.newTimeout(new FlushBufferedIncrementsTimer(),
                     interval > 0 ? interval : 1, MILLISECONDS);
  }

  /**
   * Flushes all buffered increments.
   * @param increment_buffer The buffer to flush.
   */
  private static void flushBufferedIncrements(// JAVA Y U NO HAVE TYPEDEF? F U!
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> increment_buffer) {
    // Calling this method to clean up before shutting down works solely
    // because `invalidateAll()' will *synchronously* remove everything.
    // The Guava documentation says "Discards all entries in the cache,
    // possibly asynchronously" but in practice the code in `LocalCache'
    // works as follows:
    //
    //   for each segment:
    //     segment.clear
    //
    // Where clearing a segment consists in:
    //
    //   lock the segment
    //   for each active entry:
    //     add entry to removal queue
    //   null out the hash table
    //   unlock the segment
    //   for each entry in removal queue:
    //     call the removal listener on that entry
    //
    // So by the time the call to `invalidateAll()' returns, every single
    // buffered increment will have been dealt with, and it is thus safe
    // to shutdown the rest of the client to let it complete all outstanding
    // operations.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flushing " + increment_buffer.size() + " buffered increments");
    }
    synchronized (increment_buffer) {
      increment_buffer.invalidateAll();
    }
  }

  /**
   * Creates the increment buffer according to current configuration.
   */
  private void makeIncrementBuffer() {
    final int size = increment_buffer_size;
    increment_buffer = BufferedIncrement.newCache(this, size);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created increment buffer of " + size + " entries");
    }
  }

  /** Singleton callback to handle responses of incrementColumnValue RPCs.  */
  private static final Callback<Long, Object> icv_done =
    new Callback<Long, Object>() {
      public Long call(final Object response) {
        if (response instanceof Long) {
          return (Long) response;
        } else {
          throw new InvalidResponseException(Long.class, response);
        }
      }
      public String toString() {
        return "type incrementColumnValue response";
      }
    };

  /**
   * Atomically increments a value in HBase.
   * @param request The increment request.
   * @param durable If {@code true}, the success of this RPC guarantees that
   * HBase has stored the edit in a <a href="#durability">durable</a> fashion.
   * When in doubt, use {@link #atomicIncrement(AtomicIncrementRequest)}.
   * @return The deferred {@code long} value that results from the increment.
   */
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request,
                                        final boolean durable) {
    request.setDurable(durable);
    return atomicIncrement(request);
  }

  /**
   * Stores data in HBase.
   * <p>
   * Note that this provides no guarantee as to the order in which subsequent
   * {@code put} requests are going to be applied to the backend.  If you need
   * ordering, you must enforce it manually yourself by starting the next
   * {@code put} once the {@link Deferred} of this one completes successfully.
   * @param request The {@code put} request.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * TODO(tsuna): Document failures clients are expected to handle themselves.
   */
  public Deferred<Object> put(final PutRequest request) {
    num_puts.increment();
    return sendRpcToRegion(request);
  }

  /**
   * Atomic Compare-And-Set (CAS) on a single cell.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>, and
   * won't be subject to the {@link #setFlushInterval flush interval}.  This
   * entails that write throughput will be lower with this method as edits
   * have to be sent out to the wire one by one.
   * <p>
   * This request enables you to atomically update the value of an existing
   * cell in HBase using a CAS operation.  It's like a {@link PutRequest}
   * except that you also pass an expected value.  If the last version of the
   * cell identified by your {@code PutRequest} matches the expected value,
   * HBase will atomically update it to the new value.
   * <p>
   * If the expected value is the empty byte array, HBase will atomically
   * create the cell provided that it doesn't exist already. This can be used
   * to ensure that your RPC doesn't overwrite an existing value.  Note
   * however that this trick cannot be used the other way around to delete
   * an expected value atomically.
   * @param edit The new value to write.
   * @param expected The expected value of the cell to compare against.
   * <strong>This byte array will NOT be copied.</strong>
   * @return A deferred boolean, if {@code true} the CAS succeeded, otherwise
   * the CAS failed because the value in HBase didn't match the expected value
   * of the CAS request.
   * @since 1.3
   */
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                         final byte[] expected) {
    return sendRpcToRegion(new CompareAndSetRequest(edit, expected))
      .addCallback(CAS_CB);
  }

  /**
   * Atomic Compare-And-Set (CAS) on a single cell.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>.
   * @see #compareAndSet(PutRequest, byte[])
   * @param edit The new value to write.
   * @param expected The expected value of the cell to compare against.
   * This string is assumed to use the platform's default charset.
   * @return A deferred boolean, if {@code true} the CAS succeeded, otherwise
   * the CAS failed because the value in HBase didn't match the expected value
   * of the CAS request.
   * @since 1.3
   */
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                         final String expected) {
    return compareAndSet(edit, expected.getBytes());
  }

  /**
   * Atomically insert a new cell in HBase.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>.
   * <p>
   * This is equivalent to calling
   * {@link #compareAndSet(PutRequest, byte[]) compareAndSet}{@code (edit,
   * EMPTY_ARRAY)}
   * @see #compareAndSet(PutRequest, byte[])
   * @param edit The new value to insert.
   * @return A deferred boolean, {@code true} if the edit got atomically
   * inserted in HBase, {@code false} if there was already a value in the
   * given cell.
   * @since 1.3
   */
  public Deferred<Boolean> atomicCreate(final PutRequest edit) {
    return compareAndSet(edit, EMPTY_ARRAY);
  }

  /** Callback to type-check responses of {@link CompareAndSetRequest}.  */
  private static final class CompareAndSetCB implements Callback<Boolean, Object> {

    public Boolean call(final Object response) {
      if (response instanceof Boolean) {
        return (Boolean)response;
      } else {
        throw new InvalidResponseException(Boolean.class, response);
      }
    }

    public String toString() {
      return "type compareAndSet response";
    }

  }

  /** Singleton callback for responses of {@link CompareAndSetRequest}.  */
  private static final CompareAndSetCB CAS_CB = new CompareAndSetCB();

  /**
   * Acquires an explicit row lock.
   * <p>
   * For a description of what row locks are, see {@link RowLock}.
   * @param request The request specify which row to lock.
   * @return a deferred {@link RowLock}.
   * @see #unlockRow
   */
  public Deferred<RowLock> lockRow(final RowLockRequest request) {
    num_row_locks.increment();
    return sendRpcToRegion(request).addCallbacks(
      new Callback<RowLock, Object>() {
        public RowLock call(final Object response) {
          if (response instanceof Long) {
            return new RowLock(request.getRegion().name(), (Long) response);
          } else {
            throw new InvalidResponseException(Long.class, response);
          }
        }
        public String toString() {
          return "type lockRow response";
        }
      }, Callback.PASSTHROUGH);
  }

  /**
   * Releases an explicit row lock.
   * <p>
   * For a description of what row locks are, see {@link RowLock}.
   * @param lock The lock to release.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public Deferred<Object> unlockRow(final RowLock lock) {
    final byte[] region_name = lock.region();
    final RegionInfo region = regions_cache.get(region_name);
    if (knownToBeNSREd(region)) {
      // If this region has been NSRE'd, we can't possibly still hold a lock
      // on one of its rows, as this would have prevented it from splitting.
      // So let's just pretend the row has been unlocked.
      return Deferred.fromResult(null);
    }
    final RegionClient client = clientFor(region);
    if (client == null) {
      // Oops, we no longer know anything about this client or region.  Our
      // cache was probably invalidated while the client was holding the lock.
      LOG.warn("Cannot release " + lock + ", no connection open for "
               + Bytes.pretty(region_name));
      return Deferred.fromResult(null);
    }
    final HBaseRpc release = new RowLockRequest.ReleaseRequest(lock, region);
    release.setRegion(region);
    final Deferred<Object> d = release.getDeferred();
    client.sendRpc(release);
    return d;
  }

  /**
   * Deletes data from HBase.
   * @param request The {@code delete} request.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   */
  public Deferred<Object> delete(final DeleteRequest request) {
    num_deletes.increment();
    return sendRpcToRegion(request);
  }

  /**
   * Eagerly prefetches and caches a table's region metadata from HBase.
   * @param table The name of the table whose metadata you intend to prefetch.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has no special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @since 1.5
   */
  public Deferred<Object> prefetchMeta(final String table) {
    return prefetchMeta(table.getBytes(), EMPTY_ARRAY, EMPTY_ARRAY);
  }

  /**
   * Eagerly prefetches and caches part of a table's region metadata from HBase.
   * <p>
   * The part to prefetch is identified by a row key range, given by
   * {@code start} and {@code stop}.
   * @param table The name of the table whose metadata you intend to prefetch.
   * @param start The start of the row key range to prefetch metadata for.
   * @param stop The end of the row key range to prefetch metadata for.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has no special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @since 1.5
   */
  public Deferred<Object> prefetchMeta(final String table,
                                       final String start,
                                       final String stop) {
    return prefetchMeta(table.getBytes(), start.getBytes(), stop.getBytes());
  }

  /**
   * Eagerly prefetches and caches a table's region metadata from HBase.
   * @param table The name of the table whose metadata you intend to prefetch.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has no special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @since 1.5
   */
  public Deferred<Object> prefetchMeta(final byte[] table) {
    return prefetchMeta(table, EMPTY_ARRAY, EMPTY_ARRAY);
  }

  /**
   * Eagerly prefetches and caches part of a table's region metadata from HBase.
   * <p>
   * The part to prefetch is identified by a row key range, given by
   * {@code start} and {@code stop}.
   * @param table The name of the table whose metadata you intend to prefetch.
   * @param start The start of the row key range to prefetch metadata for.
   * @param stop The end of the row key range to prefetch metadata for.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has no special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @since 1.5
   */
  public Deferred<Object> prefetchMeta(final byte[] table,
                                       final byte[] start,
                                       final byte[] stop) {
    // We're going to scan .META. for the table between the row keys and filter
    // out all but the latest entries on the client side.  Whatever remains
    // will be inserted into the region cache.

    // But we don't want to do this for .META. or -ROOT-.
    if (Bytes.equals(table, META) || Bytes.equals(table, ROOT)) {
      return Deferred.fromResult(null);
    }

    // Create the scan bounds.
    final byte[] meta_start = createRegionSearchKey(table, start);
    // In this case, we want the scan to start immediately at the
    // first entry, but createRegionSearchKey finds the last entry.
    meta_start[meta_start.length - 1] = 0;

    // The stop bound is trickier.  If the user wants the whole table,
    // expressed by passing EMPTY_ARRAY, then we need to append a null
    // byte to the table name (thus catching all rows in the desired
    // table, but excluding those from others.)  If the user specifies
    // an explicit stop key, we must leave the table name alone.
    final byte[] meta_stop;
    if (stop.length == 0) {
      meta_stop = createRegionSearchKey(table, stop); // will return "table,,:"
      meta_stop[table.length] = 0;  // now have "table\0,:"
      meta_stop[meta_stop.length - 1] = ',';  // now have "table\0,,"
    } else {
      meta_stop = createRegionSearchKey(table, stop);
    }

    if (rootregion == null) {
      // If we don't know where the root region is, we don't yet know whether
      // there is even a -ROOT- region at all (pre HBase 0.95).  So we can't
      // start scanning meta right away, because we don't yet know whether
      // meta is named ".META." or "hbase:meta".  So instead we first check
      // whether the table exists, which will force us to do a first meta
      // lookup (and therefore figure out what the name of meta is).
      class RetryPrefetch implements Callback<Object, Object> {
        public Object call(final Object unused) {
          return prefetchMeta(table, start, stop);
        }
        public String toString() {
          return "retry prefetchMeta(" + Bytes.pretty(table) + ", "
            + Bytes.pretty(start) + ", " + Bytes.pretty(stop) + ")";
        }
      }
      return ensureTableExists(table).addCallback(new RetryPrefetch());
    }

    final Scanner meta_scanner = newScanner(has_root ? META : HBASE96_META);
    meta_scanner.setStartKey(meta_start);
    meta_scanner.setStopKey(meta_stop);

    class PrefetchMeta
      implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
      public Object call(final ArrayList<ArrayList<KeyValue>> results) {
        if (results != null && !results.isEmpty()) {
          for (final ArrayList<KeyValue> row : results) {
            discoverRegion(row);
          }
          return meta_scanner.nextRows().addCallback(this);
        }
        return null;
      }

      public String toString() {
        return "prefetchMeta scanner=" + meta_scanner;
      }
    }

    return meta_scanner.nextRows().addCallback(new PrefetchMeta());
  }

  /**
   * Sends an RPC targeted at a particular region to the right RegionServer.
   * <p>
   * This method is package-private so that the low-level {@link RegionClient}
   * can retry RPCs when handling a {@link NotServingRegionException}.
   * @param request The RPC to send.  This RPC <b>must</b> specify a single
   * specific table and row key.
   * @return The deferred result of the RPC (whatever object or exception was
   * de-serialized back from the network).
   */
  Deferred<Object> sendRpcToRegion(final HBaseRpc request) {
    if (cannotRetryRequest(request)) {
      return tooManyAttempts(request, null);
    }
    request.attempt++;
    final byte[] table = request.table;
    final byte[] key = request.key;
    final RegionInfo region = getRegion(table, key);

    final class RetryRpc implements Callback<Deferred<Object>, Object> {
      public Deferred<Object> call(final Object arg) {
        if (arg instanceof NonRecoverableException) {
          // No point in retrying here, so fail the RPC.
          HBaseException e = (NonRecoverableException) arg;
          if (e instanceof HasFailedRpcException
              && ((HasFailedRpcException) e).getFailedRpc() != request) {
            // If we get here it's because a dependent RPC (such as a META
            // lookup) has failed.  Therefore the exception we're getting
            // indicates that the META lookup failed, but we need to return
            // to our caller here that it's their RPC that failed.  Here we
            // re-create the exception but with the correct RPC in argument.
            e = e.make(e, request);  // e is likely a PleaseThrottleException.
          }
          request.callback(e);
          return Deferred.fromError(e);
        }
        return sendRpcToRegion(request);  // Retry the RPC.
      }
      public String toString() {
        return "retry RPC";
      }
    }

    if (region != null) {
      if (knownToBeNSREd(region)) {
        final NotServingRegionException nsre =
          new NotServingRegionException("Region known to be unavailable",
                                        request);
        final Deferred<Object> d = request.getDeferred();
        handleNSRE(request, region.name(), nsre);
        return d;
      }
      final RegionClient client = clientFor(region);
      if (client != null && client.isAlive()) {
        request.setRegion(region);
        final Deferred<Object> d = request.getDeferred();
        client.sendRpc(request);
        return d;
      }
    }
    return locateRegion(table, key).addBothDeferring(new RetryRpc());
  }

  /**
   * Returns how many lookups in {@code -ROOT-} were performed.
   * <p>
   * This number should remain low.  It will be 1 after the first access to
   * HBase, and will increase by 1 each time the {@code .META.} region moves
   * to another server, which should seldom happen.
   * <p>
   * This isn't to be confused with the number of times we looked up where
   * the {@code -ROOT-} region itself is located.  This happens even more
   * rarely and a message is logged at the INFO whenever it does.
   * @since 1.1
   * @deprecated This method will be removed in release 2.0.  Use
   * {@link #stats}{@code .}{@link ClientStats#rootLookups rootLookups()}
   * instead.
   */
  @Deprecated
  public long rootLookupCount() {
    return root_lookups.get();
  }

  /**
   * Returns how many lookups in {@code .META.} were performed (uncontended).
   * <p>
   * This number indicates how many times we had to lookup in {@code .META.}
   * where a key was located.  This only counts "uncontended" lookups, where
   * the thread was able to acquire a "permit" to do a {@code .META.} lookup.
   * The majority of the {@code .META.} lookups should fall in this category.
   * @since 1.1
   * @deprecated This method will be removed in release 2.0.  Use
   * {@link #stats}{@code
   * .}{@link ClientStats#uncontendedMetaLookups uncontendedMetaLookups()}
   * instead.
   */
  @Deprecated
  public long uncontendedMetaLookupCount() {
    return meta_lookups_with_permit.get();
  }

  /**
   * Returns how many lookups in {@code .META.} were performed (contended).
   * <p>
   * This number indicates how many times we had to lookup in {@code .META.}
   * where a key was located.  This only counts "contended" lookups, where the
   * thread was unable to acquire a "permit" to do a {@code .META.} lookup,
   * because there were already too many {@code .META.} lookups in flight.
   * In this case, the thread was delayed a bit in order to apply a bit of
   * back-pressure on the caller, to avoid creating {@code .META.} storms.
   * The minority of the {@code .META.} lookups should fall in this category.
   * @since 1.1
   * @deprecated This method will be removed in release 2.0.  Use
   * {@link #stats}{@code
   * .}{@link ClientStats#contendedMetaLookups contendedMetaLookups()}
   * instead.
   */
  @Deprecated
  public long contendedMetaLookupCount() {
    return meta_lookups_wo_permit.get();
  }

  /**
   * Checks whether or not an RPC can be retried once more.
   * @param rpc The RPC we're going to attempt to execute.
   * @return {@code true} if this RPC already had too many attempts,
   * {@code false} otherwise (in which case it's OK to retry once more).
   * @throws NonRecoverableException if the request has had too many attempts
   * already.
   */
  static boolean cannotRetryRequest(final HBaseRpc rpc) {
    return rpc.attempt > MAX_RETRY_ATTEMPTS;
  }

  /**
   * Returns a {@link Deferred} containing an exception when an RPC couldn't
   * succeed after too many attempts.
   * @param request The RPC that was retried too many times.
   * @param cause What was cause of the last failed attempt, if known.
   * You can pass {@code null} if the cause is unknown.
   */
  static Deferred<Object> tooManyAttempts(final HBaseRpc request,
                                          final HBaseException cause) {
    // TODO(tsuna): At this point, it's possible that we have to deal with
    // a broken META table where there's a hole.  For the sake of good error
    // reporting, at this point we should try to getClosestRowBefore + scan
    // META in order to verify whether there's indeed a hole, and if there's
    // one, throw a BrokenMetaException explaining where the hole is.
    final Exception e = new NonRecoverableException("Too many attempts: "
                                                    + request, cause);
    request.callback(e);
    return Deferred.fromError(e);
  }

  // --------------------------------------------------- //
  // Code that find regions (in our cache or using RPCs) //
  // --------------------------------------------------- //

  /**
   * Locates the region in which the given row key for the given table is.
   * <p>
   * This does a lookup in the .META. / -ROOT- table(s), no cache is used.
   * If you want to use a cache, call {@link #getRegion} instead.
   * @param table The table to which the row belongs.
   * @param key The row key for which we want to locate the region.
   * @return A deferred called back when the lookup completes.  The deferred
   * carries an unspecified result.
   * @see #discoverRegion
   */
  private Deferred<Object> locateRegion(final byte[] table, final byte[] key) {
    final boolean is_meta = Bytes.equals(table, META);
    final boolean is_root = !is_meta && Bytes.equals(table, ROOT);
    // We don't know in which region this row key is.  Let's look it up.
    // First, see if we already know where to look in .META.
    // Except, obviously, we don't wanna search in META for META or ROOT.
    final byte[] meta_key = is_root ? null : createRegionSearchKey(table, key);
    final byte[] meta_name;
    final RegionInfo meta_region;
    if (has_root) {
      meta_region = is_meta || is_root ? null : getRegion(META, meta_key);
      meta_name = META;
    } else {
      meta_region = META_REGION;
      meta_name = HBASE96_META;
    }

    if (meta_region != null) {  // Always true with HBase 0.95 and up.
      // Lookup in .META. which region server has the region we want.
      final RegionClient client = (has_root
                                   ? region2client.get(meta_region) // Pre 0.95
                                   : rootregion);                  // Post 0.95
      if (client != null && client.isAlive()) {
        final boolean has_permit = client.acquireMetaLookupPermit();
        if (!has_permit) {
          // If we failed to acquire a permit, it's worth checking if someone
          // looked up the region we're interested in.  Every once in a while
          // this will save us a META lookup.
          if (getRegion(table, key) != null) {
            return Deferred.fromResult(null);  // Looks like no lookup needed.
          }
        }
        final Deferred<Object> d =
          client.getClosestRowBefore(meta_region, meta_name, meta_key, INFO)
          .addCallback(meta_lookup_done);
        if (has_permit) {
          final class ReleaseMetaLookupPermit implements Callback<Object, Object> {
            public Object call(final Object arg) {
              client.releaseMetaLookupPermit();
              return arg;
            }
            public String toString() {
              return "release .META. lookup permit";
            }
          };
          d.addBoth(new ReleaseMetaLookupPermit());
          meta_lookups_with_permit.increment();
        } else {
          meta_lookups_wo_permit.increment();
        }
        // This errback needs to run *after* the callback above.
        return d.addErrback(newLocateRegionErrback(table, key));
      }
    }

    // Make a local copy to avoid race conditions where we test the reference
    // to be non-null but then it becomes null before the next statement.
    final RegionClient rootregion = this.rootregion;
    if (rootregion == null || !rootregion.isAlive()) {
      return zkclient.getDeferredRoot();
    } else if (is_root) {  // Don't search ROOT in ROOT.
      return Deferred.fromResult(null);  // We already got ROOT (w00t).
    }
    // The rest of this function is only executed with HBase 0.94 and before.

    // Alright so we don't even know where to look in .META.
    // Let's lookup the right .META. entry in -ROOT-.
    final byte[] root_key = createRegionSearchKey(META, meta_key);
    final RegionInfo root_region = new RegionInfo(ROOT, ROOT_REGION,
                                                  EMPTY_ARRAY);
    root_lookups.increment();
    return rootregion.getClosestRowBefore(root_region, ROOT, root_key, INFO)
      .addCallback(root_lookup_done)
      // This errback needs to run *after* the callback above.
      .addErrback(newLocateRegionErrback(table, key));
  }

  /** Callback executed when a lookup in META completes.  */
  private final class MetaCB implements Callback<Object, ArrayList<KeyValue>> {
    public Object call(final ArrayList<KeyValue> arg) {
      return discoverRegion(arg);
    }
    public String toString() {
      return "locateRegion in META";
    }
  };
  private final MetaCB meta_lookup_done = new MetaCB();

  /** Callback executed when a lookup in -ROOT- completes.  */
  private final class RootCB implements Callback<Object, ArrayList<KeyValue>> {
    public Object call(final ArrayList<KeyValue> arg) {
      return discoverRegion(arg);
    }
    public String toString() {
      return "locateRegion in ROOT";
    }
  };
  private final RootCB root_lookup_done = new RootCB();

  /**
   * Creates a new callback that handles errors during META lookups.
   * <p>
   * This errback should be added *after* adding the callback that invokes
   * {@link #discoverRegion} so it can properly fill in the table name when
   * a {@link TableNotFoundException} is thrown (because the low-level code
   * doesn't know about tables, it only knows about regions, but for proper
   * error reporting users need the name of the table that wasn't found).
   * @param table The table to which the row belongs.
   * @param key The row key for which we want to locate the region.
   */
  private Callback<Object, Exception> newLocateRegionErrback(final byte[] table,
                                                             final byte[] key) {
    return new Callback<Object, Exception>() {
      public Object call(final Exception e) {
        if (e instanceof TableNotFoundException) {
          return new TableNotFoundException(table);  // Populate the name.
        } else if (e instanceof RecoverableException) {
          // Retry to locate the region.  TODO(tsuna): exponential backoff?
          // XXX this can cause an endless retry loop (particularly if the
          // address of -ROOT- in ZK is stale when we start, this code is
          // going to retry in an almost-tight loop until the znode is
          // updated).
          return locateRegion(table, key);
        }
        return e;
      }
      public String toString() {
        return "locateRegion errback";
      }
    };
  }

  /**
   * Creates the META key to search for in order to locate the given key.
   * @param table The table the row belongs to.
   * @param key The key to search for in META.
   * @return A row key to search for in the META table, that will help us
   * locate the region serving the given {@code (table, key)}.
   */
  private static byte[] createRegionSearchKey(final byte[] table,
                                              final byte[] key) {
    // Rows in .META. look like this:
    //   tablename,startkey,timestamp
    final byte[] meta_key = new byte[table.length + key.length + 3];
    System.arraycopy(table, 0, meta_key, 0, table.length);
    meta_key[table.length] = ',';
    System.arraycopy(key, 0, meta_key, table.length + 1, key.length);
    meta_key[meta_key.length - 2] = ',';
    // ':' is the first byte greater than '9'.  We always want to find the
    // entry with the greatest timestamp, so by looking right before ':'
    // we'll find it.
    meta_key[meta_key.length - 1] = ':';
    return meta_key;
  }

  /**
   * Searches in the regions cache for the region hosting the given row.
   * @param table The table to which the row belongs.
   * @param key The row key for which we want to find the region.
   * @return {@code null} if our cache doesn't know which region is currently
   * serving that key, in which case you'd have to look that information up
   * using {@link #locateRegion}.  Otherwise returns the cached region
   * information in which we currently believe that the given row ought to be.
   */
  private RegionInfo getRegion(final byte[] table, final byte[] key) {
    if (has_root) {
      if (Bytes.equals(table, ROOT)) {               // HBase 0.94 and before.
        return new RegionInfo(ROOT, ROOT_REGION, EMPTY_ARRAY);
      }
    } else if (Bytes.equals(table, HBASE96_META)) {  // HBase 0.95 and up.
      return META_REGION;
    }

    byte[] region_name = createRegionSearchKey(table, key);
    Map.Entry<byte[], RegionInfo> entry = regions_cache.floorEntry(region_name);
    if (entry == null) {
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("getRegion(table=" + Bytes.pretty(table) + ", key="
      //            + Bytes.pretty(key) + "): cache miss (nothing found).");
      //}
      return null;
    }

    if (!isCacheKeyForTable(table, entry.getKey())) {
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("getRegion(table=" + Bytes.pretty(table) + ", key="
      //            + Bytes.pretty(key) + "): cache miss (diff table):"
      //            + " region=" + entry.getValue());
      //}
      return null;
    }

    region_name = null;
    final RegionInfo region = entry.getValue();
    entry = null;

    final byte[] stop_key = region.stopKey();
    if (stop_key != EMPTY_ARRAY
        // If the stop key is an empty byte array, it means this region is the
        // last region for this table and this key ought to be in that region.
        && Bytes.memcmp(key, stop_key) >= 0) {
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("getRegion(table=" + Bytes.pretty(table) + ", key="
      //            + Bytes.pretty(key) + "): miss (key beyond stop_key):"
      //            + " region=" + region);
      //}
      return null;
    }

    //if (LOG.isDebugEnabled()) {
    //  LOG.debug("getRegion(table=" + Bytes.pretty(table) + ", key="
    //            + Bytes.pretty(key) + "): cache hit, found: " + region);
    //}
    return region;
  }

  /**
   * Checks whether or not the given cache key is for the given table.
   * @param table The table for which we want the cache key to be.
   * @param cache_key The cache key to check.
   * @return {@code true} if the given cache key is for the given table,
   * {@code false} otherwise.
   */
  private static boolean isCacheKeyForTable(final byte[] table,
                                            final byte[] cache_key) {
    // Check we found an entry that's really for the requested table.
    for (int i = 0; i < table.length; i++) {
      if (table[i] != cache_key[i]) {  // This table isn't in the map, we found
        return false;                  // a key which is for another table.
      }
    }

    // Make sure we didn't find another key that's for another table
    // whose name is a prefix of the table name we were given.
    return cache_key[table.length] == ',';
  }

  /**
   * Adds a new region to our regions cache.
   * @param meta_row The (parsed) result of the
   * {@link RegionClient#getClosestRowBefore} request sent to the
   * .META. (or -ROOT-) table.
   * @return The client serving the region we discovered, or {@code null} if
   * this region isn't being served right now (and we marked it as NSRE'd).
   */
  private RegionClient discoverRegion(final ArrayList<KeyValue> meta_row) {
    if (meta_row.isEmpty()) {
      throw new TableNotFoundException();
    }
    String host = null;
    int port = -42;
    RegionInfo region = null;
    byte[] start_key = null;
    for (final KeyValue kv : meta_row) {
      final byte[] qualifier = kv.qualifier();
      if (Arrays.equals(REGIONINFO, qualifier)) {
        final byte[][] tmp = new byte[1][];  // Yes, this is ugly.
        region = RegionInfo.fromKeyValue(kv, tmp);
        if (knownToBeNSREd(region)) {
          invalidateRegionCache(region.name(), true, "has marked it as split.");
          return null;
        }
        start_key = tmp[0];
      } else if (Arrays.equals(SERVER, qualifier)
                 && kv.value() != EMPTY_ARRAY) {  // Empty during NSRE.
        final byte[] hostport = kv.value();
        int colon = hostport.length - 1;
        for (/**/; colon > 0 /* Can't be at the beginning */; colon--) {
          if (hostport[colon] == ':') {
            break;
          }
        }
        if (colon == 0) {
          throw BrokenMetaException.badKV(region, "an `info:server' cell"
            + " doesn't contain `:' to separate the `host:port'"
            + Bytes.pretty(hostport), kv);
        }
        host = getIP(new String(hostport, 0, colon));
        try {
          port = parsePortNumber(new String(hostport, colon + 1,
                                            hostport.length - colon - 1));
        } catch (NumberFormatException e) {
          throw BrokenMetaException.badKV(region, "an `info:server' cell"
            + " contains an invalid port: " + e.getMessage() + " in "
            + Bytes.pretty(hostport), kv);
        }
      }
      // TODO(tsuna): If this is the parent of a split region, there are two
      // other KVs that could be useful: `info:splitA' and `info:splitB'.
      // Need to investigate whether we can use those as a hint to update our
      // regions_cache with the daughter regions of the split.
    }
    if (start_key == null) {
      throw new BrokenMetaException("It didn't contain any"
        + " `info:regioninfo' cell:  " + meta_row);
    }

    final byte[] region_name = region.name();
    if (host == null) {
      // When there's no `info:server' cell, it typically means that the
      // location of this region is about to be updated in META, so we
      // consider this as an NSRE.
      invalidateRegionCache(region_name, true, "no longer has it assigned.");
      return null;
    }

    // 1. Record the region -> client mapping.
    // This won't be "discoverable" until another map points to it, because
    // at this stage no one knows about this region yet, so another thread
    // may be looking up that region again while we're in the process of
    // publishing our findings.
    final RegionClient client = newClient(host, port);
    final RegionClient oldclient = region2client.put(region, client);
    if (client == oldclient) {  // We were racing with another thread to
      return client;            // discover this region, we lost the race.
    }
    RegionInfo oldregion;
    int nregions;
    // If we get a ConnectException immediately when trying to connect to the
    // RegionServer, Netty delivers a CLOSED ChannelStateEvent from a "boss"
    // thread while we may still be handling the OPEN event in an NIO thread.
    // Locking the client prevents it from being able to buffer requests when
    // this happens.  After we release the lock, then it will find it's dead.
    synchronized (client) {
      // Don't put any code between here and the next put (see next comment).

      // 2. Store the region in the sorted map.
      // This will effectively "publish" the result of our work to other
      // threads.  The window between when the previous `put' becomes visible
      // to all other threads and when we're done updating the sorted map is
      // when we may unnecessarily re-lookup the same region again.  It's an
      // acceptable trade-off.  We avoid extra synchronization complexity in
      // exchange of occasional duplicate work (which should be rare anyway).
      oldregion = regions_cache.put(region_name, region);

      // 3. Update the reverse mapping created in step 1.
      // This is done last because it's only used to gracefully handle
      // disconnections and isn't used for serving.
      final ArrayList<RegionInfo> regions = client2regions.get(client);
      synchronized (regions) {
        regions.add(region);
        nregions = regions.size();
      }
    }

    // Don't interleave logging with the operations above, in order to attempt
    // to reduce the duration of the race windows.
    LOG.info((oldclient == null ? "Added" : "Replaced") + " client for"
             + " region " + region + ", which was "
             + (oldregion == null ? "added to" : "updated in") + " the"
             + " regions cache.  Now we know that " + client + " is hosting "
             + nregions + " region" + (nregions > 1 ? 's' : "") + '.');

    return client;
  }

  /**
   * Invalidates any cached knowledge about the given region.
   * <p>
   * This is typically used when a region migrates because of a split
   * or a migration done by the region load balancer, which causes a
   * {@link NotServingRegionException}.
   * <p>
   * This is package-private so that the low-level {@link RegionClient} can do
   * the invalidation itself when it gets a {@link NotServingRegionException}
   * back from a RegionServer.
   * @param region_name The name of the region to invalidate in our caches.
   * @param mark_as_nsred If {@code true}, after removing everything we know
   * about this region, we'll store a special marker in our META cache to mark
   * this region as "known to be NSRE'd", so that subsequent requests to this
   * region will "fail-fast".
   * @param reason If not {@code null}, will be used to log an INFO message
   * about the cache invalidation done.
   */
  private void invalidateRegionCache(final byte[] region_name,
                                     final boolean mark_as_nsred,
                                     final String reason) {
    if ((region_name == META_REGION_NAME && !has_root)  // HBase 0.95+
        || region_name == ROOT_REGION) {                // HBase <= 0.94
      if (reason != null) {
        LOG.info("Invalidated cache for " + (has_root ? "-ROOT-" : ".META.")
                 + " as " + rootregion + ' ' + reason);
      }
      rootregion = null;
      return;
    }
    final RegionInfo oldregion = mark_as_nsred
      ? regions_cache.put(region_name, new RegionInfo(EMPTY_ARRAY, region_name,
                                                      EMPTY_ARRAY))
      : regions_cache.remove(region_name);
    final RegionInfo region = (oldregion != null ? oldregion
                               : new RegionInfo(EMPTY_ARRAY, region_name,
                                                EMPTY_ARRAY));
    final RegionClient client = region2client.remove(region);

    if (oldregion != null && !Bytes.equals(oldregion.name(), region_name)) {
      // XXX do we want to just re-add oldregion back?  This exposes another
      // race condition (we re-add it and overwrite yet another region change).
      LOG.warn("Oops, invalidated the wrong regions cache entry."
               + "  Meant to remove " + Bytes.pretty(region_name)
               + " but instead removed " + oldregion);
    }

    if (client == null) {
      return;
    }
    final ArrayList<RegionInfo> regions = client2regions.get(client);
    if (regions != null) {
      // `remove()' on an ArrayList causes an array copy.  Should we switch
      // to a LinkedList instead?
      synchronized (regions) {
        regions.remove(region);
      }
    }
    if (reason != null) {
      LOG.info("Invalidated cache for " + region + " as " + client
               + ' ' + reason);
    }
  }

  /**
   * Returns true if this region is known to be NSRE'd and shouldn't be used.
   * @see #handleNSRE
   */
  private static boolean knownToBeNSREd(final RegionInfo region) {
    return region.table() == EMPTY_ARRAY;
  }

  /**
   * Low and high watermarks when buffering RPCs due to an NSRE.
   * @see #handleNSRE
   * XXX TODO(tsuna): Don't hardcode.
   */
  private static short NSRE_LOW_WATERMARK  =  1000;
  private static short NSRE_HIGH_WATERMARK = 10000;

  /** Log a message for every N RPCs we buffer due to an NSRE.  */
  private static final short NSRE_LOG_EVERY      =   500;

  /**
   * Handles the {@link NotServingRegionException} for the given RPC.
   * <p>
   * This code will take ownership of the RPC in the sense that it will become
   * responsible for re-scheduling the RPC later once the NSRE situation gets
   * resolved by HBase.
   *
   * <h1>NSRE handling logic</h1>
   * Whenever we get an NSRE for the first time for a particular region, we
   * will add an entry for this region in the {@link #got_nsre} map.  We also
   * replace the entry for this region in {@link #regions_cache} with a special
   * entry that indicates that this region is known to be unavailable for now,
   * due to the NSRE.  This entry is said to be special because it belongs to
   * the table with an empty name (which is otherwise impossible).  This way,
   * new RPCs that are sent out can still hit our local cache instead of
   * requiring a META lookup and be directly sent to this method so they can
   * be queued to wait until the NSRE situation is resolved by HBase.
   * <p>
   * When we first get an NSRE, we also create a "probe" RPC, the goal of
   * which is to periodically poke HBase and check whether the NSRE situation
   * was resolved.  The way we poke HBase is to send an "exists" RPC (which
   * is actually just a "get" RPC that returns true or false instead of
   * returning any data) for the table / key of the first RPC to trigger the
   * NSRE.  As soon as the probe returns successfully, we know HBase resolved
   * the NSRE situation and the region is back online.  Note that it doesn't
   * matter what the result of the probe is, the only thing that matters is
   * that the probe doesn't get NSRE'd.
   * <p>
   * Once the probe RPC succeeds, we flush out all the RPCs that are pending
   * for the region that got NSRE'd.  When the probe fails, it's periodically
   * re-scheduled with an exponential-ish backoff.
   * <p>
   * We put a cap on the number of RPCs we'll keep on hold while we wait for
   * the NSRE to be resolved.  Say you have a high throughput application
   * that's producing 100k write operations per second.  Even if it takes
   * HBase just a second to bring the region back online, the application
   * will have generated over 100k RPCs before we realize we're good to go.
   * This means the application can easily run itself out of memory if we let
   * the queue grow unbounded.  To prevent that from happening, the code has
   * a low watermark and a high watermark on the number of pending RPCs for
   * a particular region.  Once the low watermark is hit, one RPC will be
   * failed with a {@link PleaseThrottleException}.  This is an advisory
   * warning that HBase isn't keeping up and that the application should
   * slow down its HBase usage momentarily.  After hitting the low watermark,
   * further RPCs that are still getting NSRE'd on the same region will get
   * buffered again until we hit the high watermark.  Once the high watermark
   * is hit, all subsequent RPCs that get NSRE'd will immediately fail with a
   * {@link PleaseThrottleException} (and they will fail-fast).
   * @param rpc The RPC that failed or is going to fail with an NSRE.
   * @param region_name The name of the region this RPC is going to.
   * Obviously, this method cannot be used for RPCs that aren't targeted
   * at a particular region.
   * @param e The exception that caused (or may cause) this RPC to fail.
   */
  void handleNSRE(HBaseRpc rpc,
                  final byte[] region_name,
                  final RecoverableException e) {
    num_nsre_rpcs.increment();
    final boolean can_retry_rpc = !cannotRetryRequest(rpc);
    boolean known_nsre = true;  // We already aware of an NSRE for this region?
    ArrayList<HBaseRpc> nsred_rpcs = got_nsre.get(region_name);
    HBaseRpc exists_rpc = null;  // Our "probe" RPC.
    if (nsred_rpcs == null) {  // Looks like this could be a new NSRE...
      final ArrayList<HBaseRpc> newlist = new ArrayList<HBaseRpc>(64);
      // In HBase 0.95 and up, the exists RPC can't use the empty row key,
      // which could happen if we were trying to scan from the beginning of
      // the table.  So instead use "\0" as the key.
      exists_rpc = GetRequest.exists(rpc.table, probeKey(rpc.key));
      newlist.add(exists_rpc);
      if (can_retry_rpc) {
        newlist.add(rpc);
      }
      nsred_rpcs = got_nsre.putIfAbsent(region_name, newlist);
      if (nsred_rpcs == null) {  // We've just put `newlist'.
        nsred_rpcs = newlist;    //   => We're the first thread to get
        known_nsre = false;      //      the NSRE for this region.
      }
    }

    if (known_nsre) {  // Some RPCs seem to already be pending due to this NSRE
      boolean reject = true;  // Should we reject this RPC (too many pending)?
      int size;               // How many RPCs are already pending?

      synchronized (nsred_rpcs) {
        size = nsred_rpcs.size();
        // If nsred_rpcs is empty, there was a race with another thread which
        // is executing RetryNSREd.call and that just cleared this array and
        // removed nsred_rpcs from got_nsre right after we got the reference,
        // so we need to add it back there, unless another thread already
        // did it (in which case we're really unlucky and lost 2 races).
        if (size == 0) {
          final ArrayList<HBaseRpc> added =
            got_nsre.putIfAbsent(region_name, nsred_rpcs);
          if (added == null) {  // We've just put `nsred_rpcs'.
            exists_rpc = GetRequest.exists(rpc.table, probeKey(rpc.key));
            nsred_rpcs.add(exists_rpc);  // We hold the lock on nsred_rpcs
            if (can_retry_rpc) {
              nsred_rpcs.add(rpc);         // so we can safely add those 2.
            }
            known_nsre = false;  // We mistakenly believed it was known.
          } else {  // We lost the second race.
            // Here we synchronize on two different references without any
            // apparent ordering guarantee, which can typically lead to
            // deadlocks.  In this case though we're fine, as any other thread
            // that still has a reference to `nsred_rpcs' is gonna go through
            // this very same code path and will lock `nsred_rpcs' first
            // before finding that it too lost 2 races, so it'll lock `added'
            // second.  So there's actually a very implicit ordering.
            if (can_retry_rpc) {
              synchronized (added) {  // Won't deadlock (explanation above).
                if (added.isEmpty()) {
                  LOG.error("WTF?  Shouldn't happen!  Lost 2 races and found"
                            + " an empty list of NSRE'd RPCs (" + added
                            + ") for " + Bytes.pretty(region_name));
                  exists_rpc = GetRequest.exists(rpc.table, probeKey(rpc.key));
                  added.add(exists_rpc);
                } else {
                  exists_rpc = added.get(0);
                }
                if (can_retry_rpc) {
                  added.add(rpc);  // Add ourselves in the existing array...
                }
              }
            }
            nsred_rpcs = added;  // ... and update our reference.
          }
        }
        // If `rpc' is the first element in nsred_rpcs, it's our "probe" RPC,
        // in which case we must not add it to the array again.
        else if ((exists_rpc = nsred_rpcs.get(0)) != rpc) {
          if (size < NSRE_HIGH_WATERMARK) {
            if (size == NSRE_LOW_WATERMARK) {
              nsred_rpcs.add(null);  // "Skip" one slot.
            } else if (can_retry_rpc) {
              reject = false;
              if (nsred_rpcs.contains(rpc)) {  // XXX O(n) check...  :-/
                LOG.error("WTF?  Trying to add " + rpc + " twice to NSREd RPC"
                          + " on " + Bytes.pretty(region_name));
              } else {
                nsred_rpcs.add(rpc);
              }
            }
          }
        } else {           // This is our probe RPC.
          reject = false;  // So don't reject it.
        }
      } // end of the synchronized block.

      // Stop here if this is a known NSRE and `rpc' is not our probe RPC.
      if (known_nsre && exists_rpc != rpc) {
        if (size != NSRE_HIGH_WATERMARK && size % NSRE_LOG_EVERY == 0) {
          final String msg = "There are now " + size
            + " RPCs pending due to NSRE on " + Bytes.pretty(region_name);
          if (size + NSRE_LOG_EVERY < NSRE_HIGH_WATERMARK) {
            LOG.info(msg);  // First message logged at INFO level.
          } else {
            LOG.warn(msg);  // Last message logged with increased severity.
          }
        }
        if (reject) {
          rpc.callback(new PleaseThrottleException(size + " RPCs waiting on "
            + Bytes.pretty(region_name) + " to come back online", e, rpc,
            exists_rpc.getDeferred()));
        }
        return;  // This NSRE is already known and being handled.
      }
    }

    num_nsres.increment();
    // Mark this region as being NSRE'd in our regions_cache.
    invalidateRegionCache(region_name, true, (known_nsre ? "still " : "")
                          + "seems to be splitting or closing it.");

    // Need a `final' variable to access from within the inner class below.
    final ArrayList<HBaseRpc> rpcs = nsred_rpcs;  // Guaranteed non-null.
    final HBaseRpc probe = exists_rpc;  // Guaranteed non-null.
    nsred_rpcs = null;
    exists_rpc = null;

    if (known_nsre && probe.attempt > 1) {
      // Our probe is almost guaranteed to cause a META lookup, so virtually
      // every time we retry it its attempt count will be incremented twice
      // (once for a META lookup, once to send the actual probe).  Here we
      // decrement the attempt count to "de-penalize" the probe from causing
      // META lookups, because that's what we want it to do.  If the probe
      // is lucky and doesn't trigger a META lookup (rare) it'll get a free
      // extra attempt, no big deal.
      probe.attempt--;
    } else if (!can_retry_rpc) {
      // `rpc' isn't a probe RPC and can't be retried, make it fail-fast now.
      rpc.callback(tooManyAttempts(rpc, e));
    }

    rpc = null;  // No longer need this reference.

    // Callback we're going to add on our probe RPC.  When this callback gets
    // invoked, it means that our probe RPC completed, so NSRE situation seems
    // resolved and we can retry all the RPCs that were waiting on that region.
    // We also use this callback as an errback to avoid leaking RPCs in case
    // of an unexpected failure of the probe RPC (e.g. a RegionServer dying
    // while it's splitting a region, which would cause a connection reset).
    final class RetryNSREd implements Callback<Object, Object> {
      public Object call(final Object arg) {
        if (arg instanceof Exception) {
          LOG.warn("Probe " + probe + " failed", (Exception) arg);
        }
        ArrayList<HBaseRpc> removed = got_nsre.remove(region_name);
        if (removed != rpcs && removed != null) {  // Should never happen.
          synchronized (removed) {                 // But just in case...
            synchronized (rpcs) {
              LOG.error("WTF?  Impossible!  Removed the wrong list of RPCs"
                + " from got_nsre.  Was expecting list@"
                + System.identityHashCode(rpcs) + " (size=" + rpcs.size()
                + "), got list@" + System.identityHashCode(removed)
                + " (size=" + removed.size() + ')');
            }
            for (final HBaseRpc r : removed) {
              if (r != null && r != probe) {
                sendRpcToRegion(r);  // We screwed up but let's not lose RPCs.
              }
            }
            removed.clear();
          }
        }
        removed = null;

        synchronized (rpcs) {
          if (LOG.isDebugEnabled()) {
            if (arg instanceof Exception) {
              LOG.debug("Retrying " + rpcs.size() + " RPCs on NSREd region "
                  + Bytes.pretty(region_name));
            } else {
              LOG.debug("Retrying " + rpcs.size() + " RPCs now that the NSRE on "
                  + Bytes.pretty(region_name) + " seems to have cleared");
            }
          }
          final Iterator<HBaseRpc> i = rpcs.iterator();
          if (i.hasNext()) {
            HBaseRpc r = i.next();
            if (r != probe) {
              LOG.error("WTF?  Impossible!  Expected first == probe but first="
                        + r + " and probe=" + probe);
              sendRpcToRegion(r);
            }
            while (i.hasNext()) {
              if ((r = i.next()) != null) {
                sendRpcToRegion(r);
              }
            }
          } else {
            LOG.error("WTF?  Impossible!  Empty rpcs array=" + rpcs
                      + " found by " + this);
          }
          rpcs.clear();
        }

        return arg;
      }
      public String toString() {
        return "retry other RPCs NSRE'd on " + Bytes.pretty(region_name);
      }
    };

    // It'll take a short while for HBase to clear the NSRE.  If a
    // region is being split, we should be able to use it again pretty
    // quickly, but if a META entry is stale (e.g. due to RegionServer
    // failures / restarts), it may take up to several seconds.
    final class NSRETimer implements TimerTask {
      public void run(final Timeout timeout) {
        if (probe.attempt == 0) {  // Brand new probe.
          probe.getDeferred().addBoth(new RetryNSREd());
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Done waiting after NSRE on " + Bytes.pretty(region_name)
                    + ", retrying " + probe);
        }
        // Make sure the probe will cause a META lookup.
        invalidateRegionCache(region_name, false, null);
        sendRpcToRegion(probe);  // Restart the RPC.
      }
      public String toString() {
        return "probe NSRE " + probe;
      }
    };

    // Linear backoff followed by exponential backoff.  Some NSREs can be
    // resolved in a second or so, some seem to easily take ~6 seconds,
    // sometimes more when a RegionServer has failed and the master is slowly
    // splitting its logs and re-assigning its regions.
    final int wait_ms = probe.attempt < 4
      ? 200 * (probe.attempt + 2)     // 400, 600, 800, 1000
      : 1000 + (1 << probe.attempt);  // 1016, 1032, 1064, 1128, 1256, 1512, ..
    newTimeout(new NSRETimer(), wait_ms);
  }

  /**
   * Some arbitrary junk that is unlikely to appear in a real row key.
   * @see probeKey
   */
  protected static byte[] PROBE_SUFFIX = {
    ':', 'A', 's', 'y', 'n', 'c', 'H', 'B', 'a', 's', 'e',
    '~', 'p', 'r', 'o', 'b', 'e', '~', '<', ';', '_', '<',
  };

  /**
   * Returns a newly allocated key to probe, to check a region is online.
   * Sometimes we need to "poke" HBase to see if a region is online or a table
   * exists.  Given a key, we prepend some unique suffix to make it a lot less
   * likely that we hit a real key with our probe, as doing so might have some
   * implications on the RegionServer's memory usage.  Yes, some people with
   * very large keys were experiencing OOM's in their RegionServers due to
   * AsyncHBase probes.
   */
  private static byte[] probeKey(final byte[] key) {
    final byte[] testKey = new byte[key.length + 64];
    System.arraycopy(key, 0, testKey, 0, key.length);
    System.arraycopy(PROBE_SUFFIX, 0,
                     testKey, testKey.length - PROBE_SUFFIX.length,
                     PROBE_SUFFIX.length);
    return testKey;
  }

  // ----------------------------------------------------------------- //
  // Code that manages connection and disconnection to Region Servers. //
  // ----------------------------------------------------------------- //

  /**
   * Returns a client to communicate with a Region Server.
   * <p>
   * Note that this method is synchronized, so only one client at a time can
   * be created.  In practice this shouldn't be a problem as this method is
   * not expected to be frequently called.
   * @param host The normalized <strong>IP address</strong> of the region
   * server.  Passing a hostname or a denormalized IP address will work
   * silently and will result in unnecessary extra connections (clients are
   * cached, which is why always using the normalized IP address will result
   * in fewer connections).
   * @param port The port on which the region server is serving.
   * @return A client for this region server.
   */
  private RegionClient newClient(final String host, final int port) {
    // This big synchronized block is required because using a
    // ConcurrentHashMap wouldn't be sufficient.  We could still have 2
    // threads attempt to create the same client at the same time, and they
    // could both test the map at the same time and create 2 instances.
    final String hostport = host + ':' + port;

    RegionClient client;
    SocketChannel chan = null;
    synchronized (ip2client) {
      client = ip2client.get(hostport);
      if (client != null && client.isAlive()) {
        return client;
      }

      // We don't use Netty's ClientBootstrap class because it makes it
      // unnecessarily complicated to have control over which ChannelPipeline
      // exactly will be given to the channel.  It's over-designed.
      final RegionClientPipeline pipeline = new RegionClientPipeline();
      client = pipeline.init();
      chan = channel_factory.newChannel(pipeline);
      ip2client.put(hostport, client);  // This is guaranteed to return null.
    }
    client2regions.put(client, new ArrayList<RegionInfo>());
    num_connections_created.increment();
    // Configure and connect the channel without locking ip2client.
    final SocketChannelConfig config = chan.getConfig();
    config.setConnectTimeoutMillis(5000);
    config.setTcpNoDelay(true);
    // Unfortunately there is no way to override the keep-alive timeout in
    // Java since the JRE doesn't expose any way to call setsockopt() with
    // TCP_KEEPIDLE.  And of course the default timeout is >2h.  Sigh.
    config.setKeepAlive(true);
    chan.connect(new InetSocketAddress(host, port));  // Won't block.
    return client;
  }

  /**
   * A {@link DefaultChannelPipeline} that gives us a chance to deal with
   * certain events before any handler runs.
   * <p>
   * We hook a couple of methods in order to report disconnection events to
   * the {@link HBaseClient} so that it can clean up its connection caches
   * ASAP to avoid using disconnected (or soon to be disconnected) sockets.
   * <p>
   * Doing it this way is simpler than having a first handler just to handle
   * disconnection events, to which we'd need to pass a callback to invoke
   * to report the event back to the {@link HBaseClient}.
   */
  private final class RegionClientPipeline extends DefaultChannelPipeline {

    /**
     * Have we already disconnected?.
     * We use this to avoid doing the cleanup work for the same client more
     * than once, even if we get multiple events indicating that the client
     * is no longer connected to the RegionServer (e.g. DISCONNECTED, CLOSED).
     * No synchronization needed as this is always accessed from only one
     * thread at a time (equivalent to a non-shared state in a Netty handler).
     */
    private boolean disconnected = false;

    RegionClientPipeline() {
    }

    /**
     * Initializes this pipeline.
     * This method <strong>MUST</strong> be called on each new instance
     * before it's used as a pipeline for a channel.
     */
    RegionClient init() {
      final RegionClient client = new RegionClient(HBaseClient.this);
      super.addLast("handler", client);
      return client;
    }

    @Override
    public void sendDownstream(final ChannelEvent event) {
      //LoggerFactory.getLogger(RegionClientPipeline.class)
      //  .debug("hooked sendDownstream " + event);
      if (event instanceof ChannelStateEvent) {
        handleDisconnect((ChannelStateEvent) event);
      }
      super.sendDownstream(event);
    }

    @Override
    public void sendUpstream(final ChannelEvent event) {
      //LoggerFactory.getLogger(RegionClientPipeline.class)
      //  .debug("hooked sendUpstream " + event);
      if (event instanceof ChannelStateEvent) {
        handleDisconnect((ChannelStateEvent) event);
      }
      super.sendUpstream(event);
    }

    private void handleDisconnect(final ChannelStateEvent state_event) {
      if (disconnected) {
        return;
      }
      switch (state_event.getState()) {
        case OPEN:
          if (state_event.getValue() == Boolean.FALSE) {
            break;  // CLOSED
          }
          return;
        case CONNECTED:
          if (state_event.getValue() == null) {
            break;  // DISCONNECTED
          }
          return;
        default:
          return;  // Not an event we're interested in, ignore it.
      }

      disconnected = true;  // So we don't clean up the same client twice.
      try {
        final RegionClient client = super.get(RegionClient.class);
        SocketAddress remote = super.getChannel().getRemoteAddress();
        // At this point Netty gives us no easy way to access the
        // SocketAddress of the peer we tried to connect to, so we need to
        // find which entry in the map was used for the rootregion.  This
        // kinda sucks but I couldn't find an easier way.
        if (remote == null) {
          remote = slowSearchClientIP(client);
        }

        // Prevent the client from buffering requests while we invalidate
        // everything we have about it.
        synchronized (client) {
          removeClientFromCache(client, remote);
        }
      } catch (Exception e) {
        LoggerFactory.getLogger(RegionClientPipeline.class)
          .error("Uncaught exception when handling a disconnection of "
                 + getChannel(), e);
      }
    }

  }

  /**
   * Performs a slow search of the IP used by the given client.
   * <p>
   * This is needed when we're trying to find the IP of the client before its
   * channel has successfully connected, because Netty's API offers no way of
   * retrieving the IP of the remote peer until we're connected to it.
   * @param client The client we want the IP of.
   * @return The IP of the client, or {@code null} if we couldn't find it.
   */
  private InetSocketAddress slowSearchClientIP(final RegionClient client) {
    String hostport = null;
    synchronized (ip2client) {
      for (final Map.Entry<String, RegionClient> e : ip2client.entrySet()) {
        if (e.getValue() == client) {
          hostport = e.getKey();
          break;
        }
      }
    }

    if (hostport == null) {
      HashMap<String, RegionClient> copy;
      synchronized (ip2client) {
        copy = new HashMap<String, RegionClient>(ip2client);
      }
      LOG.error("WTF?  Should never happen!  Couldn't find " + client
                + " in " + copy);
      return null;
    }

    LOG.warn("Couldn't connect to the RegionServer @ " + hostport);
    final int colon = hostport.indexOf(':', 1);
    if (colon < 1) {
      LOG.error("WTF?  Should never happen!  No `:' found in " + hostport);
      return null;
    }
    final String host = getIP(hostport.substring(0, colon));
    int port;
    try {
      port = parsePortNumber(hostport.substring(colon + 1,
                                                hostport.length()));
    } catch (NumberFormatException e) {
      LOG.error("WTF?  Should never happen!  Bad port in " + hostport, e);
      return null;
    }
    return new InetSocketAddress(host, port);
  }

  /**
   * Removes all the cache entries referred to the given client.
   * @param client The client for which we must invalidate everything.
   * @param remote The address of the remote peer, if known, or null.
   */
  private void removeClientFromCache(final RegionClient client,
                                     final SocketAddress remote) {
    if (client == rootregion) {
      LOG.info("Lost connection with the "
               + (has_root ? "-ROOT-" : ".META.") + " region");
      rootregion = null;
    }
    ArrayList<RegionInfo> regions = client2regions.remove(client);
    if (regions != null) {
      // Make a copy so we don't need to synchronize on it while iterating.
      RegionInfo[] regions_copy;
      synchronized (regions) {
        regions_copy = regions.toArray(new RegionInfo[regions.size()]);
        regions = null;
        // If any other thread still has a reference to `regions', their
        // updates will be lost (and we don't care).
      }
      for (final RegionInfo region : regions_copy) {
        final byte[] table = region.table();
        final byte[] stop_key = region.stopKey();
        // If stop_key is the empty array:
        //   This region is the last region for this table.  In order to
        //   find the start key of the last region, we append a '\0' byte
        //   at the end of the table name and search for the entry with a
        //   key right before it.
        // Otherwise:
        //   Search for the entry with a key right before the stop_key.
        final byte[] search_key =
          createRegionSearchKey(stop_key.length == 0
                                ? Arrays.copyOf(table, table.length + 1)
                                : table, stop_key);
        final Map.Entry<byte[], RegionInfo> entry =
          regions_cache.lowerEntry(search_key);
        if (entry != null && entry.getValue() == region) {
          // Invalidate the regions cache first, as it's the most damaging
          // one if it contains stale data.
          regions_cache.remove(entry.getKey());
          LOG.debug("Removed from regions cache: {}", region);
        }
        final RegionClient oldclient = region2client.remove(region);
        if (client == oldclient) {
          LOG.debug("Association removed: {} -> {}", region, client);
        } else if (oldclient != null) {  // Didn't remove what we expected?!
          LOG.warn("When handling disconnection of " + client
                   + " and removing " + region + " from region2client"
                   + ", it was found that " + oldclient + " was in fact"
                   + " serving this region");
        }
      }
    }

    if (remote == null) {
      return;  // Can't continue without knowing the remote address.
    }

    String hostport = null;
    if (remote instanceof InetSocketAddress) {
      final InetSocketAddress sock = (InetSocketAddress) remote;
      final InetAddress addr = sock.getAddress();
      if (addr == null) {
        LOG.error("WTF?  Unresolved IP for " + remote
                  + ".  This shouldn't happen.");
        return;
      } else {
        hostport = addr.getHostAddress() + ':' + sock.getPort();
      }
    } else {
        LOG.error("WTF?  Found a non-InetSocketAddress remote: " + remote
                  + ".  This shouldn't happen.");
        return;
    }

    RegionClient old;
    synchronized (ip2client) {
      old = ip2client.remove(hostport);
    }
    LOG.debug("Removed from IP cache: {} -> {}", hostport, client);
    if (old == null) {
      LOG.warn("When expiring " + client + " from the client cache (host:port="
               + hostport + "), it was found that there was no entry"
               + " corresponding to " + remote + ".  This shouldn't happen.");
    }
  }

  // ---------------- //
  // ZooKeeper stuff. //
  // ---------------- //

  /**
   * Helper to locate the -ROOT- region through ZooKeeper.
   * <p>
   * We don't watch the file of the -ROOT- region.  We just asynchronously
   * read it once to find -ROOT-, then we close our ZooKeeper session.
   * There are a few reasons for this.  First of all, the -ROOT- region
   * doesn't move often.  When it does, and when we need to use it, we'll
   * realize that -ROOT- is no longer where we though it was and we'll find
   * it again.  Secondly, maintaining a session open just to watch the
   * -ROOT- region is a waste of resources both on our side and on ZK's side.
   * ZK is chatty, it will frequently send us heart beats that will keep
   * waking its event thread, etc.  Third, if the application we're part of
   * already needs to maintain a session with ZooKeeper anyway, we can't
   * easily share it with them anyway, because of ZooKeeper's API.  Indeed,
   * unfortunately the ZooKeeper API requires that the {@link ZooKeeper}
   * object be re-created when the session is invalidated (due to a
   * disconnection or a timeout), which means that it's impossible to
   * share the {@link ZooKeeper} object.  Ideally in an application there
   * should be only one instance, but their poor API makes it impractical,
   * since the instance must be re-created when the session is invalidated,
   * which entails that one entity should own the reconnection process and
   * have a way of giving everyone else the new instance.  This is extremely
   * cumbersome so I don't expect anyone to do this, which is why we manage
   * our own instance.
   */
  protected final class ZKClient implements Watcher {

    /** The specification of the quorum, e.g. "host1,host2,host3"  */
    private final String quorum_spec;

    /** The base path under which is the znode for the -ROOT- region.  */
    private final String base_path;

    /**
     * Our ZooKeeper instance.
     * Must grab this' monitor before accessing.
     */
    private ZooKeeper zk;

    /**
     * When we're not connected to ZK, users who are trying to access the
     * -ROOT- region can queue up here to be called back when it's available.
     * Must grab this' monitor before accessing.
     */
    private ArrayList<Deferred<Object>> deferred_rootregion;

    /**
     * Constructor.
     * @param quorum_spec The specification of the quorum, e.g.
     * {@code "host1,host2,host3"}.
     * @param base_path The base path under which is the znode for the
     * -ROOT- region.
     */
    public ZKClient(final String quorum_spec, final String base_path) {
      this.quorum_spec = quorum_spec;
      this.base_path = base_path;
    }

    /**
     * Returns a deferred that will be called back once we found -ROOT-.
     * @return A deferred which will be invoked with an unspecified argument
     * once we know where -ROOT- is.  Note that by the time you get called
     * back, we may have lost the connection to the -ROOT- region again.
     */
    public Deferred<Object> getDeferredRoot() {
      final Deferred<Object> d = new Deferred<Object>();
      synchronized (this) {
        try {
          connectZK();  // Kick off a connection if needed.
          if (deferred_rootregion == null) {
            LOG.info("Need to find the "
                     + (has_root ? "-ROOT-" : ".META.") + " region");
            deferred_rootregion = new ArrayList<Deferred<Object>>();
          }
          deferred_rootregion.add(d);
        } catch (NonRecoverableException e) {
          LOG.error(e.getMessage(), e.getCause());
          d.callback(e);
        }
      }
      return d;
    }

    /**
     * Like {@link getDeferredRoot} but returns null if we're not already
     * trying to find -ROOT-.
     * In other words calling this method doesn't trigger a -ROOT- lookup
     * unless there's already one in flight.
     * @return @{code null} if -ROOT- isn't being looked up right now,
     * otherwise a deferred which will be invoked with an unspecified argument
     * once we know where -ROOT- is.  Note that by the time you get called
     * back, we may have lost the connection to the -ROOT- region again.
     */
    Deferred<Object> getDeferredRootIfBeingLookedUp() {
      synchronized (this) {
        if (deferred_rootregion == null) {
          return null;
        }
        final Deferred<Object> d = new Deferred<Object>();
        deferred_rootregion.add(d);
        return d;
      }
    }

    /**
     * Atomically returns and {@code null}s out the current list of
     * Deferreds waiting for the -ROOT- region.
     */
    private ArrayList<Deferred<Object>> atomicGetAndRemoveWaiters() {
      synchronized (this) {
        try {
          return deferred_rootregion;
        } finally {
          deferred_rootregion = null;
        }
      }
    }

    /**
     * Processes a ZooKeeper event.
     * <p>
     * This method is called back by {@link ZooKeeper} from its main event
     * thread.  So make sure you don't block.
     * @param event The event to process.
     */
    public void process(final WatchedEvent event) {
      LOG.debug("Got ZooKeeper event: {}", event);
      try {
        switch (event.getState()) {
          case SyncConnected:
            getRootRegion();
            break;
          default:
            disconnectZK();
            // Reconnect only if we're still trying to locate -ROOT-.
            synchronized (this) {
              if (deferred_rootregion != null) {
                LOG.warn("No longer connected to ZooKeeper, event=" + event);
                connectZK();
              }
            }
            return;
        }
      } catch (Exception e) {
        LOG.error("Uncaught exception when handling event " + event, e);
        return;
      }
      LOG.debug("Done handling ZooKeeper event: {}", event);
    }

    /**
     * Connects to ZooKeeper.
     * @throws NonRecoverableException if something from which we can't
     * recover happened -- e.g. us being unable to resolve the hostname
     * of any of the zookeeper servers.
     */
    private void connectZK() {
      try {
        // Session establishment is asynchronous, so this won't block.
        synchronized (this) {
          if (zk != null) {  // Already connected.
            return;
          }
          zk = new ZooKeeper(quorum_spec, 5000, this);
        }
      } catch (UnknownHostException e) {
        // No need to retry, we usually cannot recover from this.
        throw new NonRecoverableException("Cannot connect to ZooKeeper,"
          + " is the quorum specification valid? " + quorum_spec, e);
      } catch (IOException e) {
        LOG.error("Failed to connect to ZooKeeper", e);
        // XXX don't retry recursively, create a timer with an exponential
        // backoff and schedule the reconnection attempt for later.
        connectZK();
      }
    }

    /**
     * Disconnects from ZooKeeper.
     * <p>
     * <strong>This method is blocking.</strong>  Unfortunately, ZooKeeper
     * doesn't offer an asynchronous API to close a session at this time.
     * It waits until the server responds to the {@code closeSession} RPC.
     */
    public void disconnectZK() {
      synchronized (this) {
        if (zk == null) {
          return;
        }
        try {
          // I'm not sure but I think both the client and the server race to
          // close the socket, which often causes the DEBUG spam:
          //   java.net.SocketException: Socket is not connected
          // When the client attempts to close its socket after its OS and
          // JVM are done processing the TCP FIN and it's already closed.
          LOG.debug("Ignore any DEBUG exception from ZooKeeper");
          final long start = System.nanoTime();
          zk.close();
          LOG.debug("ZooKeeper#close completed in {}ns",
                    System.nanoTime() - start);
        } catch (InterruptedException e) {
          // The signature of the method pretends that it can throw an
          // InterruptedException, but this is a lie, the code of that
          // method will never throw this type of exception.
          LOG.error("Should never happen", e);
        }
        zk = null;
      }
    }

    /** Schedule a timer to retry {@link #getRootRegion} after some time.  */
    private void retryGetRootRegionLater() {
      newTimeout(new TimerTask() {
          public void run(final Timeout timeout) {
            if (!getRootRegion()) {  // Try to read the znodes
              connectZK();  // unless we need to connect first.
            }
          }
        }, 1000 /* milliseconds */);
    }

    /**
     * Puts a watch in ZooKeeper to monitor the file of the -ROOT- region.
     * This method just registers an asynchronous callback.
     */
    final class ZKCallback implements AsyncCallback.DataCallback {

      /**
       * HBASE-3065 (r1151751) prepends meta-data in ZooKeeper files.
       * The meta-data always starts with this magic byte.
       */
      protected static final byte MAGIC = (byte) 0xFF;

      private static final byte UNKNOWN = 0;  // Callback still pending.
      private static final byte FOUND = 1;    // We found the znode.
      private static final byte NOTFOUND = 2; // The znode didn't exist.

      private byte found_root;
      private byte found_meta;  // HBase 0.95 and up

      public void processResult(final int rc, final String path,
                                final Object ctx, final byte[] data,
                                final Stat stat) {
        final boolean is_root;  // True if ROOT znode, false if META znode.
        if (path.endsWith("/root-region-server")) {
          is_root = true;
        } else if (path.endsWith("/meta-region-server")) {
          is_root = false;
        } else {
          LOG.error("WTF? We got a callback from ZooKeeper for a znode we did"
                    + " not expect: " + path + " / stat: " + stat + " / data: "
                    + Bytes.pretty(data));
          retryGetRootRegionLater();
          return;
        }

        if (rc == Code.NONODE.intValue()) {
          final boolean both_znode_failed;
          if (is_root) {
            found_root = NOTFOUND;
            both_znode_failed = found_meta == NOTFOUND;
          } else {  // META (HBase 0.95 and up)
            found_meta = NOTFOUND;
            both_znode_failed = found_root == NOTFOUND;
          }
          if (both_znode_failed) {
            LOG.error("The znode for the -ROOT- region doesn't exist!");
            retryGetRootRegionLater();
          }
          return;
        } else if (rc != Code.OK.intValue()) {
          LOG.error("Looks like our ZK session expired or is broken, rc="
                    + rc + ": " + Code.get(rc));
          disconnectZK();
          connectZK();
          return;
        }
        if (data == null || data.length == 0 || data.length > Short.MAX_VALUE) {
          LOG.error("The location of the -ROOT- region in ZooKeeper is "
                    + (data == null || data.length == 0 ? "empty"
                       : "too large (" + data.length + " bytes!)"));
          retryGetRootRegionLater();
          return;  // TODO(tsuna): Add a watch to wait until the file changes.
        }

        final RegionClient client;
        if (is_root) {
          found_root = FOUND;
          client = handleRootZnode(data);
        } else {  // META (HBase 0.95 and up)
          found_meta = FOUND;
          client = handleMetaZnode(data);
        }

        if (client == null) {         // We failed to get a client.
          retryGetRootRegionLater();  // So retry later.
          return;
        }

        final ArrayList<Deferred<Object>> ds = atomicGetAndRemoveWaiters();
        if (ds != null) {
          for (final Deferred<Object> d : ds) {
            d.callback(client);
          }
        }

        disconnectZK();
        // By the time we're done, we may need to find -ROOT- again.  So
        // check to see if there are people waiting to find it again, and if
        // there are, re-open a new session with ZK.
        // TODO(tsuna): This typically happens when the address of -ROOT- in
        // ZK is stale.  In this case, we should setup a watch to get
        // notified once the znode gets updated, instead of continuously
        // polling ZK and creating new sessions.
        synchronized (ZKClient.this) {
          if (deferred_rootregion != null) {
            connectZK();
          }
        }
      }

      /** Returns a new client for the RS found in the root-region-server.  */
      @SuppressWarnings("fallthrough")
      protected RegionClient handleRootZnode(final byte[] data) {
        // There are 3 cases.  Older versions of HBase encode the location
        // of the root region as "host:port", 0.91 uses "host,port,startcode"
        // and newer versions of 0.91 use "<metadata>host,port,startcode"
        // where the <metadata> starts with MAGIC, then a 4 byte integer,
        // then that many bytes of meta data.
        boolean newstyle;     // True if we expect a 0.91 style location.
        final short offset;   // Bytes to skip at the beginning of data.
        short firstsep = -1;  // Index of the first separator (':' or ',').
        if (data[0] == MAGIC) {
          newstyle = true;
          final int metadata_length = Bytes.getInt(data, 1);
          if (metadata_length < 1 || metadata_length > 65000) {
            LOG.error("Malformed meta-data in " + Bytes.pretty(data)
                      + ", invalid metadata length=" + metadata_length);
            return null;  // TODO(tsuna): Add a watch to wait until the file changes.
          }
          offset = (short) (1 + 4 + metadata_length);
        } else {
          newstyle = false;  // Maybe true, the loop below will tell us.
          offset = 0;
        }
        final short n = (short) data.length;
        // Look for the first separator.  Skip the offset, and skip the
        // first byte, because we know the separate can only come after
        // at least one byte.
        loop: for (short i = (short) (offset + 1); i < n; i++) {
           switch (data[i]) {
            case ',':
              newstyle = true;
              /* fall through */
            case ':':
              firstsep = i;
              break loop;
          }
        }
        if (firstsep == -1) {
          LOG.error("-ROOT- location doesn't contain a separator"
                    + " (':' or ','): " + Bytes.pretty(data));
          return null;  // TODO(tsuna): Add a watch to wait until the file changes.
        }
        final String host;
        final short portend;  // Index past where the port number ends.
        if (newstyle) {
          host = new String(data, offset, firstsep - offset);
          short i;
          for (i = (short) (firstsep + 2); i < n; i++) {
            if (data[i] == ',') {
              break;
            }
          }
          portend = i;  // Port ends on the comma.
        } else {
          host = new String(data, 0, firstsep);
          portend = n;  // Port ends at the end of the array.
        }
        final int port = parsePortNumber(new String(data, firstsep + 1,
                                                    portend - firstsep - 1));
        final String ip = getIP(host);
        if (ip == null) {
          LOG.error("Couldn't resolve the IP of the -ROOT- region from "
                    + host + " in \"" + Bytes.pretty(data) + '"');
          return null;  // TODO(tsuna): Add a watch to wait until the file changes.
        }
        LOG.info("Connecting to -ROOT- region @ " + ip + ':' + port);
        has_root = true;
        final RegionClient client = rootregion = newClient(ip, port);
        return client;
      }

      /**
       * Returns a new client for the RS found in the meta-region-server.
       * This is used in HBase 0.95 and up.
       */
      protected RegionClient handleMetaZnode(final byte[] data) {
        if (data[0] != MAGIC) {
          LOG.error("Malformed META region meta-data in " + Bytes.pretty(data)
                    + ", invalid leading magic number: " + data[0]);
          return null;
        }

        final int metadata_length = Bytes.getInt(data, 1);
        if (metadata_length < 1 || metadata_length > 65000) {
          LOG.error("Malformed META region meta-data in " + Bytes.pretty(data)
                    + ", invalid metadata length=" + metadata_length);
          return null;  // TODO(tsuna): Add a watch to wait until the file changes.
        }
        short offset = (short) (1 + 4 + metadata_length);

        final int pbuf_magic = Bytes.getInt(data, offset);
        if (pbuf_magic != PBUF_MAGIC) {
          LOG.error("Malformed META region meta-data in " + Bytes.pretty(data)
                    + ", invalid magic number=" + pbuf_magic);
          return null;  // TODO(tsuna): Add a watch to wait until the file changes.
        }
        offset += 4;

        final String ip;
        final int port;
        try {
          final ZooKeeperPB.MetaRegionServer meta =
            ZooKeeperPB.MetaRegionServer.newBuilder()
            .mergeFrom(data, offset, data.length - offset).build();
          ip = getIP(meta.getServer().getHostName());
          port = meta.getServer().getPort();
        } catch (InvalidProtocolBufferException e) {
          LOG.error("Failed to parse the protobuf in " + Bytes.pretty(data), e);
          return null;  // TODO(tsuna): Add a watch to wait until the file changes.
        }

        LOG.info("Connecting to .META. region @ " + ip + ':' + port);
        has_root = false;
        final RegionClient client = rootregion = newClient(ip, port);
        return client;
      }

    }

    /**
     * Attempts to lookup the ROOT region (or META, if 0.95 and up).
     * @return true if a lookup was kicked off, false if not because we
     * weren't connected to ZooKeeper.
     */
    private boolean getRootRegion() {
      synchronized (this) {
        if (zk != null) {
          LOG.debug("Finding the -ROOT- or .META. region in ZooKeeper");
          final ZKCallback cb = new ZKCallback();
          zk.getData(base_path + "/root-region-server", this, cb, null);
          zk.getData(base_path + "/meta-region-server", this, cb, null);
          return true;
        }
      }
      return false;
    }

  }

  // --------------- //
  // Little helpers. //
  // --------------- //

  /**
   * Gets a hostname or an IP address and returns the textual representation
   * of the IP address.
   * <p>
   * <strong>This method can block</strong> as there is no API for
   * asynchronous DNS resolution in the JDK.
   * @param host The hostname to resolve.
   * @return The IP address associated with the given hostname,
   * or {@code null} if the address couldn't be resolved.
   */
  private static String getIP(final String host) {
    final long start = System.nanoTime();
    try {
      final String ip = InetAddress.getByName(host).getHostAddress();
      final long latency = System.nanoTime() - start;
      if (latency > 500000/*ns*/ && LOG.isDebugEnabled()) {
        LOG.debug("Resolved IP of `" + host + "' to "
                  + ip + " in " + latency + "ns");
      } else if (latency >= 3000000/*ns*/) {
        LOG.warn("Slow DNS lookup!  Resolved IP of `" + host + "' to "
                 + ip + " in " + latency + "ns");
      }
      return ip;
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve the IP of `" + host + "' in "
                + (System.nanoTime() - start) + "ns");
      return null;
    }
  }

  /**
   * Parses a TCP port number from a string.
   * @param portnum The string to parse.
   * @return A strictly positive, validated port number.
   * @throws NumberFormatException if the string couldn't be parsed as an
   * integer or if the value was outside of the range allowed for TCP ports.
   */
  private static int parsePortNumber(final String portnum)
    throws NumberFormatException {
    final int port = Integer.parseInt(portnum);
    if (port <= 0 || port > 65535) {
      throw new NumberFormatException(port == 0 ? "port is zero" :
                                      (port < 0 ? "port is negative: "
                                       : "port is too large: ") + port);
    }
    return port;
  }

}
