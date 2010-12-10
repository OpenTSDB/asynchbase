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
   * Internal implementation documentation
   * -------------------------------------
   *
   * This class is the heart of the HBase client.  Being an HBase client
   * essentially boils down to keeping track of where regions are and
   * sending RPCs to the right RegionServer.  RegionServers don't know
   * much about tables, they only care about regions.  In order to be
   * efficient (read: usable), a good HBase client *must* cache where
   * each region is found to be (until this knowledge is invalidated).
   *
   * As the user of this class uses HBaseClient with different tables and
   * different rows, this HBaseClient will slowly build a local cached copy
   * of the -ROOT- and .META. tables.  It's like for virtual memory: the
   * first time you access a page, there's a TLB miss, and you need to go
   * through (typically) a 2-layer page table to find the actual address,
   * which is then cached in the TLB before the faulty instruction gets
   * restarted.  Well, we do pretty much the same thing here, except at
   * an abstraction level that's a bazillion light years away from the TLB.
   *
   * TODO(tsuna): Address the following.
   *
   * - Properly handle disconnects.
   *    - Attempt to reconnect a couple of times, see if it was a transient
   *      network blip.
   *    - If the -ROOT- region is unavailable when we start, we should
   *      put a watch in ZK instead of polling it every second.
   * - Use a global Timer (Netty's HashedWheelTimer is good) to handle all
   *   the timed tasks.
   *     - Exponential backoff for things like reconnecting, retrying to
   *       talk to ZK to find the -ROOT- region and such.
   *     - Handling RPC timeouts.
   * - Proper retry logic in various places:
   *     - Retry to find -ROOT- when -ROOT- is momentarily dead.
   *     - Don't retry to find a region forever, if there's a hole in .META.
   *       we'll simply never find that region.
   *     - I don't think NSREs are properly handled everywhere, e.g. when
   *       opening a scanner.
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

  private static final byte[] ROOT = new byte[] { '-', 'R', 'O', 'O', 'T', '-' };
  private static final byte[] ROOT_REGION = new byte[] { '-', 'R', 'O', 'O', 'T', '-', ',', ',', '0' };
  private static final byte[] META = new byte[] { '.', 'M', 'E', 'T', 'A', '.' };
  private static final byte[] INFO = new byte[] { 'i', 'n', 'f', 'o' };
  private static final byte[] REGIONINFO = new byte[] { 'r', 'e', 'g', 'i', 'o', 'n', 'i', 'n', 'f', 'o' };
  private static final byte[] SERVER = new byte[] { 's', 'e', 'r', 'v', 'e', 'r' };

  /**
   * Executor with which we create all our threads.
   * TODO(tsuna): Get it through the ctor to share it with others.
   */
  private final Executor executor = Executors.newCachedThreadPool();

  /**
   * Timer we use to handle all our timeouts.
   * <p>
   * This is package-private so that this timer can easily be shared with the
   * other classes in this package.
   * TODO(tsuna): Get it through the ctor to share it with others.
   */
  final Timer timer = new HashedWheelTimer(20, MILLISECONDS);

  /** Up to how many milliseconds can we buffer an edit on the client side.  */
  private volatile short flush_interval = 1000;  // ms

  /**
   * Factory through which we will create all its channels / sockets.
   */
  private final NioClientSocketChannelFactory channel_factory
    = new NioClientSocketChannelFactory(executor, executor);;

  /** Watcher to keep track of the -ROOT- region in ZooKeeper.  */
  private final ZKClient zkclient;

  /**
   * The client currently connected to the -ROOT- region.
   * If this is {@code null} then we currently don't know where the -ROOT-
   * region is and we're waiting for a notification from ZooKeeper to tell
   * us where it is.
   */
  private volatile RegionClient rootregion;

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
    zkclient = new ZKClient(quorum_spec, base_path);
  }

  /**
   * Flushes to HBase any buffered client-side write operation.
   * <p>
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  public Deferred<Object> flush() {
    final ArrayList<Deferred<Object>> d =
      new ArrayList<Deferred<Object>>(client2regions.size()
                                      + got_nsre.size() * 8);
    // Bear in mind that we're traversing a ConcurrentHashMap, so we may get
    // clients that have been removed from the map since we started iterating.
    for (final RegionClient client : client2regions.keySet()) {
      d.add(client.flush());
    }
    for (final ArrayList<HBaseRpc> nsred : got_nsre.values()) {
      synchronized (nsred) {
        for (final HBaseRpc rpc : nsred) {
          // TODO(tsuna): This is brittle, need to remember to edit this when
          // adding new RPCs that change data in HBase.  Not good.
          if (rpc instanceof PutRequest
              || rpc instanceof AtomicIncrementRequest
              || rpc instanceof DeleteRequest) {
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
    if (flush_interval < 0) {
      throw new IllegalArgumentException("Negative: " + flush_interval);
    }
    try {
      return this.flush_interval;
    } finally {
      this.flush_interval = flush_interval;
    }
  }

  /**
   * Returns the maximum time (in milliseconds) for which edits can be buffered.
   * <p>
   * The default value is an unspecified and implementation dependant, but is
   * guaranteed to be non-zero.
   * @see #setFlushInterval
   */
  public short getFlushInterval() {
    return flush_interval;
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
        // This terminates the Executor.  TODO(tsuna): Don't do this if
        // the executor doesn't belong to us (right now it always does).
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
      final class RetryShutdown implements Callback<Object, Object> {
        public Object call(final Object arg) {
          shutdown();
          return arg;
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
            ip2client.clear();
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
    final GetRequest dummy = new GetRequest(table, EMPTY_ARRAY);
    if (family != EMPTY_ARRAY) {
      dummy.family(family);
    }
    @SuppressWarnings("unchecked")
    final Deferred<Object> d = (Deferred) get(dummy);
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
   * @return A deferred scanner ID.
   */
  Deferred<Long> openScanner(final Scanner scanner) {
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
  private static final Callback<Long, Object> scanner_opened =
    new Callback<Long, Object>() {
      public Long call(final Object response) {
        if (response instanceof Long) {
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
   * Package-private access point for {@link Scanner}s to scan more rows.
   * @param scanner The scanner to use.
   * @param nrows The maximum number of rows to retrieve.
   * @return A deferred row.
   */
  Deferred<Object> scanNextRows(final Scanner scanner) {
    final RegionInfo region = scanner.currentRegion();
    final RegionClient client = (region == null ? null
                                 : region2client.get(region));
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
    final RegionClient client = (region == null ? null
                                 : region2client.get(region));
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
    return sendRpcToRegion(request).addCallbacks(icv_done,
                                                 Callback.PASSTHROUGH);
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
    return sendRpcToRegion(request);
  }

  /**
   * Acquires an explicit row lock.
   * <p>
   * For a description of what row locks are, see {@link RowLock}.
   * @param request The request specify which row to lock.
   * @return a deferred {@link RowLock}.
   * @see #unlockRow
   */
  public Deferred<RowLock> lockRow(final RowLockRequest request) {
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
    final RegionClient client = (region == null ? null
                                 : region2client.get(region));
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
    return sendRpcToRegion(request);
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
    final byte[] table = request.table();
    final byte[] key = request.key();
    final RegionInfo region = getRegion(table, key);

    final class RetryRpc implements Callback<Deferred<Object>, Object> {
      public Deferred<Object> call(final Object arg) {
        if (arg instanceof NonRecoverableException) {
          return Deferred.fromError((NonRecoverableException) arg);
        }
        return sendRpcToRegion(request);
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
        final Deferred<Object> d = request.getDeferred()
          .addBothDeferring(new RetryRpc());
        handleNSRE(request, region.name(), nsre);
        return d;
      }
      final RegionClient client = (Bytes.equals(region.table(), ROOT)
                                   ? rootregion : region2client.get(region));
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
   * Checks whether or not an RPC can be retried once more.
   * @param rpc The RPC we're going to attempt to execute.
   * @return {@code true} if this RPC already had too many attempts,
   * {@code false} otherwise (in which case it's OK to retry once more).
   * @throws NonRecoverableException if the request has had too many attempts
   * already.
   */
  static boolean cannotRetryRequest(final HBaseRpc rpc) {
    return rpc.attempt > 10;  // XXX Don't hardcode.
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
    final RegionInfo meta_region = (is_meta || is_root
                                    ? null : getRegion(META, meta_key));

    if (meta_region != null) {
      // Lookup in .META. which region server has the region we want.
      final RegionClient client = region2client.get(meta_region);
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
          client.getClosestRowBefore(meta_region, META, meta_key, INFO)
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
    // Alright so we don't even know where to look in .META.
    // Let's lookup the right .META. entry in -ROOT-.
    final byte[] root_key = createRegionSearchKey(META, meta_key);
    final RegionInfo root_region = new RegionInfo(ROOT, ROOT_REGION,
                                                  EMPTY_ARRAY);
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
    if (Bytes.equals(table, ROOT)) {
      return new RegionInfo(ROOT, ROOT_REGION, EMPTY_ARRAY);
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
      throw new BrokenMetaException(null, "It didn't contain any"
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
    // this happens.  After we release the lock, the it will find it's dead.
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
      ArrayList<RegionInfo> regions = client2regions.get(client);
      if (regions == null) {
        final ArrayList<RegionInfo> newlist = new ArrayList<RegionInfo>();
        regions = client2regions.putIfAbsent(client, newlist);
        if (regions == null) {   // We've just put `newlist'.
          regions = newlist;
        }
      }
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
    if (region_name == ROOT_REGION) {
      if (reason != null) {
        LOG.info("Invalidated cache for -ROOT- as " + rootregion
                 + ' ' + reason);
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
  private static final short NSRE_LOW_WATERMARK  =  1000;
  private static final short NSRE_HIGH_WATERMARK = 10000;

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
                  final NotServingRegionException e) {
    final boolean can_retry_rpc = !cannotRetryRequest(rpc);
    boolean known_nsre = true;  // We already aware of an NSRE for this region?
    ArrayList<HBaseRpc> nsred_rpcs = got_nsre.get(region_name);
    HBaseRpc exists_rpc = null;  // Our "probe" RPC.
    if (nsred_rpcs == null) {  // Looks like this could be a new NSRE...
      final ArrayList<HBaseRpc> newlist = new ArrayList<HBaseRpc>(64);
      exists_rpc = GetRequest.exists(rpc.table, rpc.key);
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
            exists_rpc = GetRequest.exists(rpc.table, rpc.key);
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
                  exists_rpc = GetRequest.exists(rpc.table, rpc.key);
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
            + Bytes.pretty(region_name) + " to come back online", e, rpc));
        }
        return;  // This NSRE is already known and being handled.
      }
    }

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
            LOG.debug("Retrying " + rpcs.size() + " RPCs now that the NSRE on "
                      + Bytes.pretty(region_name) + " seems to have cleared");
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
    timer.newTimeout(new NSRETimer(), wait_ms, MILLISECONDS);
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
      chan = channel_factory.newChannel(new RegionClientPipeline());
      client = chan.getPipeline().get(RegionClient.class);
      ip2client.put(hostport, client);  // This is guaranteed to return null.
    }
    // Configure and connect the channel without locking ip2client.
    final SocketChannelConfig config = chan.getConfig();
    config.setConnectTimeoutMillis(5000);
    config.setTcpNoDelay(true);
    config.setKeepAlive(true);  // TODO(tsuna): Is this really needed?
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
      // "hello" handler will remove itself from the pipeline after 1st RPC.
      super.addLast("hello", RegionClient.SayHelloFirstRpc.INSTANCE);
      super.addLast("handler", new RegionClient(HBaseClient.this));
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
      LOG.info("Lost connection with the -ROOT- region");
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
  private final class ZKClient implements Watcher {

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
        if (deferred_rootregion == null) {
          LOG.info("Need to find the -ROOT- region");
          deferred_rootregion = new ArrayList<Deferred<Object>>();
        }
        connectZK();  // Kick off a connection if needed.
        deferred_rootregion.add(d);
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

    /**
     * Puts a watch in ZooKeeper to monitor the file of the -ROOT- region.
     * This method just registers an asynchronous callback.
     */
    private void getRootRegion() {
      final AsyncCallback.DataCallback cb = new AsyncCallback.DataCallback() {
        public void processResult(final int rc, final String path,
                                  final Object ctx, final byte[] data,
                                  final Stat stat) {
          if (rc == Code.NONODE.intValue()) {
            LOG.error("The znode for the -ROOT- region doesn't exist!");
            final DataCallback dcb = this;
            timer.newTimeout(new TimerTask() {
                public void run(final Timeout timeout) {
                  final ZooKeeper zk = ZKClient.this.zk;
                  if (zk != null) {
                    LOG.debug("Retrying to find the -ROOT- region in ZooKeeper");
                    zk.getData(base_path + "/root-region-server",
                               ZKClient.this, dcb, null);
                  } else {
                    connectZK();
                  }
                }
              }, 1000, MILLISECONDS);
            return;
          } else if (rc != Code.OK.intValue()) {
            LOG.error("Looks like our ZK session expired or is broken, rc="
                      + rc + ": " + Code.get(rc));
            disconnectZK();
            connectZK();
            return;
          }
          final String root = new String(data);
          final int portsep = root.lastIndexOf(':');
          if (portsep < 0) {
            LOG.error("Couldn't find the port of the -ROOT- region in \""
                      + root + '"');
            return;  // TODO(tsuna): Add a watch to wait until the file changes.
          }
          final String host = getIP(root.substring(0, portsep));
          if (host == null) {
            LOG.error("Couldn't resolve the IP of the -ROOT- region in \""
                      + root + '"');
            return;  // TODO(tsuna): Add a watch to wait until the file changes.
          }
          final int port = parsePortNumber(root.substring(portsep + 1));
          final String hostport = host + ':' + port;
          LOG.info("Connecting to -ROOT- region @ " + hostport);
          final RegionClient client = rootregion = newClient(host, port);
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
      };

      synchronized (this) {
        if (zk != null) {
          LOG.debug("Finding the -ROOT- region in ZooKeeper");
          zk.getData(base_path + "/root-region-server", this, cb, null);
        }
      }
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
