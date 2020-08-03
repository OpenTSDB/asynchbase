/*
 * Copyright (C) 2012-2020 The Async HBase Authors.  All rights reserved.
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

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.cache.CacheStats;
import com.google.common.collect.Maps;

/**
 * {@link HBaseClient} usage statistics.
 * <p>
 * This is an immutable snapshot of usage statistics of the client.
 * Please note that not all the numbers in the snapshot are collected
 * atomically, so although each individual number is up-to-date as of
 * the time this object is created, small inconsistencies between numbers
 * can arise.
 * @since 1.3
 */
public final class ClientStats {

  private final long num_connections_created;
  private final long root_lookups_with_permit;
  private final long root_lookups_wo_permit;
  private final long root_lookups_blocked;
  private final long meta_lookups_with_permit;
  private final long meta_lookups_wo_permit;
  private final long meta_lookups_blocked;
  private final long num_flushes;
  private final long num_nsres;
  private final long num_nsre_rpcs;
  private final long num_multi_rpcs;
  private final long num_gets;
  private final long num_scanners_opened;
  private final long num_scans;
  private final long num_puts;
  private final long num_appends;
  private final long num_row_locks;
  private final long num_deletes;
  private final long num_atomic_increments;
  private final CacheStats increment_buffer_stats;
  private final long inflight_rpcs;
  private final long pending_rpcs;
  private final long pending_batched_rpcs;
  private final int dead_region_clients;
  private final int region_clients;
  private final long idle_connections_closed;
  private final ExtendedStats extended_stats;

  /** Package-private constructor.  */
  ClientStats(final long num_connections_created,
              final long root_lookups_with_permit,
              final long root_lookups_wo_permit,
              final long root_lookups_blocked,
              final long meta_lookups_with_permit,
              final long meta_lookups_wo_permit,
              final long meta_lookups_blocked,
              final long num_flushes,
              final long num_nsres,
              final long num_nsre_rpcs,
              final long num_multi_rpcs,
              final long num_gets,
              final long num_scanners_opened,
              final long num_scans,
              final long num_puts,
              final long num_appends,
              final long num_row_locks,
              final long num_deletes,
              final long num_atomic_increments,
              final CacheStats increment_buffer_stats,
              final long rpcs_inflight,
              final long pending_rpcs,
              final long pending_batched_rpcs,
              final int dead_region_clients,
              final int region_clients,
              final long idle_connections_closed,
              final ExtendedStats extended_stats) {
    // JAVA Y U NO HAVE CASE CLASS LIKE SCALA?!  FFFFFUUUUUUU!!
    this.num_connections_created = num_connections_created;
    this.root_lookups_with_permit = root_lookups_with_permit;
    this.root_lookups_wo_permit = root_lookups_wo_permit;
    this.root_lookups_blocked = root_lookups_blocked;
    this.meta_lookups_with_permit = meta_lookups_with_permit;
    this.meta_lookups_wo_permit = meta_lookups_wo_permit;
    this.meta_lookups_blocked = meta_lookups_blocked;
    this.num_flushes = num_flushes;
    this.num_nsres = num_nsres;
    this.num_nsre_rpcs = num_nsre_rpcs;
    this.num_multi_rpcs = num_multi_rpcs;
    this.num_gets = num_gets;
    this.num_scanners_opened = num_scanners_opened;
    this.num_scans = num_scans;
    this.num_puts = num_puts;
    this.num_appends = num_appends;
    this.num_row_locks = num_row_locks;
    this.num_deletes = num_deletes;
    this.num_atomic_increments = num_atomic_increments;
    this.increment_buffer_stats = increment_buffer_stats;
    this.inflight_rpcs = rpcs_inflight;
    this.pending_rpcs = pending_rpcs;
    this.pending_batched_rpcs = pending_batched_rpcs;
    this.dead_region_clients = dead_region_clients;
    this.region_clients = region_clients;
    this.idle_connections_closed = idle_connections_closed;
    this.extended_stats = extended_stats;
  }

  /** Number of connections created to connect to RegionServers.  */
  public long connectionsCreated() {
    return num_connections_created;
  }

  /**
   * Returns the number of connections to region servers that were closed
   * due to being idle past one of
   * "hbase.hbase.ipc.client.connection.idle_timeout",
   * "hbase.ipc.client.connection.idle_read_timeout", or
   * "hbase.ipc.client.connection.idle_write_timeout"
   * value. 
   * @return The number of idle connections over time
   * @since 1.7
   */
  public long idleConnectionsClosed() {
    return idle_connections_closed;
  }
  
  /**
   * Returns how many lookups in {@code -ROOT-} were performed <b>with a permit</b>.
   * <p>
   * This number should remain low.  It will be 1 after the first access to
   * HBase, and will increase by 1 each time the {@code .META.} region moves
   * to another server, which should seldom happen.
   * <p>
   * This isn't to be confused with the number of times we looked up where
   * the {@code -ROOT-} region itself is located.  This happens even more
   * rarely and a message is logged at the INFO whenever it does.
   */
  public long rootLookups() {
    return root_lookups_with_permit;
  }
  
  /** @return The number of lookups against root exceeded the semaphore limit. */
  public long contendedRootLookups() {
    return root_lookups_wo_permit;
  }
  
  /** @return The number of root lookups that were blocked. */
  public long blockedRootLookups() {
    return root_lookups_blocked;
  }

  /**
   * Returns how many lookups in {@code .META.} were performed (uncontended).
   * <p>
   * This number indicates how many times we had to lookup in {@code .META.}
   * where a key was located.  This only counts "uncontended" lookups, where
   * the thread was able to acquire a "permit" to do a {@code .META.} lookup.
   * The majority of the {@code .META.} lookups should fall in this category.
   */
  public long uncontendedMetaLookups() {
    return meta_lookups_with_permit;
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
   */
  public long contendedMetaLookups() {
    return meta_lookups_wo_permit;
  }

  /** @return The number of meta lookups that were blocked. */
  public long metaLookupsBlocked() {
    return meta_lookups_blocked;
  }
  
  /** Number of calls to {@link HBaseClient#flush}.  */
  public long flushes() {
    return num_flushes;
  }

  /**
   * Number of {@code NoSuchRegionException} handled by the client.
   * <p>
   * The {@code NoSuchRegionException} is an integral part of the way HBase
   * work.  HBase clients keep a local cache of where they think each region
   * is in the cluster, but in practice things aren't static, and regions will
   * move due to load balancing, or get split into two new regions due to
   * write activity.  When this happens, clients find out "the hard way" that
   * their RPC failed because the region they tried to get to is no longer
   * there.  This causes the client to invalidate its local cache entry for
   * this region and perform a {@code .META.} lookup to find where this region
   * has moved, or find the new region to use in case of a split.
   * <p>
   * While {@code NoSuchRegionException} are expected to happen due to load
   * balancing or write load, they tend to have a large performance impact as
   * they force the clients to back off and repeatedly poll the cluster to
   * find the new location of the region.  So it's good to keep an eye on the
   * rate at which they happen to make sure it remains fairly constant and
   * low.
   * <p>
   * In a high write throughput application, if this value increases too
   * quickly it typically indicates that there are too few regions, so splits
   * are happening too often.  In this case you should manually split the hot
   * regions in order to better distribute the write load.
   * <p>
   * This number is a subset of {@link #numRpcDelayedDueToNSRE}, because each
   * {@code NoSuchRegionException} causes multiple RPCs to be delayed.
   */
  public long noSuchRegionExceptions() {
    return num_nsres;
  }

  /**
   * Number of RPCs delayed due to {@code NoSuchRegionException}s.
   * <p>
   * In a high throughput application, if this value increases too quickly
   * it typically indicates that there are too few regions, or that some
   * regions are too hot, which is causing too many RPCs to back up when
   * a region becomes temporarily unavailable.  In this case you should
   * manually split the hot regions in order to better distribute the write
   * load.
   * @see #noSuchRegionExceptions
   */
  public long numRpcDelayedDueToNSRE() {
    return num_nsre_rpcs;
  }

  /**
   * Number of batched RPCs sent to the network.
   * <p>
   * While {@link #puts} and {@link #deletes} indicate the number of RPCs
   * created at the application level, they don't reflect the actual number of
   * RPCs sent to the network because of batching (see
   * {@link HBaseClient#setFlushInterval}).
   * <p>
   * Note that {@link #deletes} can only be batched if you use HBase 0.92 or
   * above.
   */
  public long numBatchedRpcSent() {
    return num_multi_rpcs;
  }

  /** Number of calls to {@link HBaseClient#get}.  */
  public long gets() {
    return num_gets;
  }

  /** Number of scanners opened.  */
  public long scannersOpened() {
    return num_scanners_opened;
  }

  /** Number of times a scanner had to fetch data from HBase.  */
  public long scans() {
    return num_scans;
  }

  /**
   * Number calls to {@link HBaseClient#put}.
   * <p>
   * Note that this doesn't necessarily reflect the number of RPCs sent to
   * HBase due to batching (see {@link HBaseClient#setFlushInterval}).
   * @see #numBatchedRpcSent
   */
  public long puts() {
    return num_puts;
  }

  /**
   * Number calls to {@link HBaseClient#append}.
   * <p>
   * Note that this doesn't necessarily reflect the number of RPCs sent to
   * HBase due to batching (see {@link HBaseClient#setFlushInterval}).
   * @see #numBatchedRpcSent
   */
  public long appends() {
    return num_appends;
  }
  
  /** Number calls to {@link HBaseClient#lockRow}.  */
  public long rowLocks() {
    return num_row_locks;
  }

  /**
   * Number calls to {@link HBaseClient#delete}.
   * <p>
   * Note that if you use HBase 0.92 or above, this doesn't necessarily
   * reflect the number of RPCs sent to HBase due to batching (see
   * {@link HBaseClient#setFlushInterval}).
   * @see #numBatchedRpcSent
   */
  public long deletes() {
    return num_deletes;
  }

  /**
   * Number of {@link AtomicIncrementRequest} sent.
   * <p>
   * This number includes {@link AtomicIncrementRequest}s sent after being
   * buffered by {@link HBaseClient#bufferAtomicIncrement}.  The number of
   * evictions returned by {@link #incrementBufferStats} is a subset of this
   * number, and the difference between the two is the number of increments
   * that were sent directly without being buffered.
   */
  public long atomicIncrements() {
    return num_atomic_increments;
  }

  /**
   * Represents the number of RPCs that have been sent to the region client
   * and are currently waiting for a response. If this value increases then the
   * region server is likely overloaded.
   * @return the number of RPCs sent to region client waiting for response.
   * @since 1.7
   */
  public long inflightRPCs() {
    return inflight_rpcs;
  }
  
  /**
   * The number of RPCs that are queued up and ready to be sent to the region
   * server. When an RPC is sent, this number should be decremented and 
   * {@code inflightRPCs} incremented.
   * @return the number of RPCs queued and ready to be sent to region server.
   * @since 1.7
   */
  public long pendingRPCs() {
    return pending_rpcs;
  }
  
  /**
   * The number of batched RPCs waiting to be sent to the server.
   * @return the number of batched RPCs waiting to be sent to server.
   * @since 1.7
   */
  public long pendingBatchedRPCs() {
    return pending_batched_rpcs;
  }
  
  /**
   * The number of region clients that have lost their connection to the region
   * server.
   * @return the number of region clients that have lost connection to region
   * server.
   * @since 1.7
   */
  public int deadRegionClients() {
    return dead_region_clients;
  }
  
  /**
   * The number of instantiated region clients.
   * @return the number of instantiated region clients.
   * @since 1.7
   */
  public int regionClients() {
    return region_clients;
  }
  
  /** Returns statistics from the buffer used to coalesce increments.  */
  public CacheStats incrementBufferStats() {
    return increment_buffer_stats;
  }

  /** @return The extended stats. May be null if not set.
   * @since 1.9 */
  public ExtendedStats extendedStats() {
    return extended_stats;
  }
  
  /**
   * Extended stats around the client.
   * @since 1.9
   */
  public static class ExtendedStats {
    private final long bytes_read;
    private final long bytes_written;
    private final long rpcs_aged_out;
    private final Map<String, Long> exception_counters;
    
    public ExtendedStats(final long bytes_read,
                         final long bytes_written,
                         final long rpcs_aged_out,
                         final Map<Class<?>, Counter> exception_counters) {
      this.bytes_read = bytes_read;
      this.bytes_written = bytes_written;
      this.rpcs_aged_out = rpcs_aged_out;
      
      if (exception_counters != null) {
        this.exception_counters = Maps.newHashMapWithExpectedSize(exception_counters.size());
        for (final Entry<Class<?>, Counter> entry : exception_counters.entrySet()) {
          this.exception_counters.put(entry.getKey().getSimpleName(), 
              entry.getValue().get());
        }
      } else {
        this.exception_counters = null;
      }
    }
    
    /**
     * The total number of bytes read since the existence of this client.
     * @return The total number of bytes read off the socket.
     */
    public long bytesRead() {
      return bytes_read;
    }
    
    /**
     * The total number of bytes written since the existence of this client.
     * @return The total number of bytes written to the socket.
     */
    public long bytesWritten() {
      return bytes_written;
    }
    
    /** @return The number of RPCs killed because of their age. */
    public long rpcsAgedOut() {
      return rpcs_aged_out;
    }
    
    /** @return A map of exception class names to the total number of occurrences
     * since the client started. */
    public Map<String, Long> exceptionCounters() {
      return exception_counters;
    }
    
  }
}
