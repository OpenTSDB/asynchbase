/*
 * Copyright (C) 2012  The Async HBase Authors.  All rights reserved.
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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Deferred;

/**
 * Package-private class to uniquely identify a buffered atomic increment.
 * @since 1.3
 */
final class BufferedIncrement {

  private final byte[] table;
  private final byte[] key;
  private final byte[] family;
  private final byte[] qualifier;

  BufferedIncrement(final byte[] table, final byte[] key,
                    final byte[] family, final byte[] qualifier) {
    this.table = table;
    this.key = key;
    this.family = family;
    this.qualifier = qualifier;
  }

  public boolean equals(final Object other) {
    if (other == null || !(other instanceof BufferedIncrement)) {
      return false;
    }
    final BufferedIncrement incr = (BufferedIncrement) other;
    // Compare fields most likely to be different first.
    return Bytes.equals(qualifier, incr.qualifier)
      && Bytes.equals(key, incr.key)
      && Bytes.equals(family, incr.family)
      && Bytes.equals(table, incr.table);
  }

  public int hashCode() {
    return
      Arrays.hashCode(table) + 41 * (
        Arrays.hashCode(key) + 41 * (
          Arrays.hashCode(family) + 41 * (
            Arrays.hashCode(qualifier) + 41
          )
        )
      );
  }

  public String toString() {
    final StringBuilder buf =
      new StringBuilder(52 + table.length + key.length * 2 + family.length
                        + qualifier.length);
    buf.append("BufferedIncrement(table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(')');
    return buf.toString();
  }

  /** Increment amount.  */
  static final class Amount extends AtomicInteger {
    final Deferred<Long> deferred = new Deferred<Long>();

    public String toString() {
      return "Amount(" + super.get() + ", " + deferred + ")";
    }

    private static final long serialVersionUID = 1333868942;
  }

  /**
   * Creates a new cache for buffered increments.
   * @param client The client to work with.
   * @param size Max number of entries of the cache.
   */
  static LoadingCache<BufferedIncrement, Amount>
    newCache(final HBaseClient client, final int size) {
    final int ncpu = Runtime.getRuntime().availableProcessors();
    return CacheBuilder.newBuilder()
      // Beef up the concurrency level as this is the number of internal
      // segments used by the hash map.  The default is 4, which is not enough
      // for us as we typically have more than that many threads concurrently
      // accessing the map.  Because Guava's LocalCache maintains a
      // per-segment buffer of access operations not yet committed, having a
      // few more segments than we actually need helps increase the number of
      // read operations we can do on a segment of the map with no interleaved
      // writes before the segment has to acquire the lock to flush the buffer.
      // We can't control this otherwise, because it's a hard-coded constant
      // in LocalCache.DRAIN_THRESHOLD = 0x3F = 63;
      // In addition, through benchmarking on a heavily contended cache, I saw
      // that increasing the number of segments (4x to 8x number of hardware
      // threads) can help significantly boost overall throughput.
      .concurrencyLevel(ncpu * 4)
      .maximumSize(size)
      .recordStats()  // As of Guava 12, stats are disabled by default.
      .removalListener(new EvictionHandler(client))
      .build(LOADER);
  }

  /** Creates new zero-Amount for new BufferedIncrements.  */
  static final class Loader extends CacheLoader<BufferedIncrement, Amount> {

    @Override
    public Amount load(final BufferedIncrement key) {
      return new Amount();
    }

  }

  /** Singleton instance.  */
  private static final Loader LOADER = new Loader();

  /** Handles cache evictions (also called on flushes).  */
  private static final class EvictionHandler
    implements RemovalListener<BufferedIncrement, Amount> {

    private final HBaseClient client;

    EvictionHandler(final HBaseClient client) {
      this.client = client;
    }

    @Override
    public void onRemoval(final RemovalNotification<BufferedIncrement, Amount> entry) {
      final Amount amount = entry.getValue();
      // This trick with MIN_VALUE is what makes this whole increment
      // coalescing work completely without locks.  The difficulty here is
      // that another thread might be trying to increment the value at the
      // same time we're trying to flush it.  By making the value hugely
      // negative, we allow the other thread to notice that after adding its
      // own positive delta, the value has become negative, which is otherwise
      // impossible.  This makes the other thread realize that it lost the
      // race with us and that it needs to retry.  Upon retrying it will cause
      // the insertion of a new entry in the cache.
      // The consequences of this trick are twofold:
      //   - We can't support negative increments, since we give a special
      //     meaning to negative values.
      //   - We can't allow large positive increments that could cause the
      //     integer to wrap around.  Since we the maximum increment amount
      //     we allow per call is Short.MAX_VALUE, it would take
      //     (2^31 - 1) / (2^15 - 1) = 65538 increments of Short.MAX_VALUE
      //     before a flush happens to cause an overflow.  Even if this was to
      //     happen, when the increment code detects a negative value, it
      //     undoes its increment (which would cause an underflow back to a
      //     positive value).
      final int delta = amount.getAndSet(Integer.MIN_VALUE);
      final BufferedIncrement incr = entry.getKey();
      if (delta >= 0) {
        final AtomicIncrementRequest req =
          new AtomicIncrementRequest(incr.table, incr.key, incr.family,
                                     incr.qualifier, delta);
        client.atomicIncrement(req).chain(amount.deferred);
      } else {
        LoggerFactory.getLogger(EvictionHandler.class).error("WTF?  Should"
          + " never happen: negative delta " + delta + " in " + incr);
      }
    }

  }

  static final CacheStats ZERO_STATS = new CacheStats(0, 0, 0, 0, 0, 0);

}
