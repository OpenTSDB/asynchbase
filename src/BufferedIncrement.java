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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

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

  /**
   * Atomic increment amount.
   * <p>
   * This behaves like a signed 49 bit atomic integer that
   * can only be incremented/decremented a specific number
   * of times.  The {@link #update} method must be used
   * for all increment/decrement operations, as the underlying
   * raw value of the {@code long} from {@link AtomicLong} is
   * not the value of the counter, as some of its bits are
   * reserved to keep track of how many times the value
   * got changed.
   * <p>
   * Implementation details:<br/>
   * The first 49 most significant bits are the value of
   * the amount (including 1 bit for the sign), the last
   * 15 least significant bits are the number of times
   * left that {@link #update} can be called.
   * <p>
   * Again, this class opportunistically inherits from
   * {@link AtomicLong}, but <strong>don't call methods
   * from the parent class directly</strong>.
   */
  static final class Amount extends AtomicLong {

    /** Number of least-significant bits (LSB) we reserve to track updates.  */
    private final static int UPDATE_BITS = 15;
    /** Mask used to retrieve number of updates left (0x0000000000007FFFL).  */
    private final static long UPDATE_MASK = (1L << UPDATE_BITS) - 1;
    /** Mask used to get value bits we can't store (0xFFFF000000000000L).  */
    private final static long OVERFLOW_MASK = (UPDATE_MASK << (64 - UPDATE_BITS)
                                               >> 1); // Reserve the sign bit.

    /** Everyone waiting for this increment is queued up here.  */
    final Deferred<Long> deferred = new Deferred<Long>();

    /**
     * Creates a new atomic amount.
     * @param max_updates The maximum number of times {@link #update}
     * can be called.  Beyond this number of calls, the method will return
     * {@code false}.
     */
    Amount(final short max_updates) {
      super(max_updates);
      assert max_updates > 0 : "WTF: max_updates=" + max_updates;
    }

    /**
     * Atomically updates this amount.
     * @param delta The delta by which to increment the value.
     * Of course, if the delta value is negative, the value will be
     * decremented instead.
     * @return {@code true} if the update could be done, {@code false} if
     * it couldn't due to an overflow/underflow or due to reaching the
     * maximum number of times this Amount could be incremented.
     */
    final boolean update(final long delta) {
      while (true) {
        final long current = super.get();
        final int updates = numUpdatesLeft(current);
        if (updates == 0) {
          return false;  // Already too many increments.
        }
        final long new_amount = amount(current) + delta;
        if (!checkOverflow(new_amount)) {
          return false;  // Overflow, new amount doesn't fit on 49 bits.
        }
        final long next = (new_amount << UPDATE_BITS) | (updates - 1);
        if (super.compareAndSet(current, next)) {
          return true;
        }
        // else: CAS failed, loop again.
      }
    }

    /**
     * Atomically sets the number of updates left to 0 and return the value.
     * @return The raw value, not the amount by which to increment.
     * To get the raw value, use {@link #amount} on the value returned.
     */
    final long getRawAndInvalidate() {
      while (true) {
        final long current = super.get();
        // Technically, we could leave the whole value set to 0 here, but
        // to help when debugging with toString(), we restore the amount.
        final long next = amount(current) << UPDATE_BITS;
        if (super.compareAndSet(current, next)) {  // => sets updates left to 0.
          return current;
        }
        // else: CAS failed, loop again.
      }
    }

    /** The amount by which we're going to increment the value in HBase.  */
    static final long amount(final long n) {
      return n >> UPDATE_BITS;
    }

    /** The number of times left that this amount can be updated.  */
    static final int numUpdatesLeft(final long n) {
      return (int) (n & UPDATE_MASK);
    }

    /**
     * Returns {@code true} if the given value can fit without overflowing.
     */
    static final boolean checkOverflow(final long value) {
      // If the amount was positive, then the MSBs must remain 0.  Any 1 in
      // the MSBs would indicate that the value has become too big or has
      // become negative due to an overflow.  Similarly, if the amount was
      // negative, then MSBs must remain 1.
      final long masked = value & OVERFLOW_MASK;
      return masked == 0 || masked == OVERFLOW_MASK;
    }

    public String toString() {
      final long n = super.get();
      return "Amount(" + amount(n) + ", "
        + numUpdatesLeft(n) + " updates left, " + deferred + ')';
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

    /**
     * Max number of increments/decrements per counter before we force-flush.
     * This limit is comes from the max callback chain length on a Deferred.
     */
    private static final short MAX_UPDATES = 16383;

    @Override
    public Amount load(final BufferedIncrement key) {
      return new Amount(MAX_UPDATES);
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
      final long raw = amount.getRawAndInvalidate();
      final long delta = Amount.amount(raw);
      if (Amount.numUpdatesLeft(raw) == Loader.MAX_UPDATES) {
        // This amount was never incremented, because the number of updates
        // left is still the original number.  Therefore this is an Amount
        // that has been evicted before anyone could attach any update to
        // it, so the delta must be 0, and we don't need to send this RPC.
        assert delta == 0 : "WTF? Pristine Amount with non-0 delta: " + amount;
        return;
      }
      final BufferedIncrement incr = entry.getKey();
      final AtomicIncrementRequest req =
        new AtomicIncrementRequest(incr.table, incr.key, incr.family,
                                   incr.qualifier, delta);
      client.atomicIncrement(req).chain(amount.deferred);
    }

  }

  static final CacheStats ZERO_STATS = new CacheStats(0, 0, 0, 0, 0, 0);

}
