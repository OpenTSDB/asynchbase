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

import java.util.Comparator;
import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hbase.async.HBaseClient.EMPTY_ARRAY;

/**
 * Stores basic information about a region.
 */
final class RegionInfo implements Comparable<RegionInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(RegionInfo.class);

  private final byte[] table;
  // The region name is of the form:
  //   table_name,start_key,timestamp[.MD5.]
  // So it contains the start_key.
  private final byte[] region_name;
  private final byte[] stop_key;

  /**
   * Constructor.
   */
  public RegionInfo(final byte[] table,
                    final byte[] region_name,
                    final byte[] stop_key) {
    this.table = table;
    this.region_name = region_name;
    if (stop_key.length == 0) {
      this.stop_key = EMPTY_ARRAY;
    } else {
      this.stop_key = stop_key;
    }
  }

  /** Returns the name of the table this region belongs to.  */
  public byte[] table() {
    return table;
  }

  /** Returns the name of the region.  */
  public byte[] name() {
    return region_name;
  }

  /** Returns the stop key (exclusive) of this region.  */
  public byte[] stopKey() {
    return stop_key;
  }

  /**
   * Creates a new {@link RegionInfo} from a META {@link KeyValue}.
   * @param kv The {@link KeyValue} to use, which is assumed to be from
   * the cell {@code info:regioninfo} of a {@code .META.} region.
   * @param out_start_key A (@code {new byte[1][]}).
   * The start row of the region will be stored in {@code out_start_key[0]}.
   * Think "pointer-to-pointer" in Java (yeah!).
   * @return A newly created {@link RegionInfo}.
   * If calling {@link #table} on the object returned gives a reference to
   * {@link HBaseClient#EMPTY_ARRAY}, then the META entry indicates that the
   * region has been split (and thus this entry shouldn't be used).
   * @throws RegionOfflineException if the META entry indicates that the
   * region is offline.
   * @throws BrokenMetaException if the {@link KeyValue} seems invalid.
   */
  static RegionInfo fromKeyValue(final KeyValue kv,
                                 final byte[][] out_start_key) {
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(kv.value());
    final byte version = buf.readByte();
    if (version != 0) {
      LOG.warn("Unsupported region info version: " + version
               + " in .META.  entry: " + kv);
      // Keep going anyway, just in case the new version is backwards
      // compatible somehow.  If it's not, we'll just fail later.
    }
    final byte[] stop_key = HBaseRpc.readByteArray(buf);
    final boolean offline = buf.readByte() != 0;
    final long region_id = buf.readLong();
    final byte[] region_name = HBaseRpc.readByteArray(buf);
    // TODO(tsuna): Can we easily de-dup this array with another RegionInfo?
    byte[] table;
    try {
      table = tableFromRegionName(region_name);
    } catch (IllegalArgumentException e) {
      throw BrokenMetaException.badKV(null, "an `info:regioninfo' cell"
                                      + " has a " + e.getMessage(), kv);
    }
    final boolean split = buf.readByte() != 0;
    final byte[] start_key = HBaseRpc.readByteArray(buf);
    // Table description and hash code are left, but we don't care.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Got " + Bytes.pretty(table) + "'s region ["
                + Bytes.pretty(start_key) + '-'
                + Bytes.pretty(stop_key) + ") offline=" + offline
                + ", region_id=" + region_id + ", region_name="
                + Bytes.pretty(region_name) + ", split=" + split);
    }
    // RegionServers set both `offline' and `split' to `false' on the parent
    // region after it's been split.  We normally don't expect to ever observe
    // such regions as any META lookup should find the new daughter regions.
    // But just in case, we make sure to not throw an exception in this case.
    if (offline && !split) {
      throw new RegionOfflineException(region_name);
    }
    // If the region has been split, we put a special marker instead of
    // the table name to indicate that this region has been split.
    final RegionInfo region = new RegionInfo(split ? EMPTY_ARRAY : table,
                                             region_name, stop_key);
    out_start_key[0] = start_key;
    return region;
  }

  /**
   * Given the name of a region, returns the name of the table it belongs to.
   * @throws IllegalArgumentException if the name of the region is malformed.
   */
  static byte[] tableFromRegionName(final byte[] region_name) {
    int comma = 1;  // Can't be at the beginning.
    for (/**/; comma < region_name.length; comma++) {
      if (region_name[comma] == ',') {
        break;
      }
    }
    if (comma == region_name.length) {
      throw new IllegalArgumentException("Malformed region name, contains no"
        + " comma: " + Bytes.pretty(region_name));
    }
    return Arrays.copyOf(region_name, comma);
  }

  @Override
  public int compareTo(final RegionInfo other) {
    return Bytes.memcmp(region_name, other.region_name);
  }

  public boolean equals(final Object other) {
    if (other == null || !(other instanceof RegionInfo)) {
      return false;
    }
    return compareTo((RegionInfo) other) == 0;
  }

  public int hashCode() {
    return Arrays.hashCode(table)
      ^ Arrays.hashCode(region_name)
      ^ Arrays.hashCode(stop_key);
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(48  // boilerplate
      // the stop_key is likely to contain non-ascii characters, so
      // let's multiply its length by 2 to avoid re-allocations.
      + region_name.length + stop_key.length * 2);
    buf.append("RegionInfo(table=");
    if (table == EMPTY_ARRAY) {
      buf.append("<NSRE marker>");
    } else {
      Bytes.pretty(buf, table);
    }
    buf.append(", region_name=");
    Bytes.pretty(buf, region_name);
    buf.append(", stop_key=");
    Bytes.pretty(buf, stop_key);
    buf.append(')');
    return buf.toString();
  }

  /** Singleton to compare region names.  */
  static final RegionNameCmp REGION_NAME_CMP = new RegionNameCmp();

  /**
   * Comparator for region names.
   * We can't just use {@link Bytes.MEMCMP} because it doesn't play nicely
   * with the way META keys are built as the first region has an empty start
   * key.  Let's assume we know about those 2 regions in our cache:
   * <pre>
   *   .META.,,1
   *   tableA,,1273018455182
   * </pre>
   * We're given an RPC to execute on {@code tableA}, row {@code \000} (1 byte
   * row key containing a 0).  If we use {@code memcmp} to sort the entries in
   * the cache, when we search for the entry right before {@code tableA,\000,:}
   * we'll erroneously find {@code .META.,,1} instead of the entry for first
   * region of {@code tableA}.
   * <p>
   * Since this scheme breaks natural ordering, we need this comparator to
   * implement a special version of {@code memcmp} to handle this scenario.
   */
  private static final class RegionNameCmp implements Comparator<byte[]> {

    private RegionNameCmp() {  // Can't instantiate outside of this class.
    }

    @Override
    public int compare(final byte[] a, final byte[] b) {
      final int length = Math.min(a.length, b.length);
      if (a == b) {  // Do this after accessing a.length and b.length
        return 0;    // in order to NPE if either a or b is null.
      }
      // Reminder: region names are of the form:
      //   table_name,start_key,timestamp[.MD5.]
      // First compare the table names.
      int i;
      for (i = 0; i < length; i++) {
        final byte ai = a[i];  // Saves one pointer deference every iteration.
        final byte bi = b[i];  // Saves one pointer deference every iteration.
        if (ai != bi) {  // The name of the tables differ.
          if (ai == ',') {
            return -1001;  // `a' has a smaller table name.  a < b
          } else if (bi == ',') {
            return 1001;  // `b' has a smaller table name.  a > b
          }
          return (ai & 0xFF) - (bi & 0xFF);  // "promote" to unsigned.
        }
        if (ai == ',') {  // Remember: at this point ai == bi.
          break;  // We're done comparing the table names.  They're equal.
        }
      }

      // Now find the last comma in both `a' and `b'.  We need to start the
      // search from the end as the row key could have an arbitrary number of
      // commas and we don't know its length.
      final int a_comma = findCommaFromEnd(a, i);
      final int b_comma = findCommaFromEnd(b, i);
      // If either `a' or `b' is followed immediately by another comma, then
      // they are the first region (it's the empty start key).
      i++;   // No need to check against `length', there MUST be more bytes.
      if (a_comma != b_comma) {
        if (a_comma == i) {
          return -1002;  // a < b  because a is the start key, b is not.
        } else if (b_comma == i) {
          return 1002;   // a > b  because b is the start key, a is not.
        }
      }

      // Now either both are the start key (in which case we need to keep
      // comparing the "start codes" to pick up the most recent region)
      // or neither is and we simply compare the start keys.
      do {
        if (a[i] != b[i]) {
          if (i == a_comma) {
            return -1003;  // `a' has a smaller start key.  a < b
          } else if (i == b_comma) {
            return 1003;   // `b' has a smaller start key.  a > b
          }
          return (a[i] & 0xFF) - (b[i] & 0xFF);  // The start keys differ.
        }
        i++;
      } while (i < length);
      return a.length - b.length;
    }

    private static int findCommaFromEnd(final byte[] b, final int offset) {
      for (int i = b.length - 1; i > offset; i--) {
        if (b[i] == ',') {
          return i;
        }
      }
      throw new IllegalArgumentException("No comma found in " + Bytes.pretty(b)
                                         + " after offset " + offset);
    }

  }

}
