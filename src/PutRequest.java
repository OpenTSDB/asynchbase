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

import java.util.Comparator;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Puts some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class PutRequest extends HBaseRpc {

  private final byte[] family;
  private final byte[] qualifier;
  private final byte[] value;
  private final long lockid;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier,
                    final byte[] value) {
    this(table, key, family, qualifier, value, RowLock.NO_LOCK);
  }

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   * @param lock An explicit row lock to use with this request.
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier,
                    final byte[] value,
                    final RowLock lock) {
    this(table, key, family, qualifier, value, lock.id());
  }

  /**
   * Constructor.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   */
  public PutRequest(final String table,
                    final String key,
                    final String family,
                    final String qualifier,
                    final String value) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), value.getBytes(), RowLock.NO_LOCK);
  }

  /**
   * Constructor.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   * @param lock An explicit row lock to use with this request.
   */
  public PutRequest(final String table,
                    final String key,
                    final String family,
                    final String qualifier,
                    final String value,
                    final RowLock lock) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), value.getBytes(), lock.id());
  }

  /** Private constructor.  */
  private PutRequest(final byte[] table,
                     final byte[] key,
                     final byte[] family,
                     final byte[] qualifier,
                     final byte[] value,
                     final long lockid) {
    super(null, table, key);
    KeyValue.checkFamily(family);
    KeyValue.checkQualifier(qualifier);
    KeyValue.checkValue(value);
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    this.lockid = lockid;
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  byte[] family() {
    return family;
  }

  byte[] qualifier() {
    return qualifier;
  }

  byte[] value() {
    return value;
  }

  long lockid() {
    return lockid;
  }

  /**
   * This method is unused.  Every {@code PutRequest} is turned into a
   * {@link MultiPutRequest}, so this RPC is never actually sent out to
   * the wire.  Technically this class doesn't need to inherit from
   * {@link HBaseRpc} but since it's user facing, it does for consistency.
   * @throws AssertionError always.
   */
  ChannelBuffer serialize() {
    throw new AssertionError("should never be called");
  }

  /**
   * Sorts {@link PutRequest}s first by key then by table.
   * We sort by key first because applications typically manipulate few tables
   * and lots of keys, so we typically have less data to look at if we compare
   * by key first.  We also sort by lock ID.
   */
  static final RowTableComparator ROW_TABLE_CMP = new RowTableComparator();

  /** Sorts {@link PutRequest}s first by key then by table then by lock ID.  */
  private static final class RowTableComparator implements Comparator<PutRequest> {

    private RowTableComparator() {  // Can't instantiate outside of this class.
    }

    @Override
    public int compare(final PutRequest a, final PutRequest b) {
      int d;
      if ((d = Bytes.memcmp(a.key, b.key)) != 0) {
        return d;
      } else if ((d = Bytes.memcmp(a.table, b.table)) != 0) {
        return d;
      } else {
        return Long.signum(a.lockid - b.lockid);
      }
    }

  }

}
