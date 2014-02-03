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

import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.ClientPB.MutateRequest;
import org.hbase.async.generated.ClientPB.MutateResponse;
import org.hbase.async.generated.ClientPB.MutationProto;

/**
 * Deletes some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 * <h1>A note on passing {@code timestamp}s in argument</h1>
 * Irrespective of the order in which you send RPCs, a {@code DeleteRequest}
 * that is created with a specific timestamp in argument will only delete
 * values in HBase that were previously stored with a timestamp less than
 * or equal to that of the {@code DeleteRequest} unless
 * {@link #setDeleteAtTimestampOnly} is also called, in which case only the
 * value at the specified timestamp is deleted.
 */
public final class DeleteRequest extends BatchableRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifiers, HBaseRpc.IsEdit {

  private static final byte[] DELETE = new byte[] {
    'd', 'e', 'l', 'e', 't', 'e'
  };

  /** Code type used for serialized `Delete' objects.  */
  static final byte CODE = 31;

  /** Special value for {@link #qualifiers} when deleting a whole family.  */
  private static final byte[][] DELETE_FAMILY_MARKER =
    new byte[][] { HBaseClient.EMPTY_ARRAY };

  /** Special value for {@link #families} when deleting a whole row.  */
  static final byte[][] WHOLE_ROW =
      new byte[][] { HBaseClient.EMPTY_ARRAY };

  private final byte[][][] qualifiers;
  private final long[][] timestamps;

  /** Whether to delete the value only at the specified timestamp. */
  private boolean at_timestamp_only = false;

  /**
   * Constructor to delete an entire row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table, final byte[] key) {
    this(table, key, null, null, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete an entire row before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table, final byte[] key,
                       final long timestamp) {
    this(table, key, null, null, null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family) {
    this(table, key, new byte[][] { family }, null,
         null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific set of families.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families) {
    this(table, key, families, null, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final long timestamp) {
    this(table, key, new byte[][] { family }, null,
         null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier) {
      this(table, key, new byte[][] { family },
           qualifier == null ? null : new byte[][][] { { qualifier } },
           null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null}, to delete the whole family.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final long timestamp) {
      this(table, key, new byte[][] { family },
           qualifier == null ? null : new byte[][][] { { qualifier } },
           null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers) {
    this(table, key, new byte[][] { family },
         new byte[][][] { qualifiers }, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final long[] timestamps) {
    this(table, key, new byte[][] { family },
        new byte[][][] { qualifiers }, new long[][] {timestamps},
        KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells of a set of column
   * families in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers) {
    this(table, key, families, qualifiers, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells of a set of column
   * families in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @param timestamps The corresponding timestamps to use in delete.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers,
                       final long[][] timestamps) {
    this(table, key, families, qualifiers, timestamps,
        KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells of a set of column
   * families in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @param timestamps The corresponding timestamps to use in delete.
   * @param timestamp The row timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers,
                       final long[][] timestamps,
                       final long timestamp) {
    this(table, key, families, qualifiers, timestamps,
        timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final long timestamp) {
    this(table, key, new byte[][] { family },
         new byte[][][] { qualifiers }, null,
         timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers,
                       final long timestamp) {
    this(table, key, families, qualifiers,
         null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final RowLock lock) {
    this(table, key, new byte[][] { family },
         qualifier == null ? null : new byte[][][] { { qualifier } },
         null, KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final long timestamp,
                       final RowLock lock) {
    this(table, key, new byte[][] { family },
         qualifier == null ? null : new byte[][][] { { qualifier } },
         null, timestamp, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final RowLock lock) {
    this(table, key, new byte[][] { family },
         new byte[][][] { qualifiers }, null,
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers,
                       final RowLock lock) {
    this(table, key, families, qualifiers, null,
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row with a row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final long timestamp,
                       final RowLock lock) {
    this(table, key, new byte[][] { family }, new byte[][][] { qualifiers },
         null, timestamp, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row with a row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[][] families,
                       final byte[][][] qualifiers,
                       final long timestamp,
                       final RowLock lock) {
    this(table, key, families, qualifiers, null, timestamp, lock.id());
  }

  /**
   * Constructor to delete an entire row.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes(), null, null,
         null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family) {
    this(table.getBytes(), key.getBytes(), new byte[][] { family.getBytes() },
         null, null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier) {
    this(table.getBytes(), key.getBytes(), new byte[][] { family.getBytes() },
         qualifier == null ? null : new byte[][][] { { qualifier.getBytes() } },
         null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier,
                       final RowLock lock) {
    this(table.getBytes(), key.getBytes(), new byte[][] { family.getBytes() },
         qualifier == null ? null : new byte[][][] { { qualifier.getBytes() } },
         null, KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param kv The specific {@link KeyValue} to delete.  Note that if this
   * {@link KeyValue} specifies a timestamp, then this specific timestamp only
   * will be deleted.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table, final KeyValue kv) {
    this(table, kv.key(), new byte[][] { kv.family() },
         new byte[][][] { { kv.qualifier() } },
         null, kv.timestamp(), RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * @param table The table to edit.
   * @param kv The specific {@link KeyValue} to delete.  Note that if this
   * {@link KeyValue} specifies a timestamp, then this specific timestamp only
   * will be deleted.
   * @param lock An explicit row lock to use with this request.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final KeyValue kv,
                       final RowLock lock) {
    this(table, kv.key(), new byte[][] { kv.family() },
         new byte[][][] { { kv.qualifier() } },
         null, kv.timestamp(), lock.id());
  }

  /** Private constructor.  */
  private DeleteRequest(final byte[] table,
                        final byte[] key,
                        final byte[][] families,
                        final byte[][][] qualifiers,
                        final long[][] timestamps,
                        final long row_timestamp,
                        final long lockid) {
    super(table, key, families == null ? WHOLE_ROW : families,
        row_timestamp, lockid);
    checkParams(families, qualifiers, timestamps);
    this.qualifiers = qualifiers;
    this.timestamps = timestamps;
  }

  private void checkParams(final byte[][] families,
                           final byte[][][] qualifiers,
                           final long[][] timestamps) {
    if (families != null) {
      for (byte[] family : families) {
        KeyValue.checkFamily(family);
      }
    }

    if (qualifiers != null) {
      if (families == null) {
        throw new IllegalArgumentException("You can't delete specific qualifiers"
          + " without specifying which family they belong to."
          + " table=" + Bytes.pretty(table)
          + ", key=" + Bytes.pretty(key));
      } else if (families.length != qualifiers.length) {
        throw new IllegalArgumentException("Length of the qualifier array does"
            + " not match that of the family."
            + " table=" + Bytes.pretty(table)
            + ", key=" + Bytes.pretty(key));
      } else if (timestamps != null && families.length != timestamps.length) {
        throw new IllegalArgumentException(String.format(
            "Mismatch in number of families(%d) and timestamps(%d) array size.",
            families.length, timestamps.length));
      }

      for (int idx = 0; idx < families.length; idx++) {
        if (qualifiers[idx] == null) {
          continue;
        }
        if (timestamps != null) {
          if (qualifiers[idx].length != timestamps[idx].length) {
            throw new IllegalArgumentException("Found "
                + qualifiers[idx].length + " qualifiers and "
                + timestamps[idx].length + " timestamps for family "
                + families[idx] + " at index " + idx + ". Should be equal.");
          }
        }
        for (final byte[] qualifier : qualifiers[idx]) {
          KeyValue.checkQualifier(qualifier);
        }
      }
    } else if (timestamps != null) {
      throw new IllegalArgumentException("Timestamps have been specified "
          + "without specifying qualifiers.");
    }
  }

  /**
   * Deletes only the cell value with the timestamp specified in the
   * constructor.
   * <p>
   * Only applicable when qualifier(s) is also specified.
   * @since 1.5
   */
  public void setDeleteAtTimestampOnly(final boolean at_timestamp_only) {
    this.at_timestamp_only = at_timestamp_only;
  }

  /**
   * Returns whether to only delete the cell value at the timestamp.
   * @since 1.5
   */
  public boolean deleteAtTimestampOnly() {
    return at_timestamp_only;
  }

  @Override
  byte[] method(final byte server_version) {
    return (server_version >= RegionClient.SERVER_VERSION_095_OR_ABOVE
            ? MUTATE
            : DELETE);
  }

  @Override
  public byte[] table() {
    return table;
  }

  @Override
  public byte[] key() {
    return key;
  }

  @Override
  public byte[][] qualifiers() {
    return qualifiers == null ? null : qualifiers[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte[][][] getQualifiers() {
    return qualifiers;
  }

  public String toString() {
    return super.toStringWithQualifiers("DeleteRequest", families, qualifiers);
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  @Override
  byte version(final byte unused_server_version) {
    // Versions are:
    //   1: Before 0.92.0.  This method only gets called for 0.92 and above.
    //   2: HBASE-3921 in 0.92.0 added "attributes" at the end.
    //   3: HBASE-3961 in 0.92.0 allowed skipping the WAL.
    return 3;  // 3 because we allow skipping the WAL.
  }

  @Override
  byte code() {
    return CODE;
  }

  @Override
  int numKeyValues() {
    return (qualifiers != null && qualifiers[0] != null)
        ? qualifiers[0].length
        : 1;
  }

  @Override
  void serializePayload(final ChannelBuffer buf) {
    serializePayload(buf, 0);
  }

  private void serializePayload(final ChannelBuffer buf, int family_idx) {
    if (families == WHOLE_ROW) {
      return;  // No payload when deleting whole rows.
    }
    final boolean has_qualifiers =
        (qualifiers != null && qualifiers[family_idx] != null);
    final boolean has_timestamps =
        (timestamps != null && timestamps[family_idx] != null);
    // Are we deleting a whole family at once or just a bunch of columns?
    final byte type = (!has_qualifiers
                       ? KeyValue.DELETE_FAMILY
                       : (at_timestamp_only
                           ? KeyValue.DELETE
                           : KeyValue.DELETE_COLUMN));
    final byte[][] family_qualifiers = !has_qualifiers
                                        ? DELETE_FAMILY_MARKER
                                        : qualifiers[family_idx];
    // Write the KeyValues
    for (int i = 0; i < family_qualifiers.length; i++) {
      final byte[] qualifier = family_qualifiers[i];
      KeyValue.serialize(buf, type,
          (has_timestamps ? timestamps[family_idx][i] : timestamp),
          key, families[family_idx], qualifier, null);
    }
  }

  /**
   * Predicts a lower bound on the serialized size of this RPC.
   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
   * Since we use a static buffer, if the prediction is wrong and turns out
   * to be less than what we need, there will be an exception which will
   * prevent the RPC from being serialized.  That'd be a severe bug.
   */
  private int predictSerializedSize() {
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 3;  // vint: region name length (3 bytes => max length = 32768).
    size += region.name().length;  // The region name.

    size += 1;  // byte: Type of the 2nd parameter.
    size += 1;  // byte: Type again (see HBASE-2877).
    size += 1;  // byte: Version of Delete.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.
    size += 8;  // long: Timestamp.
    size += 8;  // long: Lock ID.
    size += 4;  // int:  Number of families.

    size += payloadsSize();

    return size;
  }

  @Override
  int payloadsSize() {
    int size = 0;
    if (families != WHOLE_ROW) {
      for (int i = 0; i < families.length; i++) {
        size += 1;  // vint: Family length (guaranteed on 1 byte).
        size += families[i].length;  // The column family.
        size += 4;  // int:  Number of KeyValues for this family.
        size += payloadSize(i);
      }
    }
    return size;
  }

  /** Returns the serialized size of all the {@link KeyValue}s in this RPC.  */
  @Override
  int payloadSize() {
    return payloadSize(0);
  }

  private int payloadSize(int family_idx) {
    if (families == WHOLE_ROW) {
      return 0;  // No payload when deleting whole rows.
    }
    int size = 0;
    size += 4;  // int:  Total length of the whole KeyValue.
    size += 4;  // int:  Total length of the key part of the KeyValue.
    size += 4;  // int:  Length of the value part of the KeyValue.
    size += 2;  // short:Length of the key.
    size += key.length;  // The row key (again!).
    size += 1;  // byte: Family length (again!).
    size += families[family_idx].length;  // The column family (again!).
    size += 8;  // long: The timestamp (again!).
    size += 1;  // byte: The type of KeyValue.
    final byte[][] family_qualifiers =
        (qualifiers == null || qualifiers[family_idx] == null)
          ? DELETE_FAMILY_MARKER
          : qualifiers[family_idx];
    size *= family_qualifiers.length;
    for (final byte[] qualifier : family_qualifiers) {
      size += qualifier.length;  // The column qualifier.
    }
    return size;
  }

  @Override
  MutationProto toMutationProto() {
    final MutationProto.Builder del = MutationProto.newBuilder()
      .setRow(Bytes.wrap(key))
      .setMutateType(MutationProto.MutationType.DELETE);

    if (families != WHOLE_ROW) {
      final MutationProto.ColumnValue.Builder columns = // All columns ...
          MutationProto.ColumnValue.newBuilder();
      for (int i = 0; i < families.length; i++) {
        byte[] family = families[i];
        columns.clear();
        columns.setFamily(Bytes.wrap(family));    // ... for this family.

        if (qualifiers != null) {
          final boolean has_timestamps =
              (timestamps != null && timestamps[i] != null);
          final MutationProto.DeleteType type =
              (qualifiers[i] == null
                  ? MutationProto.DeleteType.DELETE_FAMILY
                  : (at_timestamp_only
                      ? MutationProto.DeleteType.DELETE_ONE_VERSION
                      : MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS));
          // Now add all the qualifiers to delete.
          if (qualifiers[i] != null) {
            for (int j = 0; j < qualifiers[i].length; j++) {
              final MutationProto.ColumnValue.QualifierValue column =
                  MutationProto.ColumnValue.QualifierValue.newBuilder()
                  .setQualifier(Bytes.wrap(qualifiers[i][j]))
                  .setTimestamp((has_timestamps ? timestamps[i][j] : timestamp))
                  .setDeleteType(type)
                  .build();
              columns.addQualifierValue(column);
            }
          }
        }
        del.addColumnValue(columns);
      }
    }

    if (!durable) {
      del.setDurability(MutationProto.Durability.SKIP_WAL);
    }
    return del.build();
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte server_version) {
    if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
      return serializeOld(server_version);
    }

    final MutateRequest req = MutateRequest.newBuilder()
      .setRegion(region.toProtobuf())
      .setMutation(toMutationProto())
      .build();
    return toChannelBuffer(MUTATE, req);
  }

  /** Serializes this request for HBase 0.94 and before.  */
  private ChannelBuffer serializeOld(final byte server_version) {
    final ChannelBuffer buf = newBuffer(server_version,
                                        predictSerializedSize());
    buf.writeInt(2);  // Number of parameters.

    // 1st param: byte array containing region name
    writeHBaseByteArray(buf, region.name());

    // 2nd param: Delete object.
    buf.writeByte(CODE); // Code for a `Delete' parameter.
    buf.writeByte(CODE); // Code again (see HBASE-2877).
    buf.writeByte(1);    // Delete#DELETE_VERSION.  Stick to v1 here for now.
    writeByteArray(buf, key);
    buf.writeLong(timestamp);  // Maximum timestamp.
    buf.writeLong(lockid);  // Lock ID.

    // Families.
    if (families == WHOLE_ROW) {
      buf.writeInt(0);  // Number of families that follow.
      return buf;
    }

    buf.writeInt(families.length); // Number of families that follow.
    serializePayloads(buf); // All families

    return buf;
  }

  @Override
  void serializePayloads(ChannelBuffer buf) {
    for (int i = 0; i < families.length; i++) {
      // Each family is then written like so:
      writeByteArray(buf, families[i]);  // Column family name.
      final byte[][] family_qualifiers =
          (qualifiers != null && qualifiers[i] != null)
              ? qualifiers[i]
              : DELETE_FAMILY_MARKER;
      buf.writeInt(family_qualifiers.length);  // How many KeyValues for this family?
      serializePayload(buf, i);
    }
  }

  @Override
  Object deserialize(final ChannelBuffer buf, int cell_size) {
    HBaseRpc.ensureNoCell(cell_size);
    final MutateResponse resp = readProtobuf(buf, MutateResponse.PARSER);
    return null;
  }

}
