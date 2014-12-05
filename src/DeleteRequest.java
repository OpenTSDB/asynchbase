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

  /** Special value for {@link #family} when deleting a whole row.  */
  static final byte[] WHOLE_ROW = new byte[0];

  private final byte[][] qualifiers;

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
    this(table, key, null, null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table, key, null, null, timestamp, RowLock.NO_LOCK);
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
    this(table, key, family, null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table, key, family, null, timestamp, RowLock.NO_LOCK);
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
      this(table, key, family,
           qualifier == null ? null : new byte[][] { qualifier },
           KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
      this(table, key, family,
           qualifier == null ? null : new byte[][] { qualifier },
           timestamp, RowLock.NO_LOCK);
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
    this(table, key, family, qualifiers,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table, key, family, qualifiers, timestamp, RowLock.NO_LOCK);
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
    this(table, key, family,
         qualifier == null ? null : new byte[][] { qualifier },
         KeyValue.TIMESTAMP_NOW, lock.id());
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
    this(table, key, family,
         qualifier == null ? null : new byte[][] { qualifier },
         timestamp, lock.id());
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
    this(table, key, family, qualifiers, KeyValue.TIMESTAMP_NOW, lock.id());
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
    this(table, key, family, qualifiers, timestamp, lock.id());
  }

  /**
   * Constructor to delete an entire row.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes(), null, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table.getBytes(), key.getBytes(), family.getBytes(), null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier == null ? null : new byte[][] { qualifier.getBytes() },
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
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
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier == null ? null : new byte[][] { qualifier.getBytes() },
         KeyValue.TIMESTAMP_NOW, lock.id());
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
    this(table, kv.key(), kv.family(), new byte[][] { kv.qualifier() },
         kv.timestamp(), RowLock.NO_LOCK);
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
    this(table, kv.key(), kv.family(), new byte[][] { kv.qualifier() },
         kv.timestamp(), lock.id());
  }

  /** Private constructor.  */
  private DeleteRequest(final byte[] table,
                        final byte[] key,
                        final byte[] family,
                        final byte[][] qualifiers,
                        final long timestamp,
                        final long lockid) {
    super(table, key, family == null ? WHOLE_ROW : family, timestamp, lockid);
    if (family != null) {
      KeyValue.checkFamily(family);
    }

    if (qualifiers != null) {
      if (family == null) {
        throw new IllegalArgumentException("You can't delete specific qualifiers"
          + " without specifying which family they belong to."
          + "  table=" + Bytes.pretty(table)
          + ", key=" + Bytes.pretty(key));
      }
      if (qualifiers.length == 0) {
        throw new IllegalArgumentException("Don't pass an empty list of"
          + " qualifiers, this would delete the entire row of table="
          + Bytes.pretty(table) + " at key " + Bytes.pretty(key));
      }
      for (final byte[] qualifier : qualifiers) {
        KeyValue.checkQualifier(qualifier);
      }
      this.qualifiers = qualifiers;
    } else {
      // No specific qualifier to delete: delete the entire family.  Not that
      // if `family == null', we'll delete the whole row anyway.
      this.qualifiers = DELETE_FAMILY_MARKER;
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
    return qualifiers;
  }

  public String toString() {
    return super.toStringWithQualifiers("DeleteRequest", family, qualifiers);
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
    return qualifiers.length;
  }

  @Override
  void serializePayload(final ChannelBuffer buf) {
    if (family == null) {
      return;  // No payload when deleting whole rows.
    }
    // Are we deleting a whole family at once or just a bunch of columns?
    final byte type = (qualifiers == DELETE_FAMILY_MARKER
                       ? KeyValue.DELETE_FAMILY
                       : (at_timestamp_only
                          ? KeyValue.DELETE
                          : KeyValue.DELETE_COLUMN));

    // Write the KeyValues
    for (final byte[] qualifier : qualifiers) {
      KeyValue.serialize(buf, type, timestamp,
                         key, family, qualifier, null);
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
    size += 1;  // vint: Family length (guaranteed on 1 byte).
    if (family == null) {
      return size;
    }
    size += family.length;  // The column family.
    size += 4;  // int:  Number of KeyValues for this family.
    return size + payloadSize();
  }

  /** Returns the serialized size of all the {@link KeyValue}s in this RPC.  */
  @Override
  int payloadSize() {
    if (family == WHOLE_ROW) {
      return 0;  // No payload when deleting whole rows.
    }
    int size = 0;
    size += 4;  // int:  Total length of the whole KeyValue.
    size += 4;  // int:  Total length of the key part of the KeyValue.
    size += 4;  // int:  Length of the value part of the KeyValue.
    size += 2;  // short:Length of the key.
    size += key.length;  // The row key (again!).
    size += 1;  // byte: Family length (again!).
    size += family.length;  // The column family (again!).
    size += 8;  // long: The timestamp (again!).
    size += 1;  // byte: The type of KeyValue.
    size *= qualifiers.length;
    for (final byte[] qualifier : qualifiers) {
      size += qualifier.length;  // The column qualifier.
    }
    return size;
  }

  @Override
  MutationProto toMutationProto() {
    final MutationProto.Builder del = MutationProto.newBuilder()
      .setRow(Bytes.wrap(key))
      .setMutateType(MutationProto.MutationType.DELETE);

    if (family != WHOLE_ROW) {
      final MutationProto.ColumnValue.Builder columns = // All columns ...
        MutationProto.ColumnValue.newBuilder()
        .setFamily(Bytes.wrap(family));                 // ... for this family.

      final MutationProto.DeleteType type =
        (qualifiers == DELETE_FAMILY_MARKER
         ? MutationProto.DeleteType.DELETE_FAMILY
         : (at_timestamp_only
            ? MutationProto.DeleteType.DELETE_ONE_VERSION
            : MutationProto.DeleteType.DELETE_MULTIPLE_VERSIONS));

      // Now add all the qualifiers to delete.
      for (int i = 0; i < qualifiers.length; i++) {
        final MutationProto.ColumnValue.QualifierValue column =
          MutationProto.ColumnValue.QualifierValue.newBuilder()
          .setQualifier(Bytes.wrap(qualifiers[i]))
          .setTimestamp(timestamp)
          .setDeleteType(type)
          .build();
        columns.addQualifierValue(column);
      }
      del.addColumnValue(columns);
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
    if (family == WHOLE_ROW) {
      buf.writeInt(0);  // Number of families that follow.
      return buf;
    }
    buf.writeInt(1);    // Number of families that follow.

    // Each family is then written like so:
    writeByteArray(buf, family);  // Column family name.
    buf.writeInt(qualifiers.length);  // How many KeyValues for this family?
    serializePayload(buf);
    return buf;
  }

  @Override
  Object deserialize(final ChannelBuffer buf, int cell_size) {
    HBaseRpc.ensureNoCell(cell_size);
    final MutateResponse resp = readProtobuf(buf, MutateResponse.PARSER);
    return null;
  }

}
