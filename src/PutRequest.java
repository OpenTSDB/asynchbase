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
 * Puts some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 * <h1>A note on passing {@code timestamp}s in argument</h1>
 * HBase orders all the writes based on timestamps from {@code PutRequest}
 * irrespective of the actual order in which they're received or stored by
 * a RegionServer.  In other words, if you send a first {@code PutRequest}
 * with timestamp T, and then later send another one for the same table,
 * key, family and qualifier, but with timestamp T - 1, then the second
 * write will look like it was applied before the first one when you read
 * this cell back from HBase.  When manually setting timestamps, it is thus
 * strongly recommended to use real UNIX timestamps in milliseconds, e.g.
 * from {@link System#currentTimeMillis}.
 * <p>
 * If you want to let HBase set the timestamp on a write at the time it's
 * applied within the RegionServer, then use {@link KeyValue#TIMESTAMP_NOW}
 * as a timestamp.  The timestamp is set right before being written to the WAL
 * (Write Ahead Log).  Note however that this has a subtle consequence: if a
 * write succeeds from the server's point of view, but fails from the client's
 * point of view (maybe because the client got disconnected from the server
 * before the server could acknowledge the write), then if the client retries
 * the write it will create another version of the cell with a different
 * timestamp.
 */
public final class PutRequest extends BatchableRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey, HBaseRpc.HasFamily,
             HBaseRpc.HasQualifiers, HBaseRpc.HasValues, HBaseRpc.IsEdit,
             /* legacy: */ HBaseRpc.HasQualifier, HBaseRpc.HasValue {

  /** RPC Method name for HBase 0.94 and earlier.  */
  private static final byte[] PUT = { 'p', 'u', 't' };

  /** Code type used for serialized `Put' objects.  */
  static final byte CODE = 35;

  /**
   * A put with all fields set to a 1-byte array containing a zero.
   * This is useful for loops that need to start with a valid-looking edit.
   */
  static final PutRequest EMPTY_PUT;
  static {
    final byte[] zero = new byte[] { 0 };
    final byte[][] onezero = new byte[][] { zero };
    EMPTY_PUT = new PutRequest(zero, zero, zero, onezero, onezero);
    EMPTY_PUT.setRegion(new RegionInfo(zero, zero, zero));
  }

  /**
   * Invariants:
   *   - qualifiers.length == values.length
   *   - qualifiers.length > 0
   */
  private final byte[][][] qualifiers;
  private final byte[][][] values;

  /**
   * Constructor using current time.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[], byte[], long)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
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
    this(table, key, family, qualifier, value,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor for multiple columns using current time.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[][], byte[][], long)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to edit in that family.
   * @param values The corresponding values to store.
   * @throws IllegalArgumentException if {@code qualifiers.length == 0}
   * or if {@code qualifiers.length != values.length}
   * @since 1.3
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[][] qualifiers,
                    final byte[][] values) {
    this(table, key, new byte[][] { family }, new byte[][][] { qualifiers },
        new byte[][][] { values }, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor for multiple columns in multiple families using current time.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[][], byte[][], long)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @param qualifiers The column qualifiers to edit in that family.
   * @param values The corresponding values to store.
   * @throws IllegalArgumentException if {@code qualifiers.length == 0}
   * or if {@code qualifiers.length != values.length}
   * @since 1.3
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[][] families,
                    final byte[][][] qualifiers,
                    final byte[][][] values) {
    this(table, key, families , qualifiers, values,
        KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor for a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   * @param timestamp The timestamp to set on this edit.
   * @since 1.2
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier,
                    final byte[] value,
                    final long timestamp) {
    this(table, key, family, qualifier, value, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor for multiple columns with a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to edit in that family.
   * @param values The corresponding values to store.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if {@code qualifiers.length == 0}
   * or if {@code qualifiers.length != values.length}
   * @since 1.3
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[][] qualifiers,
                    final byte[][] values,
                    final long timestamp) {
    this(table, key, new byte[][] { family }, new byte[][][] { qualifiers },
        new byte[][][] { values }, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor using an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[], byte[], long, RowLock)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
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
    this(table, key, family, qualifier, value,
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor using current time and an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to edit in that family.
   * @param value The value to store.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @since 1.2
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier,
                    final byte[] value,
                    final long timestamp,
                    final RowLock lock) {
    this(table, key, family, qualifier, value, timestamp, lock.id());
  }

  /**
   * Constructor for multiple columns with current time and explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to edit in that family.
   * @param values The corresponding values to store.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if {@code qualifiers.length == 0}
   * or if {@code qualifiers.length != values.length}
   * @since 1.3
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[][] qualifiers,
                    final byte[][] values,
                    final long timestamp,
                    final RowLock lock) {
    this(table, key, new byte[][] { family }, new byte[][][] { qualifiers },
        new byte[][][] { values }, timestamp, lock.id());
  }

  /**
   * Constructor for multiple columns with current time and explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param families The column families to edit in that table.
   * @param qualifiers The column qualifiers to edit in that family.
   * @param values The corresponding values to store.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if {@code qualifiers.length == 0}
   * or if {@code qualifiers.length != values.length}
   * @since 1.3
   */
  public PutRequest(final byte[] table,
                    final byte[] key,
                    final byte[][] families,
                    final byte[][][] qualifiers,
                    final byte[][][] values,
                    final RowLock lock) {
    this(table, key, families, qualifiers, values,
        KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Convenience constructor from strings (higher overhead).
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[], byte[], long)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
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
         qualifier.getBytes(), value.getBytes(),
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Convenience constructor with explicit row lock (higher overhead).
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], byte[], byte[], byte[], byte[], long, RowLock)}
   * instead.  This constructor will let the RegionServer assign the timestamp
   * to this write at the time using {@link System#currentTimeMillis} right
   * before the write is persisted to the WAL.
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
         qualifier.getBytes(), value.getBytes(),
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor from a {@link KeyValue}.
   * @param table The table to edit.
   * @param kv The {@link KeyValue} to store.
   * @since 1.1
   */
  public PutRequest(final byte[] table,
                    final KeyValue kv) {
    this(table, kv, RowLock.NO_LOCK);
  }

  /**
   * Constructor from a {@link KeyValue} with an explicit row lock.
   * @param table The table to edit.
   * @param kv The {@link KeyValue} to store.
   * @param lock An explicit row lock to use with this request.
   * @since 1.1
   */
  public PutRequest(final byte[] table,
                    final KeyValue kv,
                    final RowLock lock) {
    this(table, kv, lock.id());
  }

  /** Private constructor.  */
  private PutRequest(final byte[] table,
                     final KeyValue kv,
                     final long lockid) {
    this(table, kv.key(), kv.family(),
        kv.qualifier(), kv.value(), kv.timestamp(), lockid);
  }

  /** Private constructor.  */
  private PutRequest(final byte[] table,
                     final byte[] key,
                     final byte[] family,
                     final byte[] qualifier,
                     final byte[] value,
                     final long timestamp,
                     final long lockid) {
    this(table, key, new byte[][] { family }, new byte[][][] { { qualifier } },
        new byte[][][] { { value } }, timestamp, lockid);
  }

  /** Private constructor.  */
  private PutRequest(final byte[] table,
                     final byte[] key,
                     final byte[][] families,
                     final byte[][][] qualifiers,
                     final byte[][][] values,
                     final long timestamp,
                     final long lockid) {
    super(table, key, families, timestamp, lockid);
    checkParams(families, qualifiers, values);
    this.qualifiers = qualifiers;
    this.values = values;
  }

  private void checkParams(final byte[][] families,
                           final byte[][][] qualifiers,
                           final byte[][][] values) {
    if (families.length != qualifiers.length) {
      throw new IllegalArgumentException(String.format(
          "Mismatch in number of families(%d) and qualifiers(%d) array size.",
          families.length, qualifiers.length));
    } else if (families.length != values.length) {
      throw new IllegalArgumentException(String.format(
          "Mismatch in number of families(%d) and values(%d) array size.",
          families.length, values.length));
    }

    for (int idx = 0; idx < families.length; idx++) {
      KeyValue.checkFamily(families[idx]);
      if (qualifiers[idx] == null || qualifiers[idx].length == 0) {
        throw new IllegalArgumentException(
            "No qualifiers are specifed for family "
            + families[idx] + " at index " + idx);
      } else if (values[idx] == null || values[idx].length == 0) {
        throw new IllegalArgumentException(
            "No values are specifed for family "
            + families[idx] + " at index " + idx);
      } else if (qualifiers[idx].length != values[idx].length) {
        throw new IllegalArgumentException("Found "
            + qualifiers[idx].length + " qualifiers and "
            + values[idx].length + " values for family "
            + families[idx] + " at index " + idx + ". Should be equal.");
      }
      for (int i = 0; i < qualifiers[idx].length; i++) {
        KeyValue.checkQualifier(qualifiers[idx][i]);
        KeyValue.checkValue(values[idx][i]);
      }
    }
  }

  @Override
  byte[] method(final byte server_version) {
    if (server_version >= RegionClient.SERVER_VERSION_095_OR_ABOVE) {
      return MUTATE;
    }
    return PUT;
  }

  @Override
  public byte[] table() {
    return table;
  }

  @Override
  public byte[] key() {
    return key;
  }

  /**
   * Returns the first qualifier of the set of edits in this RPC.
   * {@inheritDoc}
   */
  @Override
  public byte[] qualifier() {
    return qualifiers[0][0];
  }

  /**
   * Returns the qualifiers of first column family in the set of
   * edits in this RPC.
   * @since 1.3
   */
  @Override
  public byte[][] qualifiers() {
    return qualifiers[0];
  }

  /**
   * {@inheritDoc}
   * @since 1.5
   */
  @Override
  public byte[][][] getQualifiers() {
    return qualifiers;
  }

  /**
   * Returns the first value of the set of edits in this RPC.
   * {@inheritDoc}
   */
  @Override
  public byte[] value() {
    return values[0][0];
  }

  /**
   * {@inheritDoc}
   * @since 1.3
   */
  @Override
  public byte[][] values() {
    return values[0];
  }

  /**
   * {@inheritDoc}
   * @since 1.5
   */
  @Override
  public byte[][][] getValues() {
    return values;
  }

  public String toString() {
    return super.toStringWithQualifiers("PutRequest",
                                       families, qualifiers, values,
                                       ", timestamp=" + timestamp
                                       + ", lockid=" + lockid
                                       + ", durable=" + durable
                                       + ", bufferable=" + super.bufferable);
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  @Override
  byte version(final byte server_version) {
    // Versions are:
    //   1: Before 0.92.0, if we're serializing a `multiPut' RPC.
    //   2: HBASE-3921 in 0.92.0 added "attributes" at the end.
    if (server_version >= RegionClient.SERVER_VERSION_092_OR_ABOVE) {
      return 2;
    } else {
      return 1;
    }
  }

  @Override
  byte code() {
    return CODE;
  }

  @Override
  int numKeyValues() {
    return qualifiers[0].length;
  }

  @Override
  int payloadSize() {
    return payloadSize(0);
  }

  int payloadSize(int idx) {
    int size = 0;
    for (int i = 0; i < qualifiers[idx].length; i++) {
      size += KeyValue.predictSerializedSize(
          key, families[idx], qualifiers[idx][i], values[idx][i]);
    }
    return size;
  }

  @Override
  void serializePayload(final ChannelBuffer buf) {
    serializePayload(buf, 0);
  }

  private void serializePayload(final ChannelBuffer buf, int idx) {
    for (int i = 0; i < qualifiers[idx].length; i++) {
      KeyValue.serialize(buf, KeyValue.PUT, timestamp,
          key, families[idx], qualifiers[idx][i], values[idx][i]);
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

    size += predictPutSize();
    return size;
  }

  /** The raw size of the underlying `Put'.  */
  int predictPutSize() {
    int size = 0;
    size += 1;  // byte: Type of the 2nd parameter.
    size += 1;  // byte: Type again (see HBASE-2877).

    size += 1;  // byte: Version of Put.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.
    size += 8;  // long: Timestamp.
    size += 8;  // long: Lock ID.
    size += 1;  // bool: Whether or not to write to the WAL.
    size += 4;  // int:  Number of families for which we have edits.

    size += payloadsSize();

    return size;
  }

  @Override
  int payloadsSize() {
    int size = 0;
    for (int i = 0; i < families.length; i++) {
      size += 1;  // vint: Family length (guaranteed on 1 byte).
      size += families[i].length;  // The family.
      size += 4;  // int:  Number of KeyValues that follow.
      size += 4;  // int:  Total number of bytes for all those KeyValues.
      size += payloadSize(i);
    }
    return size;
  }

  @Override
  MutationProto toMutationProto() {
    final MutationProto.Builder put = MutationProto.newBuilder()
        .setRow(Bytes.wrap(key))
        .setMutateType(MutationProto.MutationType.PUT);
    if (!durable) {
      put.setDurability(MutationProto.Durability.SKIP_WAL);
    }

    final MutationProto.ColumnValue.Builder columns =
        MutationProto.ColumnValue.newBuilder();
    for (int family_idx = 0; family_idx < families.length; family_idx++) {
      columns.clear();
      columns.setFamily(Bytes.wrap(families[family_idx])); // ... for this family.

      // Now add all the qualifier-value pairs.
      for (int i = 0; i < qualifiers[family_idx].length; i++) {
        final MutationProto.ColumnValue.QualifierValue column =
            MutationProto.ColumnValue.QualifierValue.newBuilder()
            .setQualifier(Bytes.wrap(qualifiers[family_idx][i]))
            .setValue(Bytes.wrap(values[family_idx][i]))
            .setTimestamp(timestamp)
            .build();
        columns.addQualifierValue(column);
      }
      put.addColumnValue(columns);
    }

    return put.build();
  }

  /** Serializes this request.  */
  @Override
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

    // 2nd param: Put object
    serializeInto(buf);

    return buf;
  }

  @Override
  Object deserialize(final ChannelBuffer buf, int cell_size) {
    HBaseRpc.ensureNoCell(cell_size);
    final MutateResponse resp = readProtobuf(buf, MutateResponse.PARSER);
    return null;
  }

  /** Serialize the raw underlying `Put' into the given buffer.  */
  void serializeInto(final ChannelBuffer buf) {
    buf.writeByte(CODE); // Code for a `Put' parameter.
    buf.writeByte(CODE); // Code again (see HBASE-2877).
    buf.writeByte(1);    // Put#PUT_VERSION.  Stick to v1 here for now.
    writeByteArray(buf, key);  // The row key.

    buf.writeLong(timestamp);  // Timestamp.

    buf.writeLong(lockid);    // Lock ID.
    buf.writeByte(durable ? 0x01 : 0x00);  // Whether or not to use the WAL.

    buf.writeInt(families.length);  // Number of families that follow.
    serializePayloads(buf);
  }

  @Override
  void serializePayloads(ChannelBuffer buf) {
    for (int i = 0; i < families.length; i++) {
      writeByteArray(buf, families[i]);  // The column family.

      buf.writeInt(qualifiers[i].length);  // Number of "KeyValues" that follow.
      buf.writeInt(payloadSize(i));  // Size of the KV that follows.
      serializePayload(buf, i);
    }
  }

}
