/*
 * Copyright (c) 2010-2012  StumbleUpon, Inc.  All rights reserved.
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

/**
 * Puts some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 * <h1>A note on passing {@code timestamp}s in argument</h1>
 * Irrespective of the order in which you send RPCs, a {@code PutRequest}
 * that is created with a specific timestamp in argument may be inserted
 * "before" existing values in HBase that were previously stored with a
 * timestamp strictly less than that of this {@code PutRequest}.  It is
 * strongly recommended to use real UNIX timestamps in milliseconds when
 * setting them manually, e.g. from {@link System#currentTimeMillis}.
 */
public final class PutRequest extends BatchableRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey, HBaseRpc.HasFamily,
             HBaseRpc.HasQualifier, HBaseRpc.HasValue {

  private static final byte[] PUT = new byte[] { 'p', 'u', 't' };

  /** Code type used for serialized `Put' objects.  */
  static final byte CODE = 35;

  /**
   * A put with all fields set to a 1-byte array containing a zero.
   * This is useful for loops that need to start with a valid-looking edit.
   */
  static final PutRequest EMPTY_PUT;
  static {
    final byte[] zero = new byte[] { 0 };
    EMPTY_PUT = new PutRequest(zero, zero, zero, zero, zero);
    EMPTY_PUT.setRegion(new RegionInfo(zero, zero, zero));
  }

  private final byte[] qualifier;
  private final byte[] value;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue)} instead.
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
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue)} instead.
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
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue, RowLock)} instead.
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
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue, RowLock)} instead.
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
   * Constructor.
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue)} instead.
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
   * Constructor.
   * <p>
   * Note: If you want to set your own timestamp, use
   * {@link #PutRequest(byte[], KeyValue, RowLock)} instead.
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
   * Constructor.
   * @param table The table to edit.
   * @param kv The {@link KeyValue} to store.
   * @since 1.1
   */
  public PutRequest(final byte[] table,
                    final KeyValue kv) {
    this(table, kv, RowLock.NO_LOCK);
  }

  /**
   * Constructor.
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
    super(PUT, table, kv.key(), kv.family(), kv.timestamp(), lockid);
    this.qualifier = kv.qualifier();
    this.value = kv.value();
  }

  /** Private constructor.  */
  private PutRequest(final byte[] table,
                     final byte[] key,
                     final byte[] family,
                     final byte[] qualifier,
                     final byte[] value,
                     final long timestamp,
                     final long lockid) {
    super(PUT, table, key, family, timestamp, lockid);
    KeyValue.checkFamily(family);
    KeyValue.checkQualifier(qualifier);
    KeyValue.checkValue(value);
    this.qualifier = qualifier;
    this.value = value;
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
  public byte[] qualifier() {
    return qualifier;
  }

  @Override
  public byte[] value() {
    return value;
  }

  public String toString() {
    return super.toStringWithQualifier("PutRequest",
                                       family, qualifier,
                                       ", value=" + Bytes.pretty(value)
                                       + ", timestamp=" + timestamp
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
    return 1;
  }

  @Override
  int payloadSize() {
    return KeyValue.predictSerializedSize(key, family, qualifier, value);
  }

  @Override
  void serializePayload(final ChannelBuffer buf) {
    KeyValue.serialize(buf, KeyValue.PUT, timestamp, key, family,
                       qualifier, value);
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

    size += 1;  // byte: Version of Put.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.
    size += 8;  // long: Timestamp.
    size += 8;  // long: Lock ID.
    size += 1;  // bool: Whether or not to write to the WAL.
    size += 4;  // int:  Number of families for which we have edits.

    size += 1;  // vint: Family length (guaranteed on 1 byte).
    size += family.length;  // The family.
    size += 4;  // int:  Number of KeyValues that follow.
    size += 4;  // int:  Total number of bytes for all those KeyValues.

    size += payloadSize();

    return size;
  }

  /** Serializes this request.  */
  @Override
  ChannelBuffer serialize(final byte server_version) {
    final ChannelBuffer buf = newBuffer(server_version,
                                        predictSerializedSize());
    buf.writeInt(2);  // Number of parameters.

    // 1st param: byte array containing region name
    writeHBaseByteArray(buf, region.name());

    // 2nd param: Put object
    buf.writeByte(CODE); // Code for a `Put' parameter.
    buf.writeByte(CODE); // Code again (see HBASE-2877).
    buf.writeByte(1);    // Put#PUT_VERSION.  Stick to v1 here for now.
    writeByteArray(buf, key);  // The row key.

    buf.writeLong(timestamp);  // Timestamp.

    buf.writeLong(lockid);    // Lock ID.
    buf.writeByte(durable ? 0x01 : 0x00);  // Whether or not to use the WAL.

    buf.writeInt(1);  // Number of families that follow.
    writeByteArray(buf, family);  // The column family.

    buf.writeInt(1);  // Number of "KeyValues" that follow.
    buf.writeInt(payloadSize());  // Size of the KV that follows.
    serializePayload(buf);
    return buf;
  }

}
