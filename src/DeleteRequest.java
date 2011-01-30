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


import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Deletes some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class DeleteRequest extends HBaseRpc {

  private static final byte[] DELETE = new byte[] {
    'd', 'e', 'l', 'e', 't', 'e'
  };

  private final byte[] family;     // TODO(tsuna): Handle multiple families?
  private final byte[] qualifier;  // TODO(tsuna): Handle multiple qualifiers?
  private final long lockid;

  /**
   * Constructor to delete an entire row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   */
  public DeleteRequest(final byte[] table, final byte[] key) {
    this(table, key, null, null, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier) {
    this(table, key, family, qualifier, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param lock An explicit row lock to use with this request.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final RowLock lock) {
    this(table, key, family, qualifier, lock.id());
  }

  /**
   * Constructor to delete an entire row.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   */
  public DeleteRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes(), null, null, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param lock An explicit row lock to use with this request.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier,
                       final RowLock lock) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), lock.id());
  }

  /** Private constructor.  */
  private DeleteRequest(final byte[] table,
                        final byte[] key,
                        final byte[] family,
                        final byte[] qualifier,
                        final long lockid) {
    super(DELETE, table, key);
    if (family != null) {  // Right now family != null  =>  qualifier != null
      KeyValue.checkFamily(family);
      KeyValue.checkQualifier(qualifier);
    }
    this.family = family;
    this.qualifier = qualifier;
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

  long lockid() {
    return lockid;
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
    size += 4;  // int:  Total length of the whole KeyValue.
    size += 4;  // int:  Total length of the key part of the KeyValue.
    size += 4;  // int:  Length of the value part of the KeyValue.
    size += 2;  // short:Length of the key.
    size += key.length;  // The row key (again!).
    size += 1;  // byte: Family length (again!).
    size += family.length;  // The column family (again!).
    size += qualifier.length;  // The column qualifier.
    size += 8;  // long: The timestamp (again!).
    size += 1;  // byte: The type of KeyValue.
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte unused_server_version) {
    final ChannelBuffer buf = newBuffer(predictSerializedSize());
    buf.writeInt(2);  // Number of parameters.

    // 1st param: byte array containing region name
    writeHBaseByteArray(buf, region.name());

    // 2nd param: Delete object.
    buf.writeByte(31);   // Code for a `Delete' parameter.
    buf.writeByte(31);   // Code again (see HBASE-2877).
    buf.writeByte(1);    // Delete#DELETE_VERSION.  Undocumented versioning.
    writeByteArray(buf, key);
    buf.writeLong(Long.MAX_VALUE);  // Maximum timestamp.
    buf.writeLong(lockid);  // Lock ID.

    // Families.
    if (family == null) {
      buf.writeInt(0);  // Number of families that follow.
      return buf;
    }
    buf.writeInt(1);    // Number of families that follow.

    // Each family is then written like so:
    writeByteArray(buf, family);  // Column family name.
    buf.writeInt(1);              // How many KeyValues for this family?

    // Write the KeyValue
    final int total_rowkey_length = 2 + key.length + 1 + family.length
      + qualifier.length + 8 + 1;
    buf.writeInt(total_rowkey_length + 8);  // Total length of the KeyValue.
    buf.writeInt(total_rowkey_length);      // Total length of the row key.
    buf.writeInt(0);                        // Length of the (empty) value.
    buf.writeShort(key.length);
    buf.writeBytes(key);      // Duplicate key...
    buf.writeByte(family.length);
    buf.writeBytes(family);   // Duplicate column family...
    buf.writeBytes(qualifier);
    buf.writeLong(Long.MAX_VALUE);   // Timestamp (we set it to the max value).
    buf.writeByte(KeyValue.DELETE);  // Type of the KeyValue.
    // No `value' part of the KeyValue to write.
    return buf;
  }

}
