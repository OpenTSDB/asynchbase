/*
 * Copyright (c) 2010, 2011  StumbleUpon, Inc.  All rights reserved.
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
public final class DeleteRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifiers {

  private static final byte[] DELETE = new byte[] {
    'd', 'e', 'l', 'e', 't', 'e'
  };

  /** Special value for {@link #qualifiers} when deleting a whole family.  */
  private static final byte[][] DELETE_FAMILY_MARKER =
    new byte[][] { HBaseClient.EMPTY_ARRAY };

  private final byte[] family;     // TODO(tsuna): Handle multiple families?
  private final byte[][] qualifiers;
  private final long lockid;

  /**
   * Constructor to delete an entire row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table, final byte[] key) {
    this(table, key, null, null, RowLock.NO_LOCK);
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
    this(table, key, family, null, RowLock.NO_LOCK);
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
      this(table, key, family, qualifier == null ? null : new byte[][] { qualifier }, RowLock.NO_LOCK);
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
    this(table, key, family, qualifiers, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
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
    this(table, key, family, qualifier == null ? null : new byte[][] { qualifier }, lock.id());
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
    this(table, key, family, qualifiers, lock.id());
  }

  /**
   * Constructor to delete an entire row.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes(), null, null, RowLock.NO_LOCK);
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
    this(table.getBytes(), key.getBytes(), family.getBytes(), null, RowLock.NO_LOCK);
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
         qualifier == null ? null : new byte[][] { qualifier.getBytes() }, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
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
         qualifier == null ? null : new byte[][] { qualifier.getBytes() }, lock.id());
  }

  /** Private constructor.  */
  private DeleteRequest(final byte[] table,
                        final byte[] key,
                        final byte[] family,
                        final byte[][] qualifiers,
                        final long lockid) {
    super(DELETE, table, key);
    if (family != null) {
      KeyValue.checkFamily(family);
    }
    this.family = family;

    if (qualifiers != null) {
      if (family == null) {
        throw new IllegalArgumentException("You can't delete specific qualifiers"
          + " without specifying which family they belong to."
          + "  table=" + Bytes.pretty(table)
          + ", key=" + Bytes.pretty(key));
      }
      for (final byte[] qualifier : qualifiers) {
        KeyValue.checkQualifier(qualifier);
      }
      this.qualifiers = qualifiers;
    } else {
      // No specific qualifier to delete: delete the entire family.  Not that
      // if `family == null', we'll delete and setting this is harmless.
      this.qualifiers = DELETE_FAMILY_MARKER;
    }
    this.lockid = lockid;
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
  public byte[] family() {
    return family;
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
    return size + sizeOfKeyValues();
  }

  /** Returns the serialized size of all the {@link KeyValue}s in this RPC.  */
  private int sizeOfKeyValues() {
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
    if (qualifiers != null) {
      size *= qualifiers.length;
      for (final byte[] qualifier : qualifiers) {
        size += qualifier.length;  // The column qualifier.
      }
    }
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

    // Are we deleting a whole family at once or just a bunch of columns?
    final byte type = (qualifiers == DELETE_FAMILY_MARKER
                       ? KeyValue.DELETE_FAMILY : KeyValue.DELETE_COLUMN);
    // Write the KeyValues
    buf.writeInt(qualifiers.length); // Number of KeyValues that follow
    for (final byte[] qualifier : qualifiers) {
      KeyValue.serialize(buf, type, Long.MAX_VALUE,
                         key, family, qualifier, null);
    }
    return buf;
  }

}
