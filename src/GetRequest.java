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
 * Reads something from HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class GetRequest extends HBaseRpc {

  private static final byte[] GET = new byte[] { 'g', 'e', 't' };
  private static final byte[] EXISTS =
    new byte[] { 'e', 'x', 'i', 's', 't', 's' };

  private byte[] family;     // TODO(tsuna): Handle multiple families?
  private byte[] qualifier;  // TODO(tsuna): Handle multiple qualifiers?
  private long lockid = RowLock.NO_LOCK;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final byte[] table, final byte[] key) {
    super(GET, table, key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * <strong>This byte array will NOT be copied.</strong>
   */
  public GetRequest(final String table, final byte[] key) {
    this(table.getBytes(), key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes());
  }

  /**
   * Private constructor to build an "exists" RPC.
   * @param unused Unused, simply used to help the compiler find this ctor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  private GetRequest(final float unused,
                     final byte[] table,
                     final byte[] key) {
    super(EXISTS, table, key);
  }

  /**
   * Package-private factory method to build an "exists" RPC.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @return An {@link HBaseRpc} that will return a {@link Boolean}
   * indicating whether or not the given table / key exists.
   */
  static HBaseRpc exists(final byte[] table, final byte[] key) {
    return new GetRequest(0F, table, key);
  }

  /**
   * Specifies a particular column family to get.
   * @param family The column family.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest family(final byte[] family) {
    KeyValue.checkFamily(family);
    this.family = family;
    return this;
  }

  /** Specifies a particular column family to get.  */
  public GetRequest family(final String family) {
    return family(family.getBytes());
  }

  /**
   * Specifies a particular column qualifier to get.
   * @param qualifier The column qualifier.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest qualifier(final byte[] qualifier) {
    if (qualifier == null) {
      throw new NullPointerException("qualifier");
    }
    this.qualifier = qualifier;
    return this;
  }

  /** Specifies a particular column qualifier to get.  */
  public GetRequest qualifier(final String qualifier) {
    return qualifier(qualifier.getBytes());
  }

  /** Specifies an explicit row lock to use with this request.  */
  public GetRequest withRowLock(final RowLock lock) {
    lockid = lock.id();
    return this;
  }

  boolean versionSensitive() {
    return true;  // Sad.  HBASE-3174 broke backwards compatibilty!@#$%^  :(
  }

  /**
   * Predicts a lower bound on the serialized size of this RPC.
   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
   * Since we use a static buffer, if the prediction is wrong and turns out
   * to be less than what we need, there will be an exception which will
   * prevent the RPC from being serialized.  That'd be a severe bug.
   */
  private int predictSerializedSize(final byte server_version) {
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 3;  // vint: region name length (3 bytes => max length = 32768).
    size += region.name().length;  // The region name.

    size += 1;  // byte: Type of the 2nd parameter.
    size += 1;  // byte: Type again (see HBASE-2877).
    size += 1;  // byte: Version of Get.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.
    size += 8;  // long: Lock ID.
    size += 4;  // int:  Max number of versions to return.
    size += 1;  // byte: Whether or not to use a filter.
    if (server_version >= 26) {  // New in 0.90 (because of HBASE-3174).
      size += 1;  // byte: Whether or not to cache the blocks read.
    }
    size += 8;  // long: Minimum timestamp.
    size += 8;  // long: Maximum timestamp.
    size += 1;  // byte: Boolean: "all time".
    size += 4;  // int:  Number of families.
    if (family != null) {
      size += 1;  // vint: Family length (guaranteed on 1 byte).
      size += family.length;  // The family.
      size += 1;  // byte: Boolean: do we want specific qualifiers?
      if (qualifier != null) {
        size += 4;  // int:  How many qualifiers follow?
        size += 3;  // vint: Qualifier length.
        size += qualifier.length;  // The qualifier.
      }
    }
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte server_version) {
    final ChannelBuffer buf = newBuffer(predictSerializedSize(server_version));
    buf.writeInt(2);  // Number of parameters.

    // 1st param: byte array containing region name
    writeHBaseByteArray(buf, region.name());

    // 2nd param: Get object
    buf.writeByte(32);   // Code for a `Get' parameter.
    buf.writeByte(32);   // Code again (see HBASE-2877).
    buf.writeByte(1);    // Get#GET_VERSION.  Undocumented versioning of Get.
    writeByteArray(buf, key);
    buf.writeLong(lockid);  // Lock ID.
    buf.writeInt(1);     // Max number of versions to return.
    buf.writeByte(0x00); // boolean (false): whether or not to use a filter.
    // If the previous boolean was true:
    //   writeByteArray(buf, filter name as byte array);
    //   write the filter itself

    if (server_version >= 26) {  // New in 0.90 (because of HBASE-3174).
      buf.writeByte(0x01);  // boolean (true): whether to cache the blocks.
    }

    // TimeRange
    buf.writeLong(0);               // Minimum timestamp.
    buf.writeLong(Long.MAX_VALUE);  // Maximum timestamp.
    buf.writeByte(0x01);            // Boolean: "all time".
    // The "all time" boolean indicates whether or not this time range covers
    // all possible times.  Not sure why it's part of the serialized RPC...

    // Families.
    buf.writeInt(family != null ? 1 : 0);  // Number of families that follow.

    if (family != null) {
      // Each family is then written like so:
      writeByteArray(buf, family);  // Column family name.
      // Boolean: do we want specific qualifiers?
      buf.writeByte(qualifier == null ? 0x00 : 0x01);
      if (qualifier != null) {
        buf.writeInt(1);                 // How many qualifiers do we want?
        writeByteArray(buf, qualifier);  // Column qualifier name.
      }
    }
    return buf;
  }

}
