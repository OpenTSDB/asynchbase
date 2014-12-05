/*
 * Copyright (C) 2013  The Async HBase Authors.  All rights reserved.
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

import org.hbase.async.generated.FilterPB;

/**
 * Sets a binary prefix to filter results based on the column qualifier.
 * <p>
 * Efficiently compares the column qualifier bytes up to the length of the
 * prefix to see if it matches.
 * <p>
 * Only setting this filter will return all rows that match the criteria
 * but at the same will cost a full table scan.
 * @since 1.5
 */
public final class ColumnPrefixFilter extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.ColumnPrefixFilter");

  private final byte[] prefix;

  /**
   * Constructor for a UTF-8 prefix string.
   */
  public ColumnPrefixFilter(final String prefix) {
    this(Bytes.UTF8(prefix));
  }

  /**
   * Constructor.
   * @throws IllegalArgumentException if the prefix is an empty byte array.
   */
  public ColumnPrefixFilter(final byte[] prefix) {
    if (prefix.length == 0) {
      throw new IllegalArgumentException("Empty prefix");
    }
    this.prefix = prefix;
  }

  @Override
  byte[] serialize() {
    return FilterPB.ColumnPrefixFilter.newBuilder()
      .setPrefix(Bytes.wrap(prefix))
      .build()
      .toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length + 3 + prefix.length;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);     // 1
    buf.writeBytes(NAME);                  // 49
    HBaseRpc.writeByteArray(buf, prefix);  // 3 + prefix.length
  }

  public String toString() {
    return "ColumnPrefixFilter(" + Bytes.pretty(prefix) + ")";
  }

}
