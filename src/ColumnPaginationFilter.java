/*
 * Copyright (C) 2014  The Async HBase Authors.  All rights reserved.
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

import java.lang.UnsupportedOperationException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.hbase.async.generated.FilterPB;

/**
 * A filter, based on the ColumnCountGetFilter, takes two arguments: limit and offset.
 * This filter can be used for row-based indexing, where references to other tables are
 * stored across many columns, in order to efficient lookups and paginated results for
 * end users. Only most recent versions are considered for pagination.
 */
public final class ColumnPaginationFilter extends ScanFilter {
  private static final byte[] NAME = Bytes.UTF8("org.apache.hadoop"
      + ".hbase.filter.ColumnPaginationFilter");

  private int limit = 0;
  private int offset = -1;
  private byte[] columnOffset = null;

  /**
   * Initializes filter with an integer offset and limit. The offset is arrived at
   * scanning sequentially and skipping entries. @limit number of columns are
   * then retrieved. If multiple column families are involved, the columns may be spread
   * across them.
   *
   * @param limit Max number of columns to return.
   * @param offset The integer offset where to start pagination.
   */
  public ColumnPaginationFilter(final int limit, final int offset) {
    this.limit = limit;
    this.offset = offset;
  }

  /**
   * Initializes filter with a string/bookmark based offset and limit. The offset is arrived
   * at, by seeking to it using scanner hints. If multiple column families are involved,
   * pagination starts at the first column family which contains @columnOffset. Columns are
   * then retrieved sequentially upto @limit number of columns which maybe spread across
   * multiple column families, depending on how the scan is setup.
   *
   * Only supported with HBase 0.96 and greater.
   *
   * @param limit Max number of columns to return.
   * @param columnOffset The string/bookmark offset on where to start pagination.
   */
  public ColumnPaginationFilter(final int limit, final byte[] columnOffset) {
    this.limit = limit;
    this.columnOffset = columnOffset;
  }

  public ColumnPaginationFilter(final int limit, final String columnOffset) {
    this.limit = limit;
    this.columnOffset = Bytes.UTF8(columnOffset);
  }

  @Override
  byte[] serialize() {
    FilterPB.ColumnPaginationFilter.Builder builder =
        FilterPB.ColumnPaginationFilter.newBuilder();
    builder.setLimit(this.limit);
    if (this.offset >= 0) {
      builder.setOffset(this.offset);
    }
    if (this.columnOffset != null) {
      builder.setColumnOffset(Bytes.wrap(this.columnOffset));
    }
    return builder.build().toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    int size = 1 + NAME.length
        + 3 + (columnOffset == null ? 0 : columnOffset.length)
        + 4  //limit
        + 4; //offset
    return size;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);     // 1
    buf.writeBytes(NAME);                  // 53
    buf.writeInt(limit);                  // 4
    buf.writeInt(offset);                 // 4
    if (columnOffset != null) {
      //Setting a columnOffset is not supported before HBase 0.96
      throw new UnsupportedOperationException(
          "Setting a column offset by byte array is not supported before HBase 0.96");
    }
  }

  @Override
  public String toString() {
    if (this.columnOffset != null) {
      return (this.getClass().getSimpleName() + "(" + this.limit + ", " +
          Bytes.pretty(this.columnOffset) + ")");
    }
    return String.format("%s (%d, %d): predict:%d serialize:%d %s", this.getClass().getSimpleName(),
        this.limit, this.offset, predictSerializedSize(), serialize().length, Bytes.pretty(serialize()));
  }

}
