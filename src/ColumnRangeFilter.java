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
 * Filters based on a range of column qualifiers.
 * <p>
 * This filter is only compatible with HBase 0.92 and newer.
 * @since 1.5
 */
public final class ColumnRangeFilter extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop.hbase"
      + ".filter.ColumnRangeFilter");

  private final byte[] start_column;
  private final boolean start_inclusive;
  private final byte[] stop_column;
  private final boolean stop_inclusive;

  /**
   * Constructor for UTF-8 strings.
   * Equivalent to {@link #ColumnRangeFilter(byte[], boolean, byte[], boolean)
   * ColumnRangeFilter}{@code (start_column, true, stop_inclusive, true)}
   */
  public ColumnRangeFilter(final String start_column, final String stop_column) {
    this(Bytes.UTF8(start_column), true, Bytes.UTF8(stop_column), true);
  }

  /**
   * Constructor.
   * Equivalent to {@link #ColumnRangeFilter(byte[], boolean, byte[], boolean)
   * ColumnRangeFilter}{@code (start_column, true, stop_inclusive, true)}
   */
  public ColumnRangeFilter(final byte[] start_column, final byte[] stop_column) {
    this(start_column, true, stop_column, true);
  }

  /**
   * Constructor for UTF-8 strings.
   * @param start_column The column from which to start returning values.
   * If {@code null}, start scanning from the beginning of the row.
   * @param start_inclusive If {@code true}, the start column is inclusive.
   * @param stop_column The column up to which to return values.
   * If {@code null}, continue scanning until the end of the row.
   * @param stop_inclusive If {@code true}, the stop column is inclusive.
   */
  public ColumnRangeFilter(final String start_column, final boolean start_inclusive,
                           final String stop_column, final boolean stop_inclusive) {
    this(Bytes.UTF8(start_column), start_inclusive,
         Bytes.UTF8(stop_column), stop_inclusive);
  }

  /**
   * Constructor.
   * @param start_column The column from which to start returning values.
   * If {@code null}, start scanning from the beginning of the row.
   * @param start_inclusive If {@code true}, the start column is inclusive.
   * @param stop_column The column up to which to return values.
   * If {@code null}, continue scanning until the end of the row.
   * @param stop_inclusive If {@code true}, the stop column is inclusive.
   */
  public ColumnRangeFilter(final byte[] start_column, final boolean start_inclusive,
                           final byte[] stop_column, final boolean stop_inclusive) {
    this.start_column = start_column;
    this.start_inclusive = start_inclusive;
    this.stop_column = stop_column;
    this.stop_inclusive = stop_inclusive;
  }

  @Override
  byte[] serialize() {
    final FilterPB.ColumnRangeFilter.Builder filter =
      FilterPB.ColumnRangeFilter.newBuilder();
    if (start_column != null) {
      filter.setMinColumn(Bytes.wrap(start_column));
      if (start_inclusive) {
        filter.setMinColumnInclusive(true);
      }
    }
    if (stop_column != null) {
      filter.setMaxColumn(Bytes.wrap(stop_column));
      if (stop_inclusive) {
        filter.setMaxColumnInclusive(true);
      }
    }
    return filter.build().toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length
      + 1 + 3 + (start_column == null ? 0 : start_column.length) + 1
      + 1 + 3 + (stop_column == null ? 0 : stop_column.length) + 1;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);             // 1
    buf.writeBytes(NAME);                          // 48

    if (start_column == null) {
      buf.writeByte(1);                            // 1: False
      buf.writeByte(0);                            // 1: vint
    } else {
      buf.writeByte(0);                            // 1: True
      HBaseRpc.writeByteArray(buf, start_column);  // 3 + start_column.length
    }
    buf.writeByte(start_inclusive ? 1 : 0);        // 1

    if (stop_column == null) {
      buf.writeByte(1);                            // 1: False
      buf.writeByte(0);                            // 1: vint
    } else {
      buf.writeByte(0);                            // 1: True
      HBaseRpc.writeByteArray(buf, stop_column);   // 3 + stop_column.length
    }
    buf.writeByte(stop_inclusive ? 1 : 0);         // 1
  }

  public String toString() {
    return "ColumnRangeFilter(start=" + Bytes.pretty(start_column)
      + (start_inclusive ? " (in" : " (ex")
      + "clusive), stop=" + Bytes.pretty(stop_column)
      + (stop_inclusive ? " (in" : " (ex") + "clusive))";
  }

}
