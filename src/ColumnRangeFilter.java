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

public class ColumnRangeFilter extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop.hbase"
      + ".filter.ColumnRangeFilter");

  private final byte[] start_column;
  private final boolean start_inclusive;
  private final byte[] stop_column;
  private final boolean stop_inclusive;

  public ColumnRangeFilter(final byte[] start_column, final byte[] stop_column) {
    this(start_column, true, stop_column, true);
  }

  public ColumnRangeFilter(final byte[] start_column, final boolean start_inclusive,
                           final byte[] stop_column, final boolean stop_inclusive) {
    this.start_column = start_column;
    this.start_inclusive = start_inclusive;
    this.stop_column = stop_column;
    this.stop_inclusive = stop_inclusive;
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length
      + 1 + 3 + (start_column == null ? 0 : start_column.length) + 1
      + 1 + 3 + (stop_column == null ? 0 : stop_column.length) + 1;
  }

  @Override
  void serialize(ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);             // 1
    buf.writeBytes(NAME);                          // 48

    if (start_column == null) {
      buf.writeByte(0);                            // 1: False
      buf.writeByte(0);                            // 1: vint
    } else {
      buf.writeByte(1);                            // 1: True
      HBaseRpc.writeByteArray(buf, start_column);  // 3 + start_column.length
    }
    buf.writeByte(start_inclusive ? 1 : 0);        // 1

    if (stop_column == null) {
      buf.writeByte(0);                            // 1: False
      buf.writeByte(0);                            // 1: vint
    } else {
      buf.writeByte(1);                            // 1: True
      HBaseRpc.writeByteArray(buf, stop_column);   // 3 + stop_column.length
    }
    buf.writeByte(stop_inclusive ? 1 : 0);         // 1
  }

}
