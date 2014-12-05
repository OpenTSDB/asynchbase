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

import com.google.protobuf.ByteString;
import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.ComparatorPB;

/**
 * A substring comparator used in comparison filters such as RowFilter,
 * QualifierFilter, and ValueFilter. A Substring comparator matches any value
 * which contains the provided substring.
 * <p>
 * Only EQUAL and NOT_EQUAL comparisons are valid with this comparator.
 * @since 1.6
 */
public final class SubstringComparator extends FilterComparator {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.SubstringComparator");

  private final String substr;

  public SubstringComparator(String substr) {
    this.substr = substr;
  }

  public String substring() {
    return this.substr;
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  ComparatorPB.Comparator toProtobuf() {
    ByteString byte_string = ComparatorPB
        .SubstringComparator
        .newBuilder()
        .setSubstr(substr)
        .build()
        .toByteString();
    return super.toProtobuf(byte_string);
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    super.serializeOld(buf);                 // super.predictSerializedSize()
    // Write code
    buf.writeByte(0);                        // 1
    buf.writeByte((byte) NAME.length);       // 1
    buf.writeBytes(NAME);                    // NAME.length
    // writeUTF the substring
    byte[] expr_bytes = Bytes.UTF8(substr);
    buf.writeShort(expr_bytes.length);       // 2
    buf.writeBytes(expr_bytes);              // expr.length
  }

  @Override
  int predictSerializedSize() {
    return super.predictSerializedSize()
        + 1 + 1 + NAME.length
        + 2 + Bytes.UTF8(substr).length;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getSimpleName(), substr);
  }
}
