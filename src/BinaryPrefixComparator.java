/*
 * Copyright (C) 2012  The Async HBase Authors.  All rights reserved.
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
 * A binary comparator used in comparison filters. Compares byte arrays
 * lexicographically up to the length of the provided byte array.
 */
public class BinaryPrefixComparator extends FilterComparator {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.BinaryPrefixComparator");
  private static final byte CODE = 47;

  private final byte[] value;

  public BinaryPrefixComparator(byte[] value) {
    this.value = value;
  }

  public byte[] value() {
    return value.clone();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  ComparatorPB.Comparator toProtobuf() {
    ByteString byte_string = ComparatorPB
        .BinaryPrefixComparator
        .newBuilder()
        .setComparable(
            ComparatorPB
                .ByteArrayComparable
                .newBuilder()
                .setValue(Bytes.wrap(value)))
        .build()
        .toByteString();
    return super.toProtobuf(byte_string);
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    super.serializeOld(buf);              // super.predictSerializedSize()
    // Write class code
    buf.writeByte(CODE);                  // 1
    // Write value
    HBaseRpc.writeByteArray(buf, value);  // 3 + value.length
  }

  @Override
  int predictSerializedSize() {
    return super.predictSerializedSize() + 1 + 3 + value.length;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)",
        getClass().getSimpleName(),
        Bytes.pretty(value));
  }
}
