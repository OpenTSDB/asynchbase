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
 * A bitwise comparator used in comparison filters. Performs the specified
 * bitwise operation on each of the bytes with the specified byte array.
 * Returns whether the result is non-zero.
 * <p>
 * Only EQUAL and NOT_EQUAL comparisons are valid with this comparator.
 * @since 1.6
 */
public final class BitComparator extends FilterComparator {

  /** Bit operators. */
  public enum BitwiseOp {
    /** Logical 'and'. */
    AND,
    /** Logical 'or'.  */
    OR,
    /** Logical 'xor'. */
    XOR,
  }

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.BitComparator");
  private static final byte CODE = 48;

  private final byte[] value;
  private final BitwiseOp bit_operator;

  public BitComparator(byte[] value, BitwiseOp bitOperator) {
    this.value = value;
    this.bit_operator = bitOperator;
  }

  public byte[] value() {
    return value.clone();
  }

  public BitwiseOp bitwiseOperator() {
    return bit_operator;
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  ComparatorPB.Comparator toProtobuf() {
    ByteString byte_string = ComparatorPB
        .BitComparator
        .newBuilder()
        .setComparable(
            ComparatorPB
                .ByteArrayComparable
                .newBuilder()
                .setValue(Bytes.wrap(value)))
        .setBitwiseOp(ComparatorPB
            .BitComparator.BitwiseOp.valueOf(bit_operator.name()))
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
    // Write op
    final byte[] op_name = Bytes.UTF8(bit_operator.name());
    buf.writeShort(op_name.length);       // 2
    buf.writeBytes(op_name);              // op_name.length
  }

  @Override
  int predictSerializedSize() {
    return 1 + 3 + value.length + 2 + Bytes.UTF8(bit_operator.name()).length;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)",
        getClass().getSimpleName(),
        Bytes.pretty(value));
  }
}
