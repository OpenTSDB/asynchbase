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

import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.ComparatorPB;
import org.hbase.async.generated.FilterPB;
import org.hbase.async.generated.HBasePB;

/**
 * A generic scan filter to be used to filter by comparison. It takes an
 * operator (equal, greater, not equal, etc) and a filter comparator.
 * @since 1.6
 */
public abstract class CompareFilter extends ScanFilter {

  /** Comparison operators. */
  public enum CompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER,
    /** no operation */
    NO_OP,
  }

  final CompareOp compare_op;
  final FilterComparator comparator;

  public CompareFilter(final CompareOp compareOp,
                       final FilterComparator comparator) {
    this.compare_op = compareOp;
    this.comparator = comparator;
  }

  public CompareOp compareOperation() {
      return compare_op;
  }

  public FilterComparator comparator() {
      return comparator;
  }

  protected final FilterPB.CompareFilter toProtobuf() {
    final FilterPB.CompareFilter.Builder builder =
        FilterPB.CompareFilter.newBuilder();
    final ComparatorPB.Comparator comparator_pb = comparator.toProtobuf();

    if (comparator_pb != null) {
      builder.setComparator(comparator_pb);
    }

    return builder
        .setCompareOp(HBasePB.CompareType.valueOf(compare_op.name()))
        .build();
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    // Write the filter name
    buf.writeByte((byte) name().length);            // 1
    buf.writeBytes(name());                         // name().length
    // writeUTF comparison operation
    buf.writeShort(compare_op.name().length());     // 2
    buf.writeBytes(Bytes.UTF8(compare_op.name()));  // compare_op.name().length
    // Write the comparator
    comparator.serializeOld(buf);
  }

  @Override
  int predictSerializedSize() {
    return 1 + name().length
         + 2 + compare_op.name().length()
         + comparator.predictSerializedSize();
  }

  @Override
  public String toString() {
    return String.format("%s(%s, %s)",
        getClass().getSimpleName(),
        compare_op.name(),
        comparator.toString());
  }
}
