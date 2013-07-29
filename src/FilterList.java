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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import org.hbase.async.generated.FilterPB;

/**
 * Combines a list of filters into one.
 * Since 1.5
 */
public final class FilterList extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.FilterList");

  private final List<ScanFilter> filters;
  private final Operator op;

  /**
   * Operator to combine the list of filters together.
   * since 1.5
   */
  public enum Operator {
    /** All the filters must pass ("and" semantic).  */
    MUST_PASS_ALL,
    /** At least one of the filters must pass ("or" semantic).  */
    MUST_PASS_ONE,
  }

  /**
   * Constructor.
   * Equivalent to {@link #FilterList(List, Operator)
   * FilterList}{@code (filters, }{@link Operator#MUST_PASS_ALL}{@code )}
   */
  public FilterList(final List<ScanFilter> filters) {
    this(filters, Operator.MUST_PASS_ALL);
  }

  /**
   * Constructor.
   * @param filters The filters to combine.  <strong>This list does not get
   * copied, do not mutate it after passing it to this object</strong>.
   * @param op The operator to use to combine the filters together.
   * @throws IllegalArgumentException if the list of filters is empty.
   */
  public FilterList(final List<ScanFilter> filters, final Operator op) {
    if (filters.isEmpty()) {
      throw new IllegalArgumentException("Empty filter list");
    }
    this.filters = filters;
    this.op = op;
  }

  @Override
  byte[] serialize() {
    final FilterPB.FilterList.Builder filter =
      FilterPB.FilterList.newBuilder();
    if (op == Operator.MUST_PASS_ALL) {
      filter.setOperator(FilterPB.FilterList.Operator.MUST_PASS_ALL);
    } else {
      filter.setOperator(FilterPB.FilterList.Operator.MUST_PASS_ONE);
    }
    for (final ScanFilter f : filters) {
      final FilterPB.Filter nested = FilterPB.Filter.newBuilder()
        .setNameBytes(Bytes.wrap(f.name()))
        .setSerializedFilter(Bytes.wrap(f.serialize()))
        .build();
      filter.addFilters(nested);
    }
    return filter.build().toByteArray();
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    int size = 1 + NAME.length + 1 + 4;
    for (final ScanFilter filter : filters) {
      size += 2;
      size += filter.predictSerializedSize();
    }
    return size;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);   // 1
    buf.writeBytes(NAME);                //41
    buf.writeByte((byte) op.ordinal());  // 1
    buf.writeInt(filters.size());        // 4
    for (final ScanFilter filter : filters) {
      buf.writeByte(54);  // 1 : code for WritableByteArrayComparable
      buf.writeByte(0);   // 1 : code for NOT_ENCODED
      filter.serializeOld(buf);
    }
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(32 + filters.size() * 48);
    buf.append("FilterList(filters=[");
    for (final ScanFilter filter : filters) {
      buf.append(filter.toString());
      buf.append(", ");
    }
    buf.setLength(buf.length() - 2);  // Remove the last ", "
    buf.append("], op=").append(op).append(")");
    return buf.toString();
  }

}
