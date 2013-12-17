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

/**
 * Filters key values based on the existence of a dependent, or reference,
 * column. Column values in a row will be filtered unless the row contains a
 * value in the dependent column with the same time stamp.
 * <p>
 * A comparison can optionally be provided to further filter based on the value
 * of the dependent column.
 * @since 1.6
 */
public final class DependentColumnFilter extends CompareFilter {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.DependentColumnFilter");

  private final byte[] family;
  private final byte[] qualifier;
  private final boolean drop_dependent_column;

  /**
   * Creates a DependentColumnFilter with the provided dependent column,
   * additional value filter, and specification of whether the dependent
   * column should be included in the results.
   *
   * @param family of dependent column.
   * @param qualifier of dependent column.
   * @param drop_dependent_column whether to include or exclude the dependent
   *            column in the results.
   * @param value_compare_op of dependent-column value filter.
   * @param value_comparator of dependent-column value filter.
   */
  public DependentColumnFilter(final byte[] family,
                               final byte[] qualifier,
                               final boolean drop_dependent_column,
                               final CompareOp value_compare_op,
                               final FilterComparator value_comparator) {
    super(value_compare_op, value_comparator);
    this.family = family;
    this.qualifier = qualifier;
    this.drop_dependent_column = drop_dependent_column;
  }

  /**
   * Creates a DependentColumnFilter with the provided dependent column, and
   * specify whether the dependent column should be included in the results.
   *
   * @param family of dependent column.
   * @param qualifier of dependent column.
   * @param drop_dependent_column whether to include or exclude the dependent
   *            column in the results.
   */
  public DependentColumnFilter(final byte[] family,
                               final byte[] qualifier,
                               final boolean drop_dependent_column) {
    this(family, qualifier, drop_dependent_column, CompareOp.NO_OP, NO_COMPARATOR);
  }

  /**
   * Creates a DependentColumnFilter with the provided dependent column, and
   * dependent-column value comparison filter.
   *
   * @param family of dependent column.
   * @param qualifier of dependent column.
   * @param value_compare_op of value filter.
   * @param value_comparator of value filter.
   */
  public DependentColumnFilter(final byte[] family,
                               final byte[] qualifier,
                               final CompareOp value_compare_op,
                               final FilterComparator value_comparator) {
    this(family, qualifier, false, value_compare_op, value_comparator);
  }

  /**
   * Creates a DependentColumnFilter with the provided dependent column.
   *
   * @param family of dependent column.
   * @param qualifier of dependent column
   */
  public DependentColumnFilter(final byte[] family,
                               final byte[] qualifier) {
    this(family, qualifier, false);
  }

  /**
   * Returns the column family of the dependent column.
   * @return the dependent column's family.
   */
  public byte[] family() {
    return family.clone();
  }

  /**
   * Returns the qualifier of the dependent column.
   * @return the dependent column's qualifier.
   */
  public byte[] qualifier() {
    return qualifier.clone();
  }

  /**
   * Returns whether the dependent column will be included in the results.
   */
  public boolean dropDependentColumn() {
    return drop_dependent_column;
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  byte[] serialize() {
    return FilterPB
        .DependentColumnFilter
        .newBuilder()
        .setCompareFilter(toProtobuf())
        .setColumnFamily(Bytes.wrap(family))
        .setColumnQualifier(Bytes.wrap(qualifier))
        .setDropDependentColumn(drop_dependent_column)
        .build()
        .toByteArray();
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    super.serializeOld(buf);                            // super.predictSerializedSize
    HBaseRpc.writeByteArray(buf, family);               // 3 + family.length
    HBaseRpc.writeByteArray(buf, qualifier);            // 3 + qualifier.length
    buf.writeByte(drop_dependent_column ? 0x01 : 0x00); // 1
  }

  @Override
  int predictSerializedSize() {
    return super.predictSerializedSize()
        + 3 + family.length
        + 3 + qualifier.length
        + 1;
  }

  @Override
  public String toString() {
    return String.format("%s(%s, %s, %s, %s, %s)",
        getClass().getSimpleName(),
        Bytes.pretty(family),
        Bytes.pretty(qualifier),
        drop_dependent_column,
        compare_op.name(),
        comparator.toString());
  }

  // HBase uses a null in place of this value, but that is and was an awful idea.
  private static final FilterComparator NO_COMPARATOR = new FilterComparator() {
    @Override
    byte[] name() {
      return new byte[0];
    }

    @Override
    ComparatorPB.Comparator toProtobuf() {
      return null;
    }

    @Override
    void serializeOld(ChannelBuffer buf) {
      // It's unclear why the order is different here, but it is.
      buf.writeByte(14);    // Writable class code        // 1
      buf.writeByte(17);    // NullInstance class code    // 1
      super.serializeOld(buf);                            // super.predictSerializedSize()
    }

    @Override
    int predictSerializedSize() {
      return 1 + 1 + super.predictSerializedSize();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }
  };
}
