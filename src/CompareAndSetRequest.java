/*
 * Copyright (c) 2010, 2011  StumbleUpon, Inc.  All rights reserved.
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

import org.hbase.async.generated.ClientPB.Condition;
import org.hbase.async.generated.ClientPB.MutateRequest;
import org.hbase.async.generated.ClientPB.MutateResponse;
import org.hbase.async.generated.ClientPB.MutationProto;
import org.hbase.async.generated.ComparatorPB.BinaryComparator;
import org.hbase.async.generated.ComparatorPB.ByteArrayComparable;
import org.hbase.async.generated.ComparatorPB.Comparator;
import org.hbase.async.generated.HBasePB.CompareType;

/**
 * Atomically executes a Compare-And-Set (CAS) on an HBase cell.
 * <p>
 * This class is package-private just to reduce the amount of public API,
 * but it could be exposed if needed.
 * @since 1.3
 */
final class CompareAndSetRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifier, HBaseRpc.HasValue,
             HBaseRpc.IsEdit {

  /** Awesome RPC method name...  For HBase 0.94 and earlier only.  */
  private static final byte[] CHECKANDPUT = {
      'c', 'h', 'e', 'c', 'k', 'A', 'n', 'd', 'P', 'u', 't'
  };

  /** Comparator named used for HBase 0.95+.  */
  private static final ByteString BINARYCOMPARATOR =
    ByteString.copyFromUtf8("org.apache.hadoop.hbase.filter.BinaryComparator");

  /** New value.  */
  private final PutRequest put;

  /** Expected value.  */
  private final byte[] expected;

  /**
   * Constructor.
   * @param put Put request to execute if value matches.
   * @param value The expected value to compare against.
   * <strong>This byte array will NOT be copied.</strong>
   */
  public CompareAndSetRequest(final PutRequest put,
                              final byte[] expected) {
    super(put.table(), put.key());
    KeyValue.checkValue(expected);
    this.put = put;
    this.expected = expected;
  }

  @Override
  byte[] method(final byte server_version) {
    return (server_version >= RegionClient.SERVER_VERSION_095_OR_ABOVE
            ? MUTATE
            : CHECKANDPUT);
  }

  @Override
  public byte[] table() {
    return put.table();
  }

  @Override
  public byte[] key() {
    return put.key();
  }

  @Override
  public byte[] family() {
    return put.family();
  }

  @Override
  public byte[] qualifier() {
    return put.qualifier();
  }

  /**
   * Returns the expected value.
   * <p>
   * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
   */
  public byte[] expectedValue() {
    return expected;
  }

  /**
   * Returns the new value.
   * {@inheritDoc}
   */
  @Override
  public byte[] value() {
    return put.value();
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  private int predictSerializedSize() {
    int size = 0;
    size += 4;  // int: Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 3;  // vint: region name length (3 bytes => max length = 32768).
    size += region.name().length;  // The region name.

    size += 1;  // byte: Type of the 2nd parameter.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.

    size += 1;  // byte: Type of the 3rd parameter.
    size += 3;  // vint: family length (3 bytes => max length = 32768).
    size += put.family().length;  // The family name.

    size += 1;  // byte: Type of the 4th parameter.
    size += 3;  // vint: qualifier length (3 bytes => max length = 32768).
    size += put.qualifier().length;  // The qualifier key.

    size += 1;  // byte: Type of the 5th parameter.
    size += 4;  // vint: data length.
    size += expected.length;  // The data.

    // 6th parameter : put request
    size += put.predictPutSize();

    return size;
  }

  @Override
  ChannelBuffer serialize(byte server_version) {
    if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
      return serializeOld(server_version);
    }

    final ByteString key = Bytes.wrap(put.key());
    final ByteString family = Bytes.wrap(put.family());
    final ByteString qualifier = Bytes.wrap(put.qualifier());
    final ByteString value = Bytes.wrap(put.value());

    // The edit.
    final MutationProto.ColumnValue.QualifierValue qual =
      MutationProto.ColumnValue.QualifierValue.newBuilder()
      .setQualifier(qualifier)
      .setValue(value)
      .setTimestamp(put.timestamp())
      .build();
    final MutationProto.ColumnValue column =
      MutationProto.ColumnValue.newBuilder()
      .setFamily(family)
      .addQualifierValue(qual)
      .build();
    final MutationProto.Builder put = MutationProto.newBuilder()
      .setRow(key)
      .setMutateType(MutationProto.MutationType.PUT)
      .addColumnValue(column);

    // The condition that needs to match for the edit to be applied.
    final ByteArrayComparable expected = ByteArrayComparable.newBuilder()
      .setValue(Bytes.wrap(this.expected))
      .build();
    final BinaryComparator cmp = BinaryComparator.newBuilder()
      .setComparable(expected)
      .build();
    final Comparator comparator = Comparator.newBuilder()
      .setNameBytes(BINARYCOMPARATOR)
      .setSerializedComparator(Bytes.wrap(cmp.toByteArray()))
      .build();
    final Condition cond = Condition.newBuilder()
      .setRow(key)
      .setFamily(family)
      .setQualifier(qualifier)
      .setCompareType(CompareType.EQUAL)
      .setComparator(comparator)
      .build();

    final MutateRequest req = MutateRequest.newBuilder()
      .setRegion(region.toProtobuf())
      .setMutation(put.build())
      .setCondition(cond)
      .build();
    return toChannelBuffer(MUTATE, req);
  }

  /** Serializes this request for HBase 0.94 and before.  */
  private ChannelBuffer serializeOld(final byte server_version) {
    final ChannelBuffer buf = newBuffer(server_version, predictSerializedSize());
    buf.writeInt(6);  // Number of parameters.

    // 1st param: byte array: region name.
    writeHBaseByteArray(buf, region.name());

    // 2nd param: byte array: row key.
    writeHBaseByteArray(buf, put.key());

    // 3rd param: byte array: column family.
    writeHBaseByteArray(buf, put.family());

    // 4th param: byte array: qualifier.
    writeHBaseByteArray(buf, put.qualifier());

    // 5th param: byte array: expected value.
    writeHBaseByteArray(buf, expected);

    // 6th param: New value.
    put.serializeInto(buf);

    return buf;
  }

  @Override
  Boolean deserialize(final ChannelBuffer buf, final int cell_size) {
    HBaseRpc.ensureNoCell(cell_size);
    final MutateResponse resp = readProtobuf(buf, MutateResponse.PARSER);
    if (!resp.hasProcessed()) {
      throw new InvalidResponseException(
        "After a CAS on " + put + ", the protobuf in the response didn't "
        + "contain the field indicating whether the CAS was successful or not",
        resp);
    }
    return resp.getProcessed();
  }

}
