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
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import java.util.ArrayList;
import java.util.List;
import org.hbase.async.generated.FilterPB;

/**
 * Sets multiple binary prefix filters to filter results based on the column qualifiers.
 * <p>
 * Efficiently compares the column qualifier bytes up to the length of the
 * prefix to see if it matches.
 * <p>
 * Only setting this filter will return all rows that match the criteria
 * but at the same will cost a full table scan. However, it utilizes the sorting of the
 * qualifiers to seek efficiently within the row.
 * @since 1.8
 */
public final class MultipleColumnPrefixFilter extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.MultipleColumnPrefixFilter");

  private final byte[][] prefixes;

  private final int prefixesLength;

  /**
   * Constructor for UTF-8 prefix strings.
   */
  public MultipleColumnPrefixFilter(final String[] prefixes) {
      this.prefixes = new byte[prefixes.length][];
      for (int i = 0; i < prefixes.length; i++) {
        this.prefixes[i] = Bytes.UTF8(prefixes[i]);
    }
      this.prefixesLength = estimatePrefixesLength();
  }

  /**
   * Constructor.
   * @throws IllegalArgumentException if the prefixes array is empty.
   */
  public MultipleColumnPrefixFilter(final byte[][] prefixes) {
    if (prefixes.length == 0) {
      throw new IllegalArgumentException("Empty prefix");
    }
    this.prefixes = prefixes;
    this.prefixesLength = estimatePrefixesLength();
  }

  private int estimatePrefixesLength() {
      int prefix_length = 0;
      for (int i = 0; i < this.prefixes.length; i++) {
        prefix_length += this.prefixes[i].length;
    }
      return prefix_length;
  }

  @Override
  byte[] serialize() {
    List<ByteString> byte_strings = new ArrayList<ByteString>(this.prefixes.length);
    for (int i = 0; i < this.prefixes.length; i++) {
        byte_strings.add(ZeroCopyLiteralByteString.wrap(this.prefixes[i]));
    }
    return FilterPB.MultipleColumnPrefixFilter.newBuilder()
            .addAllSortedPrefixes(byte_strings)
      .build()
      .toByteArray();
  }


  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  int predictSerializedSize() {
    return 1 + NAME.length + 3 * (prefixes.length + 1) + prefixesLength;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);     // 1
    buf.writeBytes(NAME);                  // 57
    for (int i = 0; i < this.prefixes.length; i++) {
        HBaseRpc.writeByteArray(buf, prefixes[i]);  // 3 + prefix.length
    }
  }

  @Override
  public String toString() {
      StringBuilder builder  = new StringBuilder(this.prefixesLength + this.prefixes.length); // size in bytes + 1 for each separator
      for (int i = 0; i < this.prefixes.length; i++) {
        builder.append(this.prefixes[i]);
        builder.append(";");
      };
      return "MultipleColumnPrefixFilter(" + builder.toString() + ")";
  }

}
