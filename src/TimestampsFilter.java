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

import com.google.common.collect.Lists;
import org.hbase.async.generated.FilterPB;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.Collection;
import java.util.TreeSet;

/**
 * Filter that returns only cells whose timestamp (version) is
 * in the specified list of timestamps (versions).
 * <p>
 * Note: Use of this filter overrides any time range/time stamp
 * options specified using {@link org.hbase.async.Scanner#setTimeRange(long, long)}.
 * @since 1.6
 */
public final class TimestampsFilter extends ScanFilter {

  private static final byte[] NAME =
    Bytes.UTF8("org.apache.hadoop.hbase.filter.TimestampsFilter");

  private final TreeSet<Long> timestamps;

  /**
   * Create a timestamp filter which filters all key values with a timestamp not
   * in the provided collection.
   *
   * @param timestamps collection of timestamps to keep
   */
  public TimestampsFilter(Collection<Long> timestamps) {
    this.timestamps = new TreeSet<Long>(timestamps);
  }

  /**
   * Create a timestamp filter which filters all key values with a timestamp
   * not in the provided timestamps.
   *
   * @param timestamps collection of timestamps to keep
   */
  public TimestampsFilter(Long... timestamps) {
    this(Lists.newArrayList(timestamps));
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  byte[] serialize() {
    final FilterPB.TimestampsFilter.Builder builder =
        FilterPB.TimestampsFilter.newBuilder();
    builder.addAllTimestamps(timestamps);
    return builder.build().toByteArray();
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    buf.writeByte((byte) NAME.length);   //  1
    buf.writeBytes(NAME);                // 47
    buf.writeInt(timestamps.size());     //  4
    for (Long timestamp : timestamps) {
      buf.writeLong(timestamp);          // timestamps.size() * 8
    }
  }

  @Override
  int predictSerializedSize() {
    return 1 + 47 + 4 + timestamps.size() * 8;
  }
}
