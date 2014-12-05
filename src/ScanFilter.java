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

/**
 * Abstract base class for {@link org.hbase.async.Scanner} filters.
 * <p>
 * These filters are executed on the server side, inside the
 * {@code RegionServer}, while scanning.  They are useful to
 * prune uninteresting data before it gets to the network,
 * but remember that the {@code RegionServer} still has to
 * load the data before it can know whether the filter passes
 * or not, so it's generally not efficient to filter out large
 * amounts of data.
 * <p>
 * Subclasses are guaranteed to be immutable and are thus
 * thread-safe as well as usable concurrently on multiple
 * {@link org.hbase.async.Scanner} instances.
 * @since 1.5
 */
public abstract class ScanFilter {

  /** Package-private constructor to avoid sub-classing outside this package. */
  ScanFilter() {
  }

  /**
   * Returns the name of this scanner on the wire.
   * <p>
   * This method is only used with HBase 0.95 and newer.
   * The contents of the array returned MUST NOT be modified.
   */
  abstract byte[] name();

  /**
   * Serializes the byte representation to a protobuf in a byte array.
   * <p>
   * This method is only used with HBase 0.95 and newer.
   */
  abstract byte[] serialize();

  /**
   * Serializes the byte representation to the RPC channel buffer.
   * <p>
   * This method is only used with HBase 0.94 and before.
   * @param buf The RPC channel buffer to which the byte array is serialized
   */
  abstract void serializeOld(ChannelBuffer buf);

  /**
   * returns the number of bytes that it will write to the RPC channel buffer when {@code serialize}
   * is called. This method helps predict the initial size of the byte array
   * <p>
   * This method is only used with HBase 0.94 and before.
   * @return A strictly positive integer
   */
  abstract int predictSerializedSize();

}
