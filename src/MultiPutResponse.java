/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
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

import static org.hbase.async.Bytes.ByteMap;

/**
 * Stores the response of a {@code multiPut} RPC.
 * This class isn't user-facing.  Users aren't supposed to deal with this
 * response type directly, because it only contains region names, which are
 * meaningless to users.
 * @see MultiPutRequest
 */
final class MultiPutResponse {

  /**
   * Maps a region name to the index of the edit that failed.
   */
  private final ByteMap<Integer> failures = new ByteMap<Integer>();

  /**
   * Constructor.
   */
  private MultiPutResponse() {
  }

  /**
   * Returns map from region name to "index" of the first failed edit.
   * <p>
   * A {@link MultiPutRequest} is essentially a map of region name to a list
   * of edits for this region.  The "index" here will be the index in that list
   * of the first edit that wasn't applied successfully.  The edits past that
   * index may or may not have been applied successfully.
   */
  public ByteMap<Integer> failures() {
    return failures;
  }

  /**
   * Creates a new {@link MultiPutResponse} from a buffer.
   * @param buf The buffer from which to de-serialize the response.
   * @return A new instance.
   */
  public static MultiPutResponse fromBuffer(final ChannelBuffer buf) {
    final int nregions = buf.readInt();
    final MultiPutResponse response = new MultiPutResponse();
    for (int i = 0; i < nregions; i++) {
      final byte[] region_name = HBaseRpc.readByteArray(buf);
      final int failed = buf.readInt();  // Index of the first failed edit.
      if (failed >= 0) {
        response.failures.put(region_name, failed);
      }
    }
    return response;
  }

}
