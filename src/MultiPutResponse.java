/*
 * Copyright 2010 StumbleUpon, Inc.
 * This file is part of Async HBase.
 * Async HBase is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
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
