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

/**
 * Acquires an explicit row lock.
 * <p>
 * For a description of what row locks are, see {@link RowLock}.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class RowLockRequest extends HBaseRpc {

  private static final byte[] LOCK_ROW = new byte[] {
    'l', 'o', 'c', 'k', 'R', 'o', 'w'
  };

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table containing the row to lock.
   * @param key The key of the row to lock in that table.
   */
  public RowLockRequest(final byte[] table, final byte[] key) {
    super(LOCK_ROW, table, key);
  }

  /**
   * Constructor.
   * @param table The table containing the row to lock.
   * @param key The key of the row to lock in that table.
   */
  public RowLockRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes());
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  private int predictSerializedSize() {
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 3;  // vint: region name length (3 bytes => max length = 32768).
    size += region.name().length;  // The region name.
    size += 1;  // byte: Type of the 2nd parameter.
    size += 3;  // vint: row key length (3 bytes => max length = 32768).
    size += key.length;  // The row key.
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte unused_server_version) {
    final ChannelBuffer buf = newBuffer(predictSerializedSize());
    buf.writeInt(2);  // Number of parameters.

    writeHBaseByteArray(buf, region.name());
    writeHBaseByteArray(buf, key);

    return buf;
  }

  /**
   * Package-private RPC used by {@link HBaseClient} to release row locks.
   */
  static final class ReleaseRequest extends HBaseRpc {

    private static final byte[] UNLOCK_ROW = new byte[] {
      'u', 'n', 'l', 'o', 'c', 'k', 'R', 'o', 'w'
    };

    private final RowLock lock;

    /**
     * Constructor.
     * @param lock The lock we wanna release.
     * @param region The region corresponding to {@code lock.region()}.
     */
    ReleaseRequest(final RowLock lock, final RegionInfo region) {
      super(UNLOCK_ROW, region.table(),
            // This isn't actually the key we locked, but it doesn't matter
            // as this information is useless for this RPC, we simply supply
            // a key to the parent constructor to make it happy.
            region.stopKey());
      this.lock = lock;
    }

    ChannelBuffer serialize(final byte unused_server_version) {
      // num param + type 1 + region length + region + type 2 + long
      final ChannelBuffer buf = newBuffer(4 + 1 + 3 + region.name().length
                                          + 1 + 8);
      buf.writeInt(2);  // Number of parameters.
      writeHBaseByteArray(buf, region.name());
      writeHBaseLong(buf, lock.id());
      return buf;
    }

  }

}
