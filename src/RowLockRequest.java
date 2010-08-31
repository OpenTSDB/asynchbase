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
  ChannelBuffer serialize() {
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

    public ReleaseRequest(final RowLock lock) {
      super(UNLOCK_ROW);
      this.lock = lock;
    }

    ChannelBuffer serialize() {
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
