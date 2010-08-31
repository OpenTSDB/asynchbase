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
 * An explicit row lock.
 * <p>
 * Row locks can be explicitly acquired in order to serialize edits to a given
 * row.  This feature may disappear from HBase in the future, so try not to use
 * it if you can.
 * <p>
 * While a row is locked, no one else can edit that row.  Other concurrent
 * attempts to lock that row will block until the lock is released.  Beware
 * that the blocking happens inside the RegionServer, so it will tie up a
 * thread of the RegionServer.  If you have many clients contending for the
 * same row lock, you can literally starve a RegionServer by blocking all its
 * IPC threads.
 * <p>
 * Row locks can't be held indefinitely.  If you don't release a row lock after
 * a timeout configured on the server side, the lock will be released
 * automatically by the server and any further attempts to use it will yield an
 * {@link UnknownRowLockException}.
 */
public final class RowLock {

  /** Lock ID used to indicate that there's no explicit row lock.  */
  static final long NO_LOCK = -1L;

  private final byte[] region_name;
  private final long lockid;
  private final long acquired_tick = System.nanoTime();

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param region_name The name of the region on which the lock is held.
   * @param lockid The ID of the lock the server gave us.
   */
  RowLock(final byte[] region_name, final long lockid) {
    this.region_name = region_name;
    this.lockid = lockid;
  }

  /**
   * Returns for how long this lock has been held in nanoseconds.
   * <p>
   * This is a best-effort estimate of the time the lock has been held starting
   * from the point where the RPC response was received and de-serialized out
   * of the network.  Meaning: it doesn't take into account network time and
   * time spent in the client between when the RPC was received and when it was
   * fully de-serialized (e.g. time spent in kernel buffers, low-level library
   * receive buffers, time doing GC pauses and so on and so forth).
   * <p>
   * In addition, the precision of the return value depends on the
   * implementation of {@link System#nanoTime} on your platform.
   */
  public long holdNanoTime() {
    return System.nanoTime() - acquired_tick;
  }

  public String toString() {
    return "RowLock(region_name=" + Bytes.pretty(region_name)
      + ", lockid=" + lockid + ", held for " + holdNanoTime() + "ns)";
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  byte[] region() {
    return region_name;
  }

  long id() {
    return lockid;
  }

}
