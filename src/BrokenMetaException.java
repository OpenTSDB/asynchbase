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

/**
 * Indicates that the {@code .META.} or {@code -ROOT-} table is corrupted.
 */
public final class BrokenMetaException extends NonRecoverableException {

  private final byte[] table;

  /**
   * Constructor.
   * @param region The region we were looking up
   * (if known, can be {@code null} if not known).
   * @param msg A message describing as precisely as possible what's wrong
   * with the META table.
   */
  BrokenMetaException(final RegionInfo region, final String msg) {
    super("Your .META. table seems broken for "
          + (region == null ? "(unknown table)" : region)
          + ".  " + msg);
    this.table = region.table();
  }

  /**
   * Returns the name of the table for which we were trying to lookup a region.
   * @return A possibly {@code null} byte array.
   */
  public byte[] table() {
    return table;
  }

  /**
   * Helper to complain about a particular {@link KeyValue} found.
   * @param region The region we were looking up
   * (if known, can be {@code null} if not known).
   * @param msg A message describing as precisely as possible what's wrong
   * with the META table.
   * @param kv The {@link KeyValue} in which the problem was found.
   */
  static BrokenMetaException badKV(final RegionInfo region,
                                   final String msg,
                                   final KeyValue kv) {
    return new BrokenMetaException(region, "I found a row where " + msg
                                   + ".  KeyValue=" + kv);
  }

  private static final long serialVersionUID = 1280222742;

}
