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
 * Exception thrown when an attempt to use an inexistent table was made.
 */
public final class TableNotFoundException extends NonRecoverableException {

  private final byte[] table;

  /**
   * Constructor.
   */
  TableNotFoundException() {
    super("(unknown table)");
    table = null;
  }

  /**
   * Constructor.
   * @param table The table that wasn't found.
   */
  TableNotFoundException(final byte[] table) {
    super(Bytes.pretty(table));
    this.table = table;
  }

  /**
   * Returns the table that was doesn't exist.
   */
  public byte[] getTable() {
    return table;
  }

  private static final long serialVersionUID = 1280638742;

}
