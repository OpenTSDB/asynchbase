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
 * An unclassified exception that occurred on the server side.
 */
public final class RemoteException extends NonRecoverableException {

  private final String type;

  /**
   * Constructor.
   * @param type The name of the class of the remote exception.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  RemoteException(final String type, final String msg) {
    super(msg);
    this.type = type;
  }

  /**
   * Returns the name of the class of the remote exception.
   */
  public String getType() {
    return type;
  }

  private static final long serialVersionUID = 1279775242;

}
