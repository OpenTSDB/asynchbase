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
 * An exception for which it's <em>typically</em> pointless to retry
 * (such as {@link TableNotFoundException}).
 */
public class NonRecoverableException extends HBaseException {

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  NonRecoverableException(final String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   */
  NonRecoverableException(final String msg, final Throwable cause) {
    super(msg, cause);
  }

  private static final long serialVersionUID = 1280638547;

}
