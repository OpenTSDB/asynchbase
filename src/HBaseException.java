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
 * The parent class of all {@link RuntimeException} created by this package.
 */
public abstract class HBaseException extends RuntimeException {

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   */
  HBaseException(final String msg) {
    super(msg);
  }

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   */
  HBaseException(final String msg, final Throwable cause) {
    super(msg, cause);
  }

  /**
   * Factory method to make it possible to create an exception from another
   * one without having to resort to reflection, which is annoying to use.
   * Sub-classes that want to provide this internal functionality should
   * implement this method.
   * @param arg Some arbitrary parameter to help build the new instance.
   */
  HBaseException make(final Object arg) {
    throw new AssertionError("Must not be used.");
  }

  private static final long serialVersionUID = 1280638842;

}
