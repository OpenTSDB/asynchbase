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
 * Exception thrown when we try to use an invalid or expired scanner ID.
 */
public final class UnknownScannerException extends RecoverableException {

  static final String REMOTE_CLASS =
    "org.apache.hadoop.hbase.UnknownScannerException";

  UnknownScannerException(final String msg) {
    super(msg);
  }

  @Override
  UnknownScannerException make(final Object msg) {
    return new UnknownScannerException((String) msg);
  }

  private static final long serialVersionUID = 1281457342;

}
