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
 * Exception used when the server sends an invalid response to an RPC.
 */
public final class InvalidResponseException extends NonRecoverableException {

  private final Object response;

  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param response The response that was received from the server.
   */
  InvalidResponseException(final String msg, final Object response) {
    super(msg);
    this.response = response;
  }

  /**
   * Constructor for unexpected response types.
   * @param expected The type of the response that was expected.
   * @param response The response that was received from the server.
   */
  InvalidResponseException(final Class<?> expected, final Object response) {
    super("Unexpected response type.  Expected: " + expected.getName()
          + ", got: " + (response == null ? "null"
                         : response.getClass() + ", value=" + response));
    this.response = response;
  }

  /**
   * Returns the possibly {@code null} response received from the server.
   */
  public Object getResponse() {
    return response;
  }

  private static final long serialVersionUID = 1280883942;

}
