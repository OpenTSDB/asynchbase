/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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
   * Constructor.
   * @param msg The message of the exception.
   * @param cause The exception explaining why the response is invalid.
   */
  InvalidResponseException(final String msg, final Exception cause) {
    super(msg, cause);
    this.response = null;
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
