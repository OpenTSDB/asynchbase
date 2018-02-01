/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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
 * Thrown by the region server if it will block and wait to serve a request.
 * @since 1.7.2
 */
public final class RegionTooBusyException extends NotServingRegionException {

  static final String REMOTE_CLASS =
      "org.apache.hadoop.hbase.RegionTooBusyException";

  /**
   * Constructor.
   * @param msg The message of the exception, potentially with a stack trace.
   * @param failed_rpc The RPC that caused this exception, if known, or null.
   */
  RegionTooBusyException(final String msg, final HBaseRpc failed_rpc) {
    super(msg, failed_rpc);
  }

  @Override
  RegionTooBusyException make(final Object msg, final HBaseRpc rpc) {
    if (msg == this || msg instanceof RegionTooBusyException) {
      final RegionTooBusyException e = (RegionTooBusyException) msg;
      return new RegionTooBusyException(e.getMessage(), rpc);
    }
    return new RegionTooBusyException(msg.toString(), rpc);
  }

  private static final long serialVersionUID = 314159265319912017L;
}
