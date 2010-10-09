/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
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
 * Exception thrown when we try to use an invalid or expired scanner ID.
 */
public final class UnknownScannerException extends RecoverableException
implements HasFailedRpcException {

  static final String REMOTE_CLASS =
    "org.apache.hadoop.hbase.UnknownScannerException";

  final HBaseRpc failed_rpc;

  /**
   * Constructor.
   * @param msg The message of the exception, potentially with a stack trace.
   * @param failed_rpc The RPC that caused this exception, if known, or null.
   */
  UnknownScannerException(final String msg, final HBaseRpc failed_rpc) {
    super(msg + "\nCaused by RPC: " + failed_rpc);
    this.failed_rpc = failed_rpc;
  }

  public HBaseRpc getFailedRpc() {
    return failed_rpc;
  }

  @Override
  UnknownScannerException make(final Object msg, final HBaseRpc rpc) {
    return new UnknownScannerException(msg.toString(), rpc);
  }

  private static final long serialVersionUID = 1281457342;

}
