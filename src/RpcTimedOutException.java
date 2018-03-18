/*
 * Copyright (C) 2015-2018  The Async HBase Authors.  All rights reserved.
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
 * An exception where we haven't received a response for an in-flight RPC
 * @since 1.7
 */
public class RpcTimedOutException extends HBaseException {
  
  /** The RPC that failed */
  final HBaseRpc failed_rpc;
  
  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param rpc The RPC that timed out.
   */
  RpcTimedOutException(final String msg, final HBaseRpc rpc) {
    super(msg);
    failed_rpc = rpc;
  }
	
  /**
   * Constructor.
   * @param msg The message of the exception, potentially including a stack
   * trace.
   * @param cause The exception that caused this one to be thrown.
   * @param rpc The RPC that timed out.
   */
  RpcTimedOutException(final String msg, 
                       final Throwable cause, 
                       final HBaseRpc rpc) {
    super(msg, cause);
    failed_rpc = rpc;
  }

  /** @return The RPC that timed out. */
  public HBaseRpc getFailedRpc() {
    return failed_rpc;
  }
  
  @Override
  RpcTimedOutException make(final Object msg, final HBaseRpc rpc) {
    if (msg == this || msg instanceof RpcTimedOutException) {
      final RpcTimedOutException e = (RpcTimedOutException) msg;
      if (e.getCause() != null) {
        return new RpcTimedOutException(e.getMessage(), e.getCause(), rpc);
      } else {
        return new RpcTimedOutException(e.getMessage(), rpc);
      }
    }
    return new RpcTimedOutException(msg.toString(), rpc);
  }
  
  private static final long serialVersionUID = -6245448564580938789L;
  
}

