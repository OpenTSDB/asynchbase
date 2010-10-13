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
 * This exception notifies the application to throttle its use of HBase.
 * <p>
 * Since all APIs of {@link HBaseClient} are asynchronous and non-blocking,
 * it's possible that the application would produce RPCs at a rate higher
 * than HBase is able to handle.  When this happens, {@link HBaseClient}
 * will typically do some buffering up to a certain point beyond which RPCs
 * will fail-fast with this exception, to prevent the application from
 * running itself out of memory.
 * <p>
 * This exception is expected to be handled by having the application
 * throttle or pause itself for a short period of time before retrying the
 * RPC that failed with this exception as well as before sending other RPCs.
 * The reason this exception inherits from {@link NonRecoverableException}
 * instead of {@link RecoverableException} is that the usual course of action
 * when handling a {@link RecoverableException} is to retry right away, which
 * would defeat the whole purpose of this exception.  Here, we want the
 * application to <b>retry after a reasonable delay</b> as well as <b>throttle
 * the pace of creation of new RPCs</b>.  What constitutes a "reasonable
 * delay" depends on the nature of RPCs and rate at which they're produced.
 * For a write-heavy high-throughput application, this exception will
 * typically be used when HBase is in the process of splitting a region or
 * migrating a region to another server, in which case the application should
 * stop producing new writes for typically at least 1 second (or significantly
 * slow down its pace, to let {@link HBaseClient} buffer the writes).
 * <p>
 * When {@link HBaseClient} buffers RPCs, it typically uses this exception
 * with a low watermark and a high watermark.  When the buffer hits the low
 * watermark, the next (unlucky) RPC that wants to be buffered will be failed
 * with a {@code PleaseThrottleException}, to send an "advisory warning" to
 * the application that it needs to throttle itself.  All subsequent RPCs
 * that need to be buffered will be buffered until the buffer hits the high
 * watermark.  Once the high watermark has been hit, all subsequent RPCs that
 * need to be buffered will fail-fast with a {@code PleaseThrottleException}.
 * <p>
 * One effective strategy to handle this exception is to set a flag to true
 * when this exception is first emitted that causes the application to pause
 * or throttle its use of HBase.  Then you can retry the RPC that failed
 * (which is accessible through {@link #getFailedRpc}) and add a callback to
 * it in order to unset the flag once the RPC completes successfully.
 * Note that low-throughput applications will typically rarely (if ever)
 * hit the low watermark and should never hit the high watermark, so they
 * don't need complex throttling logic.
 */
public final class PleaseThrottleException extends NonRecoverableException
implements HasFailedRpcException {

  /** The RPC that was failed with this exception.  */
  private final HBaseRpc rpc;

  /**
   * Constructor.
   * @param msg A message explaining why the application has to throttle.
   * @param cause The exception that requires the application to throttle
   * itself (can be {@code null}).
   * @param rpc The RPC that was made to fail with this exception.
   */
  PleaseThrottleException(final String msg,
                          final HBaseException cause,
                          final HBaseRpc rpc) {
    super(msg, cause);
    this.rpc = rpc;
  }

  /**
   * The RPC that was made to fail with this exception.
   */
  public HBaseRpc getFailedRpc() {
    return rpc;
  }

  private static final long serialVersionUID = 1286782542;

}
