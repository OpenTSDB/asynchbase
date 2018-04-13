/*
 * Copyright (C) 2018  The Async HBase Authors.  All rights reserved.
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
package org.hbase.async.ratelimiter;

/**
 * Indicates there is not enough writes happen to reevaluate the write rate
 * 
 * @since 1.9
 */
public class NotEnoughWritesException extends Exception {
  private static final long serialVersionUID = 5406883318649964227L;

  /**
   * Default ctor.
   * @param writes current write rate
   * @param min_writes Minimum write rate required
   */
  public NotEnoughWritesException(long writes, long min_writes) {
    super(getErrorMessage(writes, min_writes));
  }

  /**
   * CTor Override.
   * @param writes current write rate
   * @param min_writes Minimum write rate required
   * @param cause any Throwable object
   */
  public NotEnoughWritesException(long writes, long min_writes, Throwable cause) {
    super(getErrorMessage(writes, min_writes), cause);
  }
  
  /**
   * 
   * @param writes current write rate
   * @param min_writes Minimum write rate required
   * @return error message
   */
  private static String getErrorMessage(long writes, long min_writes) {
    return "Current write rate, " + writes + ",less than the minimum writes "
            + min_writes + " required for rate reevaluation";
  }
}
