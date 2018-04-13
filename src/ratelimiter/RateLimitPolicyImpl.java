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
 * LimitPolicy implementation to restrict/limit the rate if succeeded
 * attempts are bellow the some acceptable value or failed attempts are
 * higher than acceptable value per time frame
 * 
 * @since 1.9
 */
public class RateLimitPolicyImpl extends LimitPolicy {
  public static final int MIN_SUCCEED_RATE = 10;
  public static final int DEF_SUCCEED_RATE = 50;

  private final int succeed_rate;
  private final int minimum_attempts_required;

  public RateLimitPolicyImpl() {
    this.succeed_rate = DEF_SUCCEED_RATE;
    this.minimum_attempts_required = MIN_ATTEMPTS_REQUD;
  }

  public RateLimitPolicyImpl(
          final int succeed_rate,
          final int minimum_attempts_required) {
    this.succeed_rate = (succeed_rate < MIN_SUCCEED_RATE || succeed_rate > 100)
            ?MIN_SUCCEED_RATE:succeed_rate;
    this.minimum_attempts_required = minimum_attempts_required;
  }

  /**
   * @param rate_limiter Rate Limiter object which stores all the stats for
   * the time frame
   * @return true if the number of succeeded attempts are less than the minimum
   * acceptable succeeded attempts per time frame or number of failed attempts
   * are grater than maximum acceptable attempts.
   * false otherwise
   * @throws NotEnoughWritesException if current write rate is less than minimum
   * write rate required to reevaluate the write rate
   */
  @Override
  public boolean limit(WriteRateLimiter rate_limiter)
          throws NotEnoughWritesException {

    if (rate_limiter.getAttempts() >= minimum_attempts_required) {
      long min_succeeded = rate_limiter.getAttempts() * this.succeed_rate / 100;
      return (rate_limiter.getSucceeded() < min_succeeded);
    }

    throw new NotEnoughWritesException(rate_limiter.getAttempts(),
            minimum_attempts_required);
  }
}
