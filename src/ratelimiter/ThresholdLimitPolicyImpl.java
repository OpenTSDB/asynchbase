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
 * attempts are bellow the minimum acceptable value or failed attempts are
 * higher than acceptable value per time frame
 * 
 * @since 1.9
 */
public class ThresholdLimitPolicyImpl extends LimitPolicy {
  public static final int MAX_FAILED_THRESHOLD = 1000;
  public static final int MIN_SUCCEED_THRESHOLD = 10;

  private final long maximum_acceptable_failed;
  private final long minimum_acceptable_succeeded;

  public ThresholdLimitPolicyImpl() {
    this.minimum_acceptable_succeeded = MIN_SUCCEED_THRESHOLD;
    this.maximum_acceptable_failed = MAX_FAILED_THRESHOLD;
  }

  public ThresholdLimitPolicyImpl(
          final long minimum_acceptable_succeeded,
          final long maximum_acceptable_failed) {
    this.minimum_acceptable_succeeded = minimum_acceptable_succeeded;
    this.maximum_acceptable_failed = maximum_acceptable_failed;
  }

  /**
   * @param rate_limiter Rate Limiter object which stores all the stats for
   * the time frame
   * @return true if the number of succeeded attempts are less than the minimum
   * acceptable succeeded attempts per time frame or number of failed attempts
   * are grater than maximum acceptable attempts.
   * false otherwise
   * Also it may return null, if the number of attempts are less than minimum
   * attempts required to take a decision.
   */
  @Override
  public boolean limit(WriteRateLimiter rate_limiter) throws NotEnoughWritesException {
    if (rate_limiter.getAttempts() >= minimum_acceptable_succeeded) {
      return (rate_limiter.getSucceeded() <= minimum_acceptable_succeeded) ||
         (rate_limiter.getFailed() > maximum_acceptable_failed);
    }

    throw new NotEnoughWritesException(rate_limiter.getAttempts(),
            minimum_acceptable_succeeded);
  }
}
