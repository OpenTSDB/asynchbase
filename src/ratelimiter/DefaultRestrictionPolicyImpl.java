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
 * RestrictionPolicy implementation to restrict/limit the rate if number of
 * good signals per time frame is less than number of bad signals.
 * 
 * @since 1.9
 */
public class DefaultRestrictionPolicyImpl extends LimitPolicy {
  private final long rate_of_change;
  private final long minimum_attempts_required;

  public DefaultRestrictionPolicyImpl(final long rate_of_change) {
    this.rate_of_change = rate_of_change;
    this.minimum_attempts_required = MIN_ATTEMPTS_REQUD;
  }

  public DefaultRestrictionPolicyImpl(final long rate_of_change,
          final long minimum_attempts_required) {
    this.rate_of_change = rate_of_change;
    this.minimum_attempts_required = minimum_attempts_required;
  }

  /**
   * @param rate_limiter Rate Limiter object which stores all the stats for
   * the time frame
   * @return true if the rate needs to be restricted, false if the rate
   * limit can be released, null if it is unable to take a decision
   */
  @Override
  public boolean limit(WriteRateLimiter rate_limiter) throws NotEnoughWritesException{
    if (rate_limiter.getAttempts() > minimum_attempts_required) {
      long diff = rate_limiter.getFailed() - rate_limiter.getSucceeded();

      if (diff > 0) {
        return (diff > rate_of_change);
      }
      else {
        return ((-1 * diff) > rate_of_change);
      }
    }

    throw new NotEnoughWritesException(rate_limiter.getAttempts(),
            minimum_attempts_required);
  }
}
