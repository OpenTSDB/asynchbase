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

import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.hbase.async.RegionClient;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It monitors the health of a system and limit the rate of flow, when the
 * system goes to unhealthy state.
 * Use its "ping" api to pass both good and bad signals, and call "isHealthy"
 * api to check whether the system is healthy or not.
 *
 * @since 1.9
 */
public class WriteRateLimiter {
  private static final Logger LOG = LoggerFactory.getLogger(WriteRateLimiter.class);

  public static enum SIGNAL {
    ATTEMPT,
    SUCCESS,
    FAILURE,
    WRITES_BLOCKED,
    BREACHED_INFLIGHT_QUEUE
  }

  /* Maximum rate limit, there will not be any rate limit beyond this point.
    This is to make sure there will not be any blocking code when system is
    healthy
   */
  public static final int RATE_DEF_MAX_THRESHOLD = 1000;

  /* Minimum rate limit, rate will not be restricted less than this, so that
    it will not completely block the flow keep some space to recover
   */
  public static final int RATE_MIN_THRESHOLD = 10;

  //Rate of change in percentage
  public static final int RATE_PERCENTAGE_CHANGE = 10;

  //Rate will be revised after this timeframe, 1 minute
  public static final int MIN_TIMEFRAME_IN_MS = 60000;

  private final AtomicLong attempts = new AtomicLong(0);
  private final AtomicLong succeeded = new AtomicLong(0);
  private final AtomicLong failed = new AtomicLong(0);
  private final AtomicLong write_blocked = new AtomicLong(0);
  private final AtomicLong breached_inflight_queue = new AtomicLong(0);

  private volatile RateLimiter rate_limiter = null;

  //Maximum rate limit, there will not be any rate limit beyond this point.
  //This is to make sure there will not be any blocking code when system is
  //healthy
  private final double max_threshold;

  //Minimum rate limit, rate will not be restricted less than this.
  //So that it will not completely block the flow keep some space to recover
  private final double min_threshold;

  //Incremental change in which the rate will be restricted
  private final double restrict_rate;

  //Incremental change in which the rate will be releaseed
  private final double release_rate;

  private boolean enable_rate_limiter;
  private boolean override_health_check = false;

  //Rate restriction policy
  private final LimitPolicy policy;
  private final RegionClient region_client;

  //Rate will be revised after this timeframe
  private final long time_frame;
  private final Timer timer;
  private Timeout timeout;

  /**
   *
   * @param region_client RegionClient where this rate limiter is attached with
   * @param enable enable Health monitor or not
   * @param timer Timer object to reset attempts counters every second
   * @throws IllegalArgumentException if it is enabled with timer is null
   */
  public WriteRateLimiter(final RegionClient region_client,
          final boolean enable,
          final Timer timer) throws IllegalArgumentException {

    this(RATE_DEF_MAX_THRESHOLD,
            RATE_MIN_THRESHOLD,
            RATE_PERCENTAGE_CHANGE,
            region_client,
            enable,
            new ThresholdLimitPolicyImpl(),
            timer,
            MIN_TIMEFRAME_IN_MS);
  }

  /**
   *
   * @param max_threshold Rate will be controlled upto this limit, after that
   * it will release the rate monitoring
   * @param min_threshold Will not limit rate bellow this limit, to make sure
   * it will not completely block the flow
   * @param percentage_change the rate of change in rate when the system is
   * going unhealthy from healthy state, and vice-versa
   * @param region_client RegionClient where this rate limiter is attached with
   * @param enable enable Health monitor or not.
   * @param policy restriction policy
   * @param timer Timer object to reset attempts counters every second
   * @param time_frame time frame in millisecond
   * @throws IllegalArgumentException if region client is null or it is enabled
   * with timer is null
   */
  public WriteRateLimiter(final int max_threshold,
          final int min_threshold,
          double percentage_change,
          final RegionClient region_client,
          final boolean enable,
          final LimitPolicy policy,
          final Timer timer,
          final int time_frame) throws IllegalArgumentException{

    if (region_client == null) {
      throw new IllegalArgumentException("RegionClient is null");
    }

    if (policy == null) {
      throw new IllegalArgumentException("Limit policy is null");
    }

    this.enable_rate_limiter = enable;
    this.region_client = region_client;
    this.policy = policy;

    if (percentage_change <= 0 || percentage_change >= 100) {
      LOG.warn("Rate of change, " +  percentage_change + ", is out of the "
              + "permissible range 1 to 99, default to " + RATE_PERCENTAGE_CHANGE
              + ", region client " + region_client);
      percentage_change = RATE_PERCENTAGE_CHANGE;
    }

    this.restrict_rate = 1 - (percentage_change/100);
    this.release_rate = 1 + (percentage_change/100);

    if (min_threshold < RATE_MIN_THRESHOLD) {
      this.min_threshold = RATE_MIN_THRESHOLD;
      LOG.warn("Minimum rate threshold, " + min_threshold + ", is less than the"
              + " minimum permissible value " + RATE_MIN_THRESHOLD
              + ", so default to " + this.min_threshold
              + ", region client " + region_client);
    }
    else {
      this.min_threshold = min_threshold;
    }

    if (max_threshold < this.min_threshold) {
      if (this.min_threshold > RATE_DEF_MAX_THRESHOLD ) {
        this.max_threshold = (int)(this.min_threshold * this.restrict_rate);
        LOG.warn("Maximum rate threshold " + max_threshold + " is less than "
                + "minimum rate threshold value, " + this.min_threshold
                + ", so reset it to " + this.max_threshold
                + ", region client " + region_client);
      }
      else {
        this.max_threshold = RATE_DEF_MAX_THRESHOLD;
        LOG.warn("Maximum rate threshold " + max_threshold +" is less than "
                + "minimum rate threshold value, " + this.min_threshold
                + ", so reset to default max value " + this.max_threshold
                + ", region client " + region_client);
      }
    }
    else {
      this.max_threshold = max_threshold;
    }

    if (time_frame < MIN_TIMEFRAME_IN_MS) {
      LOG.warn("Time frame, " +  percentage_change + ", is less than the "
              + "permissible minimum value " + MIN_TIMEFRAME_IN_MS
              + " milliseconds, so reset to default value "+ MIN_TIMEFRAME_IN_MS
              + ", region client " + region_client);
      this.time_frame = MIN_TIMEFRAME_IN_MS;
    }
    else {
      this.time_frame = time_frame;
    }

    if (enable && timer == null) {
      throw new IllegalArgumentException("Timer is null");
    }

    this.timer = timer;
    startTimer();
  }

  /**
   *
   * @return true if the system is healthy
   */
  public boolean isHealthy() {
    if (enable_rate_limiter) {
      //Check whether the write rate is limited,
      return !(rate_limiter != null && !rate_limiter.tryAcquire());
    }

    return true;
  }

  /**
   * Track the attempts
   * @param signal attempts/successful attempt/failed attempt
   */
  public void ping(SIGNAL signal) {
    if (enable_rate_limiter && !override_health_check) {
      switch(signal) {
        case ATTEMPT:
          this.attempts.incrementAndGet();
          break;
        case SUCCESS:
          this.succeeded.incrementAndGet();
          break;
        case FAILURE:
          this.failed.incrementAndGet();
          break;
        case WRITES_BLOCKED:
          this.write_blocked.incrementAndGet();
          break;
        case BREACHED_INFLIGHT_QUEUE:
          this.breached_inflight_queue.incrementAndGet();
          break;
      }
    }
  }

  /**
   * Restrict the rate, it creates a RateLimiter with initial (max) limit if
   * there is no restriction is applied yet, else rate limit will be decreased
   * again until the limit reaches min_threshold and stay there to make
   * sure that the flow will not be interrupted completely.
   * @return new rate limit, null if there is no change
   * It should be called from synchronized block
   */
  private Double restrict() {
    Double updated_rate = null;

    if (enable_rate_limiter && !override_health_check) {
      if (rate_limiter == null) {
        rate_limiter = RateLimiter.create(max_threshold);
        updated_rate = rate_limiter.getRate();
      }
      else {
        double curr_rate = rate_limiter.getRate();

        if (curr_rate > min_threshold) {
          double new_rate = curr_rate * this.restrict_rate;

          if (new_rate > min_threshold) {
            rate_limiter.setRate(new_rate);
            updated_rate = new_rate;
          }
          else {
            rate_limiter.setRate(min_threshold);
            updated_rate = min_threshold;
          }
        }
        else if (curr_rate < min_threshold) {
          //A pesimistic check
          rate_limiter.setRate(min_threshold);
          updated_rate = min_threshold;
        }
      }
    }

    return updated_rate;
  }

  /**
   * Release the rate restriction, it increases the limit until it reaches the
   * maximum threshold. Once it crossed the maximum threshold the RateLimiter
   * will be removed, and there will not be any restriction.
   * @return new rate limit, null if there is no change
   * It should be called from synchronized block
   */
  private Double release() {
    Double updated_rate = null;

    if (enable_rate_limiter && !override_health_check) {
      if (rate_limiter != null) {
        double curr_rate = rate_limiter.getRate();

        if (curr_rate < min_threshold) {
          curr_rate = min_threshold;
        }

        double new_rate = curr_rate * this.release_rate;

        if (new_rate > max_threshold) {
          rate_limiter = null;
          updated_rate = 0.0;
        }
        else {
          rate_limiter.setRate(new_rate);
          updated_rate = new_rate;
        }
      }
    }

    return updated_rate;
  }

    /**
   * Please use this functionality if you are very much sure about what you are
   * doing, it may bring the flow down.
   * This is mainly for testing purpose, to manually control the flow.
   * It will override health check, and it will be disabled.
   * @param preferred_rate preferred rate
   * @return true if it successfully overridden the rate, else false
   * @throws IllegalArgumentException if the preferred rate is less than the
   * minimum threshold.
   * @deprecated Please use this functionality if you are very much sure about
   * what you are doing, it may bring the flow down.
   * This uses synchronized block
   */
  @Deprecated
  public boolean overrideCurrentRate(int preferred_rate)
          throws IllegalArgumentException {

    if (preferred_rate < this.min_threshold) {
      throw new IllegalArgumentException("Preferred rate " + preferred_rate
              + " is less than the minimum rate threshold " + this.min_threshold
              + ", region client " + region_client);
    }

    Double current_rate = null;

    synchronized(this) {
      override_health_check = true;

      if (rate_limiter == null) {
        rate_limiter = RateLimiter.create(preferred_rate);
      }
      else {
        current_rate = rate_limiter.getRate();
        rate_limiter.setRate(preferred_rate);
      }
    }

    if (current_rate == null) {
      LOG.info("Overridden current rate limit, set it to " + preferred_rate
              + ", region client " + region_client);
    }
    else {
      LOG.info("Overridden current rate limit, updated it from " + current_rate
              + " to " + preferred_rate
              + ", region client " + region_client);
    }

    return true;
  }

  /**
   * It call the restriction policy with current number of succeeded and failed
   * attempts and restrict or release the limit based on the result. Also reset
   * both the succeeded and failed attempts counters
   * This uses synchronized block
   */
  private void reviseRateLimiter() {
    if (enable_rate_limiter && !override_health_check) {
      final boolean restrict;
      try {
        restrict = policy.limit(this);
      }
      catch(NotEnoughWritesException ex) {
        LOG.debug(ex.getMessage() + region_client);
        return;
      }

      final Double updated_rate;

      synchronized(this) {
        if (restrict) {
          updated_rate = restrict();
        }
        else  {
          updated_rate = release();
        }

        this.attempts.set(0);
        this.failed.set(0);
        this.succeeded.set(0);
        this.breached_inflight_queue.set(0);
        this.write_blocked.set(0);
      }

      if (updated_rate != null) {
        if (updated_rate == 0) {
          LOG.info("Rate limit has been removed, region client " + region_client);
        }
        else {
          LOG.info("Rate limit has been updated to " + updated_rate
                  + ", region client " + region_client);
        }
      }
    }
  }

  /**
   * Reset the overridden health check via theoverrideCurrentRate, and enable it
   * back.
   */
  public void resetOverridenHealthCheck() {
    override_health_check = false;

    synchronized(this) {
      this.attempts.set(0);
      this.failed.set(0);
      this.succeeded.set(0);
    }
  }

  @Override
  public String toString() {
    return "attempts " + this.attempts.get() +
           ", succeeded " + this.succeeded.get() +
           ", failed " + this.failed.get() +
           " & rate/second" + this.rate_limiter.getRate();
  }

  /**
   * Get the current rate limit per second, it can be null if the rate limiter
   * is not set
   * @return current rate limit per second
   */
  public Double getCurrentRate() {
    return rate_limiter==null?null:rate_limiter.getRate();
  }

  /**
   *
   * @return number of RPCs written to HBase region server within this time
   * frame
   */
  public long getAttempts() {
    return attempts.get();
  }

  /**
   *
   * @return number of responses it received from HBase region servers within
   * this time frame
   */
  public long getSucceeded() {
    return succeeded.get();
  }

  /**
   *
   * @return number of RPCs didn't receive any response from HBase region server
   * or timeouts within this time frame
   */
  public long getFailed() {
    return failed.get();
  }

  /**
   *
   * @return number of RPCs blocked because the channel was not writable, within
   * this time frame
   */
  public long getWriteBlocked() {
    return write_blocked.get();
  }

  /**
   *
   * @return number of RPCs breached in-flight rpc queue within this time frame
   */
  public long getBreachedInflightQueue() {
    return breached_inflight_queue.get();
  }

  /**
   *
   * @return true if rate limiter is enabled
   */
  public boolean enableRateLimiter() {
    return enable_rate_limiter;
  }
  /**
   * Reset the attempts counters every second to control the rate of flow per
   * second
   * This is not second aligned, instead will be called after 1000 milliseconds
   * from the first call. That means if it is called at 10:00:00.010 AM then
   * next would be 10:00:01.010 AM
   * TODO make it configurable, so that the write rate will be limited
   * based on the succeeded/failed attempts per user defined time range
   * for example, if failed signals are more than succeeded signals in last
   * 5 minutes, then reduce the write rate
   * or failed attempts are 5% more than succeeded ... or vice versa
   */
  public final void startTimer() {
    if (enableRateLimiter() && timeout == null) {
      timeout = timer.newTimeout(reset_timer, this.time_frame, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Timer to reset the health signals
   */
  private final class RateLimiterResetTimerTask implements TimerTask {
    @Override
    public void run(final Timeout timeout) {
      WriteRateLimiter.this.timeout = null;
      reviseRateLimiter();
      startTimer();
    }
    @Override
    public String toString() {
      return "Reset health signals";
    }
  };
  private final TimerTask reset_timer = new RateLimiterResetTimerTask();

  public void disableRateLimiter() {
    this.enable_rate_limiter = false;
    if (timeout != null) {
      timeout.cancel();
      timeout = null;
    }
  }
}
