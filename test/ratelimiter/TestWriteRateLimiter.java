/*
 * Copyright (C) 2018 The Async HBase Authors.  All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import org.hbase.async.AppendRequest;
import org.hbase.async.BaseTestHBaseClient.FakeTaskTimer;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseRpc;
import org.hbase.async.NotServingRegionException;
import org.hbase.async.PutRequest;
import org.hbase.async.RegionClient;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.ratelimiter.WriteRateLimiter.SIGNAL;
import org.jboss.netty.channel.Channels;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, RegionClient.class, Channels.class,
    RPCPB.ResponseHeader.class, NotServingRegionException.class,
    RPCPB.ExceptionResponse.class, HBaseRpc.class,
    AppendRequest.class, PutRequest.class, RateLimiter.class })
public class TestWriteRateLimiter {

  private WriteRateLimiter limiter;
  private FakeTaskTimer timer;
  private LimitPolicy rate_policy;
  private LimitPolicy threshold_policy;
  private RateLimiter guava_limiter;
  private RegionClient regin_client;

  @Before
  public void before() throws Exception {
    timer = new FakeTaskTimer();
    rate_policy = new RateLimitPolicyImpl();
    threshold_policy = new ThresholdLimitPolicyImpl();
    guava_limiter = mock(RateLimiter.class);
    regin_client = mock(RegionClient.class);

    PowerMockito.whenNew(RateLimiter.class).withAnyArguments().thenReturn(guava_limiter);
    PowerMockito.when(regin_client.toString()).thenReturn("rc1");
  }

  @Test
  public void ratePolicyRestrictTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    final int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }
  }
  
  @Test
  public void ratePolicyReleaseTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    final int max_iteration = 49;
    Double next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;
    limiter.overrideCurrentRate(WriteRateLimiter.RATE_MIN_THRESHOLD);
    limiter.resetOverridenHealthCheck();

    Double curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

      if (cycle < 2) {
        assertFalse(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  @Test
  public void ratePolicyFullCycleTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, rate_policy, timer, 60000);
    int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }

    max_iteration = 49;
    next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;

    curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  @Test
  public void thresholdPolicyRestrictTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, threshold_policy, timer, 60000);
    final int max_iteration = 45;

    Double curr_rate = limiter.getCurrentRate();
    assertTrue(limiter.isHealthy());
    assertNull(curr_rate);
    Double next_rate = (double) WriteRateLimiter.RATE_DEF_MAX_THRESHOLD;

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.FAILURE);
      }

      if (cycle < 20) {
        assertTrue(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertEquals((double) WriteRateLimiter.RATE_MIN_THRESHOLD,
                curr_rate, 0.1);
      }
      else {
        assertEquals(next_rate, curr_rate, 0.1);
        next_rate = next_rate -
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
      }
    }
  }

  @Test
  public void thresholdPolicyReleaseTest() throws Exception {
    limiter = new WriteRateLimiter(1000, 10, 10, regin_client,
            true, threshold_policy, timer, 60000);
    final int max_iteration = 49;
    Double next_rate = (double) WriteRateLimiter.RATE_MIN_THRESHOLD;
    limiter.overrideCurrentRate(WriteRateLimiter.RATE_MIN_THRESHOLD);
    limiter.resetOverridenHealthCheck();

    Double curr_rate = limiter.getCurrentRate();
    assertEquals(next_rate, curr_rate, 0.1);
    assertTrue(limiter.isHealthy());

    for(int cycle = 1;cycle <= max_iteration; cycle++) {
      for (int i = 0; i < 1000; i++) {
        limiter.ping(SIGNAL.ATTEMPT);
        limiter.ping(SIGNAL.SUCCESS);
      }

      if (cycle < 2) {
        assertFalse(limiter.isHealthy());
      }

//      assertNotNull(timer.getPausedTask());
      timer.continuePausedTask();
      curr_rate = limiter.getCurrentRate();

      if (cycle == max_iteration) {
        assertNull(curr_rate);
      }
      else {
        next_rate = next_rate +
              (next_rate * WriteRateLimiter.RATE_PERCENTAGE_CHANGE/100);
        assertEquals(next_rate, curr_rate, 0.1);
      }
    }
  }

  /** Simple mock RateLimiter to capture and modify calls */
  public static class MockRateLimiter {
    public RateLimiter limiter;
    public double current_rate = 0;
    public int acquire_attempts = 0;
    public boolean allow = true;
    
    public MockRateLimiter() {
      limiter = mock(RateLimiter.class);
      
      PowerMockito.mockStatic(RateLimiter.class);
      
      // since the limiter may be nulled and created anew, make sure to reset
      // counters and flags.
      when(RateLimiter.create(anyDouble())).thenAnswer(new Answer<RateLimiter>() {
        @Override
        public RateLimiter answer(InvocationOnMock invocation) throws Throwable {
          current_rate = (Double)invocation.getArguments()[0];
          acquire_attempts = 0;
          allow = true;
          return limiter;
        }
      });
      
      when(limiter.tryAcquire()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(InvocationOnMock invocation) throws Throwable {
          ++acquire_attempts;
          return allow;
        }
      });
      
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          current_rate = (Double)invocation.getArguments()[0];
          return null;
        }
      }).when(limiter).setRate(anyDouble());
      
      when(limiter.getRate()).thenAnswer(new Answer<Double>() {
        @Override
        public Double answer(InvocationOnMock invocation) throws Throwable {
          return current_rate;
        }
      });
    }
  }
}