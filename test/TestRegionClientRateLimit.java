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
package org.hbase.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.hbase.async.BaseTestHBaseClient.FakeTaskTimer;
import org.hbase.async.generated.RPCPB;
import org.jboss.netty.channel.Channels;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.util.concurrent.RateLimiter;
import com.stumbleupon.async.Deferred;
import org.hbase.async.ratelimiter.TestWriteRateLimiter.MockRateLimiter;
import org.hbase.async.ratelimiter.WriteRateLimiter;
import org.hbase.async.ratelimiter.WriteRateLimiter.SIGNAL;
import org.hbase.async.ratelimiter.LimitPolicy;
import org.hbase.async.ratelimiter.RateLimitPolicyImpl;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
           "ch.qos.*", "org.slf4j.*",
           "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, Channels.class,
  RPCPB.ResponseHeader.class, NotServingRegionException.class, 
  RegionInfo.class, RPCPB.ExceptionResponse.class, HBaseRpc.class,
  AppendRequest.class, PutRequest.class, MultiAction.class,
  RateLimiter.class })
public class TestRegionClientRateLimit extends BaseTestRegionClient {
  private static final byte[] QUALIFIER = { 's', 't', 'o' };
  
  private List<SIGNAL> error_signals;
  private Random rnd;
  private MockRateLimiter guava_limiter;
  private WriteRateLimiter rate_limiter;
  private LimitPolicy policy;
  private FakeTaskTimer timer;
  
  @Before
  public void beforeLocal() throws Exception {
    rnd = new Random(System.currentTimeMillis());
    timer = new FakeTaskTimer();
    policy = new RateLimitPolicyImpl(80, 10);
    rate_limiter = new WriteRateLimiter(20, 10, 5, region_client, true, 
        policy, timer, 60000);
    guava_limiter = new MockRateLimiter();
    Whitebox.setInternalState(region_client, "rate_limiter", rate_limiter);
  }
  
  @Test
  public void toFirstLimitWithFailures() throws Exception {
    // need at least 10 RPCs to pass through for the min limit
    for (int i = 0; i < 9; i++) {
      final HBaseRpc rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
      
      assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
      assertEquals(0.0, guava_limiter.current_rate, 0.0001);
      assertEquals(i + 1, region_client.stats().rpcsSent());
    }
    
    // this 10th rpc tips us over the edge
    HBaseRpc rpc = makeRPC();
    region_client.sendRpc(rpc);
    rate_limiter.ping(SIGNAL.FAILURE);
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20.0, guava_limiter.current_rate, 0.0001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    // pretend we're super fast and block one
    guava_limiter.allow = false;
    rpc = makeRPC();
    final Deferred<Object> deferred = rpc.getDeferred();
    region_client.sendRpc(rpc);
    try {
      deferred.join(1);
      fail("Expected a PleaseThrottleException");
    } catch (PleaseThrottleException e) { }
    // don't signal a failure
    
    assertEquals(10, region_client.stats().rpcsSent());
    assertEquals(1, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void toFirstLimitWithFailuresRecoverWithoutDecrease() 
      throws Exception {
    
    // need at least 10 RPCs to pass through for the min limit
    for (int i = 0; i < 9; i++) {
      final HBaseRpc rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
      
      assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
      assertEquals(0.0, guava_limiter.current_rate, 0.0001);
      assertEquals(i + 1, region_client.stats().rpcsSent());
    }
    
    // this 10th rpc tips us over the edge
    HBaseRpc rpc = makeRPC();
    region_client.sendRpc(rpc);
    rate_limiter.ping(SIGNAL.FAILURE);
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20.0, guava_limiter.current_rate, 0.0001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    // now recover while still under the rate
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void toFirstLimitWithFailures50pctStillFail() throws Exception {
    // With the success threshold @ 80%, we keep limiting with 5 bad 5 good rpcs
    // need at least 10 RPCs to pass through for the min limit
    for (int i = 0; i < 10; i++) {
      final HBaseRpc rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20.0, guava_limiter.current_rate, 0.0001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    // send 10 rpcs to satisfy the min attempt limit, half succeed, half fail
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 2 == 0) {
        rate_limiter.ping(SIGNAL.FAILURE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(19.0, guava_limiter.current_rate, 0.0001);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void toFirstLimitWithFailuresRecoverOnce() throws Exception {
    // need at least 10 RPCs to pass through for the min limit
    for (int i = 0; i < 10; i++) {
      final HBaseRpc rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20.0, guava_limiter.current_rate, 0.0001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    // send 10 rpcs to satisfy the min attempt limit, half succeed, half fail
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 2 == 0) {
        rate_limiter.ping(SIGNAL.FAILURE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(19.0, guava_limiter.current_rate, 0.0001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    // send 10 rpcs with enough successes above the threshold
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 9 == 0) {
        rate_limiter.ping(SIGNAL.FAILURE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(19.95, guava_limiter.current_rate, 0.0001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(30, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void toFirstLimitWithWritesBlocked() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 4 == 0) {
        rate_limiter.ping(SIGNAL.WRITES_BLOCKED);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void toFirstLimitWithBreachedInflightQueue() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 4 == 0) {
        rate_limiter.ping(SIGNAL.BREACHED_INFLIGHT_QUEUE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void toFirstLimitWithRandomErrors() throws Exception {
    for (int i = 0; i < 100; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 4 == 0) {
        rate_limiter.ping(randomErrorSignal());
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(100, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void failuresToMinimum() throws Exception {
    // fail to the bottom
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(10.0, guava_limiter.current_rate, 0.0001);
    assertEquals(140, guava_limiter.acquire_attempts);
    
    // run another couple of iterations to make sure we don't drop below the
    // minimum
    for (int i = 0; i < 20; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(10.0, guava_limiter.current_rate, 0.0001);
    assertEquals(160, guava_limiter.acquire_attempts);
    assertEquals(170, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void failuresToMinimumThenRecover() 
      throws Exception {
    // fail to the bottom
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(10.0, guava_limiter.current_rate, 0.0001);
    assertEquals(140, guava_limiter.acquire_attempts);
    
    // recover to the top. NOTE that there is a longer lag on recovery
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(19.799, guava_limiter.current_rate, 0.01);
    assertEquals(290, guava_limiter.acquire_attempts);
    assertEquals(300, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void failuresToMinimumThenRecoverThenFail() throws Exception {
    // fail to the bottom
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(10.0, guava_limiter.current_rate, 0.0001);
    assertEquals(140, guava_limiter.acquire_attempts);
    
    // recover to the top. NOTE that there is a longer lag on recovery
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    // fail again *sigh*
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20.0, guava_limiter.current_rate, 0.01);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(310, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void failuresToMinimumThenRecoverThenFailThenRecover() throws Exception {
    // fail to the bottom
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(10.0, guava_limiter.current_rate, 0.0001);
    assertEquals(140, guava_limiter.acquire_attempts);
    
    // recover to the top. NOTE that there is a longer lag on recovery
    for (int i = 0; i < 150; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    // fail again *sigh*
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20.0, guava_limiter.current_rate, 0.01);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    // recover again *yay!*
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20.0, guava_limiter.current_rate, 0.01);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    assertEquals(320, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
  }
  
  @Test
  public void failuresWithLimiterDisabled() throws Exception {
    rate_limiter = new WriteRateLimiter(20, 10, 5, region_client, false, 
        policy, timer, 60000);
    Whitebox.setInternalState(region_client, "rate_limiter", rate_limiter);
    
    // 10 failures but we never engage the limiter
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(0, guava_limiter.current_rate, 0.001);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRateFromNull() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(15);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRate() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    rate_limiter.overrideCurrentRate(15);
    
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRateOverMax() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(1000);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(1000, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(1000, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void overrideRateBelowMin() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(5);
  }
  
  @Test
  public void overrideRateAndReleaseStillFailing() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(15);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    // release the override and start to fail down
    rate_limiter.resetOverridenHealthCheck();
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(14.25, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRateOverMaxAndReleaseStillFailing() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(200);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(200, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    // release the override and start to fail down
    rate_limiter.resetOverridenHealthCheck();
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(190, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRateOverMaxAndReleaseAndRecover() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(15);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(15, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    // release the override and start to fail down
    rate_limiter.resetOverridenHealthCheck();
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(15.75, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void overrideRateOverMaxAndReleaseRecoverToNull() throws Exception {
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    rate_limiter.overrideCurrentRate(200);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(200, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    
    // release the override and start to fail down
    rate_limiter.resetOverridenHealthCheck();
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.SUCCESS);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    // make sure the rate didn't change after manual intervention
    assertEquals(200, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlockedByRateLimiter());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void noWritesNoChange() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      rate_limiter.ping(SIGNAL.FAILURE);
      assertNotNull(timer.pausedTask);
      timer.continuePausedTask();
    }
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    
    for (int i = 0; i < 10; i++) {
      timer.continuePausedTask();
    }
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
  }
  
  @Test
  public void manyWritesBetweenTicksAllFailed() throws Exception {
    for (int i = 0; i < 100; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 2 == 0) {
        rate_limiter.ping(SIGNAL.SUCCESS);
      } else {
        rate_limiter.ping(SIGNAL.FAILURE);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(100, region_client.stats().rpcsSent());
  }
  
  @Test
  public void manyWritesBetweenTicksOverThreshold() throws Exception {
    for (int i = 0; i < 100; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 8 == 0) {
        rate_limiter.ping(SIGNAL.FAILURE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(0, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(100, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void manyWritesBetweenTicksUnderThreshold() throws Exception {
    for (int i = 0; i < 100; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 4 == 0) {
        rate_limiter.ping(SIGNAL.FAILURE);
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(100, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void manyWritesBetweenTicksUnderThresholdRandomErrors() throws Exception {
    for (int i = 0; i < 100; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
      if (i % 9 == 0) {
        rate_limiter.ping(randomErrorSignal());
      } else {
        rate_limiter.ping(SIGNAL.SUCCESS);
      }
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    assertEquals(0, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(100, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void moreAttemptsThanSuccessSignals() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void moreSuccessSignalsThanAttempts() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 15; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(0, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void moreFailureSignalsThanAttempts() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 15; i++) {
      rate_limiter.ping(SIGNAL.FAILURE);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void moreRandomErrorSignalsThanAttempts() throws Exception {
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 15; i++) {
      rate_limiter.ping(randomErrorSignal());
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedHalfSuccessNewAttempts() throws Exception {
    // 10 attempts
    // 5 success
    // -RUN- => throttle
    // 5 success
    // 10 attempts
    // -RUN- => throttle
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(19, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedAllSuccessNewAttempts() throws Exception {
    // 10 attempts
    // -RUN-
    // 10 success from before
    // 10 attempts
    // -RUN-
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 10; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedAllSuccessNoOtherAttempts() throws Exception {
    // 10 attempts
    // -RUN-
    // 10 success from before
    // -RUN-
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 10; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedAllSuccessNoOtherAttemptsZeroMinAttempts() 
      throws Exception {
    timer = new FakeTaskTimer();
    policy = new RateLimitPolicyImpl(80, 0);
    rate_limiter = new WriteRateLimiter(20, 10, 5, region_client, true, 
        policy, timer, 60000);
    guava_limiter = new MockRateLimiter();
    Whitebox.setInternalState(region_client, "rate_limiter", rate_limiter);
    
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 10; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedAllSuccessSomeAttemptsThrottled() throws Exception {
    // 10 attempts
    // 5 success
    // -RUN- => throttle
    // 5 success from previous
    // 5 attempts
    // 5 attempts blocked
    // -RUN- => throttle
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    for (int i = 0; i < 10; i++) {
      if (i % 2 == 0) {
        guava_limiter.allow = false;
      } else {
        guava_limiter.allow = true;
      }
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(10, guava_limiter.acquire_attempts);
    assertEquals(15, region_client.stats().rpcsSent());
    assertEquals(5, region_client.stats().writesBlockedByRateLimiter());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  @Test
  public void offsetDelayedAllSuccessSomeAttemptsThrottledRecover() throws Exception {
    // 10 attempts
    // 5 success
    // -RUN- => throttle
    // 5 success from previous
    // 10 attempts
    // 10 attempts blocked
    // 10 success current
    // -RUN- => throttle
    for (int i = 0; i < 10; i++) {
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    for (int i = 0; i < 5; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();

    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(0, guava_limiter.acquire_attempts);
    assertEquals(10, region_client.stats().rpcsSent());
    assertNotNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
    
    for (int i = 0; i < 15; i++) {
      rate_limiter.ping(SIGNAL.SUCCESS);
    }
    for (int i = 0; i < 20; i++) {
      if (i % 2 == 0) {
        guava_limiter.allow = false;
      } else {
        guava_limiter.allow = true;
      }
      rpc = makeRPC();
      region_client.sendRpc(rpc);
    }
    
    assertNotNull(timer.pausedTask);
    timer.continuePausedTask();
    
    assertEquals(20, guava_limiter.current_rate, 0.001);
    assertEquals(20, guava_limiter.acquire_attempts);
    assertEquals(20, region_client.stats().rpcsSent());
    assertEquals(10, region_client.stats().writesBlockedByRateLimiter());
    assertNull(Whitebox.getInternalState(rate_limiter, "rate_limiter"));
  }
  
  /** @return a simple get request with the deferred set, ready to send */
  private HBaseRpc makeRPC() {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    rpc.setRegion(region);
    rpc.getDeferred();
    return rpc;
  }

  /** @return a random error signal from the map */
  private SIGNAL randomErrorSignal() {
    if (error_signals == null) {
      error_signals = new ArrayList<SIGNAL>(3);
      error_signals.add(SIGNAL.FAILURE);
      error_signals.add(SIGNAL.WRITES_BLOCKED);
      error_signals.add(SIGNAL.BREACHED_INFLIGHT_QUEUE);
    }
    return error_signals.get(rnd.nextInt(error_signals.size() - 1));
  }
}
