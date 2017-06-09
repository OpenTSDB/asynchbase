/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, RegionClient.class, HBaseRpc.class, 
  GetRequest.class, RegionInfo.class, NioClientSocketChannelFactory.class, 
  Executors.class, HashedWheelTimer.class, NioClientBossPool.class, 
  NioWorkerPool.class })
public class TestHBaseRpc extends BaseTestHBaseClient {
  private int default_timeout;
  
  @Before
  public void beforeLocal() throws Exception {
    timer.stop();
    when(regionclient.getHBaseClient()).thenReturn(client);
    Whitebox.setInternalState(client, "rpc_timeout", 60000);
    default_timeout = 60000;
  }
  
  @Test
  public void enqueueTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    assertNull(rpc.timeout_handle);
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertEquals(1, timer.tasks.size());
    assertNotNull(rpc.timeout_handle);
    assertEquals(default_timeout, rpc.getTimeout());
    assertEquals(default_timeout, (long)timer.tasks.get(0).getValue());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void enqueueTimeoutCustomTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setTimeout(42000);
    final Deferred<Object> deferred = rpc.getDeferred();
    assertNull(rpc.timeout_handle);
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertEquals(1, timer.tasks.size());
    assertNotNull(rpc.timeout_handle);
    assertEquals(42000, rpc.getTimeout());
    assertEquals(42000, (long)timer.tasks.get(0).getValue());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test (expected = IllegalStateException.class)
  public void enqueueTimeoutAlreadyTimedout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    Whitebox.setInternalState(rpc, "has_timedout", true);
    rpc.enqueueTimeout(regionclient);
  }
  
  @Test (expected = NullPointerException.class)
  public void enqueueTimeoutNullRegionClient() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.enqueueTimeout(null);
  }
  
  @Test
  public void enqueueTimeoutZeroTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.setTimeout(0);
    final Deferred<Object> deferred = rpc.getDeferred();
    assertNull(rpc.timeout_handle);
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertNull(rpc.timeout_handle);
    assertEquals(0, rpc.getTimeout());
    assertEquals(0, timer.tasks.size());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void enqueueTimeoutTimerShuttingDown() throws Exception {
    timer = mock(FakeTimer.class);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
      .thenThrow(new IllegalStateException("Shutdown!"));
    Whitebox.setInternalState(client, "rpc_timeout_timer", timer);
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    assertNull(rpc.timeout_handle);
    assertFalse(rpc.hasTimedOut());
    
    rpc.enqueueTimeout(regionclient);
    
    assertNull(rpc.timeout_handle);
    assertEquals(default_timeout, rpc.getTimeout());
    assertFalse(rpc.hasTimedOut());
    verify(regionclient, never()).removeRpc(any(HBaseRpc.class), anyBoolean());
    verify(timer).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
    try {
      deferred.join(1);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
  }
  
  @Test
  public void callback() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    final Object response = new Object();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeout_handle);
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(response);
    assertSame(response, deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackNullResult() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeout_handle);
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(null);
    assertNull(deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackException() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Deferred<Object> deferred = rpc.getDeferred();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeout_handle);
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(new NonRecoverableException("Boo!"));
    try {
      deferred.join();
    } catch (NonRecoverableException e) { }
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackWithTimeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Timeout timeout_handle = mock(Timeout.class);
    final Deferred<Object> deferred = rpc.getDeferred();
    final Object response = new Object();
    rpc.timeout_handle = timeout_handle;
    assertTrue(rpc.hasDeferred());
    
    rpc.callback(response);
    assertSame(response, deferred.join());
    assertEquals(0, rpc.attempt);
    assertFalse(rpc.hasDeferred());
    verify(timeout_handle).cancel();
  }
  
  @Test
  public void callbackWithoutDeferred() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Object response = new Object();
    assertEquals(4, rpc.attempt);
    assertNull(rpc.timeout_handle);
    assertFalse(rpc.hasDeferred());
    
    rpc.callback(response);
    assertEquals(4, rpc.attempt);
    assertFalse(rpc.hasDeferred());
  }
  
  @Test
  public void callbackWithTimeoutWithoutDeferred() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.attempt = 4;
    final Timeout timeout_handle = mock(Timeout.class);
    final Object response = new Object();
    rpc.timeout_handle = timeout_handle;
    assertFalse(rpc.hasDeferred());
    
    rpc.callback(response);
    assertEquals(4, rpc.attempt);
    assertFalse(rpc.hasDeferred());
    verify(timeout_handle).cancel();
  }

  @Test
  public void timeout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(rpc.timeout_handle);
    
    assertNull(rpc.timeout_handle);
    verify(regionclient).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) { }
  }
  
  @Test (expected = IllegalStateException.class)
  public void timeoutAlreadyTimedout() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(rpc.timeout_handle);
    // better not happen
    timer.tasks.get(0).getKey().run(rpc.timeout_handle);
  }
  
  @Test
  public void timeoutDifferentHandle() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.enqueueTimeout(regionclient);
    timer.tasks.get(0).getKey().run(mock(Timeout.class));
    
    assertNull(rpc.timeout_handle);
    verify(regionclient).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) { }
  }
  
  @Test
  public void timeoutNulledRegionClient() throws Exception {
    final GetRequest rpc = new GetRequest(TABLE, KEY, FAMILY);
    final Deferred<Object> deferred = rpc.getDeferred();
    rpc.enqueueTimeout(regionclient);
    Whitebox.setInternalState(rpc, "region_client", (RegionClient)null);
    timer.tasks.get(0).getKey().run(rpc.timeout_handle);
    
    assertNull(rpc.timeout_handle);
    verify(regionclient, never()).removeRpc(rpc, true);
    try {
      deferred.join(1);
      fail("Expected a RpcTimedOutException");
    } catch (RpcTimedOutException ex) { }
  }
}
