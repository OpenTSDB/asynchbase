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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

@PrepareForTest({ Channels.class, SecureRpcHelper.class, RegionClient.class })
public class TestRegionClientSendRpc extends BaseTestRegionClient {
  private static final byte[] QUALIFIER = new byte[] { 'Q', 'A', 'L' };
  private static final byte[] VALUE = new byte[] { 42 };

  @Before
  public void beforeLocal() throws Exception {
    when(hbase_client.getDefaultRpcTimeout()).thenReturn(60000);
    PowerMockito.mockStatic(Channels.class);
    timer.stop();
  }
  
  @Test (expected = NullPointerException.class)
  public void nullRpc() throws Exception {
    region_client.sendRpc(null);
  }
  
  @Test
  public void putBatched() throws Exception {
    when(hbase_client.getFlushInterval()).thenReturn((short)1000);
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.region = region;
    final Deferred<Object> deferred = put.getDeferred();
    
    region_client.sendRpc(put);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNotNull(batched_rpcs);
    assertEquals(1, batched_rpcs.size());
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(0, rpcs_inflight.size());
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(1, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(0, timer.tasks.size());
  }
  
  @Test
  public void putNoFlushing() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.region = region;
    final Deferred<Object> deferred = put.getDeferred();
    
    region_client.sendRpc(put);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void putBatchDisabled() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.region = region;
    put.setBufferable(false);
    final Deferred<Object> deferred = put.getDeferred();
    
    region_client.sendRpc(put);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void appendBatched() throws Exception {
    when(hbase_client.getFlushInterval()).thenReturn((short)1000);
    final AppendRequest append = new AppendRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, VALUE);
    append.region = region;
    final Deferred<Object> deferred = append.getDeferred();
    
    region_client.sendRpc(append);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNotNull(batched_rpcs);
    assertEquals(1, batched_rpcs.size());
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(append);
    assertEquals(0, rpcs_inflight.size());
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(1, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(0, timer.tasks.size());
  }
  
  @Test
  public void appendNoFlushing() throws Exception {
    final AppendRequest append = new AppendRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, VALUE);
    append.region = region;
    final Deferred<Object> deferred = append.getDeferred();
    
    region_client.sendRpc(append);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(append);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void appendBatchDisabled() throws Exception {
    final AppendRequest append = new AppendRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, VALUE);
    append.region = region;
    append.setBufferable(false);
    final Deferred<Object> deferred = append.getDeferred();
    
    region_client.sendRpc(append);
    
    try {
      deferred.join(1);
    } catch (TimeoutException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(append);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void getRequest() throws Exception {    
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    get.getDeferred(); // required to initialize the deferred
    
    region_client.sendRpc(get);
    
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
  }

  @Test
  public void getRequestNoTimeout() throws Exception {
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    get.setTimeout(0);
    get.getDeferred(); // required to initialize the deferred
    
    region_client.sendRpc(get);
    
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(0, timer.tasks.size());
  }
  
  @Test
  public void getRequestCustomTimeout() throws Exception {
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    get.setTimeout(42000);
    get.getDeferred(); // required to initialize the deferred
    
    region_client.sendRpc(get);
    
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, timer.tasks.size());
    assertEquals(42000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test (expected = AssertionError.class)
  public void getRequestUninitializedDeferred() throws Exception {
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    
    region_client.sendRpc(get);
  }
  
  @Test
  public void getRequestBlockedWriteIgnoreState() throws Exception {
    when(chan.isWritable()).thenReturn(false);
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    get.getDeferred();
    
    region_client.sendRpc(get);

    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
  }
  
  @Test
  public void getRequestBlockedWrite() throws Exception {
    when(chan.isWritable()).thenReturn(false);
    Whitebox.setInternalState(region_client, "check_write_status", true);
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    final Deferred<Object> deferred = get.getDeferred();
    
    region_client.sendRpc(get);

    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(1, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    try {
      deferred.join();
      fail("Expected a PleaseThrottleException");
    } catch (PleaseThrottleException e) { } 
  }
  
  @Test
  public void multiAction() throws Exception {
    final Counter counter = new Counter();
    Whitebox.setInternalState(hbase_client, "num_multi_rpcs", counter);
    final MultiAction ma = new MultiAction();
    final PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.setRegion(region);
    ma.add(put1);
    final PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.setRegion(region);
    ma.add(put2);
    final Deferred<Object> deferred = ma.getDeferred();
    
    region_client.sendRpc(ma);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof TimeoutException);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(ma);
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, counter.get());
  }
  
  @Test
  public void multiActionToSingle() throws Exception {
    final Counter counter = new Counter();
    Whitebox.setInternalState(hbase_client, "num_multi_rpcs", counter);
    final MultiAction ma = new MultiAction();
    final PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.setRegion(region);
    ma.add(put1);
    final Deferred<Object> deferred = ma.getDeferred();
    
    region_client.sendRpc(ma);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof TimeoutException);
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(ma);
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(0, counter.get());
  }
  
  // Throws an NPE when it tries to serialize the action
  @Test
  public void multiActionEmpty() throws Exception {
    final Counter counter = new Counter();
    Whitebox.setInternalState(hbase_client, "num_multi_rpcs", counter);
    final MultiAction ma = new MultiAction();
    final Deferred<Object> deferred = ma.getDeferred();
    
    region_client.sendRpc(ma);
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof NullPointerException);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(ma);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(1, counter.get());
  }

  @Test
  public void nullChannelPut() throws Exception {
    Whitebox.setInternalState(region_client, "chan", (Channel)null);
    
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    final Deferred<Object> deferred = put.getDeferred();

    region_client.sendRpc(put);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof TimeoutException);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(1, region_client.stats().pendingRPCs());
  }
  
  @Test
  public void nullChannelPutDead() throws Exception {
    Whitebox.setInternalState(region_client, "chan", (Channel)null);
    Whitebox.setInternalState(region_client, "dead", true);
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.setRegion(region);
    final Deferred<Object> deferred = put.getDeferred();

    region_client.sendRpc(put);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof TimeoutException);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, times(1)).sendRpcToRegion(put);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
  }
  
  @Test
  public void nullChannelPutDeadNullRegion() throws Exception {
    Whitebox.setInternalState(region_client, "chan", (Channel)null);
    Whitebox.setInternalState(region_client, "dead", true);
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    final Deferred<Object> deferred = put.getDeferred();

    region_client.sendRpc(put);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof ConnectionResetException);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
  }
  
  @Test
  public void nullChannelPutDeadFailFast() throws Exception {
    Whitebox.setInternalState(region_client, "chan", (Channel)null);
    Whitebox.setInternalState(region_client, "dead", true);
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.setRegion(region);
    put.setFailfast(true);
    final Deferred<Object> deferred = put.getDeferred();

    region_client.sendRpc(put);
    
    Exception ex = null;
    try {
      deferred.join(1);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof ConnectionResetException);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
  }

  @Test
  public void wrap() throws Exception {
    final SecureRpcHelper helper = mock(SecureRpcHelper.class);
    Whitebox.setInternalState(region_client, "secure_rpc_helper", helper);
    doAnswer(new Answer<ChannelBuffer>() {
      @Override
      public ChannelBuffer answer(final InvocationOnMock invocation) 
          throws Throwable {
        return ChannelBuffers.wrappedBuffer(new byte[]{ 42 });
      }
    }).when(helper).wrap(any(ChannelBuffer.class));
    
    final GetRequest get = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get.setRegion(region);
    get.getDeferred(); // required to initialize the deferred
    
    region_client.sendRpc(get);
    
    PowerMockito.verifyStatic(times(1));
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(get);
    assertEquals(1, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    verify(helper, times(1)).wrap(any(ChannelBuffer.class));
  }

  @Test
  public void pendingBreached() throws Exception {
    Whitebox.setInternalState(region_client, "pending_limit", 1);
    Whitebox.setInternalState(region_client, "chan", (Channel)null);
    final ArrayList<HBaseRpc> pending_rpcs = new ArrayList<HBaseRpc>(1);
    pending_rpcs.add(new GetRequest(TABLE, KEY, FAMILY));
    Whitebox.setInternalState(region_client, "pending_rpcs", pending_rpcs);
    
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.region = region;
    final Deferred<Object> deferred = put.getDeferred();
    
    region_client.sendRpc(put);
    
    try {
      deferred.join(1);
    } catch (PleaseThrottleException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(0, rpcs_inflight.size());
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(1, region_client.stats().pendingRPCs());
    assertEquals(1, region_client.stats().pendingBreached());
    assertEquals(0, timer.tasks.size());
  }
  
  @Test
  public void inflightBreached() throws Exception {
    Whitebox.setInternalState(region_client, "inflight_limit", 1);
    rpcs_inflight.put(1, new GetRequest(TABLE, KEY, FAMILY));
    
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put.setBufferable(false);
    put.region = region;
    final Deferred<Object> deferred = put.getDeferred();
    
    region_client.sendRpc(put);
    
    try {
      deferred.join(1);
    } catch (PleaseThrottleException e) { }
    final MultiAction batched_rpcs = 
        Whitebox.getInternalState(region_client, "batched_rpcs");
    assertNull(batched_rpcs);
    PowerMockito.verifyStatic(never());
    Channels.write((Channel)any(), (ChannelBuffer)any());
    verify(hbase_client, never()).sendRpcToRegion(put);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(0, region_client.stats().rpcsSent());
    assertEquals(0, region_client.stats().writesBlocked());
    assertEquals(0, region_client.stats().pendingBatchedRPCs());
    assertEquals(0, region_client.stats().pendingRPCs());
    assertEquals(0, region_client.stats().pendingBreached());
    assertEquals(1, region_client.stats().inflightBreached());
    assertEquals(0, timer.tasks.size());
  }
}