/*
 * Copyright (C) 2015 The Async HBase Authors.  All rights reserved.
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.security.sasl.SaslClient;

import org.hbase.async.MultiAction.Response;
import org.hbase.async.auth.SimpleClientAuthProvider;
import org.hbase.async.generated.CellPB.Cell;
import org.hbase.async.generated.ClientPB.GetResponse;
import org.hbase.async.generated.ClientPB.Result;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.generated.RPCPB.CellBlockMeta;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ReadOnlyChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.google.protobuf.CodedOutputStream;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

@PrepareForTest({ Channels.class, GetRequest.class, 
  ChannelHandlerContext.class })
public class TestRegionClientDecode extends BaseTestRegionClient {
  private static final VoidEnum VOID = (VoidEnum)null;
  private static final byte[] ROW = { 0, 0, 1 };
  private static final byte[] FAMILY = { 'n', 'o', 'b' };
  private static final byte[] TABLE = { 'd', 'w' };
  private static final byte[] QUALIFIER = { 'v', 'i', 'm', 'e', 's' };
  private static final byte[] VALUE = { 42 };
  private static final long TIMESTAMP = 1356998400000L;
  
  // NOTE: the TYPE of ChannelBuffer is important! ReplayingDecoderBuffer isn't
  // backed by an array and we have methods that attemp to see if they can
  // perform zero copy operations.
  
  @Before
  public void beforeLocal() throws Exception {
    when(hbase_client.getDefaultRpcTimeout()).thenReturn(60000);
    timer.stop();
  }
  
  @Test
  public void goodGetRequest() throws Exception {
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void goodGetRequestArrayBacked() throws Exception {
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }

  @Test
  public void goodGetRequestWithSecurity() throws Exception {
    injectSecurity();
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, times(1)).handleResponse(buffer, chan);
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void goodGetRequestWithSecurityArrayBacked() throws Exception {
    injectSecurity();
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, times(1)).handleResponse(buffer, chan);
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void goodGetRequestWithSecurityConsumesAll() throws Exception {
    injectSecurity();
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(false, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    when(secure_rpc_helper.handleResponse(buffer, chan)).thenReturn(null);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    
    try {
      deferred.join(500);
      fail("Expected a TimeoutException");
    } catch (TimeoutException te) { }
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void goodGetRequestWithSecurityConsumesAllArrayBacked() throws Exception {
    injectSecurity();
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    when(secure_rpc_helper.handleResponse(buffer, chan)).thenReturn(null);
    
    assertNull(region_client.decode(ctx, chan, buffer, VOID));
    
    try {
      deferred.join(500);
      fail("Expected a TimeoutException");
    } catch (TimeoutException te) { }
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void goodMultiActionResponse94() throws Exception {
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_092_OR_ABOVE);
    final MultiAction rpc = new MultiAction();
    for (int i = 0; i < 100; i++) {
      final PutRequest put = new PutRequest("test".getBytes(), "hello".getBytes(), "t".getBytes(), 
          Bytes.fromInt(100), new byte[] { 42 });
      put.setRegion(region);
      rpc.add(put);
    }
    
    final Deferred<Object> md = rpc.getDeferred();
    inflightTheRpc(201, rpc);
    region_client.decode(ctx, chan, 
        ChannelBuffers.wrappedBuffer(MULTI_ACTION_RESPONSE_094), VOID);
    
    final Response response = (Response) md.join();
    assertEquals(100, response.size());
    for (int i = 0; i < 100; i++) {
      assertEquals(MultiAction.SUCCESS, response.result(i));
    }
  }
  
  @Test
  public void pbufDeserializeFailure() throws Exception {
    // in this case we have a good length and header but the actual pbuf result
    // is missing. We pull the rpc from the inflight map and call it back with
    // the exception.
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 3, 2, 8, 42 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected a NonRecoverableException");
    } catch (NonRecoverableException ex) { }
    
    try {
      deferred.join();
      fail("Expected the join to throw a NonRecoverableException");
    } catch (NonRecoverableException ex) { }
    assertEquals(0, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void pbufDeserializeFailureArrayBacked() throws Exception {
    // in this case we have a good length and header but the actual pbuf result
    // is missing. We pull the rpc from the inflight map and call it back with
    // the exception.
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = 
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 3, 2, 8, 42 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected a NonRecoverableException");
    } catch (NonRecoverableException ex) { }
    
    try {
      deferred.join();
      fail("Expected the join to throw a NonRecoverableException");
    } catch (NonRecoverableException ex) { }
    assertEquals(0, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void pbufDeserializeFailureWHBaseException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    RuntimeException e = null;
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected a HBaseException");
    } catch (HBaseException ex) {
      e = ex;
    }
    assertTrue(e instanceof HBaseException);
    
    e = null;
    try {
      deferred.join(100);
      fail("Expected the join to throw a HBaseException");
    } catch (HBaseException ex) {
      e = ex;
    }
    assertTrue(e instanceof HBaseException);
    assertEquals(0, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void notServingRegionException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    get.region = region;
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = PBufResponses.generateException(id, 
        "org.apache.hadoop.hbase.NotServingRegionException");
    assertNull(region_client.decode(ctx, chan, buffer, VOID));

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1)).handleNSRE(any(HBaseRpc.class), 
        any(byte[].class), any(RecoverableException.class));
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void regionMovedException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    get.region = region;
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = PBufResponses.generateException(id, 
        "org.apache.hadoop.hbase.exceptions.RegionMovedException");
    assertNull(region_client.decode(ctx, chan, buffer, VOID));

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1)).handleNSRE(any(HBaseRpc.class), 
        any(byte[].class), any(RecoverableException.class));
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void noSuchColumnFamilyException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    get.region = region;
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = PBufResponses.generateException(id, 
        "org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException");
    assertNull(region_client.decode(ctx, chan, buffer, VOID));

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, never()).handleNSRE(any(HBaseRpc.class), 
        any(byte[].class), any(RecoverableException.class));
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void regionOpeningException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    get.region = region;
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = PBufResponses.generateException(id, 
        "org.apache.hadoop.hbase.exceptions.RegionOpeningException");
    assertNull(region_client.decode(ctx, chan, buffer, VOID));

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, times(1)).handleNSRE(any(HBaseRpc.class), 
        any(byte[].class), any(RecoverableException.class));
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void versionMismatchException() throws Exception {
    final int id = 42;
    final GetRequest get = new GetRequest(TABLE, ROW);
    get.region = region;
    inflightTheRpc(id, get);
    
    ChannelBuffer buffer = PBufResponses.generateException(id, 
        "org.apache.hadoop.io.VersionMismatchException");
    assertNull(region_client.decode(ctx, chan, buffer, VOID));

    assertEquals(0, rpcs_inflight.size());
    verify(hbase_client, never()).handleNSRE(any(HBaseRpc.class), 
        any(byte[].class), any(RecoverableException.class));
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void replayed() throws Exception {
    resetMockClient();
    
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final byte[] array = new byte[buffer.writerIndex()];
    buffer.readBytes(array);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(Arrays.copyOf(array, 3)))
      .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 3, 10)))
      .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 10, array.length)));
    
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    verify(secure_rpc_helper, never()).handleResponse(buffer, chan);
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void replayedMissingMiddle() throws Exception {
    resetMockClient();
    
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final byte[] array = new byte[buffer.writerIndex()];
    buffer.readBytes(array);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(Arrays.copyOf(array, 3)))
      .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 10, array.length)));
    
    
    region_client.messageReceived(ctx, event);
    try {
      region_client.messageReceived(ctx, event);
      fail("Expected an InvalidResponseException");
    } catch (InvalidResponseException ex) { }
    
    try {
      deferred.join(100);
      fail("Expected an TimeoutException");
    } catch (TimeoutException ex) { }
    
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }

  @Test
  public void replayedSecure() throws Exception {
    resetMockClient();
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "simple");
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, 
        "Cohen");
    
    final SecureRpcHelper96 secure_helper = 
        PowerMockito.spy(new SecureRpcHelper96(hbase_client, 
            region_client, new InetSocketAddress("127.0.0.1", 50512)));
    final SaslClient sasl_client = mock(SaslClient.class);
    Whitebox.setInternalState(secure_helper, "sasl_client", sasl_client);
    when(sasl_client.isComplete()).thenReturn(false);

    Whitebox.setInternalState(region_client, "secure_rpc_helper", secure_helper);

    PowerMockito.when(secure_helper.processChallenge(any(byte[].class)))
      .thenReturn(new byte[] { 24 });
    
    final int id = 42;
    final byte[] array = { 0, 0, 0, 0, 0, 0, 0, 4, 42, 24, 42, 24 };
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    final byte[][] chunks = new byte[2][];
    chunks[0] = Arrays.copyOf(array, 3);
    chunks[1] = Arrays.copyOfRange(array, 10, array.length);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(Arrays.copyOf(array, 3)))
      .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 3, 6)))
          .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 6, 11)))
      .thenReturn(ChannelBuffers.wrappedBuffer(
          Arrays.copyOfRange(array, 11, array.length)));
    
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    
    try {
      deferred.join(100);
    } catch (TimeoutException ex) { }
    
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), never()).cancel();
  }
  
  @Test
  public void replayMultiRPCInBuffer() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    inflightTheRpc(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    inflightTheRpc(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    inflightTheRpc(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[3][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;
    
    len = pbuf2.length - (pbuf2.length / 2);
    chunks[2] = Arrays.copyOfRange(pbuf2, pbuf2.length / 2, pbuf2.length);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[0]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[1]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[2]))
      .thenReturn(ChannelBuffers.wrappedBuffer(pbuf3));

    region_client.messageReceived(ctx, event);
    // we gave netty just a fragment of the first RPC so it will throw an error

    region_client.messageReceived(ctx, event);
    // now netty has the full RPC1 AND a chunk of RPC2. We'll parse all of RPC1
    // and replay to get the next chunk of 2
    
    region_client.messageReceived(ctx, event);
    // at this point we have read all of the data from the buffer so Netty will
    // discard it
    
    region_client.messageReceived(ctx, event);
    
    for (final Deferred<Object> deferred : deferreds) {
      @SuppressWarnings("unchecked")
      final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
      assertEquals(1, kvs.size());
      assertArrayEquals(ROW, kvs.get(0).key());
      assertArrayEquals(FAMILY, kvs.get(0).family());
      assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
      assertArrayEquals(VALUE, kvs.get(0).value());
      assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    }
    assertEquals(3, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    assertEquals(60000, (long)timer.tasks.get(2).getValue());
    verify(timer.timeouts.get(0)).cancel();
    verify(timer.timeouts.get(1)).cancel();
    verify(timer.timeouts.get(2)).cancel();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void replayCorruptSecondRPC() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    inflightTheRpc(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    inflightTheRpc(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    inflightTheRpc(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    // corrupt it
    pbuf2 = Arrays.copyOf(pbuf2, pbuf2.length - 4);
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[3][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;
    
    len = pbuf2.length - (pbuf2.length / 2);
    chunks[2] = Arrays.copyOfRange(pbuf2, pbuf2.length / 2, pbuf2.length);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[0]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[1]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[2]))
      .thenReturn(ChannelBuffers.wrappedBuffer(pbuf3));

    region_client.messageReceived(ctx, event);
    // we gave netty just a fragment of the first RPC so it will throw an error

    region_client.messageReceived(ctx, event);
    // now netty has the full RPC1 AND a chunk of RPC2. We'll parse all of RPC1
    // and replay the rest 

    region_client.messageReceived(ctx, event);
    // because our corrupted RPC is missing a little bit of data at the end 
    // we will proceed to replay
    
    try {
      region_client.messageReceived(ctx, event);
      fail("Expected an InvalidResponseException");
    } catch (InvalidResponseException e) { }

    // Make sure the first RPC was called back
    List<KeyValue> kvs = (List<KeyValue>)deferreds.get(0).join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
   
    // The second will toss the invalid RPC exception, causing the region client
    // to close.
    try {
      kvs = (List<KeyValue>)deferreds.get(1).join(100);
      fail("Expected a TimeoutException");
    } catch (InvalidResponseException e) { }
    
    // and the third will never have been called
    try {
      kvs = (List<KeyValue>)deferreds.get(2).join(100);
      fail("Expected a TimeoutException");
    } catch (TimeoutException e) { }
    
    assertEquals(3, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    assertEquals(60000, (long)timer.tasks.get(2).getValue());
    verify(timer.timeouts.get(0)).cancel();
    verify(timer.timeouts.get(1)).cancel();
    verify(timer.timeouts.get(2), never()).cancel();
  }

  @Test
  public void replayMoreChunks() throws Exception {
    resetMockClient();
    
    List<Deferred<Object>> deferreds = new ArrayList<Deferred<Object>>(3);
    
    final GetRequest get1 = new GetRequest(TABLE, ROW);
    deferreds.add(get1.getDeferred());
    inflightTheRpc(1, get1);
    
    final GetRequest get2 = new GetRequest(TABLE, ROW);
    deferreds.add(get2.getDeferred());
    inflightTheRpc(2, get2);
    
    final GetRequest get3 = new GetRequest(TABLE, ROW);
    deferreds.add(get3.getDeferred());
    inflightTheRpc(3, get3);
    
    byte[] pbuf1 = buildGoodResponse(true, 1).array();
    byte[] pbuf2 = buildGoodResponse(true, 2).array();
    byte[] pbuf3 = buildGoodResponse(true, 3).array();
    
    // chunk these guys up
    byte[][] chunks = new byte[6][];
    chunks[0] = Arrays.copyOf(pbuf1, pbuf1.length / 2);
    
    int len = pbuf1.length - (pbuf1.length / 2);
    byte[] buf = new byte[len + pbuf2.length / 2];
    System.arraycopy(pbuf1, pbuf1.length / 2, buf, 0, len);
    System.arraycopy(pbuf2, 0, buf, len, pbuf2.length / 2);
    chunks[1] = buf;

    len = pbuf2.length / 2;
    chunks[2] = Arrays.copyOfRange(pbuf2, len, len + 4);
    chunks[3] = Arrays.copyOfRange(pbuf2, len + 4, pbuf2.length);
    
    len = pbuf3.length / 2;
    chunks[4] = Arrays.copyOfRange(pbuf3, 0, len);
    chunks[5] = Arrays.copyOfRange(pbuf3, len, pbuf3.length);
    
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage())
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[0]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[1]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[2]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[3]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[4]))
      .thenReturn(ChannelBuffers.wrappedBuffer(chunks[5]));

    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    region_client.messageReceived(ctx, event);
    
    for (final Deferred<Object> deferred : deferreds) {
      @SuppressWarnings("unchecked")
      final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
      assertEquals(1, kvs.size());
      assertArrayEquals(ROW, kvs.get(0).key());
      assertArrayEquals(FAMILY, kvs.get(0).family());
      assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
      assertArrayEquals(VALUE, kvs.get(0).value());
      assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    }
    assertEquals(3, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    assertEquals(60000, (long)timer.tasks.get(2).getValue());
    verify(timer.timeouts.get(0)).cancel();
    verify(timer.timeouts.get(1)).cancel();
    verify(timer.timeouts.get(2)).cancel();
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void eom() throws Exception {
    // we read the whole message some how. Doesn't matter if it's array backed
    // or not in this case.
    final ChannelBuffer buffer = buildGoodResponse(true, 1);
    buffer.readerIndex(buffer.writerIndex());
    region_client.decode(ctx, chan, buffer, VOID);
  }
  
  @Test
  public void notReadableInitial() throws Exception {
    // Fails on the first call to ensureReadable because the size encoded
    // in the first 4 bytes is much larger than it should be
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 42, 1 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) { }
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 42, 1 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) { }
  }
  
  @Test
  public void negativeSize() throws Exception {
    // Fails on the first call to ensureReadable because the size encoded
    // in the first 4 bytes is much larger than it should be
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { -1, -1, -1, -1, 1 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { -1, -1, -1, -1, 1 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
  }
  
  @Test
  public void rpcTooBig() throws Exception {
    // we only accept RPCs up to 256MBs in size right now. In order to avoid
    // allocating 256MB for unit testing, we'll tell the region client to
    // skip the ensureReadable call. Just over the line is 268435456 bytes
    // See HBaseRpc.MAX_BYTE_ARRAY_MASK
    
    PowerMockito.mockStatic(RegionClient.class);
    PowerMockito.doNothing().when(RegionClient.class, "ensureReadable", 
        any(ChannelBuffer.class), anyInt());
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 16, 0, 0, 0, 1 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { 16, 0, 0, 0, 1 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException ex) { }
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void nothingAfterInitialLength() throws Exception {
    // gets into HBaseRpc.readProtobuf and tosses an exception when it tries
    // to read the varint.
    final ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 0 }));
    region_client.decode(ctx, chan, buffer, VOID);
  }
  
  @Test
  public void protobufVarintOnly() throws Exception {
    // gets into HBaseRpc.readProtobuf and tosses an exception when it tries
    // to readBytes on the non-array backed buffer
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 1 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) { }
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 1 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an IndexOutOfBoundsException");
    } catch (IndexOutOfBoundsException oob) { }
  }
  
  @Test
  public void rpcNotInMap() throws Exception {
    // doesn't matter if the rest of the message is missing, we fail as
    // the ID isn't in the map. It also doesn't matter if it's negative since
    // the ID counter can rollover
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 }));
    region_client.decode(ctx, chan, buffer, VOID);
    assertEquals(1, region_client.stats().rpcResponsesUnknown());
    
    buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(
            new byte[] { 0, 0, 0, 1, 6, 8, -42, -1, -1, -1, 15 }));
    region_client.decode(ctx, chan, buffer, VOID);
    assertEquals(2, region_client.stats().rpcResponsesUnknown());
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 2, 8, 42 });
    region_client.decode(ctx, chan, buffer, VOID);
    assertEquals(3, region_client.stats().rpcResponsesUnknown());
    
    buffer = ChannelBuffers.wrappedBuffer(
        new byte[] { 0, 0, 0, 1, 6, 8, -42, -1, -1, -1, 15 });
    region_client.decode(ctx, chan, buffer, VOID);
    assertEquals(4, region_client.stats().rpcResponsesUnknown());
  }
  
  @Test
  public void noCallId() throws Exception {
    // passes the header parsing since it has a size of zero, but then we check
    // to see if it has a call ID and it won't.
    ChannelBuffer buffer = new ReadOnlyChannelBuffer(
        ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 0 }));
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an NonRecoverableException");
    } catch (NonRecoverableException ex) { }
    
    buffer = ChannelBuffers.wrappedBuffer(new byte[] { 0, 0, 0, 1, 0 });
    try {
      region_client.decode(ctx, chan, buffer, VOID);
      fail("Expected an NonRecoverableException");
    } catch (NonRecoverableException ex) { }
  }

  @Test
  public void nullContext() throws Exception {
    // just shows we don't care about the context object
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    region_client.decode(null, chan, buffer, VOID);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test
  public void nullChannel() throws Exception {
    // just shows we don't care about the channel either
    final int id = 42;
    final ChannelBuffer buffer = buildGoodResponse(true, id);
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    inflightTheRpc(id, get);
    
    region_client.decode(ctx, null, buffer, VOID);
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.joinUninterruptibly();
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    assertEquals(1, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(0).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void emptyBuffer() throws Exception {
    // This shouldn't happen since we should only get a buffer if the socket
    // had some data.
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(new byte[] {});
    region_client.decode(ctx, chan, buf, VOID);
  }
  
  @Test (expected = NullPointerException.class)
  public void nullBuffer() throws Exception {
    // This should never happen, in theory
    region_client.decode(ctx, chan, null, VOID);
  }
  
  @Test
  public void timedoutRpcThenGoodRpc090() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_090_AND_BEFORE);    
    
    // this one has been timed out and we assume it was popped from the map
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);

    region_client.messageReceived(ctx, getMessage(
        ChannelBuffers.wrappedBuffer(MULTI_ACTION_RESPONSE_090)));
    region_client.messageReceived(ctx, getMessage(
        ChannelBuffers.wrappedBuffer(GET_RESPONSE_090)));
    
    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void timedoutRpcThenGoodRpc094() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_092_OR_ABOVE);    
    
    // this one has been timed out and we assume it was popped from the map
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);

    region_client.messageReceived(ctx, getMessage(
        ChannelBuffers.wrappedBuffer(MULTI_ACTION_RESPONSE_094)));
    region_client.messageReceived(ctx, getMessage(
        ChannelBuffers.wrappedBuffer(GET_RESPONSE_094)));
    
    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void timedoutRpcThenGoodRpc() throws Exception {
    resetMockClient();
    
    // this one has been timed out and we assume it was popped from the map
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    final ChannelBuffer buffer1 = buildGoodResponse(true, 1);
    inflightTheRpc(1, timedout);
    
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    final int id = 42;
    final ChannelBuffer buffer2 = buildGoodResponse(true, id);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    // only put the second RPC in the map
    inflightTheRpc(id, get);

    region_client.messageReceived(ctx, getMessage(buffer1));
    region_client.messageReceived(ctx, getMessage(buffer2));
    
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void combinedTimedoutRpcThenGoodRpc090() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_090_AND_BEFORE);    
    
    // this one has been timed out and we assume it was popped from the map
    // never mind that it's a get, not a multi action for this 94 test
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    final byte[] data = new byte[MULTI_ACTION_RESPONSE_090.length + 
                                 GET_RESPONSE_090.length];
    System.arraycopy(MULTI_ACTION_RESPONSE_090, 0, data, 0, 
        MULTI_ACTION_RESPONSE_090.length);
    System.arraycopy(GET_RESPONSE_090, 0, data, MULTI_ACTION_RESPONSE_090.length, 
        GET_RESPONSE_090.length);
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);
    
    region_client.messageReceived(ctx, 
        getMessage(ChannelBuffers.wrappedBuffer(data)));

    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void combinedTimedoutRpcThenGoodRpc094() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_092_OR_ABOVE);    
    
    // this one has been timed out and we assume it was popped from the map
    // never mind that it's a get, not a multi action for this 94 test
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    final byte[] data = new byte[MULTI_ACTION_RESPONSE_094.length + 
                                 GET_RESPONSE_094.length];
    System.arraycopy(MULTI_ACTION_RESPONSE_094, 0, data, 0, 
        MULTI_ACTION_RESPONSE_094.length);
    System.arraycopy(GET_RESPONSE_094, 0, data, MULTI_ACTION_RESPONSE_094.length, 
        GET_RESPONSE_094.length);
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);
    
    region_client.messageReceived(ctx, 
        getMessage(ChannelBuffers.wrappedBuffer(data)));

    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void combinedTimedoutRpcThenGoodRpc() throws Exception {
    resetMockClient();
    
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    final ChannelBuffer buffer1 = buildGoodResponse(true, 1);
    final int id = 42;
    final ChannelBuffer buffer2 = buildGoodResponse(true, id);
    final ChannelBuffer combined = new BigEndianHeapChannelBuffer(
        buffer1.writerIndex() + buffer2.writerIndex());
    combined.writeBytes(buffer1);
    combined.writeBytes(buffer2);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    // only put the second RPC in the map
    inflightTheRpc(id, get);

    region_client.messageReceived(ctx, getMessage(combined));
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void chunkedLateRpcThenGoodRpc090() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_090_AND_BEFORE);    
    
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);
    
    int cutoff = 48;
    final byte[] data = new byte[MULTI_ACTION_RESPONSE_090.length + 
                                 GET_RESPONSE_090.length - cutoff];
    System.arraycopy(MULTI_ACTION_RESPONSE_090, cutoff, data, 0, 
        MULTI_ACTION_RESPONSE_090.length - cutoff);
    System.arraycopy(GET_RESPONSE_090, 0, data, 
        MULTI_ACTION_RESPONSE_090.length - cutoff, 
        GET_RESPONSE_090.length);
    
    region_client.messageReceived(ctx, getMessage(
        Arrays.copyOfRange(MULTI_ACTION_RESPONSE_090, 0, cutoff)));
    region_client.messageReceived(ctx, getMessage(data));
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void chunkedLateRpcThenGoodRpc094() throws Exception {
    resetMockClient();
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_092_OR_ABOVE);    
    
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    final GetRequest rpc = new GetRequest(TABLE, KEY);
    final Deferred<Object> deferred = rpc.getDeferred();
    inflightTheRpc(3, rpc);
    
    int cutoff = 48;
    final byte[] data = new byte[MULTI_ACTION_RESPONSE_094.length + 
                                 GET_RESPONSE_094.length - cutoff];
    System.arraycopy(MULTI_ACTION_RESPONSE_094, cutoff, data, 0, 
        MULTI_ACTION_RESPONSE_094.length - cutoff);
    System.arraycopy(GET_RESPONSE_094, 0, data, 
        MULTI_ACTION_RESPONSE_094.length - cutoff, 
        GET_RESPONSE_094.length);
    
    region_client.messageReceived(ctx, getMessage(
        Arrays.copyOfRange(MULTI_ACTION_RESPONSE_094, 0, cutoff)));
    region_client.messageReceived(ctx, getMessage(data));
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    @SuppressWarnings("unchecked")
    final ArrayList<KeyValue> row = (ArrayList<KeyValue>)deferred.join(1);
    assertArrayEquals(new byte[] { 0, 0, 0, 100}, row.get(0).qualifier());
    assertArrayEquals("*".getBytes(), row.get(0).value());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  @Test
  public void chunkedLateRpcThenGoodRpc() throws Exception {
    resetMockClient();
    
    final GetRequest timedout = new GetRequest(TABLE, ROW);
    timedout.setTimeout(1);
    final Deferred<Object> deferred_to = timedout.getDeferred();
    inflightTheRpc(1, timedout);
    assertEquals(1, rpcs_inflight.size());
    assertEquals(1, timer.tasks.size());
    assertEquals(1, (long)timer.tasks.get(0).getValue());
    timer.tasks.get(0).getKey().run(null);
    assertEquals(0, rpcs_inflight.size());
    
    final ChannelBuffer buffer1 = buildGoodResponse(true, 1);
    final int id = 42;
    final ChannelBuffer buffer2 = buildGoodResponse(true, id);
    final ChannelBuffer combined = new BigEndianHeapChannelBuffer(
        buffer1.writerIndex() + buffer2.writerIndex());
    combined.writeBytes(buffer1);
    combined.writeBytes(buffer2);
    
    final byte[] bytes = new byte[combined.readableBytes()];
    combined.readBytes(bytes);
    
    final GetRequest get = new GetRequest(TABLE, ROW);
    final Deferred<Object> deferred = get.getDeferred();
    // only put the second RPC in the map
    inflightTheRpc(id, get);

    region_client.messageReceived(ctx, getMessage(
        Arrays.copyOfRange(bytes, 0, 48)));
    region_client.messageReceived(ctx, getMessage(
        Arrays.copyOfRange(bytes, 48, bytes.length)));
    
    try {
      deferred_to.join(100);
    } catch (RpcTimedOutException ex) { }
    
    @SuppressWarnings("unchecked")
    final List<KeyValue> kvs = (List<KeyValue>)deferred.join(100);
    assertEquals(1, kvs.size());
    assertArrayEquals(ROW, kvs.get(0).key());
    assertArrayEquals(FAMILY, kvs.get(0).family());
    assertArrayEquals(QUALIFIER, kvs.get(0).qualifier());
    assertArrayEquals(VALUE, kvs.get(0).value());
    assertEquals(TIMESTAMP, kvs.get(0).timestamp());
    assertEquals(1, region_client.stats().rpcsTimedout());
    assertEquals(2, timer.tasks.size());
    assertEquals(60000, (long)timer.tasks.get(1).getValue());
    verify(timer.timeouts.get(0), times(1)).cancel();
    verify(timer.timeouts.get(1), times(1)).cancel();
  }
  
  /**
   * Puts the RPC in the map with the given ID and tells the RPC that this
   * region client is handling it
   * @param id The ID to use
   * @param rpc The RPC
   */
  private void inflightTheRpc(final int id, final HBaseRpc rpc) {
    rpcs_inflight.put(id, rpc);
    rpc.rpc_id = id;
    rpc.enqueueTimeout(region_client);
  }
  
  /**
   * Creates a simple GetRequest response with some dummy data and a single 
   * column in a row. The request lacks cell meta data.
   * @param array_backed Whether or not the Channel Buffer should have a backing
   * array or not to exercise zero-copy code paths
   * @param id The ID of the RPC that we're responding to.
   * @return A channel buffer with a ProtoBuf object as HBase would return
   * @throws IOException If we couldn't write the ProtoBuf for some reason
   */
  private ChannelBuffer buildGoodResponse(final boolean array_backed, final int id)
      throws IOException {
    final Cell cell = Cell.newBuilder()
        .setRow(Bytes.wrap(ROW))
        .setFamily(Bytes.wrap(FAMILY))
        .setQualifier(Bytes.wrap(QUALIFIER))
        .setTimestamp(TIMESTAMP)
        .setValue(Bytes.wrap(VALUE))
        .build();
    
    final Result result = Result.newBuilder()
        .addCell(cell)
        .build();
    
    final GetResponse get_response = 
        GetResponse.newBuilder()
        .setResult(result)
        .build();
    
    // TODO - test objects that return cell blocks, possibly scanners
//    final CellBlockMeta meta = CellBlockMeta.newBuilder()
//        .setLength(cell.getSerializedSize())
//        .build();
    
    final RPCPB.ResponseHeader header = RPCPB.ResponseHeader.newBuilder()
        .setCallId(id)
        //.setCellBlockMeta(meta)
        .build();
    
    final int hlen = header.getSerializedSize();
    final int vhlen = CodedOutputStream.computeRawVarint32Size(hlen);
    final int pblen = get_response.getSerializedSize();
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[hlen + vhlen + vlen + pblen + 4];
    final CodedOutputStream out = CodedOutputStream.newInstance(buf, 4, 
        hlen + vhlen + vlen + pblen);
    
    out.writeMessageNoTag(header);
    out.writeMessageNoTag(get_response);
    
    Bytes.setInt(buf, buf.length - 4);
    if (array_backed) {
      return ChannelBuffers.wrappedBuffer(buf);
    } else {
      return new ReadOnlyChannelBuffer(ChannelBuffers.wrappedBuffer(buf));
    }
  }

  /** Simple test implementation of the HBaseException class */
  class TestingHBaseException extends HBaseException {
    private static final long serialVersionUID = 7717718589747017699L;
    TestingHBaseException(final String msg) {
      super(msg);
    }
  }

  /** Creates a new mock client that isn't spied. Necessary for the replay
   * tests
   */
  private void resetMockClient() throws Exception {
    region_client = new RegionClient(hbase_client);
    Whitebox.setInternalState(region_client, "chan", chan);
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    rpcs_inflight = Whitebox.getInternalState(
        region_client, "rpcs_inflight");
  }
  
  /**
   * Generate a mock MessageEvent from a byte array
   * @param data The data to pass on
   * @return The event to hand to messageReceived(
   */
  static MessageEvent getMessage(final byte[] data) {
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(data);
    return getMessage(buf);
  }
  
  /**
   * Generate a mock MessageEvent from a ChannelBuffer
   * @param buf The buffer to pass on
   * @return The event to hand to messageReceived(
   */
  static MessageEvent getMessage(final ChannelBuffer buf) {
    final MessageEvent event = mock(MessageEvent.class);
    when(event.getMessage()).thenReturn(buf);
    return event;
  }

  /** Response for a MultiAction request with PUTs from 0.94 */
  public static final byte[] MULTI_ACTION_RESPONSE_094 = new byte[] { 
    0x00, 0x00, 0x00, (byte) 0xc9, 0x02, 0x00, 0x00, 0x00, 0x58, 0x00, 0x00, 
    0x00, 0x00, 0x43, 0x43, 0x00, 0x00, 0x00, 0x01, 0x35, 0x74, 0x65, 0x73, 
    0x74, 0x2c, 0x2c, 0x31, 0x34, 0x34, 0x33, 0x39, 0x30, 0x39, 0x35, 0x30, 
    0x31, 0x33, 0x35, 0x30, 0x2e, 0x61, 0x35, 0x35, 0x30, 0x65, 0x38, 0x34,
    0x66, 0x36, 0x38, 0x34, 0x63, 0x63, 0x65, 0x34, 0x64, 0x33, 0x37, 0x62, 
    0x66, 0x31, 0x34, 0x66, 0x34, 0x34, 0x61, 0x33, 0x39, 0x33, 0x36, 0x34, 
    0x34, 0x2e, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x25, 
    0x25, 0x00, 0x00, 0x00, 0x00
};

  /** Response for a GET request from 0.94 */
  public static final byte[] GET_RESPONSE_094 = new byte[] { 
    0x00, 0x00, 0x00, 0x03, 0x02, 0x00, 
    0x00, 0x00, 0x36, 0x00, 0x00, 0x00, 0x00, 0x25, 0x25, 0x00, 0x00, 0x00, 
    0x23, 0x00, 0x00, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 
    0x01, 0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x74, 0x00, 0x00, 
    0x00, 0x64, 0x00, 0x00, 0x01, 0x50, 0x2f, (byte) 0xe2, (byte) 0x82, 
    (byte) 0x9f, 0x04, 0x2a };
  
  /** Response for a MultiAction request with PUTs from 0.90 */
  public static final byte[] MULTI_ACTION_RESPONSE_090 = new byte[] {
    0x00, 0x00, 0x00, (byte) 0xc9, 0x00, 0x3a, 0x3a, 0x00, 0x00, 0x00, 0x01, 
    0x35, 0x74, 0x65, 0x73, 0x74, 0x2c, 0x2c, 0x31, 0x34, 0x34, 0x33, 0x39, 
    0x32, 0x30, 0x39, 0x33, 0x35, 0x36, 0x32, 0x39, 0x2e, 0x66, 0x65, 0x36, 
    0x36, 0x35, 0x36, 0x32, 0x62, 0x37, 0x35, 0x61, 0x30, 0x30, 0x38, 0x63, 
    0x30, 0x66, 0x30, 0x63, 0x35, 0x35, 0x33, 0x32, 0x30, 0x64, 0x63, 0x65, 
    0x62, 0x37, 0x61, 0x37, 0x66, 0x2e, (byte) 0xff, (byte) 0xff, 
    (byte) 0xff, (byte) 0xff };
  
  /** Response for a GET request from 0.90 */
  public static final byte[] GET_RESPONSE_090 = new byte[] {
    0x00, 0x00, 0x00, 0x03, 0x00, 0x25, 0x25, 0x00, 0x00, 0x00, 0x23, 0x00, 
    0x00, 0x00, 0x1f, 0x00, 0x00, 0x00, 0x16, 0x00, 0x00, 0x00, 0x01, 0x00, 
    0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x01, 0x74, 0x00, 0x00, 0x00, 0x64, 
    0x00, 0x00, 0x01, 0x50, 0x30, 0x66, (byte) 0x8c, (byte) 0xad, 0x04, 0x2a };
}