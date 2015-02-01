/*
 * Copyright (C) 2014 The Async HBase Authors.  All rights reserved.
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

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ConcurrentHashMap;

import org.hbase.async.HBaseRpc;
import org.hbase.async.generated.RPCPB;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;
import static org.mockito.Matchers.any;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class,
    RPCPB.ResponseHeader.class, NotServingRegionException.class, 
    RegionInfo.class, RPCPB.ExceptionResponse.class, HBaseRpc.class })
public class TestRegionClient {
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 'k', 'e', 'y' };
  private static final byte[] FAMILY = { 'f' };
  private static final byte[] HRPC3 = new byte[] { 'h', 'r', 'p', 'c', 3 };
  
  private final static RegionInfo region = mkregion("table", "table,,1234567890");
  
  private HBaseRpc rpc = mock(HBaseRpc.class);
  private Channel chan = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
  private ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
  private ChannelStateEvent cse = mock(ChannelStateEvent.class);
  private HBaseClient hbase_client;

  private static final byte SERVER_VERSION_UNKNWON = 0;

  @Before
  public void before() throws Exception {
    hbase_client = mock(HBaseClient.class);
    
    PowerMockito.doAnswer(new Answer<RegionClient>(){
      @Override
      public RegionClient answer(InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        final String endpoint = (String)args[0] + ":" + (Integer)args[1];
        final RegionClient rc = mock(RegionClient.class);
        when(rc.getRemoteAddress()).thenReturn(endpoint);
        return rc;
      }
    }).when(hbase_client, "newClient", anyString(), anyInt());
  }
  
  @Test
  public void ctor() throws Exception {
    assertNotNull(new RegionClient(hbase_client));
  }
  
  @Test
  public void ctorNullClient() throws Exception {
    assertNotNull(new RegionClient(null));
  }

  @Test
  public void getClosestRowBefore() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client);
    rclient.getClosestRowBefore(region, TABLE, KEY, FAMILY);
    verifyPrivate(rclient).invoke("sendRpc", rpc);
  }
  
  @Test
  public void getRemoteAddressChanNotSet() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client);
    
    assertNull(Whitebox.getInternalState(rclient, "chan"));
    assertNull(rclient.getRemoteAddress());
  }
  
  @Test
  public void getRemoteAddressChanSet() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    PowerMockito.when(chan.getRemoteAddress().toString()).thenReturn("127.0.0.1");
    PowerMockito.when(rclient.getRemoteAddress()).thenCallRealMethod();
    
    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    String addy = rclient.getRemoteAddress();
    assertNotNull(addy);
    assertEquals(addy, "127.0.0.1");
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void channelDisconnected() throws Exception {
    RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    
    when(cse.getChannel()).thenReturn(chan);
    // Prevent/stub logic in super.method()
    PowerMockito.doNothing().when((ReplayingDecoder)rclient)
      .channelDisconnected(ctx, cse);
    PowerMockito.doNothing().when(rclient, "cleanup", chan);
    PowerMockito.when(rclient, "channelDisconnected", ctx, cse)
      .thenCallRealMethod();
    
    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    rclient.channelDisconnected(ctx, cse);
    assertNull(Whitebox.getInternalState(rclient, "chan"));
  }
  
  @Test
  public void channelClosed() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client);
    assertNotNull(rclient);
    rclient.becomeReady(chan, SERVER_VERSION_UNKNWON);
    
    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    rclient.channelClosed(ctx, cse);
    
    assertNull(Whitebox.getInternalState(rclient, "chan"));
    verifyPrivate(rclient).invoke("cleanup", mock(Channel.class));
  }
  
  @Test
  public void channelConnectedCDH3b3() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ChannelBuffer header = mock(ChannelBuffer.class);

    hbase_client.has_root = true;
    when(cse.getChannel()).thenReturn(chan);
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("org.hbase.async.cdh3b3"))
      .thenReturn("some value");
    PowerMockito.doNothing().when(rclient, "helloRpc", chan, header);
    PowerMockito.when(rclient, "headerCDH3b3").thenReturn(header);
    PowerMockito.field(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    PowerMockito.when(rclient, "channelConnected", ctx, cse).thenCallRealMethod();
    
    rclient.channelConnected(ctx, cse);
    
    verifyPrivate(rclient).invoke("headerCDH3b3");
    verifyPrivate(rclient).invoke("helloRpc", chan, header);
  }
  
  @Test
  public void channelConnected090() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ChannelBuffer header = mock(ChannelBuffer.class);

    hbase_client.has_root = true;
    when(cse.getChannel()).thenReturn(chan);
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("org.hbase.async.cdh3b3"))
      .thenReturn(null);
    PowerMockito.doNothing().when(rclient, "helloRpc", chan, header);
    PowerMockito.when(rclient, "header090").thenReturn(header);
    PowerMockito.field(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    PowerMockito.when(rclient, "channelConnected", ctx, cse)
      .thenCallRealMethod();
    
    rclient.channelConnected(ctx, cse);
    
    verifyPrivate(rclient).invoke("header090");
    verifyPrivate(rclient).invoke("helloRpc", chan, header);
  }
  
  @Test
  public void sendRpc() throws Exception {
      // TODO
  }
  
  @Test (expected=NonRecoverableException.class)
  public void decodeHbase95orAboveNoCallId() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    ChannelBuffer buf = mock(ChannelBuffer.class);
    RPCPB.ResponseHeader header = mock(RPCPB.ResponseHeader.class);
    PowerMockito.mockStatic(HBaseRpc.class);
      
    @SuppressWarnings("unchecked")
    ConcurrentHashMap<Integer, HBaseRpc> mcmap = mock(ConcurrentHashMap.class);
    PowerMockito.field(RegionClient.class, "rpcs_inflight").set(rclient, mcmap);
    PowerMockito.field(RegionClient.class, "server_version")
      .set(rclient, RegionClient.SERVER_VERSION_095_OR_ABOVE);

    PowerMockito.when(HBaseRpc.readProtobuf(buf, RPCPB.ResponseHeader.PARSER))
      .thenReturn(header);
    PowerMockito.when(buf, "readInt").thenReturn(0);
    PowerMockito.when(header, "hasCallId").thenReturn(false);
    PowerMockito.when(rclient, "decode", (ChannelHandlerContext)any(), 
          (Channel)any(), (ChannelBuffer)any(), (VoidEnum)any())
            .thenCallRealMethod();
      
    assertNull(rclient.decode(null, chan, buf, (VoidEnum)null));
  }
  
  @Test
  public void decodeHbase92orAboveRpcNotNull() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    ChannelBuffer buf = mock(ChannelBuffer.class);
    NotServingRegionException blowzup = mock(NotServingRegionException.class);
    HBaseRpc rpc = mock(HBaseRpc.class);
    RegionInfo region = mock(RegionInfo.class);
      
    ConcurrentHashMap<Integer, HBaseRpc> mcmap = 
        new ConcurrentHashMap<Integer, HBaseRpc>(1);
    mcmap.put(0, rpc);
    PowerMockito.field(RegionClient.class, "rpcs_inflight").set(rclient, mcmap);
    PowerMockito.field(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    PowerMockito.field(RegionClient.class, "server_version")
      .set(rclient, RegionClient.SERVER_VERSION_092_OR_ABOVE);

    PowerMockito.when(buf, "readInt").thenReturn(0);
    PowerMockito.when(rpc, "getRegion").thenReturn(region);
    PowerMockito.when(region, "name").thenReturn(Bytes.fromInt(1337));
    PowerMockito.doNothing().when(hbase_client, "handleNSRE", Mockito.any(), 
        Mockito.any(), Mockito.any());
    PowerMockito.when(rclient, "deserialize", buf, rpc).thenReturn(blowzup);
      
    PowerMockito.when(rclient, "decode", (ChannelHandlerContext)any(), 
        (Channel)any(), (ChannelBuffer)any(), (VoidEnum)any())
          .thenCallRealMethod();
    
    assertNull(rclient.decode(null, chan, buf, (VoidEnum)null));
  }
  
  @Test
  public void encode() throws Exception {
      // TODO
  }
  
  @Test
  public void deserialize() throws Exception {
      // TOD
  }
  
  @Test
  public void numberOfKeyValuesAhead() throws Exception {
      // TODO
  }
  
  @Test
  public void parseResult() throws Exception {
      // TODO
  }
  
  @Test
  public void parseResults() throws Exception {
      // TODO
  }
  
  @Test (expected=InvalidResponseException.class)
  public void badResponse() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client);
    Whitebox.invokeMethod(rclient, "badResponse", "ZOMG ERRMSG"); 
  }
  
  @Test
  public void commonHeader() throws Exception {
    RegionClient rclient = new RegionClient(hbase_client);
    final byte[] buf = new byte[42];
    
    ChannelBuffer commonHeader = 
        Whitebox.invokeMethod(rclient, "commonHeader", buf, HRPC3 );
    
    assertNotNull(commonHeader);
  }
  
  @Test
  public void header090() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    final byte[] buf = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
    final String klass = "org.apache.hadoop.io.Writable";
    ChannelBuffer header = mock(ChannelBuffer.class);
      
    PowerMockito.when(rclient, "commonHeader", buf, HRPC3).thenReturn(header);
    PowerMockito.when(header, "writerIndex").thenReturn(0);
    PowerMockito.when(rclient, "header090").thenCallRealMethod();
      
    ChannelBuffer header090 = Whitebox.invokeMethod(rclient, "header090");
      
    verifyPrivate(rclient, Mockito.atMost(1)).invoke("commonHeader", buf, HRPC3);
    verifyPrivate(header, Mockito.atMost(2)).invoke("writerIndex");
    verifyPrivate(header, Mockito.atLeast(1))
      .invoke("writerIndex", Mockito.anyInt());
    verifyPrivate(header, Mockito.atMost(3)).invoke("writeShort", klass.length());
    verifyPrivate(header, Mockito.atMost(3))
      .invoke("writeBytes", Bytes.ISO88591(klass));
    verifyPrivate(header, Mockito.atMost(1))
      .invoke("setInt", Mockito.eq(5), Mockito.anyInt());
      
    assertNotNull(header090);
  }
  
  @Test
  public void header092() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    final byte[] buf = new byte[4 + 1 + 4 + 1 + 44];
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    ChannelBuffer header = mock(ChannelBuffer.class);
    
    PowerMockito.when(rclient, "commonHeader", buf, HRPC3).thenReturn(header);
    PowerMockito.when(header, "writerIndex").thenReturn(0);
    PowerMockito.when(rclient, "header092").thenCallRealMethod();
    
    ChannelBuffer header092 = Whitebox.invokeMethod(rclient, "header092");
    
    verifyPrivate(rclient, Mockito.atMost(1)).invoke("commonHeader", buf, HRPC3);
    verifyPrivate(header, Mockito.atMost(2)).invoke("writerIndex");
    verifyPrivate(header, Mockito.atLeast(1))
      .invoke("writerIndex", Mockito.anyInt());
    verifyPrivate(header, Mockito.atMost(1)).invoke("writeByte", klass.length());
    verifyPrivate(header, Mockito.atMost(1))
      .invoke("writeBytes", Bytes.ISO88591(klass));
    verifyPrivate(header, Mockito.atMost(1))
      .invoke("setInt", Mockito.eq(5), Mockito.anyInt());
    
    assertNotNull(header092);
  }
  
  @Test
  public void headerCDH3b3() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    byte[] user = Bytes.UTF8("some value");
    byte[] buf = new byte[4 + 1 + 4 + 4 + user.length];
    ChannelBuffer header = mock(ChannelBuffer.class);
    
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("user.name", "asynchbase"))
      .thenReturn("some value");
    PowerMockito.when(rclient, "commonHeader", buf, HRPC3).thenReturn(header);
    PowerMockito.when(rclient, "headerCDH3b3").thenCallRealMethod();
      
    ChannelBuffer headerCDH3b3 = Whitebox.invokeMethod(rclient, "headerCDH3b3");
      
    verifyPrivate(rclient, Mockito.atMost(1)).invoke("commonHeader", buf, HRPC3);
    verifyPrivate(header, Mockito.atMost(2)).invoke("writeInt", Mockito.anyInt());
    verifyPrivate(header, Mockito.atMost(1)).invoke("writeBytes", user);
      
    assertNotNull(headerCDH3b3);
  }
  
  // Helpers //
  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(), 
        HBaseClient.EMPTY_ARRAY);
  }

}
