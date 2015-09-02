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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.hbase.async.generated.RPCPB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class,
    RPCPB.ResponseHeader.class, NotServingRegionException.class, 
    RegionInfo.class, RPCPB.ExceptionResponse.class, HBaseRpc.class })
public class TestRegionClient extends BaseTestRegionClient {

  @Test
  public void ctor() throws Exception {
    assertNotNull(new RegionClient(hbase_client));
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullClient() throws Exception {
    new RegionClient(null);
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
    PowerMockito.when(chan.remoteAddress().toString()).thenReturn("127.0.0.1");
    PowerMockito.when(rclient.getRemoteAddress()).thenCallRealMethod();
    
    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    String addy = rclient.getRemoteAddress();
    assertNotNull(addy);
    assertEquals(addy, "127.0.0.1");
  }
  
  @Test
  public void exceptionCaught() throws Exception {

    when(chan.isOpen()).thenReturn(true);
    when(chan.close()).thenReturn(null);
    final RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    final Throwable event = new RuntimeException("Boo!");

    when(ctx.fireExceptionCaught(event)).thenReturn(ctx);
    when(ctx.channel()).thenReturn(chan);

    rclient.exceptionCaught(ctx, event);

    verifyPrivate(rclient, never()).invoke("cleanup", chan);
    verify(chan).close();
  }

  @Test
  public void exceptionCaughtChNotOpen() throws Exception {
    final RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    final Throwable event = new RuntimeException("Boo!");

    when(ctx.fireExceptionCaught(event)).thenReturn(ctx);
    when(ctx.channel()).thenReturn(chan);

    rclient.exceptionCaught(ctx, event);

    verifyPrivate(rclient, times(1)).invoke("cleanup", chan);
    PowerMockito.verifyStatic(never());
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullEvent() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client);
    rclient.exceptionCaught(null, null);
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullChannel() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client);
    final Throwable event = new RuntimeException("Boo!");
    rclient.exceptionCaught(null, event);
  }
  
  @Test (expected = NullPointerException.class)
  public void exceptionCaughtNullException() throws Exception {
    final RegionClient rclient = new RegionClient(hbase_client);
    final Throwable event = new RuntimeException("Boo!");
    rclient.exceptionCaught(null, event);
  }
  
  @Test
  public void exceptionCaughtDifferentChannel() throws Exception {
    when(chan.isOpen()).thenReturn(true);
    // honey badger don't care; apparently we can call this with any old channel.
    final Channel ch = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
    when(ch.isOpen()).thenReturn(true);
    final RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    final Throwable event = new RuntimeException("Boo!");
    
    when(ctx.fireExceptionCaught(event)).thenReturn(ctx);
    when(ctx.channel()).thenReturn(ch);

    rclient.exceptionCaught(ctx, event);
    
    verifyPrivate(rclient, never()).invoke("cleanup", ch);
  }
  
  @Test
  public void exceptionCaughtDifferentChannelNotOpen() throws Exception {

    // honey badger don't care; apparently we can call this with any old channel.
    final Channel ch = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
    final RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);
    final Throwable event = new RuntimeException("Boo!");

    when(ctx.fireExceptionCaught(event)).thenReturn(ctx);
    when(ctx.channel()).thenReturn(ch);

    rclient.exceptionCaught(ctx, event);
    
    verifyPrivate(rclient, times(1)).invoke("cleanup", ch);
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void channelInactive() throws Exception {
    RegionClient rclient = PowerMockito.spy(new RegionClient(hbase_client));
    PowerMockito.field(RegionClient.class, "chan").set(rclient, chan);

    when(ctx.channel()).thenReturn(chan);
    // Prevent/stub logic in super.method()
    PowerMockito.doNothing().when((ReplayingDecoder)rclient)
      .channelInactive(ctx);
    PowerMockito.doNothing().when(rclient, "cleanup", chan);
    PowerMockito.when(rclient, "channelInactive", ctx)
      .thenCallRealMethod();

    assertNotNull(Whitebox.getInternalState(rclient, "chan"));
    rclient.channelInactive(ctx);
    assertNull(Whitebox.getInternalState(rclient, "chan"));
  }
  
  @Test
  public void channelActiveCDH3b3() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ByteBuf header = mock(ByteBuf.class);

    hbase_client.has_root = true;
    when(ctx.channel()).thenReturn(chan);
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("org.hbase.async.cdh3b3"))
      .thenReturn("some value");
    PowerMockito.doNothing().when(rclient, "helloRpc", chan, header);
    PowerMockito.when(rclient, "headerCDH3b3").thenReturn(header);
    PowerMockito.field(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    PowerMockito.when(rclient, "channelActive", ctx).thenCallRealMethod();

    rclient.channelActive(ctx);

    verifyPrivate(rclient).invoke("headerCDH3b3");
    verifyPrivate(rclient).invoke("helloRpc", chan, header);
  }
  
  @Test
  public void channelActive090() throws Exception {
    RegionClient rclient = mock(RegionClient.class, Mockito.RETURNS_DEEP_STUBS);
    ByteBuf header = mock(ByteBuf.class);

    hbase_client.has_root = true;
    when(ctx.channel()).thenReturn(chan);
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("org.hbase.async.cdh3b3"))
      .thenReturn(null);
    PowerMockito.doNothing().when(rclient, "helloRpc", chan, header);
    PowerMockito.when(rclient, "header090").thenReturn(header);
    PowerMockito.field(RegionClient.class, "hbase_client")
      .set(rclient, hbase_client);
    PowerMockito.when(rclient, "channelActive", ctx)
      .thenCallRealMethod();

    rclient.channelActive(ctx);

    verifyPrivate(rclient).invoke("header090");
    verifyPrivate(rclient).invoke("helloRpc", chan, header);
  }
  
  @Test (expected=NonRecoverableException.class)
  public void decodeHbase95orAboveNoCallId() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    ByteBuf buf = mock(ByteBuf.class);
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
          (ByteBuf)any(), any())
            .thenCallRealMethod();
      
    rclient.decode(null, buf, null);
  }
  
  @Test
  public void decodeHbase92orAboveRpcNotNull() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    ByteBuf buf = mock(ByteBuf.class);
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
        (ByteBuf)any(), any())
          .thenCallRealMethod();
    
    rclient.decode(null, buf, null);
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
  public void header090() throws Exception {
    RegionClient rclient = mock(RegionClient.class);
    final byte[] buf = new byte[4 + 1 + 4 + 2 + 29 + 2 + 48 + 2 + 47];
    final String klass = "org.apache.hadoop.io.Writable";
    ByteBuf header = mock(ByteBuf.class);
      
    PowerMockito.when(header, "writerIndex").thenReturn(0);
    PowerMockito.when(rclient, "header090").thenCallRealMethod();
      
    ByteBuf header090 = Whitebox.invokeMethod(rclient, "header090");
      
    verifyPrivate(header, Mockito.atMost(2)).invoke("writerIndex");
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
    ByteBuf header = mock(ByteBuf.class);
    
    PowerMockito.when(header, "writerIndex").thenReturn(0);
    PowerMockito.when(rclient, "header092").thenCallRealMethod();
    
    ByteBuf header092 = Whitebox.invokeMethod(rclient, "header092");
    
    verifyPrivate(header, Mockito.atMost(2)).invoke("writerIndex");
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
    ByteBuf header = mock(ByteBuf.class);
    
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getProperty("user.name", "asynchbase"))
      .thenReturn("some value");
    PowerMockito.when(rclient, "headerCDH3b3").thenCallRealMethod();
      
    ByteBuf headerCDH3b3 = Whitebox.invokeMethod(rclient, "headerCDH3b3");
      
    verifyPrivate(header, Mockito.atMost(2)).invoke("writeInt", Mockito.anyInt());
    verifyPrivate(header, Mockito.atMost(1)).invoke("writeBytes", user);
      
    assertNotNull(headerCDH3b3);
  }

}
