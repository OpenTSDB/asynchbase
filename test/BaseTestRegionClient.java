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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Map;

import org.hbase.async.BaseTestHBaseClient.FakeTimer;
import org.hbase.async.generated.RPCPB;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
           "ch.qos.*", "org.slf4j.*",
           "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, Channels.class,
  RPCPB.ResponseHeader.class, NotServingRegionException.class, Config.class,
  RegionInfo.class, RPCPB.ExceptionResponse.class, HBaseRpc.class, 
  SecureRpcHelper.class })
public class BaseTestRegionClient {
  protected static final String host = "127.0.0.1";
  protected static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  protected static final byte[] KEY = { 'k', 'e', 'y' };
  protected static final byte[] FAMILY = { 'f' };
  protected static final byte[] HRPC3 = new byte[] { 'h', 'r', 'p', 'c', 3 };
  protected static final byte SERVER_VERSION_UNKNOWN = 0;

  protected final static RegionInfo region = mkregion("table", "table,,1234567890");
  
  protected HBaseRpc rpc = mock(HBaseRpc.class);
  protected Channel chan;
  protected ChannelHandlerContext ctx;
  protected ChannelStateEvent cse;
  protected HBaseClient hbase_client;
  protected Config config;
  protected Map<Integer, HBaseRpc> rpcs_inflight;
  protected SecureRpcHelper secure_rpc_helper;
  protected FakeTimer timer;
  protected RegionClient region_client;
  
  @Before
  public void before() throws Exception {
    config = new Config();
    hbase_client = mock(HBaseClient.class);
    timer = new FakeTimer();
    when(hbase_client.getConfig()).thenReturn(config);
    when(hbase_client.getTimer()).thenReturn(timer);
    when(hbase_client.getRpcTimeoutTimer()).thenReturn(timer);
    when(hbase_client.getDefaultRpcTimeout()).thenReturn(60000);
    
    chan = mock(Channel.class, Mockito.RETURNS_DEEP_STUBS);
    ctx = mock(ChannelHandlerContext.class);
    cse = mock(ChannelStateEvent.class);
    secure_rpc_helper = mock(SecureRpcHelper.class);
    
    when(ctx.getChannel()).thenReturn(chan);
    final HeapChannelBufferFactory factory = new HeapChannelBufferFactory();
    when(chan.getConfig().getBufferFactory()).thenReturn(factory);
    
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
    
    region_client = PowerMockito.spy(new RegionClient(hbase_client));
    Whitebox.setInternalState(region_client, "chan", chan);
    Whitebox.setInternalState(region_client, "server_version", 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    rpcs_inflight = Whitebox.getInternalState(region_client, "rpcs_inflight");
  }
  
  /**
   * Injects the security helper mock in the class. The default is to operate
   * without security.
   */
  protected void injectSecurity() {
    Whitebox.setInternalState(region_client, "secure_rpc_helper", secure_rpc_helper);

    when(secure_rpc_helper
        .handleResponse(any(ChannelBuffer.class), any(Channel.class)))
        .thenAnswer(new Answer<ChannelBuffer>() {
          @Override
          public ChannelBuffer answer(final InvocationOnMock args)
              throws Throwable {
            return (ChannelBuffer)args.getArguments()[0];
          }
    });
  }
  
  // Helpers //
  
  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(), HBaseClient.EMPTY_ARRAY);
  }

}
