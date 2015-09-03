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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.hbase.async.HBaseClient.ZKClient;
import org.hbase.async.HBaseClient.ZKClient.ZKCallback;
import org.hbase.async.generated.HBasePB.ServerName;
import org.hbase.async.generated.ZooKeeperPB.MetaRegionServer;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;

import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, ZKClient.class, ZooKeeper.class })
public class TestZKClient {
  private final static String quorum = "localhost:2121";
  private final static String base_path = "/hbase";
  private final static Charset CHARSET = Charset.forName("ASCII");
  private final static Stat stat = mock(Stat.class);
  private HBaseClient client;
  private Config config;
  private ZKClient zk_client;
  private ZooKeeper zk = mock(ZooKeeper.class);
  private ArrayList<Deferred<Object>> deferred_rootregion;
  
  @Before
  public void before() throws Exception {
    client = mock(HBaseClient.class);
    config = new Config();
    Whitebox.setInternalState(client, "config", config);
    zk_client = PowerMockito.spy(client.new ZKClient(quorum, base_path));
    whenNew(ZooKeeper.class).withAnyArguments().thenReturn(zk);

    PowerMockito.doAnswer(new Answer<RegionClient>(){
      @Override
      public RegionClient answer(InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        final String endpoint = (String)args[0] + ":" + (Integer)args[1];
        final RegionClient rc = mock(RegionClient.class);
        when(rc.getRemoteAddress()).thenReturn(endpoint);
        return rc;
      }
    }).when(client, "newClient", anyString(), anyInt());
  }
  
  @Test
  public void ctor() throws Exception {
    assertNotNull(client.new ZKClient(quorum, base_path));
  }
  
  // nulls are allowed for now, they'll pop up later on
  @Test
  public void ctorNullQuorum() throws Exception {
    assertNotNull(client.new ZKClient(null, base_path));
  }
  
  @Test
  public void ctorEmptyQuorum() throws Exception {
    assertNotNull(client.new ZKClient("", base_path));
  }
  
  @Test
  public void ctorNullBase() throws Exception {
    assertNotNull(client.new ZKClient(quorum, null));
  }
  
  @Test
  public void ctorEmptyBase() throws Exception {
    assertNotNull(client.new ZKClient(quorum, ""));
  }

  @Test
  public void getDeferredRoot() throws Exception {
    final Deferred<Object> root = zk_client.getDeferredRoot();
    deferred_rootregion = 
        Whitebox.getInternalState(zk_client, "deferred_rootregion");
    assertNotNull(root);
    assertNotNull(deferred_rootregion);
    assertEquals(1, deferred_rootregion.size());
    verifyPrivate(zk_client).invoke("connectZK");
  }
  
  @Test
  public void getDeferredRootAdd() throws Exception {
    deferred_rootregion = new ArrayList<Deferred<Object>>(2);
    deferred_rootregion.add(new Deferred<Object>());
    Whitebox.setInternalState(zk_client, "deferred_rootregion", deferred_rootregion);
    
    final Deferred<Object> root = zk_client.getDeferredRoot();
    assertNotNull(root);
    assertNotNull(deferred_rootregion);
    assertEquals(2, deferred_rootregion.size());
    verifyPrivate(zk_client).invoke("connectZK");
  }

  @Test
  public void getDeferredRootIfBeingLookedUpNull() throws Exception {
    assertNull(zk_client.getDeferredRootIfBeingLookedUp());
    verifyPrivate(zk_client, never()).invoke("connectZK");
  }
  
  @Test
  public void getDeferredRootIfBeingLookedUp() throws Exception {
    deferred_rootregion = new ArrayList<Deferred<Object>>(2);
    deferred_rootregion.add(new Deferred<Object>());
    Whitebox.setInternalState(zk_client, "deferred_rootregion", deferred_rootregion);
    
    final Deferred<Object> root = zk_client.getDeferredRootIfBeingLookedUp();
    assertNotNull(root);
    assertNotNull(deferred_rootregion);
    assertEquals(2, deferred_rootregion.size());
    verifyPrivate(zk_client, never()).invoke("connectZK");
  }
  
  @Test (expected = NonRecoverableException.class)
  public void connectZKUnknownHostExcetion() throws Exception {
    whenNew(ZooKeeper.class).withArguments(
        anyString(), anyInt(), (Watcher)any()).thenThrow(
        new UnknownHostException("Bad Quorum"));
    zk_client.getDeferredRoot().joinUninterruptibly();
  }
  
  /**
   * WARNING: This will cause an infinite loop until we fix it. There's a TODO
   * in the code for it.
   */
//  @Test
//  public void connectZKIOEInfinitLoop() throws Exception {
//    whenNew(ZooKeeper.class).withAnyArguments().thenThrow(
//        new IOException("Bad Quorum"));
//    zk_client.getDeferredRoot().joinUninterruptibly();
//  }
  
  @Test
  public void process() throws Exception {
    final WatchedEvent event = mock(WatchedEvent.class);
    when(event.getState()).thenReturn(KeeperState.SyncConnected);
    final ZKCallback cb = zk_client.new ZKCallback();
    whenNew(ZKCallback.class).withNoArguments().thenReturn(cb);
    Whitebox.setInternalState(zk_client, "zk", zk);
    
    zk_client.process(event);
    verifyPrivate(zk_client).invoke("getRootRegion");
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verify(zk).getData("/hbase/root-region-server", zk_client, cb, null);
    verify(zk).getData("/hbase/meta-region-server", zk_client, cb, null);
  }
  
  @Test
  public void processDefault() throws Exception {
    final WatchedEvent event = mock(WatchedEvent.class);
    when(event.getState()).thenReturn(KeeperState.AuthFailed);
    deferred_rootregion = new ArrayList<Deferred<Object>>(2);
    deferred_rootregion.add(new Deferred<Object>());
    Whitebox.setInternalState(zk_client, "deferred_rootregion", deferred_rootregion);
    
    zk_client.process(event);
    verifyPrivate(zk_client, never()).invoke("getRootRegion");
    verifyPrivate(zk_client).invoke("connectZK"); // null rootregion
    verifyPrivate(zk_client).invoke("disconnectZK");
  }
  
  @Test
  public void processDefaultNonNullRootRegion() throws Exception {
    final WatchedEvent event = mock(WatchedEvent.class);
    when(event.getState()).thenReturn(KeeperState.AuthFailed);
    
    zk_client.process(event);
    verifyPrivate(zk_client, never()).invoke("getRootRegion");
    verifyPrivate(zk_client, never()).invoke("connectZK"); // null rootregion
    verifyPrivate(zk_client).invoke("disconnectZK");
  }
  
  @Test
  public void processNull() throws Exception {
    // ZK should never return a null, but if it does, we just swallow it
    zk_client.process(null);
    verifyPrivate(zk_client, never()).invoke("getRootRegion");
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
  }
  
  @Test
  public void processException() throws Exception {
    final WatchedEvent event = mock(WatchedEvent.class);
    when(event.getState()).thenThrow(new RuntimeException("Fail!"));
    
    zk_client.process(null);
    verifyPrivate(zk_client, never()).invoke("getRootRegion");
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
  }

  @Test
  public void disconnectZK() throws Exception {
    zk_client.getDeferredRoot();
    zk_client.disconnectZK();
    verify(zk).close();
    assertNull(Whitebox.getInternalState(zk_client, "zk"));
  }
  
  @Test
  public void disconnectZKInterrupted() throws Exception {
    Mockito.doThrow(new InterruptedException("Interrupted")).when(zk).close();
    zk_client.getDeferredRoot();
    zk_client.disconnectZK();
    verify(zk).close();
    assertNull(Whitebox.getInternalState(zk_client, "zk"));
  }
  
  @Test (expected = RuntimeException.class)
  public void disconnectZKUnexpected() throws Exception {
    Mockito.doThrow(new RuntimeException("Something Bad Happened!"))
      .when(zk).close();
    zk_client.getDeferredRoot();
    zk_client.disconnectZK();
    verify(zk).close();
    assertNotNull(Whitebox.getInternalState(zk_client, "zk"));
  }
  
  @Test
  public void disconnectZKNullZK() throws Exception {
    Whitebox.setInternalState(zk_client, "zk", (Object)null);
    zk_client.disconnectZK();
    verify(zk, never()).close();
  }
  
  // ------------- ZKCallback -------------
  // We never look at the context returned from ZK so it'll always be null in 
  // the tests below.
  @Test
  public void zkCallbackCtor() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    assertNotNull(cb);
  }
  
  @Test
  public void processResultRoot90() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultRoot91() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultRoot92() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final ServerName server = ServerName.newBuilder()
        .setHostName("127.0.0.1").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    cb.processResult(Code.OK.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultMetaBadMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test (expected = NullPointerException.class)
  public void processResultNullPath() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), null, null, 
        data, stat);
  }
  
  @Test
  public void processResultEmptyPath() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultUnknownPath() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/something-unknown", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeRootFirst() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeRootMetaFound() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    Whitebox.setInternalState(cb, "found_meta", (byte)1);
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeRootMetaNotFound() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    Whitebox.setInternalState(cb, "found_meta", (byte)2);
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeMetaFirst() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeMetaRootFound() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    Whitebox.setInternalState(cb, "found_root", (byte)1);
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNoNodeMetaRootNotFound() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    Whitebox.setInternalState(cb, "found_root", (byte)2);
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NONODE.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("2"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultRootBadCode() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NOAUTH.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultMetaBadCode() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.NOAUTH.intValue(), "/hbase/meta-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultNullData() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        null, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }

  @Test
  public void processResultEmptyData() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        new byte[]{}, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultTooMuchData() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    cb.processResult(Code.OK.intValue(), "/hbase/meta-region-server", null, 
        new byte[65535], stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultRootBad() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client, never()).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, never()).invoke("connectZK");
    verifyPrivate(zk_client, never()).invoke("disconnectZK");
    verifyPrivate(zk_client).invoke("retryGetRootRegionLater");
    verify(client).newTimeout((TimerTask)any(), anyLong());
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
  }
  
  @Test
  public void processResultRootCallWaiter() throws Exception {
    final Deferred<Object> root = zk_client.getDeferredRoot();
    assertNotNull(root);
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
    assertNotNull(root.joinUninterruptibly());
  }
  
  @Test
  public void processResultRootCallWaiters() throws Exception {
    final Deferred<Object> root = zk_client.getDeferredRoot();
    final Deferred<Object> root2 = zk_client.getDeferredRoot();
    assertNotNull(root);
    assertNotNull(root2);
    assertTrue(root != root2);
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    cb.processResult(Code.OK.intValue(), "/hbase/root-region-server", null, 
        data, stat);
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    verifyPrivate(zk_client, times(2)).invoke("connectZK");
    verifyPrivate(zk_client).invoke("disconnectZK");
    verifyPrivate(zk_client, never()).invoke("retryGetRootRegionLater");
    assertEquals(Byte.valueOf("1"), 
        (Byte)Whitebox.getInternalState(cb, "found_root"));
    assertEquals(Byte.valueOf("0"), 
        (Byte)Whitebox.getInternalState(cb, "found_meta"));
    assertNotNull(root.joinUninterruptibly());
    assertNotNull(root2.joinUninterruptibly());
  }
  
  @Test
  public void handleRootZnode90() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnode90Commas() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1,50511".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnode90Resolve() throws Exception {
    final InetAddress addr = mock(InetAddress.class);
    when(addr.getHostAddress()).thenReturn("192.168.1.1");
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("myzkhost")).thenReturn(addr);
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "myzkhost:50511".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("192.168.1.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "192.168.1.1", 50511);
  }

  @Test
  public void handleRootZnode90NoPort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1".getBytes(CHARSET);
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode90NotAPort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:Host2".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode90NegativePort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:-1024".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode90PortToBig() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:65536".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }
 
  @Test
  public void handleRootZnode91() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }

  @Test (expected = NumberFormatException.class)
  public void handleRootZnode91Colons() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511:1388534400000".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }

  @Test (expected = NumberFormatException.class)
  public void handleRootZnode91Colon() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1:50511,1388534400000".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }
  
  @Test
  public void handleRootZnode91Resolve() throws Exception {
    final InetAddress addr = mock(InetAddress.class);
    when(addr.getHostAddress()).thenReturn("192.168.1.1");
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("myzkhost")).thenReturn(addr);
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "myzkhost,50511,1388534400000".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("192.168.1.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "192.168.1.1", 50511);
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode91NoPort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1,1388534400000".getBytes(CHARSET);
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode91NoHost() throws Exception {
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("50511"))
      .thenThrow(new UnknownHostException("No such host 50511"));
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "50511,1388534400000".getBytes(CHARSET);
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test
  public void handleRootZnode91NoStartCode() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "127.0.0.1,50511".getBytes(CHARSET);
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnode92() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode92Colons() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1:50511:1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    cb.handleRootZnode(data);
  }
  
  @Test
  public void handleRootZnode92Colon() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1:50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnode92MissingMagic() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test
  public void handleRootZnode92MissingMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 1];
    data[0] = ZKCallback.MAGIC;
    System.arraycopy(string, 0, data, 1, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }

  @Test
  public void handleRootZnode92EmptyMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 5];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 0, 1);
    System.arraycopy(string, 0, data, 5, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test
  public void handleRootZnode92TooMuchMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 65006];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 65001, 1);
    System.arraycopy(string, 0, data, 65006, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }
  
  // TODO - to be fixed in the source code
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void handleRootZnode92UpperLimitofMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 65004];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 64999, 1);
    System.arraycopy(string, 0, data, 65004, string.length);
    
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnode92MissingHostString() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = new byte[6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);

    assertNull(cb.handleRootZnode(data));
  }
  
  @Test
  public void handleRootZnode92NullHosStringBytes() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    // no string copy here
    
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode92NoPort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test (expected = NumberFormatException.class)
  public void handleRootZnode92NoHost() throws Exception {
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("50511"))
      .thenThrow(new UnknownHostException("No such host 50511"));
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test
  public void handleRootZnode92NoStartCode() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    final RegionClient rc = cb.handleRootZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleRootZnodeUnknownHost() throws Exception {
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("myzkhost"))
      .thenThrow(new UnknownHostException("No such host myzkhost"));
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "myzkhost:50511".getBytes(CHARSET);
    assertNull(cb.handleRootZnode(data));
  }
  
  @Test (expected = RuntimeException.class)
  public void handleRootZnodeResolveException() throws Exception {
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("myzkhost"))
      .thenThrow(new RuntimeException("Pear shaped"));
    
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = "myzkhost:50511".getBytes(CHARSET);
    cb.handleRootZnode(data);
  }
  
  @Test (expected = NullPointerException.class)
  public void handleRootZnodeNull() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    cb.handleRootZnode(null);
  }
  
  @Test
  public void handleMetaZnode() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("127.0.0.1").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    assertFalse((Boolean)Whitebox.getInternalState(client, "has_root"));
  }
  
  @Test
  public void handleMetaZnodeResolve() throws Exception {
    final InetAddress addr = mock(InetAddress.class);
    when(addr.getHostAddress()).thenReturn("192.168.1.1");
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("myzkhost")).thenReturn(addr);
    
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("myzkhost").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("192.168.1.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "192.168.1.1", 50511);
  }
  
  @Test
  public void handleMetaZnodeIsRoot() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("127.0.0.1").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
    assertFalse((Boolean)Whitebox.getInternalState(client, "has_root"));
  }
  
  @Test
  public void handleMetaZnodeNotMagic() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] data = new byte[] { 0, 0 };
    assertNull(cb.handleMetaZnode(data));
  }
  
  @Test
  public void handleMetaZnodeMissingMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 1];
    data[0] = ZKCallback.MAGIC;
    System.arraycopy(string, 0, data, 1, string.length);
    
    assertNull(cb.handleMetaZnode(data));
  }
  
  @Test
  public void handleMetaZnodeEmptyMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 5];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 0, 1);
    System.arraycopy(string, 0, data, 5, string.length);
    
    assertNull(cb.handleMetaZnode(data));
  }
  
  @Test
  public void handleMetaZnodeTooMuchMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 65006];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 65001, 1);
    System.arraycopy(string, 0, data, 65006, string.length);
    
    assertNull(cb.handleMetaZnode(data));
  }
  
  // TODO - shouldn't allow this!
  @Test
  public void handleMetaZnodeNoPort() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("127.0.0.1").build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:0", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 0);
  }
  
  // Defaults to localhost apparently
  @Test
  public void handleMetaZnodeNoHost() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  // TODO - to be fixed in the source code
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void handleMetaZnodeUpperLimitofMeta() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 65004];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 64999, 1);
    System.arraycopy(string, 0, data, 65004, string.length);
    
    final RegionClient rc = cb.handleMetaZnode(data);
    assertNotNull(rc);
    assertEquals("127.0.0.1:50511", rc.getRemoteAddress());
    verifyPrivate(client).invoke("newClient", "127.0.0.1", 50511);
  }
  
  @Test
  public void handleMetaZnodeNoPBufMagic() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    final byte[] string = "127.0.0.1,50511,1388534400000".getBytes(CHARSET);
    final byte[] data = new byte[string.length + 6];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    System.arraycopy(string, 0, data, 6, string.length);
    
    assertNull(cb.handleMetaZnode(data));
  }
  
  @Test (expected = NullPointerException.class)
  public void handleMetaZnodeNull() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();
    cb.handleMetaZnode(null);
  }

  @Test
  public void handleMetaZnodeCorruptPBuf() throws Exception {
    final ZKCallback cb = zk_client.new ZKCallback();

    final ServerName server = ServerName.newBuilder()
        .setHostName("127.0.0.1").setPort(50511).build();
    final MetaRegionServer meta_server = MetaRegionServer.newBuilder()
          .setServer(server).build();
    final byte[] data = new byte[meta_server.getSerializedSize() + 10];
    data[0] = ZKCallback.MAGIC;
    Bytes.setInt(data, 1, 1);
    Bytes.setInt(data, HBaseClient.PBUF_MAGIC, 6);
    System.arraycopy(meta_server.toByteArray(), 0, data, 10, 
        meta_server.getSerializedSize());
    data[11] = 26;
    
    assertNull(cb.handleMetaZnode(data));
  }
  
}
