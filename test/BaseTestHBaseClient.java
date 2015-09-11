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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hbase.async.HBaseClient.ZKClient;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.socket.SocketChannel;
import org.jboss.netty.channel.socket.SocketChannelConfig;
import org.jboss.netty.channel.socket.nio.NioClientBossPool;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@PrepareForTest({ HBaseClient.class, RegionClient.class, HBaseRpc.class, 
  GetRequest.class, RegionInfo.class, NioClientSocketChannelFactory.class, 
  Executors.class, HashedWheelTimer.class, NioClientBossPool.class, 
  NioWorkerPool.class })
@Ignore // ignore for test runners
public class BaseTestHBaseClient {
  protected static final Charset CHARSET = Charset.forName("ASCII");
  protected static final byte[] COMMA = { ',' };
  protected static final byte[] TIMESTAMP = "1234567890".getBytes();
  protected static final byte[] INFO = getStatic("INFO");
  protected static final byte[] REGIONINFO = getStatic("REGIONINFO");
  protected static final byte[] SERVER = getStatic("SERVER");
  protected static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  protected static final byte[] KEY = { 'k', 'e', 'y' };
  protected static final byte[] KEY2 = { 'k', 'e', 'y', '2' };
  protected static final byte[] FAMILY = { 'f' };
  protected static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  protected static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  protected static final byte[] EMPTY_ARRAY = new byte[0];
  protected static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
  protected static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
  protected static final RegionInfo region = mkregion("table", "table,,1234567890");
  protected static final int RS_PORT = 50511;
  protected static final String ROOT_IP = "192.168.0.1";
  protected static final String META_IP = "192.168.0.2";
  protected static final String REGION_CLIENT_IP = "192.168.0.3";
  protected static String MOCK_RS_CLIENT_NAME = "Mock RegionClient";
  protected static String MOCK_ROOT_CLIENT_NAME = "Mock RootClient";
  protected static String MOCK_META_CLIENT_NAME = "Mock MetaClient";
  
  protected HBaseClient client = null;
  /** Extracted from {@link #client}.  */
  protected ConcurrentSkipListMap<byte[], RegionInfo> regions_cache;
  /** Extracted from {@link #client}.  */
  protected ConcurrentHashMap<RegionInfo, RegionClient> region2client;
  /** Extracted from {@link #client}.  */
  protected ConcurrentHashMap<RegionClient, ArrayList<RegionInfo>> client2regions;
  /** Extracted from {@link #client}. */
  protected ConcurrentSkipListMap<byte[], ArrayList<HBaseRpc>> got_nsre;
  /** Extracted from {@link #client}. */
  protected HashMap<String, RegionClient> ip2client;
  /** Extracted from {@link #client}. */
  protected Counter num_nsre_rpcs;
  /** Fake client supposedly connected to -ROOT-.  */
  protected RegionClient rootclient;
  /** Fake client supposedly connected to .META..  */
  protected RegionClient metaclient;
  /** Fake client supposedly connected to our fake test table.  */
  protected RegionClient regionclient;
  /** Each new region client is dumped here */
  protected List<RegionClient> region_clients = new ArrayList<RegionClient>();
  /** Fake Zookeeper client */
  protected ZKClient zkclient;
  /** Fake channel factory */
  protected NioClientSocketChannelFactory channel_factory;
  /** Fake channel returned from the factory */
  protected SocketChannel chan;
  /** Fake timer for testing */
  protected FakeTimer timer;
  
  @Before
  public void before() throws Exception {
    region_clients.clear();
    rootclient = mock(RegionClient.class);
    when(rootclient.toString()).thenReturn(MOCK_ROOT_CLIENT_NAME);
    metaclient = mock(RegionClient.class);
    when(metaclient.toString()).thenReturn(MOCK_META_CLIENT_NAME);
    regionclient = mock(RegionClient.class);
    when(regionclient.toString()).thenReturn(MOCK_RS_CLIENT_NAME);
    zkclient = mock(ZKClient.class);
    channel_factory = mock(NioClientSocketChannelFactory.class);
    chan = mock(SocketChannel.class);
    timer = new FakeTimer();
    
    when(zkclient.getDeferredRoot()).thenReturn(new Deferred<Object>());
    PowerMockito.mockStatic(Executors.class);
    PowerMockito.when(Executors.defaultThreadFactory())
      .thenReturn(mock(ThreadFactory.class));
    PowerMockito.when(Executors.newCachedThreadPool())
      .thenReturn(mock(ExecutorService.class));
    PowerMockito.whenNew(NioClientSocketChannelFactory.class).withAnyArguments()
      .thenReturn(channel_factory);
    
    PowerMockito.whenNew(HashedWheelTimer.class).withAnyArguments()
      .thenReturn(timer);
    PowerMockito.whenNew(NioClientBossPool.class).withAnyArguments()
      .thenReturn(mock(NioClientBossPool.class));
    PowerMockito.whenNew(NioWorkerPool.class).withAnyArguments()
      .thenReturn(mock(NioWorkerPool.class));
    
    client = PowerMockito.spy(new HBaseClient("test-quorum-spec"));
    Whitebox.setInternalState(client, "zkclient", zkclient);
    Whitebox.setInternalState(client, "rootregion", rootclient);
    regions_cache = Whitebox.getInternalState(client, "regions_cache");
    region2client = Whitebox.getInternalState(client, "region2client");
    client2regions = Whitebox.getInternalState(client, "client2regions");
    got_nsre = Whitebox.getInternalState(client, "got_nsre");
    ip2client = Whitebox.getInternalState(client, "ip2client");
    injectRegionInCache(meta, metaclient, META_IP + ":" + RS_PORT);
    injectRegionInCache(region, regionclient, REGION_CLIENT_IP + ":" + RS_PORT);
    
    when(channel_factory.newChannel(any(ChannelPipeline.class)))
      .thenReturn(chan);
    when(chan.getConfig()).thenReturn(mock(SocketChannelConfig.class));
    when(rootclient.toString()).thenReturn("Mock RootClient");
    
    PowerMockito.doAnswer(new Answer<RegionClient>(){
      @Override
      public RegionClient answer(InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        final String endpoint = (String)args[0] + ":" + (Integer)args[1];
        final RegionClient rc = mock(RegionClient.class);
        when(rc.getRemoteAddress()).thenReturn(endpoint);
        client2regions.put(rc, new ArrayList<RegionInfo>());
        region_clients.add(rc);
        return rc;
      }
    }).when(client, "newClient", anyString(), anyInt());
  }
 
  /**
   * Injects an entry in the local caches of the client.
   */
  protected void injectRegionInCache(final RegionInfo region,
                                   final RegionClient client,
                                   final String ip) {
    regions_cache.put(region.name(), region);
    region2client.put(region, client);
    ArrayList<RegionInfo> regions = client2regions.get(client);
    if (regions == null) {
      regions = new ArrayList<RegionInfo>(1);
      client2regions.put(client, regions);
    }
    regions.add(region);
    ip2client.put(ip, client);
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  protected void clearCaches(){
    regions_cache.clear();
    region2client.clear();
    client2regions.clear();
  }
  
  protected static <T> T getStatic(final String fieldname) {
    return Whitebox.getInternalState(HBaseClient.class, fieldname);
  }

  /**
   * Creates a fake {@code .META.} row.
   * The row contains a single entry for all keys of {@link #TABLE}.
   */
  protected static ArrayList<KeyValue> metaRow() {
    return metaRow(HBaseClient.EMPTY_ARRAY, HBaseClient.EMPTY_ARRAY);
  }

  /**
   * Creates a fake {@code .META.} row.
   * The row contains a single entry for {@link #TABLE}.
   * @param start_key The start key of the region in this entry.
   * @param stop_key The stop key of the region in this entry.
   */
  protected static ArrayList<KeyValue> metaRow(final byte[] start_key,
                                             final byte[] stop_key) {
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(start_key, stop_key, false, false, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    return row;
  }
  
  protected static KeyValue metaRegionInfo( final byte[] start_key, 
      final byte[] stop_key, final boolean offline, final boolean splitting, 
      final byte[] table) {
    final byte[] name = concat(table, COMMA, start_key, COMMA, TIMESTAMP);
    final byte is_splitting = (byte) (splitting ? 1 : 0);
    final byte[] regioninfo = concat(
      new byte[] {
        0,                        // version
        (byte) stop_key.length,   // vint: stop key length
      },
      stop_key,
      offline ? new byte[] { 1 } : new byte[] { 0 }, // boolean: offline
      Bytes.fromLong(name.hashCode()),     // long: region ID (make it random)
      new byte[] { (byte) name.length },  // vint: region name length
      name,                       // region name
      new byte[] {
        is_splitting,             // boolean: splitting
        (byte) start_key.length,  // vint: start key length
      },
      start_key
    );
    return new KeyValue(region.name(), INFO, REGIONINFO, regioninfo);
  }

  protected static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(),
                          HBaseClient.EMPTY_ARRAY);
  }

  protected static byte[] anyBytes() {
    return any(byte[].class);
  }

  /** Concatenates byte arrays together.  */
  protected static byte[] concat(final byte[]... arrays) {
    int len = 0;
    for (final byte[] array : arrays) {
      len += array.length;
    }
    final byte[] result = new byte[len];
    len = 0;
    for (final byte[] array : arrays) {
      System.arraycopy(array, 0, result, len, array.length);
      len += array.length;
    }
    return result;
  }

  /** Creates a new Deferred that's already called back.  */
  protected static <T> Answer<Deferred<T>> newDeferred(final T result) {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(result);
      }
    };
  }

  /**
   * A fake {@link Timer} implementation that fires up tasks immediately.
   * Tasks are called immediately from the current thread and a history of the
   * various tasks is logged.
   */
  static final class FakeTimer extends HashedWheelTimer {
    final List<Map.Entry<TimerTask, Long>> tasks = 
        new ArrayList<Map.Entry<TimerTask, Long>>();
    final ArrayList<Timeout> timeouts = new ArrayList<Timeout>();
    boolean run = true;
    
    @Override
    public Timeout newTimeout(final TimerTask task,
                              final long delay,
                              final TimeUnit unit) {
      try {
        tasks.add(new AbstractMap.SimpleEntry<TimerTask, Long>(task, delay));
        if (run) {
          task.run(null);  // Argument never used in this code base.
        }
        final Timeout timeout = mock(Timeout.class);
        timeouts.add(timeout);
        return timeout;     // Return value never used in this code base.
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException("Timer task failed: " + task, e);
      }
    }

    @Override
    public Set<Timeout> stop() {
      run = false;
      return new HashSet<Timeout>(timeouts);
    }
  }

  /**
   * A fake {@link org.jboss.netty.util.Timer} implementation.
   * Instead of executing the task it will store that task in a internal state
   * and provides a function to start the execution of the stored task.
   * This implementation thus allows the flexibility of simulating the
   * things that will be going on during the time out period of a TimerTask.
   * This was mainly return to simulate the timeout period for
   * alreadyNSREdRegion test, where the region will be in the NSREd mode only
   * during this timeout period, which was difficult to simulate using the
   * above {@link FakeTimer} implementation, as we don't get back the control
   * during the timeout period
   *
   * Here it will hold at most two Tasks. We have two tasks here because when
   * one is being executed, it may call for newTimeOut for another task.
   */
  static final class FakeTaskTimer extends HashedWheelTimer {

    protected TimerTask newPausedTask = null;
    protected TimerTask pausedTask = null;

    @Override
    public synchronized Timeout newTimeout(final TimerTask task,
                                           final long delay,
                                           final TimeUnit unit) {
      if (pausedTask == null) {
        pausedTask = task;
      }  else if (newPausedTask == null) {
        newPausedTask = task;
      } else {
        throw new IllegalStateException("Cannot Pause Two Timer Tasks");
      }
      return null;
    }

    @Override
    public Set<Timeout> stop() {
      return null;
    }

    public boolean continuePausedTask() {
      if (pausedTask == null) {
        return false;
      }
      try {
        if (newPausedTask != null) {
          throw new IllegalStateException("Cannot be in this state");
        }
        pausedTask.run(null);  // Argument never used in this code base
        pausedTask = newPausedTask;
        newPausedTask = null;
        return true;
      } catch (Exception e) {
        throw new RuntimeException("Timer task failed: " + pausedTask, e);
      }
    }
  }

  /**
   * Generate and return a mocked HBase RPC for testing purposes with a valid
   * Deferred that can be called on execution. 
   * @param deferred A deferred to watch for results
   * @return The RPC to pass through unit tests.
   */
  protected HBaseRpc getMockHBaseRpc(final Deferred<Object> deferred) {
    final HBaseRpc rpc = mock(HBaseRpc.class);
    rpc.attempt = 0;
    when(rpc.getDeferred()).thenReturn(deferred);
    when(rpc.toString()).thenReturn("MockRPC");
    PowerMockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (deferred != null) {
          deferred.callback(invocation.getArguments()[0]);
        } else {
          System.out.println("Deferred was null!!");
        }
        return null;
      }
    }).when(rpc).callback(Object.class);
    return rpc;
  }
}
