package org.hbase.async;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.hbase.async.HBaseClient.ZKClient;
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
  GetRequest.class, RegionInfo.class })
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
  protected Counter num_nsre_rpcs;
  /** Fake client supposedly connected to -ROOT-.  */
  protected RegionClient rootclient = mock(RegionClient.class);
  /** Fake client supposedly connected to .META..  */
  protected RegionClient metaclient = mock(RegionClient.class);
  /** Fake client supposedly connected to our fake test table.  */
  protected RegionClient regionclient = mock(RegionClient.class);
  /** Fake Zookeeper client */
  protected ZKClient zkclient = mock(ZKClient.class);
  
  @Before
  public void before() throws Exception {
    when(zkclient.getDeferredRoot()).thenReturn(new Deferred<Object>());
    
    client = PowerMockito.spy(new HBaseClient("test-quorum-spec"));
    Whitebox.setInternalState(client, "zkclient", zkclient);
    Whitebox.setInternalState(client, "rootregion", rootclient);
    // Inject a timer that always fires away immediately.
    Whitebox.setInternalState(client, "timer", new FakeTimer());
    regions_cache = Whitebox.getInternalState(client, "regions_cache");
    region2client = Whitebox.getInternalState(client, "region2client");
    client2regions = Whitebox.getInternalState(client, "client2regions");
    injectRegionInCache(meta, metaclient);
    injectRegionInCache(region, regionclient);
    
    PowerMockito.doAnswer(new Answer<RegionClient>(){
      @Override
      public RegionClient answer(InvocationOnMock invocation) throws Throwable {
        final Object[] args = invocation.getArguments();
        final String endpoint = (String)args[0] + ":" + (Integer)args[1];
        final RegionClient rc = mock(RegionClient.class);
        when(rc.getRemoteAddress()).thenReturn(endpoint);
        client2regions.put(rc, new ArrayList<RegionInfo>());
        return rc;
      }
    }).when(client, "newClient", anyString(), anyInt());
  }
 

  /**
   * Injects an entry in the local META cache of the client.
   */
  protected void injectRegionInCache(final RegionInfo region,
                                   final RegionClient client) {
    regions_cache.put(region.name(), region);
    region2client.put(region, client);
    // We don't care about client2regions in these tests.
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

  protected void setupHandleNSRE() throws Exception {
    num_nsre_rpcs = new Counter();
    Whitebox.setInternalState(client, "num_nsre_rpcs", num_nsre_rpcs);
    got_nsre = new ConcurrentSkipListMap<byte[], ArrayList<HBaseRpc>>(
            RegionInfo.REGION_NAME_CMP);
    Whitebox.setInternalState(client, "got_nsre", got_nsre);
    Whitebox.setInternalState(client, "zkclient", zkclient);
    when(zkclient.getDeferredRoot()).thenReturn(Deferred.fromResult(null));
//    PowerMockito.when(client, 
//        PowerMockito.method(HBaseClient.class, "locateRegion", byte[].class, byte[].class))
//        .withArguments((byte[])any(), (byte[])any())
//        .thenReturn(Deferred.fromResult(null));
  }
  
  /**
   * A fake {@link Timer} implementation that fires up tasks immediately.
   * Tasks are called immediately from the current thread.
   */
  static final class FakeTimer extends HashedWheelTimer {
    @Override
    public Timeout newTimeout(final TimerTask task,
                              final long delay,
                              final TimeUnit unit) {
      try {
        task.run(null);  // Argument never used in this code base.
        return null;     // Return value never used in this code base.
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException("Timer task failed: " + task, e);
      }
    }

    @Override
    public Set<Timeout> stop() {
      return null;  // Never called during tests.
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
