package org.hbase.async;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
@PrepareForTest({ HBaseClient.class, RegionClient.class, RegionInfo.class, 
  HBaseRpc.class })
public class TestHBaseClient extends BaseTestHBaseClient {

  @Before
  public void before2() {
    Whitebox.setInternalState(client, "has_root", false);
  }
  
  @Test
  public void getRegionNotCached() throws Exception {
    this.regions_cache.clear();
    assertNull(Whitebox.invokeMethod(client, "getRegion", TABLE, KEY));
  }
  
  @Test
  public void getRegionDiffKey() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "getRegion", TABLE, KEY2);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(TABLE, info.table());
    assertEquals(0, info.stopKey().length);
    assertArrayEquals("table,,1234567890".getBytes(CHARSET), info.name());
  }
  
  @Test
  public void getRegionRootAlreadyLookedUp() throws Exception {
    Whitebox.setInternalState(client, "has_root", true);
    final Object obj = Whitebox.invokeMethod(client, "getRegion", 
        HBaseClient.ROOT, HBaseClient.EMPTY_ARRAY);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(HBaseClient.ROOT, info.table());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, info.stopKey());
    assertArrayEquals(HBaseClient.ROOT_REGION, info.name());
  }
  
  @Test
  public void getRegionRootHasRoot() throws Exception {
    Whitebox.setInternalState(client, "has_root", true);
    final Object obj = Whitebox.invokeMethod(client, "getRegion", 
        HBaseClient.ROOT, null);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(HBaseClient.ROOT, info.table());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, info.stopKey());
    assertArrayEquals(HBaseClient.ROOT_REGION, info.name());
  }
  
  @Test
  public void getRegion96Meta() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "getRegion", 
        HBaseClient.HBASE96_META, null);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(HBaseClient.HBASE96_META, info.table());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, info.stopKey());
    assertArrayEquals(HBaseClient.META_REGION_NAME, info.name());
  }
  
  @Test
  public void getRegionCached() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "getRegion", TABLE, KEY);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(TABLE, info.table());
    assertEquals(0, info.stopKey().length);
    assertArrayEquals("table,,1234567890".getBytes(CHARSET), info.name());
  }

// TODO - come back to this one
//  @Test
//  public void getRegionCachedIncorrectly() throws Exception {
////    PowerMockito.doReturn(false)
////      .when(HBaseClient.class, "isCacheKeyForTable", byte[].class, byte[].class);
//    //PowerMockito.when(HBaseClient.class, "isCacheKeyForTable", any(), any()).thenReturn(false);
//    final Object obj = Whitebox.invokeMethod(client, "getRegion", TABLE, KEY);
//    assertNotNull(obj);
//    final RegionInfo info = (RegionInfo)obj;
//    assertArrayEquals(TABLE, info.table());
//    assertEquals(0, info.stopKey().length);
//    assertArrayEquals("table,,1234567890".getBytes(CHARSET), info.name());
//  }
  
  @Test
  public void getRegionCachedBeyondStopKey() throws Exception {
    final RegionInfo keyed_region = new RegionInfo(TABLE, region.name(),
        "aaa".getBytes(CHARSET));
    regions_cache.put(region.name(), keyed_region);
    assertNull(Whitebox.invokeMethod(client, "getRegion", TABLE, KEY));
  }
  
  @Test
  public void getRegionCachedAfterStopKey() throws Exception {
    final RegionInfo keyed_region = new RegionInfo(TABLE, region.name(),
        "lll".getBytes(CHARSET));
    regions_cache.put(region.name(), keyed_region);
    final Object obj = Whitebox.invokeMethod(client, "getRegion", TABLE, KEY);
    assertNotNull(obj);
    final RegionInfo info = (RegionInfo)obj;
    assertArrayEquals(TABLE, info.table());
    assertArrayEquals("lll".getBytes(CHARSET), info.stopKey());
    assertArrayEquals("table,,1234567890".getBytes(CHARSET), info.name());
  }
  
  @Test
  public void getRegionCachedSameStopKey() throws Exception {
    final RegionInfo keyed_region = new RegionInfo(TABLE, region.name(), KEY);
    regions_cache.put(region.name(), keyed_region);
    assertNull(Whitebox.invokeMethod(client, "getRegion", TABLE, KEY));
  }

  @Test
  public void createRegionSearchKey() throws Exception {
    final Object obj = Whitebox.invokeMethod(HBaseClient.class, 
        "createRegionSearchKey", TABLE, KEY);
    assertNotNull(obj);
    final byte[] key = (byte[])obj;
    assertArrayEquals("table,key,:".getBytes(CHARSET), key);
  }
  
  @Test (expected = NullPointerException.class)
  public void createRegionSearchKeyNullTable() throws Exception {
    Whitebox.invokeMethod(HBaseClient.class, "createRegionSearchKey", 
        (byte[])null, KEY);
  }

  @Test (expected = NullPointerException.class)
  public void createRegionSearchKeyNullKey() throws Exception {
    Whitebox.invokeMethod(HBaseClient.class, "createRegionSearchKey", 
        TABLE, (byte[])null);
  }
  
  @Test
  public void createRegionSearchKeyEmptyTable() throws Exception {
    final Object obj = Whitebox.invokeMethod(HBaseClient.class, 
        "createRegionSearchKey", HBaseClient.EMPTY_ARRAY, KEY);
    assertNotNull(obj);
    final byte[] key = (byte[])obj;
    assertArrayEquals(",key,:".getBytes(CHARSET), key);
  }
  
  @Test
  public void createRegionSearchKeyEmptyKey() throws Exception {
    final Object obj = Whitebox.invokeMethod(HBaseClient.class, 
        "createRegionSearchKey", TABLE, HBaseClient.EMPTY_ARRAY);
    assertNotNull(obj);
    final byte[] key = (byte[])obj;
    assertArrayEquals("table,,:".getBytes(CHARSET), key);
  }
  
  @Test
  public void isCacheKeyForTable() throws Exception {
    final boolean is_key = (Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", TABLE, region.name());
    assertTrue(is_key);
  }
  
  @Test
  public void isCacheKeyForTableDiffTable() throws Exception {
    final boolean is_key = (Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", KEY, region.name());
    assertFalse(is_key);
  }
  
  @Test
  public void isCacheKeyForTablePrefix() throws Exception {
    final byte[] diff_region = "table2,,1234567890".getBytes(CHARSET);
    final boolean is_key = (Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", TABLE, diff_region);
    assertFalse(is_key);
  }
  
  @Test
  public void isCacheKeyForTableBadCache() throws Exception {
    final byte[] diff_region = "table,".getBytes(CHARSET);
    final boolean is_key = (Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", TABLE, diff_region);
    assertTrue(is_key);
  }
  
  @Test (expected = ArrayIndexOutOfBoundsException.class)
  public void isCacheKeyForTableBadCache2() throws Exception {
    final byte[] diff_region = "table".getBytes(CHARSET);
    Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", TABLE, diff_region);
  }
  
  @Test (expected = NullPointerException.class)
  public void isCacheKeyForTableNullTable() throws Exception {
    Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", (byte[])null, region.name());
  }
  
  @Test (expected = NullPointerException.class)
  public void isCacheKeyForTableNullKey() throws Exception {
    Whitebox.invokeMethod(HBaseClient.class, 
        "isCacheKeyForTable", TABLE, (byte[])null);
  }

  @Test
  public void tooManyAttempts() throws Exception {
    final HBaseRpc rpc = getMockHBaseRpc(null);
    rpc.attempt = (byte) (10 + 1);
    final Deferred<Object> error = HBaseClient.tooManyAttempts(rpc, 
        new NoSuchColumnFamilyException("Fail!", rpc));
    assertNotNull(error);
    NonRecoverableException ex = null;
    try {
      error.joinUninterruptibly();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex instanceof NonRecoverableException);
    assertNotNull(ex.getCause());
    assertTrue(ex.getCause() instanceof NoSuchColumnFamilyException);
    verify(rpc).callback(any());
  }
  
  @Test
  public void tooManyAttemptsNullException() throws Exception {
    final HBaseRpc rpc = getMockHBaseRpc(null);
    rpc.attempt = (byte) (10 + 1);
    final Deferred<Object> error = HBaseClient.tooManyAttempts(rpc, null);
    assertNotNull(error);
    NonRecoverableException ex = null;
    try {
      error.joinUninterruptibly();
    } catch (NonRecoverableException e) {
      ex = e;
    }
    assertNotNull(ex);
    assertTrue(ex instanceof NonRecoverableException);
    assertNull(ex.getCause());
  }

  @Test
  public void knownToBeNSREdFalse() throws Exception {
    assertFalse((Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "knownToBeNSREd", region));
  }
  
  @Test
  public void knownToBeNSREdTrue() throws Exception {
    final RegionInfo nsre = new RegionInfo(HBaseClient.EMPTY_ARRAY, region.name(), 
        HBaseClient.EMPTY_ARRAY);
    assertTrue((Boolean)Whitebox.invokeMethod(HBaseClient.class, 
        "knownToBeNSREd", nsre));
  }
  
  @Test (expected = NullPointerException.class)
  public void knownToBeNSREdNullRegionInfo() throws Exception {
    Whitebox.invokeMethod(HBaseClient.class, "knownToBeNSREd", (RegionInfo)null);
  }
  
  // discoverRegion
  
  @Test
  public void discoverRegionNewRegionInfo() throws Exception {
    clearCaches();
    final Object obj = Whitebox.invokeMethod(client, "discoverRegion", 
        metaRow());
    assertNotNull(obj);
    final RegionClient region_client = (RegionClient)obj;
    assertEquals(1, regions_cache.size());
    assertEquals(1, region2client.size());
    assertEquals(1, client2regions.size());
    assertEquals("127.0.0.1:54321", region_client.getRemoteAddress());
    assertTrue(region_client == region2client.values().iterator().next());
    PowerMockito.verifyPrivate(client, never()).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test
  public void discoverRegionReplaceRegionInfo() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "discoverRegion", 
        metaRow());
    assertNotNull(obj);
    final RegionClient region_client = (RegionClient)obj;
    assertEquals(2, regions_cache.size());
    assertEquals(2, region2client.size());
    assertEquals(3, client2regions.size());
    assertEquals("127.0.0.1:54321", region_client.getRemoteAddress());
    final Iterator<RegionClient> iterator = region2client.values().iterator();
    assertTrue(region_client != iterator.next());
    assertTrue(region_client == iterator.next());
    PowerMockito.verifyPrivate(client, never()).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test
  public void discoverRegionNewRegionInfoColumnsOutOfOrder() throws Exception {
    // really should never happen, but *shrug*
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    
    final Object obj = Whitebox.invokeMethod(client, "discoverRegion", row);
    assertNotNull(obj);
    final RegionClient region_client = (RegionClient)obj;
    assertEquals(1, regions_cache.size());
    assertEquals(1, region2client.size());
    assertEquals(1, client2regions.size());
    assertTrue(region_client == region2client.values().iterator().next());
    assertEquals("127.0.0.1:54321", region_client.getRemoteAddress());
    PowerMockito.verifyPrivate(client, never()).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test
  public void discoverRegionNewRegionInfoMultiHost() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "remotehost:12345".getBytes()));
    // ignores all but the last
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    final Object obj = Whitebox.invokeMethod(client, "discoverRegion", row);
    assertNotNull(obj);
    final RegionClient region_client = (RegionClient)obj;
    assertEquals(1, regions_cache.size());
    assertEquals(1, region2client.size());
    assertEquals(1, client2regions.size());
    assertTrue(region_client == region2client.values().iterator().next());
    assertEquals("127.0.0.1:54321", region_client.getRemoteAddress());
    PowerMockito.verifyPrivate(client, never()).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test
  public void discoverRegionNewRegionInfoMissingServerColumn() throws Exception {
    // really should never happen, but *shrug*
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    
    assertNull(Whitebox.invokeMethod(client, "discoverRegion", row));
    assertEquals(1, regions_cache.size());
    assertEquals(0, region2client.size());
    assertEquals(0, client2regions.size());
    PowerMockito.verifyPrivate(client).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test (expected = BrokenMetaException.class)
  public void discoverRegionNewRegionInfoMissingRegionrColumn() throws Exception {
    // really should never happen, but *shrug*
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    Whitebox.invokeMethod(client, "discoverRegion", row);
  }
  
  @Test (expected = RegionOfflineException.class)
  public void discoverRegionNewRegionInfoOffline() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, true, false, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    Whitebox.invokeMethod(client, "discoverRegion", row);
  }
  
  @Test
  public void discoverRegionNewRegionInfoSplitting() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    // Don't know if this is valid to be online and splitting. Prolly is
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, true, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    assertNull(Whitebox.invokeMethod(client, "discoverRegion", row));
    assertEquals(1, regions_cache.size());
    assertEquals(0, region2client.size());
    assertEquals(0, client2regions.size());
    PowerMockito.verifyPrivate(client).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test
  public void discoverRegionNewRegionInfoOfflineAndSplitting() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, true, true, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    assertNull(Whitebox.invokeMethod(client, "discoverRegion", row));
    assertEquals(1, regions_cache.size());
    assertEquals(0, region2client.size());
    assertEquals(0, client2regions.size());
    PowerMockito.verifyPrivate(client).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }

  @Test (expected = BrokenMetaException.class)
  public void discoverRegionNewRegionInfoMissingPort() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost".getBytes()));
    
    Whitebox.invokeMethod(client, "discoverRegion", row);
  }
  
  @Test (expected = BrokenMetaException.class)
  public void discoverRegionNewRegionInfoNotAPort() throws Exception {
    clearCaches();
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:myport".getBytes()));
    
    Whitebox.invokeMethod(client, "discoverRegion", row);
  }
  
  @Test (expected = BrokenMetaException.class)
  public void discoverRegionNewRegionNullStartKey() throws Exception {
    PowerMockito.mockStatic(RegionInfo.class);
    PowerMockito.when(RegionInfo.fromKeyValue((KeyValue)any(), (byte[][])any()))
      .thenAnswer(new Answer<RegionInfo>() {
      @Override
      public RegionInfo answer(final InvocationOnMock invocation) throws Throwable {
        final byte[][] tmp = (byte[][])invocation.getArguments()[1];
        tmp[0] = null;
        return region;
      }
    });
    
    clearCaches();
    Whitebox.invokeMethod(client, "discoverRegion", metaRow());
  }
  
  @Test
  public void discoverRegionNewRegionInfoNullHost() throws Exception {
    mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName("54321"))
      .thenThrow(new UnknownHostException("No such host 54321"));
    clearCaches();
    
    assertNull(Whitebox.invokeMethod(client, "discoverRegion", metaRow()));
    assertEquals(1, regions_cache.size());
    assertEquals(0, region2client.size());
    assertEquals(0, client2regions.size());
    PowerMockito.verifyPrivate(client).invoke("invalidateRegionCache", 
        (byte[])any(), anyBoolean(), anyString());
  }
  
  @Test (expected = TableNotFoundException.class)
  public void discoverRegionEmpty() throws Exception {
    Whitebox.invokeMethod(client, "discoverRegion", new ArrayList<KeyValue>());
  }
  
  @Test (expected = NullPointerException.class)
  public void discoverRegionNull() throws Exception {
    Whitebox.invokeMethod(client, "discoverRegion", (ArrayList<KeyValue>)null);
  }
}
