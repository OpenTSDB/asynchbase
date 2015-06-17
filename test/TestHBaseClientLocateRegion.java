package org.hbase.async;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
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
HBaseRpc.class, RegionClientStats.class })
public class TestHBaseClientLocateRegion extends BaseTestHBaseClient {
  private Deferred<Object> root_deferred;
  private GetRequest get;
  
  @Before
  public void beforeLocal() throws Exception {
    root_deferred = new Deferred<Object>();
    when(zkclient.getDeferredRoot()).thenReturn(root_deferred);
    get = new GetRequest(TABLE, KEY);
    Whitebox.setInternalState(client, "has_root", true);
  }
  
  //-------- ROOT AND DEAD CLIENT ----------
  
  @Test
  public void locateRegionRootHadRootLookupInZK() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        HBaseClient.ROOT, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  @Test
  public void locateRegionRootNoRootLookupInZK() throws Exception {
    Whitebox.setInternalState(client, "has_root", false);
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.ROOT, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  //--------- META AND DEAD CLIENT ------------
  @Test
  public void locateRegionMetaHadRootLookupInZK() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  @Test
  public void locateRegionMeta96HadRootLookupInZK() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.HBASE96_META, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  @Test
  public void locateRegionMetaNoRootLookupInZK() throws Exception {
    Whitebox.setInternalState(client, "has_root", false);
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  @Test
  public void locateRegionMeta96NoRootLookupInZK() throws Exception {
    Whitebox.setInternalState(client, "has_root", false);
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.HBASE96_META, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  // --------------- ROOT RECURSION --------------
  // These make sure we don't check the root region for the root region
  // because that would be plain silly.
  @SuppressWarnings("unchecked")
  @Test
  public void locateRegionRootHadRootLiveClient() throws Exception {
    setLiveRootClient();
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.ROOT, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertNull(((Deferred<Object>)obj).joinUninterruptibly());
    assertCounters(0, 0, 0);
  }
  
  //--------------- META LOOKUP IN ROOT --------------
  
  @Test
  public void locateRegionMetaLiveClient() throws Exception {
    clearCaches();
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient)obj.joinUninterruptibly();
    assertCounters(1, 0, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionMetaLiveClientTableNotFound() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(
          new ArrayList<KeyValue>(0)));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(1, 0, 0);
    assertEquals(2, client2regions.size());
    TableNotFoundException ex = null;
    try {
      obj.joinUninterruptibly();
      fail("Expected a TableNotFoundException exception");
    } catch (final TableNotFoundException e) {
      ex = e;
    }
    assertArrayEquals(HBaseClient.META, ex.getTable());
  }
  
  @Test
  public void locateRegionMetaLiveClientRecoverableException() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromError(
            new RegionOfflineException(EMPTY_ARRAY)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromError(
            new RegionOfflineException(EMPTY_ARRAY)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromError(
            new RegionOfflineException(EMPTY_ARRAY)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(4, 0, 0);
    assertEquals(3, client2regions.size());
    final RegionClient rc = (RegionClient)obj.joinUninterruptibly();
    assertNotNull(client2regions.get(rc));
  }
  
  // This used to be a tight loop that would continue indefinitely since we 
  // didn't track how many times we looped.
  @Test
  public void locateRegionMetaLiveClientTooManyAttempts() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    
    final Deferred<Object> deferred = get.getDeferred();
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenAnswer(new Answer<Deferred<ArrayList<KeyValue>>>() {
          @Override
          public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.<ArrayList<KeyValue>>fromError(
              new RegionOfflineException(EMPTY_ARRAY));
          }
        });

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(12, 0, 0);
    assertEquals(2, client2regions.size());
    try {
      obj.joinUninterruptibly();
      fail("Expected a NonRecoverableException exception");
    } catch (NonRecoverableException e) { }
    try {
      deferred.join();
      fail("Expected a NonRecoverableException exception");
    } catch (NonRecoverableException e) { }
  }
  
  @Test (expected = RuntimeException.class)
  public void locateRegionMetaLiveClientNonRecoverableException() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromError(
            new RuntimeException("Boo!")));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(1, 0, 0);
    assertEquals(2, client2regions.size());
    obj.joinUninterruptibly();
  }
  
  @Test
  public void locateRegionMetaLiveClientNSREdDueToSplit() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);

    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    // Don't know if this is valid to be online and splitting. Prolly is
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, true, TABLE));
    row.add(new KeyValue(meta.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(row));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(1, 0, 0);
    assertEquals(2, client2regions.size());
    assertNull(obj.joinUninterruptibly());
  }
  
  @Test
  public void locateRegionMetaLiveClientOffline() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);

    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, true, false, TABLE));
    row.add(new KeyValue(meta.name(), INFO, SERVER, "localhost:54321".getBytes()));
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(row))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(2, 0, 0);
    assertEquals(3, client2regions.size());
    final RegionClient rc = (RegionClient)obj.joinUninterruptibly();
    assertNotNull(client2regions.get(rc));
  }
  
  @Test (expected = BrokenMetaException.class)
  public void locateRegionMetaLiveClientBrokenMeta() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);

    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    row.add(metaRegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, false, false, TABLE));
    row.add(new KeyValue(meta.name(), INFO, SERVER, "localhost:myport".getBytes()));
    
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(row));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    assertCounters(1, 0, 0);
    assertEquals(2, client2regions.size());
    obj.joinUninterruptibly();
  }
  
  // -------------- GENERAL LOOKUP ------------
  @Test
  public void locateRegionLookupInZK() throws Exception {
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  // ---------- PARAMS -----------
  @Test (expected = NullPointerException.class)
  public void locateRegionNullTable() throws Exception {
    Whitebox.invokeMethod(client, "locateRegion", get, (byte[])null, EMPTY_ARRAY);
  }
  
  @Test (expected = NullPointerException.class)
  public void locateRegionNullKey() throws Exception {
    Whitebox.invokeMethod(client, "locateRegion", get, TABLE, (byte[])null);
  }
  
  @Test
  public void locateRegionEmptyTable() throws Exception {
    final Deferred<Object> root_deferred = new Deferred<Object>();
    when(zkclient.getDeferredRoot()).thenReturn(root_deferred);
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, EMPTY_ARRAY, EMPTY_ARRAY);
    assertNotNull(obj);
    assertTrue(root_deferred == obj);
  }
  
  @Test
  public void locateRegionEmptyKey() throws Exception {
    final Deferred<Object> root_deferred = new Deferred<Object>();
    when(zkclient.getDeferredRoot()).thenReturn(root_deferred);
    
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, EMPTY_ARRAY);
    assertNotNull(obj);
    assertTrue(root_deferred == obj);
  }

  // This one is OK because we only check the RPC we have an exception when
  // getting the closest row.
  @Test
  public void locateRegionNullRequest() throws Exception {
    final Deferred<Object> root_deferred = new Deferred<Object>();
    when(zkclient.getDeferredRoot()).thenReturn(root_deferred);

    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        (HBaseRpc)null, TABLE, EMPTY_ARRAY);
    assertNotNull(obj);
    assertTrue(root_deferred == obj);
  }
  
  @Test (expected = NullPointerException.class)
  public void locateRegionNullRequestNPE() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
        .thenAnswer(new Answer<Deferred<ArrayList<KeyValue>>>() {
          @Override
          public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
              throws Throwable {
            return Deferred.<ArrayList<KeyValue>>fromError(
              new RegionOfflineException(EMPTY_ARRAY));
          }
        });
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        (HBaseRpc)null, TABLE, EMPTY_ARRAY);
    obj.join();
  }
  
  // ---------- HELPERS -----------
  
  /** Simply sets the root region to the root client and mocks the alive call */ 
  private void setLiveRootClient() {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
  }

  /**
   * Helper to check our counters
   * @param root_lookups The number of root lookups expected
   * @param meta_lookups_with_permit The number of lookups with permits
   * @param meta_lookups_wo_permit The number of lookups without permits
   */
  private void assertCounters(final int root_lookups, 
      final int meta_lookups_with_permit, final int meta_lookups_wo_permit) {
    assertEquals(root_lookups, 
        ((Counter)Whitebox.getInternalState(client, "root_lookups")).get());
    assertEquals(meta_lookups_with_permit, 
        ((Counter)Whitebox.getInternalState(client, 
            "meta_lookups_with_permit")).get());
    assertEquals(meta_lookups_wo_permit, 
        ((Counter)Whitebox.getInternalState(client, 
            "meta_lookups_wo_permit")).get());
  }
}
