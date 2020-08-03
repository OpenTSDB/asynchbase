/*
 * Copyright (C) 2014-2018 The Async HBase Authors.  All rights reserved.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.hbase.async.Scanner.OpenScannerRequest;
import org.hbase.async.Scanner.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class, RegionInfo.class, 
  HBaseRpc.class, RegionClientStats.class, Scanner.class, HBaseRpc.class,
  DeferredGroupException.class })
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
  
  //-------- ROOT AND DEAD ROOT CLIENT ----------
  @Test
  public void locateRegionRoot98HadRootLookupInZK() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.HBASE98_ROOT, EMPTY_ARRAY);
    assertTrue(root_deferred == obj);
    assertCounters(0, 0, 0);
  }
  
  @Test
  public void locateRegionRootHadRootLookupInZK() throws Exception {
    final Object obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.ROOT, EMPTY_ARRAY);
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
  
  //--------- META AND DEAD ROOT CLIENT ------------
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
  
  @Test
  public void locateRegionMeta98SplitMetaLookupInZK() throws Exception {
    client.has_root = true;
    client.split_meta = true;
    
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
  public void locateRegionMetaLiveRootClient() throws Exception {
    clearCaches();
    final RegionInfo ri = new RegionInfo(HBaseClient.ROOT, 
        HBaseClient.ROOT_REGION, EMPTY_ARRAY);
    final byte[] meta_key = HBaseClient.createRegionSearchKey(
        HBaseClient.META, EMPTY_ARRAY, false);
    final byte[] key = HBaseClient.createRegionSearchKey(HBaseClient.META, meta_key);
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
      
    verify(rootclient, times(1)).getClosestRowBefore(ri, 
        HBaseClient.ROOT, key, HBaseClient.INFO);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.joinUninterruptibly();
    assertCounters(1, 0, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionMetaSplitMetaLiveRootClient() throws Exception {
    clearCaches();
    final RegionInfo ri = new RegionInfo(HBaseClient.HBASE98_ROOT, 
        HBaseClient.HBASE98_ROOT_REGION, EMPTY_ARRAY);
    final byte[] meta_key = HBaseClient.createRegionSearchKey(
        HBaseClient.HBASE96_META, EMPTY_ARRAY, false);
    final byte[] key = HBaseClient.createRegionSearchKey(HBaseClient.HBASE96_META, meta_key);
    client.split_meta = true;
    Whitebox.setInternalState(client, "rootregion", rootclient);
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.HBASE96_META, EMPTY_ARRAY);
    
    verify(rootclient, times(1)).getClosestRowBefore(ri, 
        HBaseClient.HBASE98_ROOT, key, HBaseClient.INFO);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.joinUninterruptibly();
    assertCounters(1, 0, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionMetaSplitScanMetaLiveRootClient() throws Exception {
    clearCaches();
    final RegionInfo ri = new RegionInfo(HBaseClient.HBASE98_ROOT, 
        HBaseClient.HBASE98_ROOT_REGION, EMPTY_ARRAY);
    final byte[] meta_key = HBaseClient.createRegionSearchKey(
        HBaseClient.HBASE96_META, EMPTY_ARRAY, false);
    final byte[] key = HBaseClient.createRegionSearchKey(
        HBaseClient.HBASE96_META, meta_key);
    client.split_meta = true;
    Whitebox.setInternalState(client, "rootregion", rootclient);
    Whitebox.setInternalState(client, "scan_meta", true);
    when(rootclient.isAlive()).thenReturn(true);
    doReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()))
      .when(client).scanMeta(any(RegionClient.class), any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.HBASE96_META, EMPTY_ARRAY);
    
    verify(client, times(1)).scanMeta(rootclient, ri, 
        HBaseClient.HBASE98_ROOT, key, HBaseClient.INFO);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.joinUninterruptibly();
    assertCounters(1, 0, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionMetaLiveRootClientTableNotFound() throws Exception {
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
  public void locateRegionMetaLiveRootClientRecoverableException() throws Exception {
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
  public void locateRegionMetaLiveRootClientTooManyAttempts() throws Exception {
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
  public void locateRegionMetaLiveRootClientNonRecoverableException() throws Exception {
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
  public void locateRegionMetaLiveRootClientNSREdDueToSplit() throws Exception {
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
  public void locateRegionMetaLiveRootClientOffline() throws Exception {
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
  public void locateRegionMetaLiveRootClientBrokenMeta() throws Exception {
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
  
  //--------------- TABLE LOOKUP IN META --------------
  
  @Test
  public void locateRegionInMeta() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(metaRow()));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.join(1);
    assertCounters(0, 1, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionInMetaNoSuchTable() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    when(rootclient.getClosestRowBefore(any(RegionInfo.class), any(byte[].class), 
        any(byte[].class), any(byte[].class)))
      .thenReturn(Deferred.<ArrayList<KeyValue>>fromResult(new ArrayList<KeyValue>(0)));
    
    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, HBaseClient.META, EMPTY_ARRAY);
    assertTrue(root_deferred != obj);
    try {
      obj.join(1);
      fail("Expected TableNotFoundException");
    } catch (TableNotFoundException e) { }
    assertCounters(0, 1, 0);
    assertEquals(0, client2regions.size());
  }
  
  @Test
  public void locateRegionInMetaScan() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", true);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final ArrayList<ArrayList<KeyValue>> rows = Lists.newArrayList();
        rows.add(metaRow());
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new Response(0, rows, false, true));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.join(1);
    assertCounters(0, 1, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
  }
  
  @Test
  public void locateRegionInMetaSwitchToScan() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", false);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Deferred<ArrayList<KeyValue>>>() {
      @Override
      public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
          throws Throwable {
        final Exception e = new UnknownProtocolException("", null);
        return Deferred.fromError(e);
      }
    }).when(rootclient).getClosestRowBefore(any(RegionInfo.class), any(byte[].class),
        any(byte[].class), any(byte[].class));
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final ArrayList<ArrayList<KeyValue>> rows = Lists.newArrayList();
        rows.add(metaRow());
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new Response(0, rows, false, true));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.join(1);
    assertCounters(0, 2, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
    assertTrue((boolean) (Boolean) Whitebox.getInternalState(client, "scan_meta"));
  }
  
  @Test
  public void locateRegionInMetaSwitchToScanDGE() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", false);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Deferred<ArrayList<KeyValue>>>() {
      @Override
      public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
          throws Throwable {
        final Exception e = new UnknownProtocolException("", null);
        final DeferredGroupException dge = mock(DeferredGroupException.class);
        when(dge.getCause()).thenReturn(e);
        return Deferred.fromError(dge);
      }
    }).when(rootclient).getClosestRowBefore(any(RegionInfo.class), any(byte[].class),
        any(byte[].class), any(byte[].class));
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final ArrayList<ArrayList<KeyValue>> rows = Lists.newArrayList();
        rows.add(metaRow());
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new Response(0, rows, false, true));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    final RegionClient rc = (RegionClient) obj.join(1);
    assertCounters(0, 2, 0);
    assertEquals(1, client2regions.size());
    assertNotNull(client2regions.get(rc));
    assertTrue((boolean) (Boolean) Whitebox.getInternalState(client, "scan_meta"));
  }
  
  @Test
  public void locateRegionInMetaSwitchToScanDiffError() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", false);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Deferred<ArrayList<KeyValue>>>() {
      @Override
      public Deferred<ArrayList<KeyValue>> answer(InvocationOnMock invocation)
          throws Throwable {
        final Exception e = new TableNotFoundException(HBaseClient.ROOT);
        return Deferred.fromError(e);
      }
    }).when(rootclient).getClosestRowBefore(any(RegionInfo.class), any(byte[].class),
        any(byte[].class), any(byte[].class));
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        final ArrayList<ArrayList<KeyValue>> rows = Lists.newArrayList();
        rows.add(metaRow());
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new Response(0, rows, false, true));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    try {
      final RegionClient rc = (RegionClient) obj.join(1);
      fail("Expected TableNotFoundException");
    } catch (TableNotFoundException e) { }
    assertCounters(0, 1, 0);
    assertEquals(0, client2regions.size());
    assertFalse((boolean) (Boolean) Whitebox.getInternalState(client, "scan_meta"));
  }
  
  @Test
  public void locateRegionInMetaScanNoSuchTable() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", true);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new Response(0, null, false, true));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    try {
      obj.join(1);
      fail("Expected TableNotFoundException");
    } catch (TableNotFoundException e) { }
    assertCounters(0, 1, 0);
    assertEquals(0, client2regions.size());
  }
  
  @Test
  public void locateRegionInMetaScanException() throws Exception {
    clearCaches();
    
    Whitebox.setInternalState(client, "has_root", false);
    Whitebox.setInternalState(client, "scan_meta", true);
    
    when(rootclient.isAlive()).thenReturn(true);
    when(rootclient.acquireMetaLookupPermit()).thenReturn(true);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ((HBaseRpc) invocation.getArguments()[0]).getDeferred()
          .callback(new NonRecoverableException("Boo!"));
        return null;
      }
    }).when(rootclient).sendRpc(any(OpenScannerRequest.class));

    final Deferred<Object> obj = Whitebox.invokeMethod(client, "locateRegion", 
        get, TABLE, KEY);
    assertTrue(root_deferred != obj);
    try {
      obj.join(1);
      fail("Expected NonRecoverableException");
    } catch (NonRecoverableException e) { }
    assertCounters(0, 1, 0);
    assertEquals(0, client2regions.size());
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
        ((Counter)Whitebox.getInternalState(client, "root_lookups_with_permit")).get());
    assertEquals(meta_lookups_with_permit, 
        ((Counter)Whitebox.getInternalState(client, 
            "meta_lookups_with_permit")).get());
    assertEquals(meta_lookups_wo_permit, 
        ((Counter)Whitebox.getInternalState(client, 
            "meta_lookups_wo_permit")).get());
  }
} 
