/*
 * Copyright (c) 2011  StumbleUpon, Inc.  All rights reserved.
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

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertSame;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import static org.powermock.api.mockito.PowerMockito.mock;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
final class TestNSREs {

  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 'k', 'e', 'y' };
  private static final byte[] FAMILY = { 'f' };
  private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
  private static final RegionInfo meta = mkregion(".META.", ".META.,,1234567890");
  private static final RegionInfo region = mkregion("table", "table,,1234567890");
  private HBaseClient client = new HBaseClient("test-quorum-spec");
  /** Extracted from {@link #client}.  */
  private ConcurrentSkipListMap<byte[], RegionInfo> regions_cache;
  /** Extracted from {@link #client}.  */
  private ConcurrentHashMap<RegionInfo, RegionClient> region2client;
  /** Fake client supposedly connected to -ROOT-.  */
  private RegionClient rootclient = mock(RegionClient.class);
  /** Fake client supposedly connected to .META..  */
  private RegionClient metaclient = mock(RegionClient.class);
  /** Fake client supposedly connected to our fake test table.  */
  private RegionClient regionclient = mock(RegionClient.class);

  @Before
  public void before() throws Exception {
    Whitebox.setInternalState(client, "rootregion", rootclient);
    regions_cache = Whitebox.getInternalState(client, "regions_cache");
    region2client = Whitebox.getInternalState(client, "region2client");
    injectRegionInCache(meta, metaclient);
    injectRegionInCache(region, regionclient);
  }

  /**
   * Injects an entry in the local META cache of the client.
   */
  private void injectRegionInCache(final RegionInfo region,
                                   final RegionClient client) {
    regions_cache.put(region.name(), region);
    region2client.put(region, client);
    // We don't care about client2regions in these tests.
  }

  @Test
  public void simpleGet() throws Exception {
    // Just a simple test, no tricks, no problems, to verify we can
    // successfully mock out a complete get.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(),
                          HBaseClient.EMPTY_ARRAY);
  }

  private static byte[] anyBytes() {
    return any(byte[].class);
  }

  /** Creates a new Deferred that's already called back.  */
  private static <T> Answer<Deferred<T>> newDeferred(final T result) {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(result);
      }
    };
  }

}
