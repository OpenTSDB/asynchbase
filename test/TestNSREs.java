/*
 * Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertSame;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.api.support.membermodification.MemberModifier;
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

  private static final byte[] COMMA = { ',' };
  private static final byte[] TIMESTAMP = "1234567890".getBytes();
  private static final byte[] INFO = getStatic("INFO");
  private static final byte[] REGIONINFO = getStatic("REGIONINFO");
  private static final byte[] SERVER = getStatic("SERVER");
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
    // Inject a timer that always fires away immediately.
    Whitebox.setInternalState(client, "timer", new FakeTimer());
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

  @Test
  public void simpleNSRE() throws Exception {
    // Attempt to get a row, get an NSRE back, do a META lookup,
    // find the new location, try again, succeed.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    // First access triggers an NSRE.
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        // We completely stub out the RegionClient, which normally does this.
        client.handleNSRE(get, get.getRegion().name(),
                          new NotServingRegionException("test", get));
        return null;
      }
    }).when(regionclient).sendRpc(get);
    // So now we do a meta lookup.
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(), anyBytes()))
      .thenAnswer(newDeferred(metaRow()));
    // This is the client where the region moved to after the NSRE.
    final RegionClient newregionclient = mock(RegionClient.class);
    final Method newClient = MemberMatcher.method(HBaseClient.class, "newClient");
    MemberModifier.stub(newClient).toReturn(newregionclient);
    when(newregionclient.isAlive()).thenReturn(true);
    // Answer the "exists" probe we use to check if the NSRE is still there.
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        exist.getDeferred().callback(true);
        return null;
      }
    }).when(newregionclient).sendRpc(any(GetRequest.class));
    // Answer our actual get request.
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(newregionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  @Test
  public void doubleNSRE() throws Exception {
    // Attempt to get a row, get an NSRE back, do a META lookup,
    // find the new location, get another NSRE that directs us to another
    // region, do another META lookup, try again, succeed.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    // First access triggers an NSRE.
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        // We completely stub out the RegionClient, which normally does this.
        client.handleNSRE(get, get.getRegion().name(),
                          new NotServingRegionException("test 1", get));
        return null;
      }
    }).when(regionclient).sendRpc(get);
    // So now we do a meta lookup.
    when(metaclient.isAlive()).thenReturn(true);  // [1]
    // The lookup tells us that now this key is in another daughter region.
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(), anyBytes()))
      .thenAnswer(newDeferred(metaRow(KEY, HBaseClient.EMPTY_ARRAY)));  // [2]
    // This is the client of the daughter region.
    final RegionClient newregionclient = mock(RegionClient.class);
    final Method newClient = MemberMatcher.method(HBaseClient.class, "newClient");
    MemberModifier.stub(newClient).toReturn(newregionclient);
    when(newregionclient.isAlive()).thenReturn(true);
    // Make the exist probe fail with another NSRE.
    doAnswer(new Answer() {
      private byte attempt = 0;
      @SuppressWarnings("fallthrough")
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        switch (attempt++) {
          case 0:  // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                              new NotServingRegionException("test 2", exist));
            break;
          case 1:  // Second attempt succeeds.
          case 2:  // First probe succeeds.
            exist.getDeferred().callback(true);
            break;
          default:
            throw new AssertionError("Shouldn't be here");
        }
        return null;
      }
    }).when(newregionclient).sendRpc(any(GetRequest.class));
    // Do a second meta lookup (behavior already set at [1]).
    // The second lookup returns the same daughter region (re-use [2]).

    // Answer our actual get request.
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        return null;
      }
    }).when(newregionclient).sendRpc(get);

    assertSame(row, client.get(get).joinUninterruptibly());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static <T> T getStatic(final String fieldname) {
    return Whitebox.getInternalState(HBaseClient.class, fieldname);
  }

  /**
   * Creates a fake {@code .META.} row.
   * The row contains a single entry for all keys of {@link #TABLE}.
   */
  private static ArrayList<KeyValue> metaRow() {
    return metaRow(HBaseClient.EMPTY_ARRAY, HBaseClient.EMPTY_ARRAY);
  }


  /**
   * Creates a fake {@code .META.} row.
   * The row contains a single entry for {@link #TABLE}.
   * @param start_key The start key of the region in this entry.
   * @param stop_key The stop key of the region in this entry.
   */
  private static ArrayList<KeyValue> metaRow(final byte[] start_key,
                                             final byte[] stop_key) {
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(2);
    final byte[] name = concat(TABLE, COMMA, start_key, COMMA, TIMESTAMP);
    final byte[] regioninfo = concat(
      new byte[] {
        0,                        // version
        (byte) stop_key.length,   // vint: stop key length
      },
      stop_key,
      new byte[] { 0 },           // boolean: not offline
      Bytes.fromLong(name.hashCode()),     // long: region ID (make it random)
      new byte[] { (byte) name.length },  // vint: region name length
      name,                       // region name
      new byte[] {
        0,                        // boolean: not splitting
        (byte) start_key.length,  // vint: start key length
      },
      start_key
    );
    row.add(new KeyValue(region.name(), INFO, REGIONINFO, regioninfo));
    row.add(new KeyValue(region.name(), INFO, SERVER, "localhost:54321".getBytes()));
    return row;
  }

  private static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(),
                          HBaseClient.EMPTY_ARRAY);
  }

  private static byte[] anyBytes() {
    return any(byte[].class);
  }

  /** Concatenates byte arrays together.  */
  private static byte[] concat(final byte[]... arrays) {
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
  private static <T> Answer<Deferred<T>> newDeferred(final T result) {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(result);
      }
    };
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

}
