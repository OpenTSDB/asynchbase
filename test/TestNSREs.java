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

import com.stumbleupon.async.Callback;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.junit.Assert.assertTrue;

import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

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
  private static final byte[] KEY2 = { 'b', 'r', 'o' };
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


  @Test
  public void alreadyNSREdRegion() throws Exception {
    // This test is to reproduce the retrial of RPC that was to a Region which
    // is known as NSRE at the time of initial sending. When a RPC is assigned
    // to a region that is already known as NSRE at the time to initial sent
    // (sendRpcToRegion), handleNSRE will be called, but at the same time in
    // the code a RetryRpc() callback is attached which will resend the RPC
    // even after the RPC succeeded when the NSRE is finished. This will be
    // mainly problematic with AtomicIncrementRequests.

    // mainGet is the RPC that will exhibit the above said behaviour of
    // resending it twice both the times returning success.
    final GetRequest mainGet = new GetRequest(TABLE, KEY);
    // triggerGet RPC is the RPC that will be used to trigger the NSRE for the
    // region, so the behaviour of RegionClient for this RPC would be to
    // return NSRE the first time and then for second time it will be called
    // back with result
    final GetRequest triggerGet = new GetRequest(TABLE, KEY);
    // Since the both the RPCs are same, the below is the result for them
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    // Always this region client will be always returns true
    when(regionclient.isAlive()).thenReturn(true);

    // The region client's behaviour for the triggerRpc, as mentioned above
    // this RPC is mainly used to invalidate the region cache of the client
    // so this will make the knownToBeNSREd to return true for the region
    doAnswer(new Answer() {
      private int attempt = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GetRequest triggerGet = (GetRequest) args[0];
        switch (attempt++) {
          case 0:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(triggerGet, triggerGet.getRegion().name(),
                new NotServingRegionException("Trigger NSRE", triggerGet));
            break;
          case 1:
            // trigger the callback with the result
            triggerGet.callback(row);
            break;
          default:
            throw new AssertionError("Can Never Happen");
        }
        return null;
      }
    }).when(regionclient).sendRpc(eq(triggerGet));


    // Now since the handleNSRE function, this will create a probe RPC and
    // will invalidate the region cache before retry of the probe RPC.  So we
    // will configure the meta_client to return this region for the look up
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(),
                                        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    // This will make sure that whenever region lookup happens the same
    // region client, on which we have stubbed the calls
    final Method newClient = MemberMatcher.method(HBaseClient.class,
                                                  "newClient");
    MemberModifier.stub(newClient).toReturn(regionclient);


    // Now we write the NSRE logic for the probe and hence we defined the
    // argument matcher for things other than the original get Request and
    // trigger get Request
    doAnswer(new Answer() {
      private int attempt = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        switch (attempt++) {
          case 0:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                new NotServingRegionException("exist 1", exist));
            break;
          case 1:
            // We stub out the RegionClient, which normally does this.
            client.handleNSRE(exist, exist.getRegion().name(),
                new NotServingRegionException("exist 2", exist));
            break;
          case 2:
            // NSRE cleared here, start the callback chain for exist RPC
            exist.callback(null);
            break;
          default:
            // This should never happen
            throw new AssertionError("Never Happens");
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat(new ArgumentMatcher<HBaseRpc>() {
      @Override
      public boolean matches(Object that) {
        return that != mainGet && that != triggerGet;
      }
    }));

    // Now the class stubbing for the mainGet RPC, whenever the call
    // is made for this RPC we just start the callback chain of the RPC.
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest getMain = (GetRequest) args[0];
        // stubbing out the entire decode method in the region client
        getMain.callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(eq(mainGet));

    // Now we swap the timer in client with our taskTimer which helps in making
    // the mainGet request during the period of wait for probe, in which case
    // the code path for the alreadyNSREd region will kick in.
    FakeTaskTimer taskTimer = new FakeTaskTimer();
    HashedWheelTimer originalTimer = Whitebox.getInternalState(client,
                                                               "timer");
    Whitebox.setInternalState(client, "timer", taskTimer);


    // Code for execution of the test start the execution of triggerGet, this
    // will create the probe RPC now the region will be in a NSREd state
    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(triggerGet);
    // now execute the mainGet
    Deferred<ArrayList<KeyValue>> mainRpcDeferred = client.get(mainGet);
    // now swap the FakeTaskTimer() with the previous Timer
    Whitebox.setInternalState(client, "timer", originalTimer);
    // now start the task that was paused
    boolean execute = taskTimer.continuePausedTask();
    assertTrue(execute);

    // Check the return result is same for trigger
    assertSame(row, triggerRpcDeferred.joinUninterruptibly());
    // For the trigger the RPC is sent only twice
    // once for the initial trigger for the NSRE
    // other is the final time when NSRE is cleared
    verify(regionclient, times(2)).sendRpc(triggerGet);


    // Check the return result is same for main
    assertSame(row, mainRpcDeferred.joinUninterruptibly());
    // Number of times this RPC is sent to regionServer
    verify(regionclient, times(1)).sendRpc(mainGet);
  }


  @Test
  public void probeRpcTooManyRetriesCallBack() throws Exception {
    // This test is demonstrate that when probe RPC expires with the number of
    // tries (it will happen in around approx 10 sec) its call back chain will
    // start executing, in which case all the RPCs in that are in the NSRE
    // list will be called sendRpcToRegion assuming that NSRE is cleared. But
    // in this path if the first few RPC's response returns with region being
    // NSREd before the other RPC's client.sendRpc is triggered (this is quite
    // possible if above 1000 RPCs are waiting on NSRE because these RPCs may
    // be waiting on meta region lookup and before this clears up, the first
    // few RPCs may have returned NSRE Exception from the region server) all
    // the remaining RPCs will go through knownToBeNSREd codepath in which
    // retryRpc callback will be added to these RPC's deferred in which case
    // all of them will be resent again after the clearing of NSRE. This can
    // be disastrous in the case of probe RPC getting expired a few number of
    // times.


    // Number of times the probe RPC should expire.
    final int probe_expire_count = 5;

    // dummyGet[]: These are the getRequests for which the RetryRpc()
    // callback will be attached
    final GetRequest[] dummyGet = {new GetRequest(TABLE, KEY),
                                   new GetRequest(TABLE, KEY),
                                   new GetRequest(TABLE, KEY)};

    // triggerGet: This RPC is used to trigger the initial NSRE and also used
    // to pause the probe execution by the FakeTaskTimer
    final GetRequest triggerGet = new GetRequest(TABLE, KEY);
    // Since the both the RPCs are same, the below is the result for them
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    // The Timers which will be swapped to simulate the pause for probe RPC
    final FakeTaskTimer taskTimer = new FakeTaskTimer();
    final HashedWheelTimer originalTimer = Whitebox.getInternalState(client,
                                                                     "timer");

    // Always this region client will be always returns true.
    when(regionclient.isAlive()).thenReturn(true);

    // Now since the handleNSRE function, this will create a probe RPC and
    // will invalidate the region cache before retry of the probe RPC.  So we
    // will configure the meta_client to return this region for the look up.
    when(metaclient.isAlive()).thenReturn(true);
    when(metaclient.getClosestRowBefore(eq(meta), anyBytes(), anyBytes(),
                                        anyBytes()))
        .thenAnswer(newDeferred(metaRow()));
    // This will make sure that whenever region lookup happens the same
    // region client, on which we have stubbed the calls.
    final Method newClient = MemberMatcher.method(HBaseClient.class,
                                                  "newClient");
    MemberModifier.stub(newClient).toReturn(regionclient);

    // behaviour for the triggerGet
    doAnswer(new Answer() {
      private int attempt = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        GetRequest triggerGet = (GetRequest) args[0];
        attempt++;
        if (attempt <= probe_expire_count + 1) {
          // we will swap the internal timer with taskTimer which will pause
          // the execution of the probe RPC and thus will allow calling the
          // sendRpcToRegion for the dummyRpcs.
          Whitebox.setInternalState(client, "timer", taskTimer);
          // We stub out the RegionClient, which normally does this.
          client.handleNSRE(triggerGet, triggerGet.getRegion().name(),
              new NotServingRegionException("Trigger NSRE", triggerGet));
        } else if (attempt == probe_expire_count + 2) {
          // this is the case where NSRE is cleared
          // trigger the callback with the result
          triggerGet.callback(row);
        } else {
            throw new AssertionError("Can Never Happen");
        }
        return null;
      }
    }).when(regionclient).sendRpc(eq(triggerGet));


    // Now we write the NSRE logic for the probe and hence we defined the
    // argument matcher for things other than the original GetRequest and
    // trigger GetRequest.
    doAnswer(new Answer() {
      private int attempt = 0;
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        attempt++;
        if (attempt < (probe_expire_count * 10 + 4)) {
          // We stub out the RegionClient, which normally does this.
          client.handleNSRE(exist, exist.getRegion().name(),
            new NotServingRegionException("exist 1", exist));
        } else if (attempt == (probe_expire_count * 10 + 4)) {
          // NSRE on the region is cleared here
          exist.callback(null);
        } else {
          // This should never happen
          throw new AssertionError("Never Happens");
        }
        return null;
      }
    }).when(regionclient).sendRpc(argThat(new ArgumentMatcher<HBaseRpc>() {
      @Override
      public boolean matches(Object that) {
        return that != dummyGet[0] && that != triggerGet
          && that != dummyGet[1] && that != dummyGet[2];
      }
    }));



    // Now the class stubbing for the dummyGet RPC, whenever the call
    // is made for this RPC we just start the callback chain of the RPC.
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        final GetRequest dummyGet = (GetRequest) args[0];
        // stubbing out the entire decode method in the region client
        dummyGet.callback(row);
        return null;
      }
    }).when(regionclient).sendRpc(argThat(new ArgumentMatcher<HBaseRpc>() {
      @Override
      public boolean matches(Object that) {
        return (that == dummyGet[0]
                || that == dummyGet[1]
                || that == dummyGet[2]);
      }
    }));


    // Main test code starts here
    // Start the execution of triggerGet, this will create the probe RPC
    // now the region will be in a NSREd state
    Deferred<ArrayList<KeyValue>> triggerRpcDeferred = client.get(triggerGet);

    // execute the dummyRpcs now
    final Deferred<ArrayList<KeyValue>>[] dummyRpcDeferred = new Deferred[]{
        client.get(dummyGet[0]),
        client.get(dummyGet[1]),
        client.get(dummyGet[2])};

    Whitebox.setInternalState(client, "timer", originalTimer);
    int taskTimerPauses = 0;
    while(taskTimer.continuePausedTask()) {
      taskTimerPauses++;
      Whitebox.setInternalState(client, "timer", originalTimer);
    }

    // See the mock of regionclient.sendRpc method for this RPC for the
    // explanation for this
    verify(regionclient, times(probe_expire_count + 2)).sendRpc(triggerGet);
    // TaskTimer will be paused probe_expire_count + 1 times
    assertEquals(probe_expire_count + 1, taskTimerPauses);

    // Output of the triggerRpc
    assertSame(row, triggerRpcDeferred.joinUninterruptibly());

    for (int i = 0; i < 3; i++) {
      // Check the output is same
      assertSame(row, dummyRpcDeferred[i].join());
      // Check the number of times RPC is sent to region client this will be
      // equals to probe_expire_count for each RetryRpc during failure
      //  + 1 after the NSRE is cleared
      verify(regionclient, times(1)).sendRpc(dummyGet[i]);
    }
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

    private TimerTask newPausedTask = null;
    private TimerTask pausedTask = null;

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

}
