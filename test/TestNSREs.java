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

import org.jboss.netty.util.HashedWheelTimer;

import com.stumbleupon.async.Deferred;

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

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
final class TestNSREs extends BaseTestHBaseClient {
  @Test
  public void simpleGet() throws Exception {
    // Just a simple test, no tricks, no problems, to verify we can
    // successfully mock out a complete get.
    final GetRequest get = new GetRequest(TABLE, KEY);
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);

    when(regionclient.isAlive()).thenReturn(true);
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
      public Object answer(final InvocationOnMock invocation) {
        Object[] args = invocation.getArguments();
        final GetRequest exist = (GetRequest) args[0];
        exist.getDeferred().callback(true);
        return null;
      }
    }).when(newregionclient).sendRpc(any(GetRequest.class));
    // Answer our actual get request.
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    doAnswer(new Answer<Object>() {
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
    @SuppressWarnings("unchecked")
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

}
