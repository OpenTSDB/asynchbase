/*
 * Copyright (C) 2015-2016  The Async HBase Authors.  All rights reserved.
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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hbase.async.generated.ClientPB.MultiRequest;
import org.hbase.async.generated.ClientPB.MultiResponse;
import org.hbase.async.generated.ClientPB.RegionAction;
import org.hbase.async.generated.ClientPB.RegionActionResult;
import org.hbase.async.generated.ClientPB.ResultOrException;
import org.hbase.async.generated.ClientPB.RegionActionResult.Builder;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ RowLock.class })
public class TestMultiAction extends BaseTestHBaseClient {
  protected static final RegionInfo region2 = 
      mkregion("table", "table,A,1234567890");
  protected static final RegionInfo region3 = 
      mkregion("table", "table,B,1234567890");
  
  @Test
  public void multiComparator() throws Exception {
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.region = region;
    
    // same
    assertEquals(0, MultiAction.MULTI_CMP.compare(put1, put2));
    
    // diff region on 2
    put2.region = mkregion("table", "table,a,1234567890");
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) < 0);
    
    // diff region on 1
    put1.region = mkregion("table", "table,a,1234567890");
    put2.region = region;
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) > 0);
    
    // diff table on 2
    put1.region = region;
    put2.region = mkregion("table", "atable,,1234567890");
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) > 0);
    
    // diff table on 1
    put1.region = mkregion("table", "atable,,1234567890");
    put2.region = region;
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) < 0);
    
    // different RPC types
    DeleteRequest delete1 = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
    delete1.region = region;
    put1.region = region;
    put2.region = region;
    assertTrue(MultiAction.MULTI_CMP.compare(delete1, put2) < 0);
    assertTrue(MultiAction.MULTI_CMP.compare(put1, delete1) > 0);
    
    // different keys
    put1 = new PutRequest(TABLE, KEY2, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) > 0);
    assertTrue(MultiAction.MULTI_CMP.compare(put2, put1) < 0);
    
    // different families
    put1 = new PutRequest(TABLE, KEY, "t".getBytes(), QUALIFIER, VALUE);
    put1.region = region;
    assertTrue(MultiAction.MULTI_CMP.compare(put1, put2) > 0);
    assertTrue(MultiAction.MULTI_CMP.compare(put2, put1) < 0);
    
    // apparently we don't care about qualifiers!
    put1 = new PutRequest(TABLE, KEY, FAMILY, "hobbes".getBytes(), VALUE);
    put1.region = region;
    assertEquals(0, MultiAction.MULTI_CMP.compare(put1, put2));
  }

  @Test
  public void multiComparatorErrors() throws Exception {
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    
    // missing region
    try {
      MultiAction.MULTI_CMP.compare(put1, put2);
      fail("Excepted an NPE");
    } catch (NullPointerException e) { }
    
    // null rpcs
    try {
      MultiAction.MULTI_CMP.compare(put1, null);
      fail("Excepted an NPE");
    } catch (NullPointerException e) { }
    
    try {
      MultiAction.MULTI_CMP.compare(null, put1);
      fail("Excepted an NPE");
    } catch (NullPointerException e) { }
    
    // ctors for the RPCs *should* prevent null keys, codes and families
  }
  
  @Test
  public void add() throws Exception {
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
    delete.region = region;
    AppendRequest append = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append.region = region;
    
    // same type, different qualifiers
    MultiAction multi = new MultiAction();
    assertEquals(0, multi.size()); // make sure it's empty
    
    multi.add(put1);
    multi.add(put2);
    assertEquals(2, multi.size());
    
    // all kinds of types
    multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    multi.add(delete);
    multi.add(append);
    assertEquals(4, multi.size());
    
    // double up on the same RPC as this is allowed
    multi = new MultiAction();
    multi.add(put1);
    multi.add(put1);
    assertEquals(2, multi.size());
  }
  
  @Test
  public void addErrors() throws Exception {
    PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    
    // null argument
    MultiAction multi = new MultiAction();
    try {
      multi.add(null);
      fail("Excepted an NPE");
    } catch (NullPointerException e) { }
    
    // missing the region
    multi = new MultiAction();
    try {
      multi.add(put);
      fail("Excepted an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no region name
    multi.region = new RegionInfo(TABLE, null, HBaseClient.EMPTY_ARRAY);
    multi = new MultiAction();
    try {
      multi.add(put);
      fail("Excepted an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // locks SHALL NOT PASS!
    final RowLock lock = mock(RowLock.class);
    put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, 42, lock);
    put.region = region;
    try {
      multi.add(put);
      fail("Excepted an AssertionError");
    } catch (AssertionError e) { }
  }

  // NOTE: The following are tests for HBase 0.96 and up
  
  @Test
  public void serdesOrdering() throws Exception {
    PutRequest put1 = new PutRequest(TABLE, concat(KEY, new byte[] { 2 }), 
        FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    PutRequest put3 = new PutRequest(TABLE, concat(KEY, new byte[] { 2 }), 
        FAMILY, "myqual".getBytes(), VALUE);
    put3.region = region2;
    PutRequest put4 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put4.region = region2;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    multi.add(put4);
    
    ChannelBuffer buffer = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buffer.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    HBaseRpc.readProtoBufVarint(buffer);
    byte[] bytes = new byte[buffer.writerIndex() - buffer.readerIndex()];
    buffer.readBytes(bytes);
    MultiRequest parsed = MultiRequest.parseFrom(bytes);
    assertEquals(2, parsed.getRegionActionCount());
    
    RegionAction actions = parsed.getRegionAction(0);
    assertEquals(2, actions.getActionCount());
    assertArrayEquals(region.name(), actions.getRegion().getValue().toByteArray());
    assertArrayEquals(KEY, actions.getAction(0).getMutation().getRow().toByteArray());
    assertArrayEquals(concat(KEY, new byte[] { 2 }), 
        actions.getAction(1).getMutation().getRow().toByteArray());
    
    actions = parsed.getRegionAction(1);
    assertEquals(2, actions.getActionCount());
    assertArrayEquals(region2.name(), actions.getRegion().getValue().toByteArray());
    assertArrayEquals(KEY, actions.getAction(0).getMutation().getRow().toByteArray());
    assertArrayEquals(concat(KEY, new byte[] { 2 }), 
        actions.getAction(1).getMutation().getRow().toByteArray());
  }
  
  @Test
  public void deserializePuts() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeDeletes() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));

    DeleteRequest delete1 = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
    delete1.region = region;
    DeleteRequest delete2 = new DeleteRequest(TABLE, KEY, FAMILY, "myqual".getBytes());
    delete2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(delete1);
    multi.add(delete2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeOneGoodOneBad() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateException(new RuntimeException("Boo!"), 1));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) instanceof RuntimeException);
    final RuntimeException e = (RuntimeException)decoded.result(1);
    assertTrue(e.getMessage().contains("Boo!"));
  }
  
  @Test
  public void deserializeOneGoodOneBadFlip() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateException(new RuntimeException("Boo!"), 0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) instanceof RuntimeException);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    final RuntimeException e = (RuntimeException)decoded.result(0);
    assertTrue(e.getMessage().contains("Boo!"));
  }
  
  @Test
  public void deserializeOneGoodOneNSRE() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    results.add(PBufResponses.generateException(
        new NotServingRegionException("MyNSRE!", put2), 1));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) instanceof RuntimeException);
    final RuntimeException e = (RuntimeException)decoded.result(1);
    assertTrue(e.getMessage().contains("MyNSRE!"));
  }
  
  /** Won't happen with puts but we run it to prove we don't care about the
   *  response for puts */
  @Test
  public void deserializePutsWithValues() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    
    results.add(PBufResponses.kvToROE(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE), 0));
    results.add(PBufResponses.kvToROE(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE), 1));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  /** This shouldn't happen. It means we had n RPC in our multi action but 
      somehow HBase returned n+ responses. */
  @Test (expected = IndexOutOfBoundsException.class)
  public void deserializeMissingRPC() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);

    multi.deserialize(PBufResponses.encodeResponse(
        PBufResponses.generateMultiActionResponse(results)), 0);
  }
  
  /** This shouldn't happen either. HBase should respond to ALL RPCs */
  @Test (expected = InvalidResponseException.class)
  public void deserializeExtraRpc() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    PutRequest put3 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put3.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    
    multi.deserialize(
        PBufResponses.encodeResponse(
            PBufResponses.generateMultiActionResponse(results)), 0);
  }

  /** This shouldn't happen either. It would mean HBase goofed up the
   * indices on the way back to us. */
  @Test (expected = InvalidResponseException.class)
  public void deserializeBadHBaseIndices() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(0));
 
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    multi.deserialize(
        PBufResponses.encodeResponse(
            PBufResponses.generateMultiActionResponse(results)), 0);
  }

  @Test
  public void deserializeAppendsNoResponseExpected() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    append2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  /** Also shouldn't happen since HBase returns a NULL for successful Append
   * RPCs instead of an empty result like it does for Puts. If the behavior 
   * changes down the line, we'll be ready! */
  @Test
  public void deserializeAppendsNoResponseExpected_ButItDid() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    append2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeAppendsNoResponseButGotOne() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.kvToROE(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE), 0));
    results.add(PBufResponses.kvToROE(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE), 1));

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    append2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeAppendsWithResponse() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    results.add(PBufResponses.kvsToROE(kvs, 0));
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs1 = new ArrayList<KeyValue>(1);
    kvs1.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
    results.add(PBufResponses.kvsToROE(kvs1, 1));

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    append1.returnResult(true);
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    append2.region = region;
    append2.returnResult(true);
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) instanceof KeyValue);
    assertEquals(1, ((KeyValue)decoded.result(0)).timestamp());
    assertTrue(decoded.result(1) instanceof KeyValue);
    assertEquals(2, ((KeyValue)decoded.result(1)).timestamp());
  }
  
  /** If we ask for data, HBase should give us data, therefore this shouldn't 
   * happen ever. But if it does, we assume the RPCs were successful and we 
   * return an empty array. */
  @Test
  public void deserializeAppendsAskedForResponseButGotZilch() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    append1.returnResult(true);
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    append2.region = region;
    append2.returnResult(true);
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializAppendsMixedResponse() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    results.add(PBufResponses.kvsToROE(kvs, 0));
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs2 = new ArrayList<KeyValue>(1);
    kvs2.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
    results.add(PBufResponses.kvsToROE(kvs2, 2));

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    append1.returnResult(true);
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append2.region = region;
    AppendRequest append3 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append3.region = region;
    append3.returnResult(true);
    
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    multi.add(append3);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(3, decoded.size());
    assertTrue(decoded.result(0) instanceof KeyValue);
    assertEquals(1, ((KeyValue)decoded.result(0)).timestamp());
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(2) instanceof KeyValue);
    assertEquals(2, ((KeyValue)decoded.result(2)).timestamp());
  }
  
  @Test
  public void deserializAppendsAndException() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    results.add(PBufResponses.kvsToROE(kvs, 0));
    
    // in this result, there will be exception
    results.add(PBufResponses.generateException(new RuntimeException("Boo!"), 1));
    
    // in each result, there will be one cell data responsed
    final List<KeyValue> kvs2 = new ArrayList<KeyValue>(1);
    kvs2.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
    results.add(PBufResponses.kvsToROE(kvs2, 2));

    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    append1.returnResult(true);
    AppendRequest append2 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append2.region = region;
    append2.returnResult(true);
    AppendRequest append3 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append3.region = region;
    append3.returnResult(true);
    
    MultiAction multi = new MultiAction();
    multi.add(append1);
    multi.add(append2);
    multi.add(append3);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(3, decoded.size());
    assertTrue(decoded.result(0) instanceof KeyValue);
    assertEquals(1, ((KeyValue)decoded.result(0)).timestamp());
    assertTrue(decoded.result(1) instanceof RuntimeException);
    final RuntimeException e = (RuntimeException)decoded.result(1);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(2) instanceof KeyValue);
    assertEquals(2, ((KeyValue)decoded.result(2)).timestamp());
  }
  
  @Test
  public void deserializePutsAndAppendsNoResponse() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(2));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(append1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(3, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(2) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializePutsAndAppendsResponse() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.kvToROE(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE), 1));
    results.add(PBufResponses.generateEmptyResult(2));

    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    AppendRequest append1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    append1.region = region;
    append1.returnResult(true);
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(append1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    assertEquals(3, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) instanceof KeyValue);
    assertEquals(1, ((KeyValue)decoded.result(1)).timestamp());
    assertTrue(decoded.result(2) == MultiAction.SUCCESS);
  }

  @Test
  public void deserializePutsAndAppendsTrailingAppends() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(1);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(2));
    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
    
    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
    rars.add(rar1);
    MultiAction multi = new MultiAction();
    
    PutRequest rpc1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    rpc1.region = region;
    AppendRequest rpc2 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    rpc2.region = region;
    PutRequest rpc3 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    rpc3.region = region;
    AppendRequest rpc4 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    rpc4.region = region;
    multi.add(rpc1);
    multi.add(rpc2);
    multi.add(rpc3);
    multi.add(rpc4);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
    assertEquals(4, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(3) == MultiAction.SUCCESS);
    assertTrue(decoded.result(3) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeActionException() throws Exception {
    final Builder rar = RegionActionResult.newBuilder();
    rar.setException(PBufResponses.buildException(new RuntimeException("Boo!")));
    MultiResponse response =  MultiResponse.newBuilder()
        .addRegionActionResult(rar.build()).build();
    
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, "myqual".getBytes(), VALUE);
    put2.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(response), 0);
    assertEquals(2, decoded.size());
    assertTrue(decoded.result(0) instanceof RuntimeException);
    RuntimeException e = (RuntimeException)decoded.result(0);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(1) instanceof RuntimeException);
    e = (RuntimeException)decoded.result(1);
    assertTrue(e.getMessage().contains("Boo!"));
  }

  @Test
  public void deserializeMultipleActions() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
    results.clear();

    results.add(PBufResponses.generateEmptyResult(2));
    results.add(PBufResponses.generateEmptyResult(3));
    final RegionActionResult rar2 = PBufResponses.generateRegionActionResult(results);
    
    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
    rars.add(rar1);
    rars.add(rar2);
    
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.region = region;
    PutRequest put3 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put3.region = region;
    PutRequest put4 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put4.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    multi.add(put4);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
    assertEquals(4, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(2) == MultiAction.SUCCESS);
    assertTrue(decoded.result(3) == MultiAction.SUCCESS);
  }
  
  @Test
  public void deserializeMultipleActionsOneFailed() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
    final Builder rar2 = RegionActionResult.newBuilder();
      rar2.setException(PBufResponses.buildException(new RuntimeException("Boo!")));
    
    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
    rars.add(rar1);
    rars.add(rar2.build());
    
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.region = region;
    PutRequest put3 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put3.region = region;
    PutRequest put4 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put4.region = region;
    MultiAction multi = new MultiAction();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    multi.add(put4);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
    assertEquals(4, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(2) instanceof RuntimeException);
    RuntimeException e = (RuntimeException)decoded.result(2);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(3) instanceof RuntimeException);
    e = (RuntimeException)decoded.result(3);
    assertTrue(e.getMessage().contains("Boo!"));
  }

  @Test
  public void deserializeMultiRegionOneFailed() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
    final Builder rar2 = RegionActionResult.newBuilder();
      rar2.setException(PBufResponses.buildException(new RuntimeException("Boo!")));
    
    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
    rars.add(rar1);
    rars.add(rar2.build());
    
    MultiAction multi = new MultiAction();
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region2;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.region = region;
    PutRequest put3 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put3.region = region2;
    PutRequest put4 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put4.region = region;
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    multi.add(put4);
    Collections.sort(multi.batch(), MultiAction.SORT_BY_REGION_AND_KEY);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
    assertEquals(4, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
    assertTrue(decoded.result(2) instanceof RuntimeException);
    RuntimeException e = (RuntimeException)decoded.result(2);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(3) instanceof RuntimeException);
    e = (RuntimeException)decoded.result(3);
    assertTrue(e.getMessage().contains("Boo!"));
  }
  
  @Test
  public void deserializeMultiRegionTwoFailed() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(1);
    results.add(PBufResponses.generateEmptyResult(0));
    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
    final Builder rar2 = RegionActionResult.newBuilder();
    rar2.setException(PBufResponses.buildException(new RuntimeException("Boo!")));
    results.clear();
    results.add(PBufResponses.generateEmptyResult(3));
    final RegionActionResult rar3 = PBufResponses.generateRegionActionResult(results);
    
    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
    rars.add(rar1);
    rars.add(rar2.build());
    rars.add(rar3);
    MultiAction multi = new MultiAction();
    
    PutRequest put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put1.region = region;
    PutRequest put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2.region = region2;
    PutRequest put3 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put3.region = region2;
    PutRequest put4 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put4.region = region3;
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    multi.add(put4);
    Collections.sort(multi.batch(), MultiAction.SORT_BY_REGION_AND_KEY);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
    assertEquals(4, decoded.size());
    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
    assertTrue(decoded.result(1) instanceof RuntimeException);
    RuntimeException e = (RuntimeException)decoded.result(1);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(2) instanceof RuntimeException);
    e = (RuntimeException)decoded.result(2);
    assertTrue(e.getMessage().contains("Boo!"));
    assertTrue(decoded.result(3) == MultiAction.SUCCESS);
  }
  
  // TODO - enable and fix this to test cell sizing the code works on a split
  // minicluster.
  // TODO - test deserializing with a cell size.
//  @Test
//  public void deserializeMultipleActionsAppendsWithResponses() throws Exception {
//    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
//    final List<KeyValue> kvs = new ArrayList<KeyValue>(2);
//    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
//    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
//    results.add(PBufResponses.kvsToROE(kvs, 0));
//    results.add(PBufResponses.kvsToROE(kvs, 1));
//    
//    final RegionActionResult rar1 = PBufResponses.generateRegionActionResult(results);
//    results.clear();
//
//    final List<KeyValue> kvs2 = new ArrayList<KeyValue>(2);
//    kvs2.add(new KeyValue(KEY, FAMILY, QUALIFIER, 3, VALUE));
//    kvs2.add(new KeyValue(KEY, FAMILY, QUALIFIER, 4, VALUE));
//    results.add(PBufResponses.kvsToROE(kvs2, 2));
//    results.add(PBufResponses.kvsToROE(kvs2, 3));
//    final RegionActionResult rar2 = PBufResponses.generateRegionActionResult(results);
//    
//    final List<RegionActionResult> rars = new ArrayList<RegionActionResult>(2);
//    rars.add(rar1);
//    rars.add(rar2);
//    
//    AppendRequest rpc1 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
//    rpc1.region = region;
//    rpc1.returnResult(true);
//    AppendRequest rpc2 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
//    rpc2.region = region;
//    rpc2.returnResult(true);
//    AppendRequest rpc3 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
//    rpc3.region = region;
//    rpc3.returnResult(true);
//    AppendRequest rpc4 = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
//    rpc4.region = region;
//    rpc4.returnResult(true);
//    MultiAction multi = new MultiAction();
//    multi.add(rpc1);
//    multi.add(rpc2);
//    multi.add(rpc3);
//    multi.add(rpc4);
//    
//    final MultiAction.Response decoded = 
//        (MultiAction.Response)multi.deserialize(
//            PBufResponses.encodeResponse(
//                PBufResponses.generateMultiActionResponseFromRars(rars)), 0);
//    assertEquals(4, decoded.size());
//    assertTrue(decoded.result(0) == MultiAction.SUCCESS);
//    assertTrue(decoded.result(1) == MultiAction.SUCCESS);
//    assertTrue(decoded.result(2) == MultiAction.SUCCESS);
//    assertTrue(decoded.result(3) == MultiAction.SUCCESS);
//  }
  
  @Test
  public void deserializeGetsWithResponse() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    final List<KeyValue> kvs = new ArrayList<KeyValue>(2);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
    results.add(PBufResponses.kvsToROE(kvs, 0));
    results.add(PBufResponses.kvsToROE(kvs, 1));

    GetRequest get1 = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get1.region = region;
    GetRequest get2 = new GetRequest(TABLE, KEY, FAMILY, "myqual".getBytes());
    get2.region = region;
    
    MultiAction multi = new MultiAction();
    multi.add(get1);
    multi.add(get2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    
    assertEquals(2, decoded.size());
    
    Object result1 = decoded.result(0);
    assertTrue(result1 instanceof ArrayList<?>);
    ArrayList<KeyValue> result1_cells = (ArrayList<KeyValue>)(result1);
    assertEquals(2, result1_cells.size());
    assertEquals(1, result1_cells.get(0).timestamp());
    assertEquals(2, result1_cells.get(1).timestamp());
    
    Object result2 = decoded.result(1);
    assertTrue(result2 instanceof ArrayList<?>);
    ArrayList<KeyValue> result2_cells = (ArrayList<KeyValue>)(result2);
    assertEquals(2, result2_cells.size());
    assertEquals(1, result2_cells.get(0).timestamp());
    assertEquals(2, result2_cells.get(1).timestamp());
  }
  
  @Test
  public void deserializeGetsWithResponseException() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    
    // the first result with a cell data
    final List<KeyValue> kvs = new ArrayList<KeyValue>(2);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    results.add(PBufResponses.kvsToROE(kvs, 0));
    
    // the second result with an runtime exception
    results.add(PBufResponses.generateException(new RuntimeException("foo!"), 1));

    GetRequest get1 = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get1.region = region;
    GetRequest get2 = new GetRequest(TABLE, KEY, FAMILY, "myqual".getBytes());
    get2.region = region;
    
    MultiAction multi = new MultiAction();
    multi.add(get1);
    multi.add(get2);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    
    assertEquals(2, decoded.size());
    
    Object result1 = decoded.result(0);
    assertTrue(result1 instanceof ArrayList<?>);
    ArrayList<KeyValue> result1_cells = (ArrayList<KeyValue>)(result1);
    assertEquals(1, result1_cells.size());
    assertEquals(1, result1_cells.get(0).timestamp());
    
    Object result2 = decoded.result(1);
    assertTrue(result2 instanceof HBaseException);
  }
  
  @Test
  public void deserializeGetsWithAssociatedCells() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateAssociatedROE(1, 0));
    results.add(PBufResponses.generateAssociatedROE(1, 1));

    GetRequest get1 = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get1.region = region;
    GetRequest get2 = new GetRequest(TABLE, KEY, FAMILY, "myqual".getBytes());
    get2.region = region;
    
    MultiAction multi = new MultiAction();
    multi.add(get1);
    multi.add(get2);
    
    // two associated cells following the PB response
    final List<KeyValue> kvs = new ArrayList<KeyValue>(2);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 2, VALUE));
    AbstractMap.SimpleEntry<ChannelBuffer, Integer> cbuf_cells = PBufResponses.encodeResponseWithAssocaitedCells(
        PBufResponses.generateMultiActionResponse(results), kvs);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(cbuf_cells.getKey(), cbuf_cells.getValue().intValue());
    
    assertEquals(2, decoded.size());
    
    Object result1 = decoded.result(0);
    assertTrue(result1 instanceof ArrayList<?>);
    ArrayList<KeyValue> result1_cells = (ArrayList<KeyValue>)(result1);
    assertEquals(1, result1_cells.size());
    assertEquals(1, result1_cells.get(0).timestamp());
    
    Object result2 = decoded.result(1);
    assertTrue(result2 instanceof ArrayList<?>);
    ArrayList<KeyValue> result2_cells = (ArrayList<KeyValue>)(result2);
    assertEquals(1, result2_cells.size());
    assertEquals(2, result2_cells.get(0).timestamp());
  }
  
  @Test
  public void deserializeGetsWithAssociatedCellsAndException() throws Exception {
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateAssociatedROE(1, 0));
    results.add(PBufResponses.generateException(new RuntimeException("foo!"), 1));

    GetRequest get1 = new GetRequest(TABLE, KEY, FAMILY, QUALIFIER);
    get1.region = region;
    GetRequest get2 = new GetRequest(TABLE, KEY, FAMILY, "myqual".getBytes());
    get2.region = region;
    
    MultiAction multi = new MultiAction();
    multi.add(get1);
    multi.add(get2);
    
    // two associated cells following the PB response
    final List<KeyValue> kvs = new ArrayList<KeyValue>(1);
    kvs.add(new KeyValue(KEY, FAMILY, QUALIFIER, 1, VALUE));
    AbstractMap.SimpleEntry<ChannelBuffer, Integer> cbuf_cells = PBufResponses.encodeResponseWithAssocaitedCells(
        PBufResponses.generateMultiActionResponse(results), kvs);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response)multi.deserialize(cbuf_cells.getKey(), cbuf_cells.getValue().intValue());
    
    assertEquals(2, decoded.size());
    
    Object result1 = decoded.result(0);
    assertTrue(result1 instanceof ArrayList<?>);
    ArrayList<KeyValue> result1_cells = (ArrayList<KeyValue>)(result1);
    assertEquals(1, result1_cells.size());
    assertEquals(1, result1_cells.get(0).timestamp());
    
    Object result2 = decoded.result(1);
    assertTrue(result2 instanceof HBaseException);
  }
  
  @Test
  public void simpleMultiGets() throws Exception {
    final List<GetRequest> gets = new ArrayList<GetRequest>();
    final GetRequest get = new GetRequest(TABLE, KEY);
    gets.add(get);
    final GetRequest get2 = new GetRequest(TABLE, KEY2);
    gets.add(get2);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);
    
    final KeyValue KV2 = new KeyValue(KEY2, FAMILY, QUALIFIER, VALUE);
    final ArrayList<KeyValue> row2 = new ArrayList<KeyValue>(1);
    row2.add(KV2);

    when(regionclient.isAlive()).thenReturn(true);
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        get2.getDeferred().callback(row2);
        return null;
      }
    }).when(regionclient).sendRpc(any(MultiAction.class));

    List<GetResultOrException> result = client.get(gets).joinUninterruptibly();
    assertSame(row, result.get(0).getCells());
    assertSame(row2, result.get(1).getCells());
  }
  
  @Test
  public void simpleMultiGetsException() throws Exception {
    final List<GetRequest> gets = new ArrayList<GetRequest>();
    final GetRequest get = new GetRequest(TABLE, KEY);
    gets.add(get);
    final GetRequest get2 = new GetRequest(TABLE, KEY2);
    gets.add(get2);
    
    final ArrayList<KeyValue> row = new ArrayList<KeyValue>(1);
    row.add(KV);
    
    final KeyValue KV2 = new KeyValue(KEY2, FAMILY, QUALIFIER, VALUE);
    final ArrayList<KeyValue> row2 = new ArrayList<KeyValue>(1);
    row2.add(KV2);

    when(regionclient.isAlive()).thenReturn(true);
    doAnswer(new Answer() {
      public Object answer(final InvocationOnMock invocation) {
        get.getDeferred().callback(row);
        get2.getDeferred().callback(new NoSuchColumnFamilyException("boo", get2));
        return null;
      }
    }).when(regionclient).sendRpc(any(MultiAction.class));

    List<GetResultOrException> result = client.get(gets).joinUninterruptibly();
    assertSame(row, result.get(0).getCells());
    assertTrue(null == result.get(0).getException());
    assertTrue(null == result.get(1).getCells());
    assertTrue(result.get(1).getException() != null);
  }
  
}
