/*
 * Copyright (C) 2020  The Async HBase Authors.  All rights reserved.
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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.hbase.async.generated.ClientPB.MultiRequest;
import org.hbase.async.generated.ClientPB.MultiResponse;
import org.hbase.async.generated.ClientPB.MutationProto.MutationType;
import org.hbase.async.generated.ClientPB.ResultOrException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.TimeoutException;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class, Channel.class })
public class TestMultiActionAppend {
  
  protected static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  protected static final byte[] KEY = { 'k', 'e', 'y' };
  protected static final byte[] KEY2 = { 'k', 'e', 'y', '2' };
  protected static final byte[] FAMILY = { 'f' };
  protected static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  protected static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  protected static final byte[] EMPTY_ARRAY = new byte[0];
  protected static final RegionInfo TABLE_REGION = mkregion("table", "table,,1234567890");
  protected static final RegionInfo TABLE_REGION2 = mkregion("table", "table,key2,1234567891");
  protected static final RegionInfo PRE94_META = mkregion(".META.", ".META.,,1234567890");
  
  private HBaseClient client;
  private RegionClient rc;
  private PutRequest put1;
  private PutRequest put2;
  private PutRequest put3;
  private PutRequest put4;
  private PutRequest put5;
  private PutRequest put6;
  private DeleteRequest delete;
  private AppendRequest append;
  private Channel channel;
  
  @Before
  public void beforeLocal() {
    put1 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    put2 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, new byte[] { 'v', '2' });
    put3 = new PutRequest(TABLE, KEY, FAMILY, new byte[] { 'q', '2' }, VALUE);
    put4 = new PutRequest(TABLE, KEY, new byte[] { 'f', '2' }, QUALIFIER, VALUE);
    put5 = new PutRequest(TABLE, new byte[] { 'k', '2' }, FAMILY, QUALIFIER, VALUE);
    put6 = new PutRequest(new byte[] { 't', '2' }, KEY, FAMILY, QUALIFIER, VALUE);
    delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
    append = new AppendRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE);
    
    put1.region = TABLE_REGION;
    put2.region = TABLE_REGION;
    put3.region = TABLE_REGION;
    put4.region = TABLE_REGION;
    put5.region = TABLE_REGION;
    put6.region = PRE94_META;
    delete.region = TABLE_REGION;
    append.region = TABLE_REGION;
    
    client = mock(HBaseClient.class);
    Config config = new Config();
    when(client.getConfig()).thenReturn(config);
    rc = new RegionClient(client, null);
    channel = mock(Channel.class);
    when(channel.getLocalAddress()).thenReturn(mock(SocketAddress.class));
    Whitebox.setInternalState(rc, "chan", channel);
  }
  
  @Test
  public void serialize1Put() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, multi.batch().size());
    assertSame(put1, multi.batch().get(0));
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(1, mr.getRegionAction(0).getActionCount());
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, 
        mr.getRegionAction(0).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void response1Put() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    Deferred<Object> p1 = put1.getDeferred();
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);

    rc.addMultiActionCallbacks(multi);
    
    // NOTE: This couldn't really happen if they're part of the same region.
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    multi.callback(decoded);
    
    assertSame(MultiAction.SUCCESS, p1.join(1));
  }
  
  @Test
  public void serialize2PutsSameColumn() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, multi.batch().size());
    assertNotSame(put1, multi.batch().get(0));
    assertNotSame(put2, multi.batch().get(0));
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(1, mr.getRegionAction(0).getActionCount());
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void response2PutsSameColumn() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    rc.addMultiActionCallbacks(multi);
    
    // NOTE: This couldn't really happen if they're part of the same region.
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    multi.callback(decoded);
    
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
  }
  
  @Test
  public void serialize3PutsSameColumn() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    PutRequest put7 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, new byte[] { 'v', '3' });
    put7.setRegion(TABLE_REGION);
    multi.add(put7);
    
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, multi.batch().size());
    assertNotSame(put1, multi.batch().get(0));
    assertNotSame(put2, multi.batch().get(0));
    assertNotSame(put3, multi.batch().get(0));
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(1, mr.getRegionAction(0).getActionCount());
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value(), put7.value()), 
        mr.getRegionAction(0).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void response3PutsSameColumn() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    PutRequest put7 = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, new byte[] { 'v', '3' });
    put7.setRegion(TABLE_REGION);
    multi.add(put7);
    
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    rc.addMultiActionCallbacks(multi);
    
    // NOTE: This couldn't really happen if they're part of the same region.
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    multi.callback(decoded);
    
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put7.getDeferred().join(1));
  }
  
  @Test
  public void serialize2PutsSameColumnPlusDiffQualifier() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(2, multi.batch().size());
    assertSame(put3, multi.batch().get(0));
    assertNotSame(put1, multi.batch().get(1));
    assertNotSame(put2, multi.batch().get(1));
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // diff qualifier sorted to top
    assertArrayEquals(put3.qualifier(), mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void response2PutsSameColumnPlusDiffQualifier() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    multi.add(put3);
    Deferred<Object> p3 = put3.getDeferred();
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    

    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    
    rc.addMultiActionCallbacks(multi);
    multi.callback(decoded);
    assertSame(MultiAction.SUCCESS, p3.join(1));
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
  }
  
  @Test
  public void serialize2PutsSameColumnPlusDiffQualifierDiffOrder() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put3);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);

    assertEquals(2, multi.batch().size());
    assertSame(put3, multi.batch().get(0));
    assertNotSame(put1, multi.batch().get(1));
    assertNotSame(put2, multi.batch().get(1));
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // diff qualifier
    assertArrayEquals(put3.qualifier(), mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }

  @Test
  public void serialize2PutsSameColumnPlusDiffFamily() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put4);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // diff family
    assertArrayEquals(put4.family, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void serialize2PutsSameColumnPlusDiffKey() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put5);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // diff key
    assertArrayEquals(put5.key, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void serialize2PutsSameColumnPlusDiffTable() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put6);
    multi.add(put2);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(2, mr.getRegionActionCount());
    assertEquals(1, mr.getRegionAction(0).getActionCount());
    
    // diff table
    assertArrayEquals(PRE94_META.name(), mr.getRegionAction(0).getRegion().getValue().toByteArray());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(VALUE, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(1).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(1).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(1).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void serialize2PutsSameColumnPlusDiffType() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    multi.add(delete);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // delete happens first for now.
    assertEquals(MutationType.DELETE, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void serializeDeleteNoQualifier() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    delete = new DeleteRequest(TABLE, KEY, FAMILY);
    delete.region = TABLE_REGION;
    multi.add(delete);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(1, mr.getRegionActionCount());
    assertEquals(2, mr.getRegionAction(0).getActionCount());
    
    // delete happens first for now.
    assertEquals(MutationType.DELETE, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertArrayEquals(HBaseClient.EMPTY_ARRAY, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    
    // append
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(1).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(1).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(1).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(1).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void responseWithDelete() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    delete = new DeleteRequest(TABLE, KEY, FAMILY);
    delete.region = TABLE_REGION;
    Deferred<Object> d = delete.getDeferred();
    multi.add(delete);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    
    rc.addMultiActionCallbacks(multi);
    multi.callback(decoded);
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, d.join(1));
  }
  
  @Test
  public void responseWithDeletePutsFailed() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    delete = new DeleteRequest(TABLE, KEY, FAMILY);
    delete.region = TABLE_REGION;
    Deferred<Object> d = delete.getDeferred();
    multi.add(delete);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    rc.addMultiActionCallbacks(multi);
    
    // NOTE: This couldn't really happen if they're part of the same region.
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateEmptyResult(0));
    results.add(PBufResponses.generateException(
        NotServingRegionException.REMOTE_CLASS, "Boo!", 1));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    multi.callback(decoded);
    try {
      put1.getDeferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException ex) { }
    try {
      put2.getDeferred().join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException ex) { }
    verify(client, never()).handleNSRE(eq(put1), 
        eq(put1.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
    verify(client, never()).handleNSRE(eq(put2), 
        eq(put2.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
    verify(client, times(1)).handleNSRE(eq(multi.batch().get(1)), 
        eq(put1.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
    assertSame(MultiAction.SUCCESS, d.join(1));
    verify(client, never()).handleNSRE(eq(delete), 
        eq(delete.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
  }
  
  @Test
  public void responseWithDeleteDeleteFailed() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    delete = new DeleteRequest(TABLE, KEY, FAMILY);
    delete.region = TABLE_REGION;
    Deferred<Object> d = delete.getDeferred();
    multi.add(delete);
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    rc.addMultiActionCallbacks(multi);
    
    // NOTE: This couldn't really happen if they're part of the same region.
    final List<ResultOrException> results = new ArrayList<ResultOrException>(2);
    results.add(PBufResponses.generateException(
        NotServingRegionException.REMOTE_CLASS, "Boo!", 0));
    results.add(PBufResponses.generateEmptyResult(1));
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                PBufResponses.generateMultiActionResponse(results)), 0);
    multi.callback(decoded);
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
    verify(client, never()).handleNSRE(eq(put1), 
        eq(put1.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
    verify(client, never()).handleNSRE(eq(put2), 
        eq(put2.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
    try {
      d.join(1);
      fail("Expected TimeoutException");
    } catch (TimeoutException ex) { }
    verify(client, times(1)).handleNSRE(eq(delete), 
        eq(delete.getRegion().name()), 
        any(NotServingRegionException.class), 
        anyString());
  }
  
  @Test
  public void serializeMultiRegions() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    
    PutRequest put7 = new PutRequest(TABLE, KEY2, FAMILY, QUALIFIER, VALUE);
    put7.setRegion(TABLE_REGION2);
    multi.add(put7);
    PutRequest put8 = new PutRequest(TABLE, KEY2, FAMILY, QUALIFIER, new byte[] { 'v', '3' });
    put8.setRegion(TABLE_REGION2);
    multi.add(put8);
    
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    final MultiRequest mr = HBaseRpc.readProtobuf(buf, MultiRequest.PARSER);
    
    assertEquals(2, multi.batch().size());
    
    assertEquals(2, mr.getRegionActionCount());
    assertEquals(1, mr.getRegionAction(0).getActionCount());
    assertEquals(1, mr.getRegionAction(1).getActionCount());
    
    // append to region 1
    assertEquals(MutationType.PUT, mr.getRegionAction(0).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY, mr.getRegionAction(0).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(0).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put2.value()), 
        mr.getRegionAction(0).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
    
    // append to region 2
    assertEquals(MutationType.PUT, mr.getRegionAction(1).getAction(0).getMutation()
        .getMutateType());
    assertArrayEquals(KEY2, mr.getRegionAction(1).getAction(0).getMutation()
        .getRow().toByteArray());
    assertEquals(1, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValueCount());
    assertArrayEquals(FAMILY, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getFamily().toByteArray());
    assertEquals(1, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValueCount());
    assertArrayEquals(QUALIFIER, mr.getRegionAction(1).getAction(0).getMutation()
        .getColumnValue(0).getQualifierValue(0).getQualifier().toByteArray());
    assertArrayEquals(BaseTestHBaseClient.concat(VALUE, put8.value()), 
        mr.getRegionAction(1).getAction(0).getMutation()
          .getColumnValue(0).getQualifierValue(0).getValue().toByteArray());
  }
  
  @Test
  public void responseMultiRegions() throws Exception {
    final MultiActionAppend multi = new MultiActionAppend();
    multi.add(put1);
    multi.add(put2);
    
    PutRequest put7 = new PutRequest(TABLE, KEY2, FAMILY, QUALIFIER, VALUE);
    put7.setRegion(TABLE_REGION2);
    multi.add(put7);
    PutRequest put8 = new PutRequest(TABLE, KEY2, FAMILY, QUALIFIER, new byte[] { 'v', '3' });
    put8.setRegion(TABLE_REGION2);
    multi.add(put8);
    
    final ChannelBuffer buf = multi.serialize(RegionClient.SERVER_VERSION_095_OR_ABOVE);
    buf.readerIndex(4 + 19 + MultiAction.MMULTI.length);
    
    final MultiAction.Response decoded = 
        (MultiAction.Response) multi.deserialize(
            PBufResponses.encodeResponse(
                MultiResponse.newBuilder()
                  .addRegionActionResult(PBufResponses.generateRegionActionResult(
                      Lists.newArrayList(PBufResponses.generateEmptyResult(0))))
                  .addRegionActionResult(PBufResponses.generateRegionActionResult(
                      Lists.newArrayList(PBufResponses.generateEmptyResult(1))))
        .build()), 0);
    
    rc.addMultiActionCallbacks(multi);
    multi.callback(decoded);
    assertSame(MultiAction.SUCCESS, put1.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put2.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put7.getDeferred().join(1));
    assertSame(MultiAction.SUCCESS, put8.getDeferred().join(1));
  }
  
  protected static RegionInfo mkregion(final String table, final String name) {
    return new RegionInfo(table.getBytes(), name.getBytes(),
                          HBaseClient.EMPTY_ARRAY);
  }
  
  protected static RegionInfo mkregion(final byte[] table, final byte[] name) {
    return new RegionInfo(table, name,HBaseClient.EMPTY_ARRAY);
  }

}
