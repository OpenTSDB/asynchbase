/*
 * Copyright (C) 2015 The Async HBase Authors.  All rights reserved.
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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.doAnswer;
import static org.junit.Assert.assertTrue;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
public class TestDeleteRequest extends BaseTestHBaseClient {
  private static final long TIMESTAMP_LONG = 1234567890;
  private static final byte[][] QUALIFIERS = {{'q', 'u', 'a', 'l'},
    {'f', 'i' ,'e', 'r'}};
  private static final RowLock LOCK = new RowLock(region.name(), 1234567890);
  private static Boolean sendDelete; 
  private Counter num_deletes;
  
  @Before
  public void beforeTest() throws Exception {
    sendDelete = false;
    num_deletes = Whitebox.getInternalState(client, "num_deletes");
  }

  /**
   * Test a simple delete request
   * @throws Exception
   */
  @Test
  public void simpleDelete() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test a delete that deletes a Key for a time
   * @throws Exception
   */
  @Test
  public void deleteWithTime() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, TIMESTAMP_LONG);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test a delete that deletes based on family
   * @throws Exception
   */
  @Test
  public void deleteWithFamily() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes based on family and time
   * @throws Exception
   */
  @Test
  public void deleteWithFamilyAndTime() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        TIMESTAMP_LONG);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with qualifier
   * @throws Exception
   */
  @Test
  public void deleteWithQualifier() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIER);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with Qualifier and time
   * @throws Exception
   */
  @Test
  public void deleteWithQualifierAndTime() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, TIMESTAMP_LONG);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with multiple qualifiers
   * @throws Exception
   */
  @Test
  public void deleteWithColumn() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, QUALIFIERS);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with multiple qualifiers and time
   * @throws Exception
   */
  @Test
  public void deleteWithColumnAndTime() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        QUALIFIERS, TIMESTAMP_LONG);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with taking a row lock
   * @throws Exception
   */
  @Test
  public void deleteWithLock() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, LOCK);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with a specific row lock and time
   * @throws Exception
   */
  @Test
  public void deleteWithLockAndTime() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        QUALIFIER, TIMESTAMP_LONG, LOCK);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that deletes with multiple qualifiers and row lock
   * @throws Exception
   */
  @Test
  public void deleteWithColumnAndLock() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KEY, FAMILY, 
        QUALIFIERS, LOCK);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Test delete that takes input as Strings
   * @throws Exception
   */
  @Test
  public void deleteWithString() throws Exception {
    final DeleteRequest delete = new DeleteRequest(new String(TABLE), 
        new String(KEY));
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Tests delete that takes family input as String
   * @throws Exception
   */
  @Test
  public void deleteWithFamilyString() throws Exception {
    final DeleteRequest delete = new DeleteRequest(new String(TABLE), 
        new String(KEY), new String(FAMILY));
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Tests delete that takes qualifier input as String
   * @throws Exception
   */
  @Test
  public void deleteWithQualifierString() throws Exception {
    final DeleteRequest delete = new DeleteRequest(new String(TABLE), 
        new String(KEY), new String(FAMILY), new String(QUALIFIER));
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Tests delete that takes input as String and a row lock
   * @throws Exception
   */
  @Test
  public void deleteWithStringLock() throws Exception {
    final DeleteRequest delete = new DeleteRequest(new String(TABLE), 
        new String(KEY), new String(FAMILY), new String(QUALIFIER), LOCK);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Tests delete that takes input KeyValue pair
   * @throws Exception
   */
  @Test
  public void deleteKV() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KV);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }

  /**
   * Tests delete that takes input KeyValue pair with a row lock
   * @throws Exception
   */
  @Test
  public void deleteKVWithLock() throws Exception {
    final DeleteRequest delete = new DeleteRequest(TABLE, KV, LOCK);
    stubbing(delete);
    client.delete(delete);
    Mockito.verify(client).sendRpcToRegion(delete);
    Mockito.verify(regionclient).sendRpc(delete);
    assertTrue(sendDelete);
    assertEquals(1, num_deletes.get());
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void stubbing(final DeleteRequest delete){
    doAnswer(new Answer<Object>(){
      public Object answer(final InvocationOnMock invocation){
        delete.getDeferred().callback(null);
        sendDelete = true;
        return null;
      }
    }).when(regionclient).sendRpc(delete);

    doAnswer(new Answer<Object>(){
      public Object answer(final InvocationOnMock invocation){
        regionclient.sendRpc(delete);
        return null;
      }
    }).when(client).sendRpcToRegion(delete);
  }

}