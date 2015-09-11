/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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
final class TestPutRequest extends BaseTestHBaseClient {
  private static final long TIMESTAMP_LONG = 1234567890;
  private static final byte[][] QUALIFIERS = {{'q', 'u', 'a', 'l'},
    {'f', 'i' ,'e', 'r'}};
  private static final byte[][] VALUES = {{'v', 'a', 'l', 'u', 'e'},
    {'v', 'a', 'l', 'u', 'e', 's'}};
  private static final RowLock LOCK = new RowLock(region.name(), 1234567890);
  private static Boolean sendPut; 
  private Counter num_puts;
  
  @Before
  public void beforeTest() throws Exception {
    sendPut = false;
    num_puts = Whitebox.getInternalState(client, "num_puts");
  }

  /**
   * Tests simple put that uses the current timestamp
   * @throws Exception
   */
  @Test
  public void simplePut() throws Exception {
    final PutRequest put = new PutRequest(TABLE,KEY, FAMILY, QUALIFIER, VALUE);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put request with specific timestamp
   * @throws Exception
   */
  @Test
  public void putWithTime() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, 
        TIMESTAMP_LONG);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put that have multiple column qualifiers and values
   * @throws Exception
   */
  @Test
  public void putWithColumns() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIERS, VALUES, 
        TIMESTAMP_LONG);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put with a specific row lock
   * @throws Exception
   */
  @Test
  public void putWithLock() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, 
        LOCK);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put with a specific timestamp and row lock
   * @throws Exception
   */
  @Test
  public void putWithLockAndTime() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIER, VALUE, 
        TIMESTAMP_LONG, LOCK);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put with multiple column qualifier, values and row lock
   * @throws Exception
   */
  @Test
  public void putWithColumnAndLock() throws Exception {
    final PutRequest put = new PutRequest(TABLE, KEY, FAMILY, QUALIFIERS, VALUES,
        TIMESTAMP_LONG, LOCK);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put with strings instead of bytes.
   * @throws Exception
   */
  @Test
  public void putStrings() throws Exception {
    final PutRequest put = new PutRequest(new String(TABLE), new String(KEY), 
        new String(FAMILY), new String(QUALIFIER), new String(VALUE));
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  /**
   * Test put with KeyValue pair input
   * @throws Exception
   */
  @Test
  public void putKV() throws Exception{
    final PutRequest put = new PutRequest(TABLE, KV);
    stubbing(put);
    client.put(put);
    Mockito.verify(client).sendRpcToRegion(put);
    Mockito.verify(regionclient).sendRpc(put);
    assertTrue(sendPut);
    assertEquals(1, num_puts.get());
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void stubbing(final PutRequest put){
    doAnswer(new Answer<Object>(){
      public Object answer(final InvocationOnMock invocation){
        put.getDeferred().callback(null);
        sendPut = true;
        return null;
      }
    }).when(regionclient).sendRpc(put); 

    doAnswer(new Answer<Object>(){
      public Object answer(final InvocationOnMock invocation){
        regionclient.sendRpc(put);
        return null;
      }
    }).when(client).sendRpcToRegion(put);
  }

}