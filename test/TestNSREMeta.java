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

import java.util.Arrays;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestNSREMeta {
  final static byte[] TABLE = new byte[] { 't' };
  final static byte[] NAME = new byte[] { 't', ',', 0x42, ',', 
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};
  final static byte[] STOP_KEY = new byte[] { 0x43 };
  final static RecoverableException EX = new NotServingRegionException("Boo!", null);
  final static RegionInfo REGION = new RegionInfo(TABLE, NAME, STOP_KEY);
  
  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNegativeTimestamp() {
    new NSREMeta(-2L, "klaatu", REGION, EX);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullRemoteAddress() {
    new NSREMeta(342L, null, REGION, EX);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorEmptyRemoteAddress() {
    new NSREMeta(12345L, "", REGION, EX);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullRegion() {
    new NSREMeta(8493L, "barada", null, EX);
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullName() {
    new NSREMeta(8493L, "barada", new RegionInfo(TABLE, null, STOP_KEY), EX);
  }

  @Test
  public void testConstructor() {
    final long timestamp = 584L;
    final String address = "localhost";

    final NSREMeta meta = new NSREMeta(timestamp, address, REGION, EX);

    assertEquals(timestamp, meta.getTime());
    assertEquals(address, meta.getRemoteAddress());
    assertTrue(Arrays.equals(new byte[] { 0x42 }, meta.getStartKey()));
    assertTrue(Arrays.equals(STOP_KEY, meta.getStopKey()));
  }

  @Test
  public void testToStringIntegrated() {
    final String string = new NSREMeta(1234567890L, "127.0.0.1:5011", REGION, EX)
      .toString();
    assertTrue(string.contains("time=1234567890"));
    assertTrue(string.contains("remoteAddress=127.0.0.1:5011"));
    assertTrue(string.contains("region_name=\"t,B,1234567890\""));
    assertTrue(string.contains("stop_key=\"C\""));
    assertTrue(string.contains(
        "originalException=org.hbase.async.NotServingRegionException: Boo!"));
  }

  @Test
  public void testEqualsAndHashCode() {
    final NSREMeta one = new NSREMeta(50L, new String("omg"),
      new RegionInfo(TABLE, NAME, STOP_KEY), EX);
    final NSREMeta two = new NSREMeta(50L, "o" + "m" + "g",
        new RegionInfo(TABLE, NAME, STOP_KEY), EX);

    new EqualsTester().
      addEqualityGroup(one, two).
      testEquals();
  }
}

