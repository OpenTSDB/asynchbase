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

import com.google.common.testing.EqualsTester;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestNSREEvent {
  final static byte[] TABLE = new byte[] { 't' };
  final static byte[] NAME_A = new byte[] { 't', ',', 0x42, ',', 
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};
  final static byte[] NAME_B = new byte[] { 't', ',', 0x43, ',', 
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '1'};
  final static byte[] STOP_KEY_A = new byte[] { 0x44 };
  final static byte[] STOP_KEY_B = new byte[] { 0x43 };
  final static RecoverableException EX = new NotServingRegionException("Boo!", null);
  final static RegionInfo REGION_A = new RegionInfo(TABLE, NAME_A, STOP_KEY_A);
  final static RegionInfo REGION_B = new RegionInfo(TABLE, NAME_B, STOP_KEY_A);
  final static RegionInfo REGION_C = new RegionInfo(TABLE, NAME_A, STOP_KEY_B);

  private static final NSREMeta DUMMY_OLD_META = new NSREMeta(0L, "localhost",
    REGION_A, EX);

  private static final NSREMeta DUMMY_NEW_META = new NSREMeta(10L, "localhost",
    REGION_B, null);

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullRegionName() {
    new NSREEvent(null, DUMMY_OLD_META, DUMMY_NEW_META);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullOldMetadata() {
    new NSREEvent(NAME_B, null, DUMMY_NEW_META);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorNullNewMetadata() {
    new NSREEvent(NAME_B, DUMMY_OLD_META, null);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testConstructorMalorderedTimestamps() {
    final NSREMeta old_meta = new NSREMeta(400L, "whatever.yahoo.com",
      REGION_A, EX);
    final NSREMeta new_meta = new NSREMeta(0L, "whatever.yahoo.com",
      REGION_B, EX);

    new NSREEvent(NAME_B, old_meta, new_meta);
  }

  @Test
  public void testConstructorIntegrated() {
    final NSREMeta old_meta = new NSREMeta(1234L, "localhost", REGION_A, EX);
    final NSREMeta new_meta = new NSREMeta(4567L, "localhost", REGION_B, EX);

    final NSREEvent event = new NSREEvent(NAME_B, old_meta, new_meta);

    assertEquals(NSREEvent.Reason.SPLIT, event.getReason());
    assertEquals(1234L, event.getDetectionTime());
    assertEquals(4567L, event.getResolutionTime());
    assertEquals(4567L - 1234L, event.getDuration());
  }

  @Test
  public void testWhySplit() {
    // Same server and start key, but different stop key.
    final NSREMeta old_meta = new NSREMeta(1234L, "localhost", REGION_A, EX);
    NSREMeta new_meta = new NSREMeta(4567L, "localhost", REGION_B, EX);
    assertEquals(NSREEvent.Reason.SPLIT, NSREEvent.why(old_meta, new_meta));

    // Same server and stop key, but different start key.
    new_meta = new NSREMeta(5678L, "localhost", REGION_C, EX);
    assertEquals(NSREEvent.Reason.SPLIT, NSREEvent.why(old_meta, new_meta));
  }

  @Test
  public void testWhyMoved() {
    // Same start and stop keys, but different server.
    final NSREMeta old_meta = new NSREMeta(1234L, "localhost", REGION_A, EX);
    final NSREMeta new_meta = new NSREMeta(4567L, "elsewhere", REGION_A, EX);

    assertEquals(NSREEvent.Reason.MOVED, NSREEvent.why(old_meta, new_meta));
  }

  @Test
  public void testWhyCompactedOrOffline() {
    // Same server, start key, and stop key.
    final NSREMeta old_meta = new NSREMeta(1234L, "localhost", REGION_A, EX);
    final NSREMeta new_meta = old_meta;

    assertEquals(NSREEvent.Reason.COMPACTED_OR_OFFLINE,
      NSREEvent.why(old_meta, new_meta));
  }

  /**
   * We believe this case will most often occur when a region server crashes
   * during a split.
   */
  @Test
  public void testWhyMovedAndSplit() {
    // Different server and start key, but different stop key.
    final NSREMeta old_meta = new NSREMeta(17L, "here", REGION_A, EX);
    NSREMeta new_meta = new NSREMeta(20L, "there", REGION_B, EX);
    assertEquals(NSREEvent.Reason.MOVED_AND_SPLIT,
      NSREEvent.why(old_meta, new_meta));

    // Different server and stop key, but same start key.
    new_meta = new NSREMeta(21L, "there", REGION_C, EX);
    assertEquals(NSREEvent.Reason.MOVED_AND_SPLIT,
      NSREEvent.why(old_meta, new_meta));
  }

  @Test
  public void testWhyUnknown() {
    // Same server, different start and stop keys.
    // (Would be a different region -- makes no sense.)
    final NSREMeta old_meta = new NSREMeta(17L, "here", REGION_A, EX);
    NSREMeta new_meta = new NSREMeta(18L, "here", 
        new RegionInfo(TABLE, new byte[] { 't', ',', 0x01, ',', '1' }, 
            STOP_KEY_B), EX);
    assertEquals(NSREEvent.Reason.UNKNOWN, NSREEvent.why(old_meta, new_meta));

    // Different server, start key, and stop key.
    // (Different region on different server -- even less meaningful.)
    new_meta = new NSREMeta(19L, "there",  
        new RegionInfo(TABLE, new byte[] { 't', ',', 0x01, ',', '1' }, 
        STOP_KEY_B), EX);
    assertEquals(NSREEvent.Reason.UNKNOWN, NSREEvent.why(old_meta, new_meta));
  }

  @Test
  public void testEqualsAndHashCode() {
    final NSREMeta a = new NSREMeta(50L, new String("omg"), REGION_A, EX);
    final NSREMeta b = new NSREMeta(75L, "o" + "m" + "g", REGION_A, EX);
    final NSREMeta c = new NSREMeta(60L, "never", REGION_B, EX);
    final NSREMeta d = new NSREMeta(112L, "n" + "ev" + "er", REGION_B, EX);

    final NSREEvent one = new NSREEvent(new byte[] {'x'}, a, b);
    final NSREEvent two = new NSREEvent(new byte[] {'x'}, a, b);
    final NSREEvent three = new NSREEvent(new byte[] {'z'}, c, d);
    final NSREEvent four = new NSREEvent(new byte[] {'z'}, c, d);
    new EqualsTester().
      addEqualityGroup(one, two).
      addEqualityGroup(three, four).
      testEquals();
  }
}

