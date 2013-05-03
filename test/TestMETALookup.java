/*
 * Copyright (C) 2012  The Async HBase Authors.  All rights reserved.
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

import java.util.Comparator;

import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.fail;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for META lookups and associated regressions.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest
final class TestMETALookup {

  private static final Comparator<byte[]> cmp = RegionInfo.REGION_NAME_CMP;

  @Test
  public void testRegionCmp() {
    // Different table names.
    assertGreater("table,,1234567890", ".META.,,1234567890");
    // Any key is greater than the start key.
    assertGreater("table,foo,1234567890", "table,,1234567890");
    // Different keys.
    assertGreater("table,foo,1234567890", "table,bar,1234567890");
    // Shorter key is smaller than longer key.
    assertGreater("table,fool,1234567890", "table,foo,1234567890");
    // Properly handle keys that contain commas.
    assertGreater("table,a,,c,1234567890", "table,a,,b,1234567890");
    // If keys are equal, then start code should break the tie.
    assertGreater("table,foo,1234567891", "table,foo,1234567890");
    // Make sure that a start code being a prefix of another is handled.
    assertGreater("table,foo,1234567890", "table,foo,123456789");
    // If both are start keys, then start code should break the tie.
    assertGreater("table,,1234567891", "table,,1234567890");
    // The value `:' is always greater than any start code.
    assertGreater("table,foo,:", "table,foo,9999999999");
    // Issue 27: searching for key "8,\001" and region key is "8".
    assertGreater("table,8,\001,:", "table,8,1339667458224");
  }

  /** Ensures that {@code sa > sb} in the META cache.  */
  private static void assertGreater(final String sa, final String sb) {
    final byte[] a = sa.getBytes();
    final byte[] b = sb.getBytes();
    int c = cmp.compare(a, b);
    if (c <= 0) {
      fail("compare(" + Bytes.pretty(a) + ", " + Bytes.pretty(b) + ")"
           + " returned " + c + ", but was expected to return > 0");
    }
    c = cmp.compare(b, a);
    if (c >= 0) {
      fail("compare(" + Bytes.pretty(b) + ", " + Bytes.pretty(a) + ")"
           + " returned " + c + ", but was expected to return < 0");
    }
  }

}
