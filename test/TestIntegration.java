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
package org.hbase.async.test;

import java.util.ArrayList;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import org.slf4j.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import com.stumbleupon.async.Callback;

import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;

import org.hbase.async.test.Common;

/**
 * Basic integration and regression tests for asynchbase.
 *
 * Requires a locally running HBase cluster.
 */
final class TestIntegration {

  private static final Logger LOG = Common.logger(TestIntegration.class);

  private static final short FAST_FLUSH = 10;

  public static void main(final String[] args) throws Exception {
    preFlightTest(args);
    LOG.info("Starting integration tests");
    putRead(args);
    putReadDeleteRead(args);
  }

  /** Ensures the table/family we use for our test exists. */
  private static void preFlightTest(final String[] args) throws Exception {
    final HBaseClient client = Common.getOpt(TestIncrementCoalescing.class,
                                             args);
    try {
      client.ensureTableFamilyExists(args[0], args[1]).join();
    } finally {
      client.shutdown().join();
    }
  }

  /** Write a single thing to HBase and read it back. */
  private static void putRead(final String[] args) throws Exception {
    final HBaseClient client = Common.getOpt(TestIncrementCoalescing.class,
                                             args);
    client.setFlushInterval(FAST_FLUSH);
    try {
      final String table = args[0];
      final String family = args[1];
      final double write_time = System.currentTimeMillis();
      final PutRequest put = new PutRequest(table, "k", "f", "q", "val");
      final GetRequest get = new GetRequest(table, "k")
        .family(family).qualifier("q");
      client.put(put).join();
      final ArrayList<KeyValue> kvs = client.get(get).join();
      assertEquals(1, kvs.size());
      final KeyValue kv = kvs.get(0);
      assertEq("k", kv.key());
      assertEq(family, kv.family());
      assertEq("q", kv.qualifier());
      assertEq("val", kv.value());
      final double kvts = kv.timestamp();
      assertEquals(write_time, kvts, 5000.0);  // Within five seconds.
    } finally {
      client.shutdown().join();
    }
    LOG.info("putRead: PASS");
  }

  /** Write a single thing to HBase and read it back, delete it, read it. */
  private static void putReadDeleteRead(final String[] args) throws Exception {
    final HBaseClient client = Common.getOpt(TestIncrementCoalescing.class,
                                             args);
    client.setFlushInterval(FAST_FLUSH);
    try {
      final String table = args[0];
      final String family = args[1];
      final PutRequest put = new PutRequest(table, "k", "f", "q", "val");
      final GetRequest get = new GetRequest(table, "k")
        .family(family).qualifier("q");
      client.put(put).join();
      final ArrayList<KeyValue> kvs = client.get(get).join();
      assertEquals(1, kvs.size());
      assertEq("val", kvs.get(0).value());
      final DeleteRequest del = new DeleteRequest(table, "k", "f", "q");
      client.delete(del).join();
      final ArrayList<KeyValue> kvs2 = client.get(get).join();
      assertEquals(0, kvs2.size());
    } finally {
      client.shutdown().join();
    }
    LOG.info("putReadDeleteRead: PASS");
  }

  private static void assertEq(final String expect, final byte[] actual) {
    assertArrayEquals(expect.getBytes(), actual);
  }

}
