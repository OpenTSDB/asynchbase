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

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import org.hbase.async.TableNotFoundException;

import org.hbase.async.test.Common;

/**
 * Basic integration and regression tests for asynchbase.
 *
 * Requires a locally running HBase cluster.
 */
final class TestIntegration {

  private static final Logger LOG = Common.logger(TestIntegration.class);

  private static final short FAST_FLUSH = 10;
  /** Path to HBase home so we can run the HBase shell.  */
  private static final String HBASE_HOME;
  static {
    HBASE_HOME = System.getenv("HBASE_HOME");
    if (HBASE_HOME == null) {
      throw new RuntimeException("Please set the HBASE_HOME environment"
                                 + " variable.");
    }
    final File dir = new File(HBASE_HOME);
    if (!dir.isDirectory()) {
      throw new RuntimeException("No such directory: " + HBASE_HOME);
    }
  }
  /** Whether or not to truncate existing tables during tests.  */
  private static final boolean TRUNCATE =
    System.getenv("TEST_NO_TRUNCATE") == null;

  public static void main(final String[] args) throws Exception {
    preFlightTest(args);
    LOG.info("Starting integration tests");
    putRead(args);
    putReadDeleteRead(args);
    regression25(args);
  }

  /** Ensures the table/family we use for our test exists. */
  private static void preFlightTest(final String[] args) throws Exception {
    final HBaseClient client = Common.getOpt(TestIncrementCoalescing.class,
                                             args);
    try {
      createOrTruncateTable(client, args[0], args[1]);
    } finally {
      client.shutdown().join();
    }
  }

  /** Creates or truncates the given table name. */
  private static void createOrTruncateTable(final HBaseClient client,
                                            final String table,
                                            final String family)
    throws Exception {
    try {
      client.ensureTableFamilyExists(table, family).join();
      truncateTable(table);
    } catch (TableNotFoundException e) {
      createTable(table, family);
      createOrTruncateTable(client, table, family);  // Check again.
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
      final PutRequest put = new PutRequest(table, "k", family, "q", "val");
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
      final PutRequest put = new PutRequest(table, "k", family, "q", "val");
      final GetRequest get = new GetRequest(table, "k")
        .family(family).qualifier("q");
      client.put(put).join();
      final ArrayList<KeyValue> kvs = client.get(get).join();
      assertEquals(1, kvs.size());
      assertEq("val", kvs.get(0).value());
      final DeleteRequest del = new DeleteRequest(table, "k", family, "q");
      client.delete(del).join();
      final ArrayList<KeyValue> kvs2 = client.get(get).join();
      assertEquals(0, kvs2.size());
    } finally {
      client.shutdown().join();
    }
    LOG.info("putReadDeleteRead: PASS");
  }

  /** Regression test for issue #25. */
  private static void regression25(final String[] args) throws Exception {
    final HBaseClient client = Common.getOpt(TestIncrementCoalescing.class,
                                             args);
    client.setFlushInterval(FAST_FLUSH);
    try {
      final String table1 = args[0] + "1";
      final String table2 = args[0] + "2";
      final String family = args[1];
      createOrTruncateTable(client, table1, family);
      createOrTruncateTable(client, table2, family);
      for (int i = 0; i < 2; i++) {
        final PutRequest put;
        final String key = 'k' + String.valueOf(i);
        if (i % 2 == 0) {
          put = new PutRequest(table1, key, family, "q", "v");
        } else {
          put = new PutRequest(table2, key, family, "q", "v");
        }
        final DeleteRequest delete = new DeleteRequest(put.table(), put.key());
        client.delete(delete);
        client.put(put);
      }
      client.flush().joinUninterruptibly();
    } finally {
      client.shutdown().join();
    }
    LOG.info("regression25: PASS");
  }

  private static void assertEq(final String expect, final byte[] actual) {
    assertArrayEquals(expect.getBytes(), actual);
  }

  private static void createTable(final String table,
                                  final String family) throws Exception {
    LOG.info("Creating table " + table + " with family " + family);
    hbaseShell("create '" + table + "',"
               + " {NAME => '" + family + "', VERSIONS => 2}");
  }

  private static void truncateTable(final String table) throws Exception {
    if (!TRUNCATE) {
      return;
    }
    LOG.warn("Truncating table " + table + "...");
    for (int i = 3; i >= 0; i--) {
      LOG.warn(i + " Press Ctrl-C if you care about " + table);
      Thread.sleep(1000);
    }
    hbaseShell("truncate '" + table + '\'');
  }

  private static void hbaseShell(final String command) throws Exception {
    final ProcessBuilder pb = new ProcessBuilder();
    pb.command(HBASE_HOME + "/bin/hbase", "shell");
    pb.environment().remove("HBASE_HOME");
    LOG.info("Running HBase shell command: " + command);
    final Process shell = pb.start();
    try {
      final OutputStream stdin = shell.getOutputStream();
      stdin.write(command.getBytes());
      stdin.write('\n');
      stdin.flush();  // Technically the JDK doesn't guarantee that close()
      stdin.close();  // will flush(), so better do it explicitly to be safe.
      // Let's hope that the HBase shell doesn't print more than 4KB of shit
      // on stderr, otherwise we're getting deadlocked here.  Yeah seriously.
      // Dealing with subprocesses in Java is such a royal PITA.
      printLines("stdout", shell.getInputStream());  // Confusing method name,
      printLines("stderr", shell.getErrorStream());  // courtesy of !@#$% JDK.
      final int rv = shell.waitFor();
      if (rv != 0) {
        throw new RuntimeException("hbase shell returned " + rv);
      }
    } finally {
      shell.destroy();  // Required by the fucking JDK, no matter what.
    }
  }

  private static void printLines(final String what, final InputStream in)
    throws Exception {
    final BufferedReader r = new BufferedReader(new InputStreamReader(in));
    String line;
    while ((line = r.readLine()) != null) {
      LOG.info('(' + what + ") " + line);
    }
  }

}
