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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import org.slf4j.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.powermock.reflect.Whitebox;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.Bytes;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.ColumnRangeFilter;
import org.hbase.async.DeleteRequest;
import org.hbase.async.FilterList;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.KeyValue;
import org.hbase.async.NoSuchColumnFamilyException;
import org.hbase.async.PutRequest;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.hbase.async.TableNotFoundException;

import org.hbase.async.test.Common;

/**
 * Basic integration and regression tests for asynchbase.
 *
 * Requires a locally running HBase cluster.
 */
final public class TestIntegration {

  private static final Logger LOG = Common.logger(TestIntegration.class);

  private static final short FAST_FLUSH = 10;    // milliseconds
  private static final short SLOW_FLUSH = 1000;  // milliseconds
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

  private static String table;
  private static String family;
  private static String[] args;
  private HBaseClient client;

  public static void main(final String[] args) throws Exception {
    preFlightTest(args);
    table = args[0];
    family = args[1];
    TestIntegration.args = args;
    LOG.info("Starting integration tests");
    final JUnitCore junit = new JUnitCore();
    final JunitListener listener = new JunitListener();
    junit.addListener(listener);
    final String singleTest = System.getenv("TEST_NAME");
    final Request req;
    if (singleTest != null) {
      req = Request.method(TestIntegration.class, singleTest);
    } else {
      req = Request.aClass(TestIntegration.class);
    }
    final Result result = junit.run(req);
    LOG.info("Ran " + result.getRunCount() + " tests in "
             + result.getRunTime() + "ms");
    if (!result.wasSuccessful()) {
      LOG.error(result.getFailureCount() + " tests failed: "
                + result.getFailures());
      System.exit(1);
    }
    LOG.info("All tests passed!");
  }

  @Before
  public void setUp() {
    client = Common.getOpt(TestIntegration.class, args);
  }

  @After
  public void tearDown() throws Exception {
    client.shutdown().join();
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
  @Test
  public void putRead() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final double write_time = System.currentTimeMillis();
    final PutRequest put = new PutRequest(table, "k", family, "q", "val");
    final GetRequest get = new GetRequest(table, "k", family, "q");
    client.put(put).join();
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    final KeyValue kv = kvs.get(0);
    assertEq("k", kv.key());
    assertEq(family, kv.family());
    assertEq("q", kv.qualifier());
    assertEq("val", kv.value());
    final double kvts = kv.timestamp();
    assertEquals(write_time, kvts, 5000.0);  // Within five seconds.
  }

  /** Write a single thing to HBase and read it back, delete it, read it. */
  @Test
  public void putReadDeleteRead() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put = new PutRequest(table, "k", family, "q", "val");
    final GetRequest get = new GetRequest(table, "k", family, "q");
    client.put(put).join();
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    assertEq("val", kvs.get(0).value());
    final DeleteRequest del = new DeleteRequest(table, "k", family, "q");
    client.delete(del).join();
    final ArrayList<KeyValue> kvs2 = client.get(get).join();
    assertSizeIs(0, kvs2);
  }

  /**
   * Write two values to a HBase column and read them back,
   * delete one, and read back the other.
   */
  @Test
  public void putReadDeleteAtTimestamp() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    byte[] t = table.getBytes();
    byte[] k = "kprd@ts".getBytes();
    byte[] f = family.getBytes();
    // Make the qualifier unique to avoid running into HBASE-9879.
    byte[] q = ("q" + System.currentTimeMillis()
                + "-" + System.nanoTime()).getBytes();
    byte[] v1 = "val1".getBytes();
    byte[] v2 = "val2".getBytes();
    final PutRequest put1 = new PutRequest(t, k, f, q, v1, 100L);
    final PutRequest put2 = new PutRequest(t, k, f, q, v2, 200L);
    client.put(put1).join();
    client.put(put2).join();
    final GetRequest get = new GetRequest(t, k, f, q).maxVersions(2);
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(2, kvs);
    assertEq("val2", kvs.get(0).value());
    assertEq("val1", kvs.get(1).value());
    final DeleteRequest del = new DeleteRequest(t, k, f, q, 200L);
    del.setDeleteAtTimestampOnly(true);
    client.delete(del).join();
    final ArrayList<KeyValue> kvs2 = client.get(get).join();
    assertSizeIs(1, kvs2);
    assertEq("val1", kvs2.get(0).value());
  }

  /** Basic scan test. */
  @Test
  public void basicScan() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put1 = new PutRequest(table, "s1", family, "q", "v1");
    final PutRequest put2 = new PutRequest(table, "s2", family, "q", "v2");
    final PutRequest put3 = new PutRequest(table, "s3", family, "q", "v3");
    Deferred.group(client.put(put1), client.put(put2),
                   client.put(put3)).join();
    // Scan the same 3 rows created above twice.
    for (int i = 0; i < 2; i++) {
      LOG.info("------------ iteration #" + i);
      final Scanner scanner = client.newScanner(table);
      scanner.setStartKey("s0");
      scanner.setStopKey("s9");
      // Callback class to keep scanning recursively.
      class cb implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
        private int n = 0;
        public Object call(final ArrayList<ArrayList<KeyValue>> rows) {
          if (rows == null) {
            return null;
          }
          n++;
          try {
            assertSizeIs(1, rows);
            final ArrayList<KeyValue> kvs = rows.get(0);
            final KeyValue kv = kvs.get(0);
            assertSizeIs(1, kvs);
            assertEq("s" + n, kv.key());
            assertEq("q", kv.qualifier());
            assertEq("v" + n, kv.value());
            return scanner.nextRows(1).addCallback(this);
          } catch (AssertionError e) {
            // Deferred doesn't catch Errors on purpose, so transform any
            // assertion failure into an Exception.
            throw new RuntimeException("Asynchronous failure", e);
          }
        }
      }
      try {
        scanner.nextRows(1).addCallback(new cb()).join();
      } finally {
        scanner.close().join();
      }
    }
  }

  /** Scan with multiple qualifiers. */
  @Test
  public void scanWithQualifiers() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put1 = new PutRequest(table, "k", family, "a", "val1");
    final PutRequest put2 = new PutRequest(table, "k", family, "b", "val2");
    final PutRequest put3 = new PutRequest(table, "k", family, "c", "val3");
    Deferred.group(client.put(put1), client.put(put2),
                   client.put(put3)).join();
    final Scanner scanner = client.newScanner(table);
    scanner.setFamily(family);
    scanner.setQualifiers(new byte[][] { { 'a' }, { 'c' } });
    final ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows(2).join();
    assertSizeIs(1, rows);
    final ArrayList<KeyValue> kvs = rows.get(0);
    assertSizeIs(2, kvs);
    assertEq("val1", kvs.get(0).value());
    assertEq("val3", kvs.get(1).value());
  }

  /** Write a few KVs and delete them in one batch */
  @Test
  public void multiDelete() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put2 = new PutRequest(table, "mdk1", family, "q2", "val2");
    client.put(put2).join();
    final PutRequest put3 = new PutRequest(table, "mdk2", family, "q3", "val3");
    client.put(put3).join();
    final PutRequest put1 = new PutRequest(table, "mdk1", family, "q1", "val1");
    client.put(put1).join();
    final DeleteRequest del2 = new DeleteRequest(table, "mdk1", family, "q2");
    final DeleteRequest del3 = new DeleteRequest(table, "mdk2", family, "q3");
    final DeleteRequest del1 = new DeleteRequest(table, "mdk1", family, "q1");
    Deferred.group(client.delete(del2), client.delete(del3),
                   client.delete(del1)).join();
    GetRequest get = new GetRequest(table, "mdk1");
    ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(0, kvs);
    get = new GetRequest(table, "mdk2");
    kvs = client.get(get).join();
    assertSizeIs(0, kvs);
  }

  /** Write a few KVs in different regions and delete them in one batch */
  @Test
  public void multiRegionMultiDelete() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final String table1 = args[0] + "1";
    final String table2 = args[0] + "2";
    createOrTruncateTable(client, table1, family);
    createOrTruncateTable(client, table2, family);
    final PutRequest put2 = new PutRequest(table1, "mdk1", family, "q2", "val2");
    client.put(put2).join();
    final PutRequest put3 = new PutRequest(table1, "mdk2", family, "q3", "val3");
    client.put(put3).join();
    final PutRequest put1 = new PutRequest(table2, "mdk1", family, "q1", "val1");
    client.put(put1).join();
    final DeleteRequest del2 = new DeleteRequest(table1, "mdk1", family, "q2");
    final DeleteRequest del3 = new DeleteRequest(table1, "mdk2", family, "q3");
    final DeleteRequest del1 = new DeleteRequest(table2, "mdk1", family, "q1");
    Deferred.group(client.delete(del2), client.delete(del3),
                   client.delete(del1)).join();
    GetRequest get = new GetRequest(table1, "mdk1");
    ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(0, kvs);
    get = new GetRequest(table1, "mdk2");
    kvs = client.get(get).join();
    assertSizeIs(0, kvs);
    get = new GetRequest(table2, "mdk1");
    kvs = client.get(get).join();
    assertSizeIs(0, kvs);
  }

  /** Attempt to write a column family that doesn't exist. */
  @Test
  public void putNonexistentFamily() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put = new PutRequest(table, "k", family + family,
                                          "q", "val");
    try {
      client.put(put).join();
    } catch (NoSuchColumnFamilyException e) {
      assertEquals(put, e.getFailedRpc());
      return;
    }
    throw new AssertionError("Should never be here");
  }

  /** Send a bunch of edits with one that references a non-existent family. */
  @Test
  public void multiPutWithOneBadRpcInBatch() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put1 = new PutRequest(table, "mk1", family, "m1", "mpb1");
    // The following edit is destined to a non-existent family.
    final PutRequest put2 = new PutRequest(table, "mk2", family + family,
                                           "m2", "mpb2");
    final PutRequest put3 = new PutRequest(table, "mk3", family, "m3", "mpb3");
    try {
      final ArrayList<Deferred<Object>> ds = new ArrayList<Deferred<Object>>(3);
      ds.add(client.put(put1));
      ds.add(client.put(put2));
      ds.add(client.put(put3));
      Deferred.groupInOrder(ds).join();
    } catch (DeferredGroupException e) {
      final ArrayList<Object> results = e.results();
      final Object res2 = results.get(1);
      if (!(res2 instanceof NoSuchColumnFamilyException)) {
        throw new AssertionError("res2 wasn't a NoSuchColumnFamilyException: "
                                 + res2);
      }
      assertEquals(put2, ((NoSuchColumnFamilyException) res2).getFailedRpc());
      final GetRequest get1 = new GetRequest(table, "mk1", family, "m1");
      ArrayList<KeyValue> kvs = client.get(get1).join();
      assertSizeIs(1, kvs);
      assertEq("mpb1", kvs.get(0).value());
      final GetRequest get2 = new GetRequest(table, "mk2", family, "m2");
      assertSizeIs(0, client.get(get2).join());
      final GetRequest get3 = new GetRequest(table, "mk3", family, "m3");
      kvs = client.get(get3).join();
      assertSizeIs(1, kvs);
      assertEq("mpb3", kvs.get(0).value());
      return;
    }
    throw new AssertionError("Should never be here");
  }

  /** Lots of buffered counter increments from multiple threads. */
  @Test
  public void bufferedIncrementStressTest() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key1 = "cnt1".getBytes();  // Spread the increments..
    final byte[] key2 = "cnt2".getBytes();  // .. over these two counters.
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del1 = new DeleteRequest(table, key1, family, qual);
    final DeleteRequest del2 = new DeleteRequest(table, key2, family, qual);
    Deferred.group(client.delete(del1), client.delete(del2)).join();

    final int nthreads = Runtime.getRuntime().availableProcessors() * 2;
    // The magic number comes from the limit on callbacks that Deferred
    // imposes.  We spread increments over two counters, hence the x 2.
    final int incr_per_thread = 8192 / nthreads * 2;
    final boolean[] successes = new boolean[nthreads];

    final class IncrementThread extends Thread {
      private final int num;
      public IncrementThread(final int num) {
        super("IncrementThread-" + num);
        this.num = num;
      }
      public void run() {
        try {
          doIncrements();
          successes[num] = true;
        } catch (Throwable e) {
          successes[num] = false;
          LOG.error("Uncaught exception", e);
        }
      }
      private void doIncrements() {
        for (int i = 0; i < incr_per_thread; i++) {
          final byte[] key = i % 2 == 0 ? key1 : key2;
          bufferIncrement(table, key, family, qual, 1);
        }
      }
    }

    final IncrementThread[] threads = new IncrementThread[nthreads];
    for (int i = 0; i < nthreads; i++) {
      threads[i] = new IncrementThread(i);
    }
    LOG.info("Starting to generate increments");
    for (int i = 0; i < nthreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < nthreads; i++) {
      threads[i].join();
    }

    LOG.info("Flushing all buffered increments.");
    client.flush().joinUninterruptibly();
    LOG.info("Done flushing all buffered increments.");

    // Check that we the counters have the expected value.
    final GetRequest[] gets = { mkGet(table, key1, family, qual),
                                mkGet(table, key2, family, qual) };
    for (final GetRequest get : gets) {
      final ArrayList<KeyValue> kvs = client.get(get).join();
      assertSizeIs(1, kvs);
      assertEquals(incr_per_thread * nthreads / 2,
                   Bytes.getLong(kvs.get(0).value()));
    }

    for (int i = 0; i < nthreads; i++) {
      assertEquals(true, successes[i]);  // Make sure no exception escaped.
    }
  }

  /** Increment coalescing with values too large to be coalesced. */
  @Test
  public void incrementCoalescingWithAmountsTooBig() throws Exception {
    client.setFlushInterval(SLOW_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key = "cnt".getBytes();
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del = new DeleteRequest(table, key, family, qual);
    del.setBufferable(false);
    client.delete(del).join();
    final long big = 1L << 48;  // Too big to be coalesced.
    final ArrayList<KeyValue> kvs = Deferred.group(
      bufferIncrement(table, key, family, qual, big),
      bufferIncrement(table, key, family, qual, big)
    ).addCallbackDeferring(new Callback<Deferred<ArrayList<KeyValue>>,
                                        ArrayList<Long>>() {
      public Deferred<ArrayList<KeyValue>> call(final ArrayList<Long> incs) {
        final GetRequest get = new GetRequest(table, key)
          .family(family).qualifier(qual);
        return client.get(get);
      }
    }).join();
    assertSizeIs(1, kvs);
    assertEquals(big + big, Bytes.getLong(kvs.get(0).value()));
    // Check we sent the right number of RPCs.
    assertEquals(2, client.stats().atomicIncrements());
  }

  /** Increment coalescing with large values that overflow. */
  @Test
  public void incrementCoalescingWithOverflowingAmounts() throws Exception {
    client.setFlushInterval(SLOW_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key = "cnt".getBytes();
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del = new DeleteRequest(table, key, family, qual);
    del.setBufferable(false);
    client.delete(del).join();
    final long big = 1L << 47;
    // First two RPCs can be coalesced.
    bufferIncrement(table, key, family, qual, big);
    bufferIncrement(table, key, family, qual, 1);
    // This one would cause an overflow, so will be sent as a separate RPC.
    // Overflow would happen because the max value is (1L << 48) - 1.
    bufferIncrement(table, key, family, qual, big);
    client.flush().joinUninterruptibly();
    final GetRequest get = new GetRequest(table, key)
      .family(family).qualifier(qual);
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    assertEquals(big + 1 + big, Bytes.getLong(kvs.get(0).value()));
    // Check we sent the right number of RPCs.
    assertEquals(2, client.stats().atomicIncrements());
  }

  /** Increment coalescing with negative values and underflows. */
  @Test
  public void incrementCoalescingWithUnderflowingAmounts() throws Exception {
    client.setFlushInterval(SLOW_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key = "cnt".getBytes();
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del = new DeleteRequest(table, key, family, qual);
    del.setBufferable(false);
    client.delete(del).join();
    final long big = -1L << 47;
    // First two RPCs can be coalesced.
    bufferIncrement(table, key, family, qual, big);
    bufferIncrement(table, key, family, qual, -1);
    // This one would cause an underflow, so will be sent as a separate RPC.
    // Overflow would happen because the max value is -1L << 48.
    bufferIncrement(table, key, family, qual, big);
    client.flush().joinUninterruptibly();
    final GetRequest get = new GetRequest(table, key)
      .family(family).qualifier(qual);
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    assertEquals(big - 1 + big, Bytes.getLong(kvs.get(0).value()));
    // Check we sent the right number of RPCs.
    assertEquals(2, client.stats().atomicIncrements());
  }

  /** Increment coalescing where the coalesced sum ends up being zero. */
  @Test
  public void incrementCoalescingWithZeroSumAmount() throws Exception {
    client.setFlushInterval(SLOW_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key = "cnt".getBytes();
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del = new DeleteRequest(table, key, family, qual);
    del.setBufferable(false);
    client.delete(del).join();
    bufferIncrement(table, key, family, qual, 1);
    bufferIncrement(table, key, family, qual, 2);
    bufferIncrement(table, key, family, qual, -3);
    client.flush().joinUninterruptibly();
    final GetRequest get = new GetRequest(table, key)
      .family(family).qualifier(qual);
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    assertEquals(0, Bytes.getLong(kvs.get(0).value()));
    // The sum was 0, but must have sent the increment anyway.
    assertEquals(1, client.stats().atomicIncrements());
  }

  /** Helper method to create an atomic increment request.  */
  private Deferred<Long> bufferIncrement(final byte[] table,
                                         final byte[] key, final byte[] family,
                                         final byte[] qual, final long value) {
    return
      client.bufferAtomicIncrement(new AtomicIncrementRequest(table, key,
                                                              family, qual,
                                                              value));
  }

  /** Helper method to create a get request.  */
  private static GetRequest mkGet(final byte[] table, final byte[] key,
                                  final byte[] family, final byte[] qual) {
    return new GetRequest(table, key).family(family).qualifier(qual);
  }

  /** Test regexp-based row key filtering.  */
  @Test
  public void keyRegexpFilter() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    final PutRequest put1 = new PutRequest(table, "krf accept:by the filter",
                                           family, "q", "krfv1");
    final PutRequest put2 = new PutRequest(table, "krf filtered out",
                                           family, "q", "krfv2");
    final PutRequest put3 = new PutRequest(table, "krf this is Accepted too",
                                           family, "q", "krfv3");
    Deferred.group(client.put(put1), client.put(put2),
                   client.put(put3)).join();
    final Scanner scanner = client.newScanner(table);
    scanner.setFamily(family);
    scanner.setStartKey("krf ");
    scanner.setStopKey("krf!");
    scanner.setKeyRegexp("[Aa]ccept(ed)?");
    final ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
    assertSizeIs(2, rows);
    ArrayList<KeyValue> kvs = rows.get(0);
    assertSizeIs(1, kvs);
    assertEq("krfv1", kvs.get(0).value());
    kvs = rows.get(1);
    assertSizeIs(1, kvs);
    assertEq("krfv3", kvs.get(0).value());
  }

  /** Simple column prefix filter tests.  */
  @Test
  public void columnPrefixFilter() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    // Keep only rows with a column qualifier that starts with "qa".
    final PutRequest put1 = new PutRequest(table, "cpf1", family, "qa1", "v1");
    final PutRequest put2 = new PutRequest(table, "cpf1", family, "qa2", "v2");
    final PutRequest put3 = new PutRequest(table, "cpf2", family, "qa3", "v3");
    final PutRequest put4 = new PutRequest(table, "cpf2", family, "qb4", "v4");
    Deferred.group(Deferred.group(client.put(put1), client.put(put2)),
                   Deferred.group(client.put(put3), client.put(put4))).join();
    final Scanner scanner = client.newScanner(table);
    scanner.setFamily(family);
    scanner.setStartKey("cpf1");
    scanner.setStopKey("cpf3");
    scanner.setFilter(new ColumnPrefixFilter("qa"));
    final ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
    assertSizeIs(2, rows);
    ArrayList<KeyValue> kvs = rows.get(0);
    assertSizeIs(2, kvs);
    assertEq("v1", kvs.get(0).value());
    assertEq("v2", kvs.get(1).value());
    kvs = rows.get(1);
    assertSizeIs(1, kvs);
    assertEq("v3", kvs.get(0).value());
  }

  /** Simple column range filter tests.  */
  @Test
  public void columnRangeFilter() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    // Keep rows that have a qualifier in between "qb" (inclusive) and "qd4"
    // (exclusive).  So only v2 and v3 should be returned by the scanner.
    final PutRequest put1 = new PutRequest(table, "crf1", family, "qa1", "v1");
    final PutRequest put2 = new PutRequest(table, "crf1", family, "qb2", "v2");
    final PutRequest put3 = new PutRequest(table, "crf2", family, "qc3", "v3");
    final PutRequest put4 = new PutRequest(table, "crf2", family, "qd4", "v4");
    Deferred.group(Deferred.group(client.put(put1), client.put(put2)),
                   Deferred.group(client.put(put3), client.put(put4))).join();
    final Scanner scanner = client.newScanner(table);
    scanner.setFamily(family);
    scanner.setStartKey("crf1");
    scanner.setStopKey("crf3");
    scanner.setFilter(new ColumnRangeFilter("qb", true, "qd4", false));
    final ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
    assertSizeIs(2, rows);  // One KV from row "fl1" and one from "fl2".
    ArrayList<KeyValue> kvs = rows.get(0);
    assertSizeIs(1, kvs);
    assertEq("v2", kvs.get(0).value());
    kvs = rows.get(1);
    assertSizeIs(1, kvs);
    assertEq("v3", kvs.get(0).value());
  }

  /** Simple column filter list tests.  */
  @Test
  public void filterList() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
    // Keep rows that have both:
    //   - a row key that is exactly either "fl1" or "fl2".
    //   - a qualifier in between "qb" (inclusive) and "qd4" (exclusive).
    final ArrayList<ScanFilter> filters = new ArrayList<ScanFilter>(2);
    filters.add(new ColumnRangeFilter("qb", true, "qd4", false));
    filters.add(new KeyRegexpFilter("fl[12]$"));
    // Filtered out as we're looking due to qualifier being out of range:
    final PutRequest put1 = new PutRequest(table, "fl1", family, "qa1", "v1");
    // Kept by the filter:
    final PutRequest put2 = new PutRequest(table, "fl1", family, "qb2", "v2");
    // Filtered out because the row key doesn't match the regexp:
    final PutRequest put3 = new PutRequest(table, "fl1a", family, "qb3", "v3");
    // Kept by the filter:
    final PutRequest put4 = new PutRequest(table, "fl2", family, "qc4", "v4");
    // Filtered out because the qualifier is on the exclusive upper bound:
    final PutRequest put5 = new PutRequest(table, "fl2", family, "qd5", "v5");
    // Filtered out because the qualifier is past the upper bound:
    final PutRequest put6 = new PutRequest(table, "fl2", family, "qd6", "v6");
    Deferred.group(Deferred.group(client.put(put1), client.put(put2),
                                  client.put(put3)),
                   Deferred.group(client.put(put4), client.put(put5),
                                  client.put(put6))).join();
    final Scanner scanner = client.newScanner(table);
    scanner.setFamily(family);
    scanner.setStartKey("fl0");
    scanner.setStopKey("fl9");
    scanner.setFilter(new FilterList(filters));
    final ArrayList<ArrayList<KeyValue>> rows = scanner.nextRows().join();
    assertSizeIs(2, rows);  // One KV from row "fl1" and one from "fl2".
    ArrayList<KeyValue> kvs = rows.get(0);
    assertSizeIs(1, kvs);   // KV from "fl1":
    assertEq("v2", kvs.get(0).value());
    kvs = rows.get(1);
    assertSizeIs(1, kvs);   // KV from "fl2":
    assertEq("v4", kvs.get(0).value());
  }

  @Test
  public void prefetchMeta() throws Exception {
    // Prefetch the metadata for a given table, then invasively probe the
    // region cache to demonstrate it is filled.
    client.prefetchMeta(table).join();

    Object region_info = Whitebox.invokeMethod(client, "getRegion",
                                               table.getBytes(),
                                               HBaseClient.EMPTY_ARRAY);
    assertNotNull(region_info);
  }

  /** Regression test for issue #2. */
  @Test
  public void regression2() throws Exception {
    try {
      final PutRequest put1 = new PutRequest(table, "k1", family, "q", "val1");
      final PutRequest put2 = new PutRequest(table, "k2", family, "q", "val2");
      LOG.info("Before calling put()");
      client.put(put1);
      client.put(put2);
      LOG.info("After calling put()");
    } finally {
      LOG.info("Before calling flush()");
      // Flushing immediately a cold client used to be troublesome because we
      // wouldn't do a good job at making sure that we can let the client do
      // the entire start-up dance (find ROOT, META, issue pending queries...).
      client.flush().join();
      LOG.info("After calling flush()");
      assertEquals(1, client.stats().numBatchedRpcSent());
    }
  }

  /** Regression test for issue #25. */
  @Test
  public void regression25() throws Exception {
    client.setFlushInterval(FAST_FLUSH);
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
  }

  /** Regression test for issue #40 (which was actually Netty bug #474). */
  @Test
  public void regression40() throws Exception {
    // Cause a META lookup first to avoid some DEBUG-level spam due to the
    // long key below.
    client.ensureTableFamilyExists(table, family).join();
    client.setFlushInterval(FAST_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    // 980 was empirically found to be the minimum size with which
    // Netty bug #474 gets triggered.  Bug got fixed in Netty 3.5.8.
    final byte[] key = new byte[980];
    key[0] = 'k';
    key[1] = '4';
    key[2] = '0';
    key[key.length - 1] = '*';
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final PutRequest put = new PutRequest(table, key, family, qual,
                                          new byte[0] /* empty */);
    final GetRequest get = new GetRequest(table, key);
    client.put(put).join();
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    KeyValue kv = kvs.get(0);
    assertEq("q", kv.qualifier());
    assertEq("", kv.value());
  }

  /** Regression test for issue #41. */
  @Test
  public void regression41() throws Exception {
    client.setFlushInterval(SLOW_FLUSH);
    final byte[] table = TestIntegration.table.getBytes();
    final byte[] key = "cnt".getBytes();
    final byte[] family = TestIntegration.family.getBytes();
    final byte[] qual = { 'q' };
    final DeleteRequest del = new DeleteRequest(table, key, family, qual);
    del.setBufferable(false);
    client.delete(del).join();
    final int iterations = 100000;
    for (int i = 0; i < iterations; i++) {
      bufferIncrement(table, key, family, qual, 1);
    }
    client.flush().joinUninterruptibly();
    final GetRequest get = new GetRequest(table, key)
      .family(family).qualifier(qual);
    final ArrayList<KeyValue> kvs = client.get(get).join();
    assertSizeIs(1, kvs);
    assertEquals(iterations, Bytes.getLong(kvs.get(0).value()));
  }

  private static <T> void assertSizeIs(final int size,
                                       final Collection<T> list) {
    final int actual = list.size();
    if (size != actual) {
      throw new AssertionError("List was expected to contain " + size
                 + " items but was found to contain " + actual + ": " + list);
    }
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

  private static final class JunitListener extends RunListener {
    @Override
    public void testStarted(final Description description) {
      LOG.info("Running test " + description.getMethodName());
    }

    @Override
    public void testFinished(final Description description) {
      LOG.info("Done running test " + description.getMethodName());
    }

    @Override
    public void testFailure(final Failure failure) {
      LOG.error("Test failed: " + failure.getDescription().getMethodName(),
                failure.getException());
    }

    @Override
    public void testIgnored(final Description description) {
      LOG.info("Test ignored: " + description.getMethodName());
    }
  }

}
