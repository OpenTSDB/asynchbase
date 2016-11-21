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

import com.google.common.cache.CacheStats;
import com.stumbleupon.async.Callback;
import org.junit.Ignore;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Random;

/**
 * Integration test for multi-column increment coalescing.
 *
 * Requires a locally running HBase cluster.
 */
@Ignore // ignore for test runners
final class TestMultiColumnIncrementCoalescing {

  private static final Logger LOG =
    Common.logger(TestMultiColumnIncrementCoalescing.class);

  public static void main(final String[] args) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.warn("Debug logging enabled, this test will flood it pretty hard.");
    }
    if (Runtime.getRuntime().maxMemory() < 1992294400L) {
      LOG.error("This test requires at least 2GB of RAM to run.");
      LOG.error("Use JVM_ARGS='-Xmx2g -Xms2g'.");
      System.exit(3);
    }
    final HBaseClient client = Common.getOpt(TestMultiColumnIncrementCoalescing.class,
                                             args);
    final byte[] table = args[0].getBytes();
    final byte[] family = args[1].getBytes();
    try {
      test(client, table, family);
    } finally {
      client.shutdown().join();
    }
  }

  private static volatile boolean failed = false;
  private static final Callback<Exception, Exception> LOG_ERROR =
    new Callback<Exception, Exception>() {
      public Exception call(final Exception e) {
        LOG.error("RPC failed", e);
        failed = true;
        return e;
      }
    };

  /** Number of increments for each row.  */
  private static final int ICV_PER_ROW = 1000;

  /** Number of different keys we'll do increments on.  */
  private static final int NUM_ROWS = 1000;

  private static final byte[][] QUALIFIERS = { new byte[] {'c', 'n', 't', '1'}, new byte[] {'c', 'n', 't', '2'}};

  private static void test(final HBaseClient client,
                           final byte[] table,
                           final byte[] family) throws Exception {
    final Random rnd = new Random();

    final class IncrementThread extends Thread {

      private final int num;

      IncrementThread(final int num) {
        super("IncrementThread-" + num);
        this.num = num;
      }

      public void run() {
        for (int iteration = 0; iteration < ICV_PER_ROW; iteration++) {
          final int r = rnd.nextInt(NUM_ROWS);
          final int n = r + NUM_ROWS;
          for (int i = r; i < n; i++) {
            micv(i);
          }
        }
      }

      private void micv(final int i) {
        final byte[] key = key(i);
        final MultiColumnAtomicIncrementRequest incr =
          new MultiColumnAtomicIncrementRequest(table, key, family, QUALIFIERS);
        client.bufferMultiColumnAtomicIncrement(incr).addErrback(LOG_ERROR);
      }

    }

    client.ensureTableFamilyExists(table, family).join();

    LOG.info("Deleting existing rows...");
    for (int i = 0; i < NUM_ROWS; i++) {
      for (byte[] qualifier: QUALIFIERS) {
        client.delete(new DeleteRequest(table, key(i), family, qualifier))
            .addErrback(LOG_ERROR);
      }
    }
    client.flush().join();
    LOG.info("Done deleting existing rows.");

    final int nthreads = Runtime.getRuntime().availableProcessors() * 2;
    final IncrementThread[] threads = new IncrementThread[nthreads];
    for (int i = 0; i < nthreads; i++) {
      threads[i] = new IncrementThread(i);
    }
    long timing = System.nanoTime();
    for (int i = 0; i < nthreads; i++) {
      threads[i].start();
    }
    for (int i = 0; i < nthreads; i++) {
      threads[i].join();
    }
    client.flush().join();
    timing = (System.nanoTime() - timing) / 1000000;
    final int nrpcs = nthreads * ICV_PER_ROW * NUM_ROWS;
    LOG.info(nrpcs + " increments in " + timing + "ms = "
             + (nrpcs * 1000L / timing) + "/s");
    final CacheStats stats = client.stats().incrementBufferStats();
    LOG.info("Increments coalesced: " + stats.hitCount());
    LOG.info("Increments sent to HBase: " + stats.missCount());
    LOG.info("  due to cache evictions: " + stats.evictionCount());

    LOG.info("Reading all counters back from HBase and checking values...");
    for (byte[] qualifier: QUALIFIERS) {
      final Scanner scanner = client.newScanner(table);
      scanner.setStartKey(key(0));
      scanner.setStopKey(key(NUM_ROWS));
      scanner.setFamily(family);
      scanner.setQualifier(qualifier);
      ArrayList<ArrayList<KeyValue>> rows;
      final long expected = nthreads * ICV_PER_ROW;
      while ((rows = scanner.nextRows().join()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final long value = Bytes.getLong(row.get(0).value());
          if (value != expected) {
            LOG.error("Invalid count in " + row.get(0) + ": " + value);
            failed = true;
          }
        }
      }
    }
    LOG.info("Done checking counter values.");

    if (failed) {
      LOG.error("At least one counter increment failed!");
      System.exit(2);
    }
  }

  /** Returns the key to increment for a given number.  */
  private static final byte[] key(final int i) {
    return new byte[] { 'c', 'n', 't',
      (byte) ((i / 100 % 10) + '0'),
      (byte) ((i / 10 % 10) + '0'),
      (byte) ((i % 10) + '0')
    };
  }

}
