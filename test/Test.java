/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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

import com.google.common.base.Joiner;
import org.junit.Ignore;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple command-line interface to quickly test async HBase.
 */
@Ignore // ignore for test runners
final class Test {

  private static final Logger LOG = Common.logger(Test.class);

  private static final HashMap<String, Cmd> commands;
  static {
    commands = new HashMap<String, Cmd>();
    commands.put("icv", new icv());
    commands.put("micv", new micv());
    commands.put("scan", new scan());
    commands.put("mscan", new mscan());
    final get get = new get();  // get get get!!
    commands.put("get", get);
    commands.put("lget", get);
    final put put = new put();  // put put put!!
    commands.put("put", put);
    commands.put("lput", put);
    final delete delete = new delete();  // delete delete delete!!
    commands.put("delete", delete);
    commands.put("ldelete", delete);
    final cas cas = new cas();  // cas cas cas!!
    commands.put("cas", cas);
    commands.put("lcas", cas);
  }

  private static void printUsage() {
    System.err.println("Usage: " + Test.class.getSimpleName()
                       + " <zk quorum> <cmd> <table> [args]\n"
                       + "Available commands:\n"
                       + "  get <table> <key> [family] [qualifiers ...]\n"
                       + "  icv <table> <key> <family> <qualifier> [amount]\n"
                       + "  micv <table> <key> <family> <qualifiers> [amounts]\n"
                       + "  put <table> <key> <family> <qualifier> <value>\n"
                       + "  delete <table> <key> [<family> [<qualifier>]]\n"
                       + "  scan <table> [start] [family] [qualifier] [stop] [regexp]\n"
                       + "  mscan <table> [start] [family[:qualifier[,qualifier]]/[family...]] [stop] [regexp]\n"
                       + "  cas <table> <key> <family> <qualifier> <expected> <value>\n"
                       + "Variants that acquire an explicit row-lock:\n"
                       + "  lget <table> <key> [family] [qualifiers ...]\n"
                       + "  lput <table> <key> <family> <qualifier> <value>\n"
                       + "  ldelete <table> <key> <family> <qualifier>\n"
                       + "  lcas <table> <key> <family> <qualifier> <expected> <value>\n"
                      );
  }

  private static void fatalUsage(final String msg, final int rv) {
    System.err.println(msg);
    printUsage();
    System.exit(rv);
  }

  private static void ensureArguments(final String[] args,
                                      final int min,
                                      final int max) {
    if (args.length < min) {
      fatalUsage("Not enough arguments, need at least " + min, 1);
    } else if (args.length > max && max > min) {
      fatalUsage("Too many arguments, need at most " + max, 1);
    }
  }

  public static void main(final String[] args) throws Exception {
    ensureArguments(args, 3, -1);

    final Cmd cmd = commands.get(args[1]);
    if (cmd == null) {
      fatalUsage("Unknown command: " + args[1], 2);
    }

    final HBaseClient client = new HBaseClient(args[0]);

    try {
      cmd.execute(client, args);
    } catch (Exception e) {
      LOG.error("Unexpected exception caught in main", e);
    }

    System.out.println("Starting shutdown...");
    LOG.debug("Shutdown returned " + client.shutdown().joinUninterruptibly());
    System.out.println("Exiting...");
  }

  private static interface Cmd {
    void execute(HBaseClient client, String[] args) throws Exception;
  }

  private static final class get implements Cmd {
    public void execute(final HBaseClient client, String[] args) throws Exception {
      ensureArguments(args, 4, 64);
      final GetRequest get = new GetRequest(args[2], args[3]);
      if (args.length > 4) {
        get.family(args[4]);
      }
      if (args.length > 5) {
        if (args.length == 6) {
          get.qualifier(args[5]);
        } else {
          final byte[][] qualifiers = new byte[args.length - 5][];
          for (int i = 5; i < args.length; i++) {
            qualifiers[i - 5] = args[i].getBytes();
          }
          get.qualifiers(qualifiers);
        }
      }
      RowLock lock = null;
      if (args[1].charAt(0) == 'l') {  // locked version of the command
        final RowLockRequest rlr = new RowLockRequest(args[2], args[3]);
        lock = client.lockRow(rlr).joinUninterruptibly();
        LOG.info("Acquired explicit row lock: " + lock);
      }
      args = null;
      try {
        final ArrayList<KeyValue> result = client.get(get).joinUninterruptibly();
        LOG.info("Get result=" + result);
      } catch (Exception e) {
        LOG.error("Get failed", e);
      } finally {
        if (lock != null) {
          client.unlockRow(lock).joinUninterruptibly();
          LOG.info("Released explicit row lock: " + lock);
        }
      }
    }
  }

  private static final class icv implements Cmd {
    public void execute(final HBaseClient client, String[] args) {
      ensureArguments(args, 6, 7);
      final AtomicIncrementRequest icv =
        new AtomicIncrementRequest(args[2], args[3], args[4], args[5]);
      if (args.length > 6) {
        icv.setAmount(Long.parseLong(args[6]));
      }
      args = null;
      try {
        final long result = client.atomicIncrement(icv).joinUninterruptibly();
        LOG.info("ICV result=" + result);
      } catch (Exception e) {
        LOG.error("ICV failed", e);
      }
    }
  }

  private static final class micv implements Cmd {
    public void execute(final HBaseClient client, String[] args) {
      ensureArguments(args, 6, 7);
      final MultiColumnAtomicIncrementRequest micv =
        new MultiColumnAtomicIncrementRequest(args[2], args[3], args[4], args[5].split(","));
      if (args.length > 6) {
        String[] raw = args[6].split(",");
        long[] amounts = new long[raw.length];
        for(int i = 0; i < amounts.length; i++) {
          amounts[i] = Long.parseLong(raw[i]);
        }
        micv.setAmounts(amounts);
      }
      args = null;
      try {
        final Map<byte[], Long> result = client.atomicIncrements(micv).joinUninterruptibly();
        final String formatted = Joiner.on("->").withKeyValueSeparator(";").join(result);
        LOG.info("MICV result=" + formatted);
      } catch (Exception e) {
        LOG.error("ICV failed", e);
      }
    }
  }

  private static final class put implements Cmd {
    public void execute(final HBaseClient client, String[] args) throws Exception {
      ensureArguments(args, 7, 7);
      RowLock lock = null;
      if (args[1].charAt(0) == 'l') {  // locked version of the command
        final RowLockRequest rlr = new RowLockRequest(args[2], args[3]);
        lock = client.lockRow(rlr).joinUninterruptibly();
        LOG.info("Acquired explicit row lock: " + lock);
      }
      final PutRequest put = lock == null
        ? new PutRequest(args[2], args[3], args[4], args[5], args[6])
        : new PutRequest(args[2], args[3], args[4], args[5], args[6], lock);
      args = null;
      try {
        final Object result = client.put(put).joinUninterruptibly();
        LOG.info("Put result=" + result);
      } catch (Exception e) {
        LOG.error("Put failed", e);
      } finally {
        if (lock != null) {
          client.unlockRow(lock).joinUninterruptibly();
          LOG.info("Released explicit row lock: " + lock);
        }
      }
    }
  }

  private static final class delete implements Cmd {
    public void execute(final HBaseClient client, String[] args) throws Exception {
      ensureArguments(args, 4, 6);
      RowLock lock = null;
      if (args[1].charAt(0) == 'l') {  // locked version of the command
        ensureArguments(args, 6, 6);
        final RowLockRequest rlr = new RowLockRequest(args[2], args[3]);
        lock = client.lockRow(rlr).joinUninterruptibly();
        LOG.info("Acquired explicit row lock: " + lock);
      }
      final DeleteRequest delete;
      if (lock == null) {
        switch (args.length) {
          case 4: delete = new DeleteRequest(args[2], args[3]); break;
          case 5: delete = new DeleteRequest(args[2], args[3], args[4]); break;
          case 6: delete = new DeleteRequest(args[2], args[3], args[4], args[5]); break;
          default: throw new AssertionError("Should never be here");
        }
      } else {
        delete = new DeleteRequest(args[2], args[3], args[4], args[5], lock);
      }
      args = null;
      try {
        final Object result = client.delete(delete).joinUninterruptibly();
        LOG.info("Delete result=" + result);
      } catch (Exception e) {
        LOG.error("Delete failed", e);
      } finally {
        if (lock != null) {
          client.unlockRow(lock).joinUninterruptibly();
          LOG.info("Released explicit row lock: " + lock);
        }
      }
    }
  }

  private static final class scan implements Cmd {
    @SuppressWarnings("fallthrough")
    public void execute(final HBaseClient client, String[] args) {
      ensureArguments(args, 3, 8);
      final Scanner scanner = client.newScanner(args[2]);
      switch (args.length) {
        case 8: scanner.setKeyRegexp(args[7]);
        case 7: scanner.setStopKey(args[6]);
        case 6: scanner.setQualifier(args[5]);
        case 5: scanner.setFamily(args[4]);
        case 4: scanner.setStartKey(args[3]);
      }
      args = null;
      LOG.info("Start scanner=" + scanner);
      try {
        ArrayList<ArrayList<KeyValue>> rows;
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          LOG.info("scanned results=" + rows + " from " + scanner);
        }
      } catch (Exception e) {
        LOG.error("Scan failed", e);
      }
    }
  }

  private static final class mscan implements Cmd {
    @SuppressWarnings("fallthrough")
    public void execute(final HBaseClient client, String[] args) {
      ensureArguments(args, 3, 7);
      final Scanner scanner = client.newScanner(args[2]);
      switch (args.length) {
        case 7: scanner.setKeyRegexp(args[6]);
        case 6: scanner.setStopKey(args[5]);
        case 5:
          final String columns = args[4];
          final ArrayList<byte[]> families = new ArrayList<byte[]>();
          final ArrayList<byte[][]> qualifiers = new ArrayList<byte[][]>();
          for (String spec : columns.split("/")) {
            final String[] family = spec.split(":");
            families.add(family[0].getBytes());
            if (family.length == 1) {
              qualifiers.add(null);
            } else {
              final String[] quals = family[1].split(",");
              final byte[][] qb = new byte[quals.length][];
              for (int i = 0; i < qb.length; i++) {
                qb[i] = quals[i].getBytes();
              }
              qualifiers.add(qb);
            }
          }
          scanner.setFamilies(families.toArray(new byte[families.size()][]),
                              qualifiers.toArray(new byte[qualifiers.size()][][]));
        case 4: scanner.setStartKey(args[3]);
      }
      LOG.info("Start scanner=" + scanner);
      try {
        ArrayList<ArrayList<KeyValue>> rows;
        while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
          LOG.info("scanned results=" + rows + " from " + scanner);
        }
      } catch (Exception e) {
        LOG.error("Scan failed", e);
      }
    }
  }

  private static final class cas implements Cmd {
    public void execute(final HBaseClient client, String[] args) throws Exception {
      ensureArguments(args, 8, 8);
      RowLock lock = null;
      if (args[1].charAt(0) == 'l') {  // locked version of the command
        final RowLockRequest rlr = new RowLockRequest(args[2], args[3]);
        lock = client.lockRow(rlr).joinUninterruptibly();
        LOG.info("Acquired explicit row lock: " + lock);
      }
      final PutRequest put = lock == null
        ? new PutRequest(args[2], args[3], args[4], args[5], args[7])
        : new PutRequest(args[2], args[3], args[4], args[5], args[7], lock);
      final String expected = args[6];
      args = null;
      try {
        final boolean ok = client.compareAndSet(put, expected).joinUninterruptibly();
        LOG.info("CAS "
                 + (ok ? "succeeded" : "failed: value wasn't " + expected));
      } catch (Exception e) {
        LOG.error("CAS failed", e);
      } finally {
        if (lock != null) {
          client.unlockRow(lock).joinUninterruptibly();
          LOG.info("Released explicit row lock: " + lock);
        }
      }
    }
  }

}
