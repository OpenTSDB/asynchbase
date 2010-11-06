/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
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
// no package

import java.util.ArrayList;
import java.util.HashMap;

import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.RowLock;
import org.hbase.async.RowLockRequest;
import org.hbase.async.Scanner;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Simple command-line interface to quickly test async HBase.
 */
final class Test {

  private static final Logger LOG = LoggerFactory.getLogger(Test.class);
  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  private static final HashMap<String, Cmd> commands;
  static {
    commands = new HashMap<String, Cmd>();
    commands.put("icv", new icv());
    commands.put("scan", new scan());
    final get get = new get();  // get get get!!
    commands.put("get", get);
    commands.put("lget", get);
    final put put = new put();  // put put put!!
    commands.put("put", put);
    commands.put("lput", put);
    final delete delete = new delete();  // delete delete delete!!
    commands.put("delete", delete);
    commands.put("ldelete", delete);
  }

  private static void printUsage() {
    System.err.println("Usage: " + Test.class.getSimpleName()
                       + " <zk quorum> <cmd> <table> [args]\n"
                       + "Available commands:\n"
                       + "  get <table> <key> [family] [qualifier]\n"
                       + "  icv <table> <key> <family> <qualifier> [amount]\n"
                       + "  put <table> <key> <family> <qualifier> <value>\n"
                       + "  delete <table> <key> [<family> <qualifier>]\n"
                       + "  scan <table> [start] [family] [qualifier] [stop] [regexp]\n"
                       + "Variants that acquire an explicit row-lock:\n"
                       + "  lget <table> <key> [family] [qualifier]\n"
                       + "  lput <table> <key> <family> <qualifier> <value>\n"
                       + "  ldelete <table> <key> <family> <qualifier>\n"
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
    System.exit(0);
  }

  private static interface Cmd {
    void execute(HBaseClient client, String[] args) throws Exception;
  }

  private static final class get implements Cmd {
    public void execute(final HBaseClient client, String[] args) throws Exception {
      ensureArguments(args, 4, 6);
      final GetRequest get = new GetRequest(args[2], args[3]);
      if (args.length > 4) {
        get.family(args[4]);
      }
      if (args.length > 5) {
        get.qualifier(args[5]);
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
      final DeleteRequest delete = lock == null
        ? (args.length == 4
           ? new DeleteRequest(args[2], args[3])
           : new DeleteRequest(args[2], args[3], args[4], args[5]))
        : new DeleteRequest(args[2], args[3], args[4], args[5], lock);
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

}
