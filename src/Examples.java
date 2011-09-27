/*
 * Copyright (c) 2010, 2011  StumbleUpon, Inc.  All rights reserved.
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

import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

//
// create 'hbase_async_test', {NAME => 'fam1'}, {NAME => 'fam2'}
//

public class Examples
{
    final static String TABLE = "hbase_async_test";

    public static void usage()
    {
        System.err.printf("usage:\n");
        System.err.printf("  Examples <quorum> [-put <count>]\n");
        System.err.printf("  Examples <quorum> [-get <row>]\n");
        System.err.printf("  Examples <quorum> [-scan]\n");
    }

    public static void main(String[] argv) throws Exception
    {
        if (argv.length < 2) {
            System.err.printf("error: invalid arguments\n");
            usage();
            System.exit(1);
        }
        String quorum = argv[0];

        int result = 1;
        HBaseClient client = new HBaseClient(quorum);
        try {
            if (argv[1].equals("-put")) {
                result = put(client, argv);
            } else if (argv[1].equals("-get")) {
                result = get(client, argv);
            } else if (argv[1].equals("-scan")) {
                result = scan(client, argv);
            } else {
                System.err.printf("error: invalid arguments\n");
                usage();
            }
        } finally {
            client.shutdown();
        }

        if (result != 0) {
            System.err.printf("exit code %d\n", result);
        }
        System.exit(result);
    }

    public static int put(HBaseClient client, String[] argv) throws Exception
    {
        int count = 0, n = Integer.valueOf(argv[2]);

        for (int i = 0 ; i < n ; i++) {
            client.put(new PutRequest(TABLE, "key" + i, "fam1", "col1", "value1"));
            client.put(new PutRequest(TABLE, "key" + i, "fam1", "col2", "value2"));
            client.put(new PutRequest(TABLE, "key" + i, "fam2", "col1", "value3"));
            client.put(new PutRequest(TABLE, "key" + i, "fam2", "col2", "value4"));
        }
        System.out.printf("put %,d values\n", n);
        
        if (true) {
            // REMIND: this should not be needed, but we need it because of a race condition in shutdown
            Thread.sleep(5000);
            client.flush().join();
        }
        return 0;
    }

    public static int get(HBaseClient client, String[] argv) throws Exception
    {
        int count = 0;
        for (KeyValue val : client.get(new GetRequest(TABLE, argv[2])).join()) {
            print(count++, val);
        }
        System.out.printf("got %,d values\n", count);
        return count > 0 ? 0 : 1;
    }

    public static int scan(HBaseClient client, String[] argv) throws Exception
    {
        int count = 0;
        Scanner scanner = client.newScanner(TABLE);
        try {
            for (;;) {
                ArrayList<ArrayList<KeyValue>> results = scanner.nextRows(5).join();
                if (results == null) {
                    break;
                }
                for (ArrayList<KeyValue> values : results) {
                    for (KeyValue val : values) {
                        print(count++, val);
                    }
                }
            }
        } finally {
            scanner.close();
        }
        System.out.printf("found %,d values\n", count);
        return count > 0 ? 0 : 1;
    }

    static void print(int count, KeyValue val)
    {
        System.out.printf("%4d: %s, %s, %s, %s, %s\n", count, TABLE, new String(val.key()), new String(val.family()), new String(val.qualifier()), new String(val.value()));
    }
}
