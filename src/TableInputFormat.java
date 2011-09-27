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
package org.hbase.async;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.stumbleupon.async.Deferred;

// REMIND: need faster scanner, using large streaming reads and callbacks
// REMIND: support scanning multiple column families
// REMIND: support scanning time ranges

/**
 * Input format for map reduce jobs.
 *
 * <ul>
 * <li><b>hbase.async.cluster</b> - quorum spec for the cluster
 * <li><b>hbase.mapreduce.inputtable</b> - the name of the input table
 * <li><b>hbase.mapreduce.scan.families</b> - column families to scan, seperated by spaces or commas
 * <li><b>hbase.mapreduce.scan.columns</b> - column to scan, prefixed by family, seperated by spaces or commas
 * </ul>
 *
 * @author Arthur van Hoff
 */
public class TableInputFormat extends InputFormat<BytesWritable,List<KeyValue>> implements Configurable 
{
    private final static byte[] META = ".META.".getBytes();
    private final static byte[] INFO = "info".getBytes();
    private final static byte[] SERVER = "server".getBytes();

    Configuration conf;

    public @Override RecordReader<BytesWritable,List<KeyValue>> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException 
    {
        return new TableRecordReader(conf, (TableSplit)split);
    }

    public @Override List<InputSplit> getSplits(JobContext context) throws IOException 
    {
        List<InputSplit> list = new ArrayList<InputSplit>();
        byte[] table = Bytes.UTF8(conf.get("hbase.mapreduce.inputtable"));
        HBaseClient client = new HBaseClient(conf.get("hbase.async.cluster"));
        try {
            TableSplit prev = null;
            final Scanner scanner = client.newScanner(META);
            try {
                scanner.setStartKey(table);
                scanner.setFamily(INFO);
                scanner.setQualifier(SERVER);

                String prefix = new String(table) + ",";

                try {
                    for (int b = 0 ; ; b++) {
                        Deferred<ArrayList<ArrayList<KeyValue>>> rows = scanner.nextRows(1000);
                        ArrayList<ArrayList<KeyValue>> results = rows.join();
                        if (results == null || results.size() == 0) {
                            break;
                        }
                        for (ArrayList<KeyValue> v : results) {
                            for (KeyValue kv : v) {
                                byte[] key = kv.key();
                                int i = 0;
                                for (; i < table.length && table[i] == key[i] ; i++);
                                if (i == table.length && key[i] == ',') {
                                    int j = key.length - 1;
                                    for (i++; key[j] != ',' ; j--);
                                    TableSplit next = new TableSplit();
                                    next.table = table;
                                    String hostport = new String(kv.value());
                                    next.host = hostport.substring(0, hostport.indexOf(':'));
                                    if (j - i > 0) {
                                        next.start = new byte[j - i];
                                        System.arraycopy(key, i, next.start, 0, next.start.length);
                                    }
                                    if (prev != null) {
                                        prev.end = next.start;
                                    }
                                    list.add(next);
                                    prev = next;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new IOException(e);
                }
            
            } finally {
                try {
                    scanner.close().join();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

        } finally {
            client.shutdown();
        }
        return list;
    }
        
    public @Override Configuration getConf() 
    {
        return conf;
    }
    public @Override void setConf(Configuration conf)
    {
        this.conf = conf;
    }

    //
    // Table split
    //
    public static class TableSplit extends InputSplit implements Writable, Comparable<TableSplit>
    {
        public byte[] table;
        public byte[] start;
        public byte[] end;
        public String host;

        public @Override String[] getLocations() 
        {
            return new String[] {host};
        }
        public @Override long getLength() 
        {
            return 0;
        }
        private byte[] readByteArray(DataInput in) throws IOException
        {
            byte[] data = new byte[in.readInt()];
            in.readFully(data);
            return data;
        }
        public @Override void readFields(DataInput in) throws IOException 
        {
            table = readByteArray(in);
            start = readByteArray(in);
            end = readByteArray(in);
            host = new String(readByteArray(in));
        }
        private void writeByteArray(DataOutput out, byte[] data) throws IOException
        {
            out.writeInt(data.length);
            out.write(data, 0, data.length);
        }
        public @Override void write(DataOutput out) throws IOException 
        {
            writeByteArray(out, table);
            writeByteArray(out, start);
            writeByteArray(out, end);
            writeByteArray(out, Bytes.UTF8(host));
        }
        public @Override String toString() 
        {
            return String.format("TableSplit[%s,%s,%s,%s]", new String(table), new String(start), new String(end), host);
        }
        public @Override int compareTo(TableSplit split) 
        {
            return Bytes.memcmp(start, split.start);
        }
        public @Override boolean equals(Object other) 
        {
            if (other instanceof TableSplit) {
                TableSplit o = (TableSplit)other;
                return Bytes.equals(table, o.table) && Bytes.equals(start, o.start) && Bytes.equals(end, o.end) && host.equals(o.host);
            }
            return false;
        }
    }
    
    //
    // Record Reader
    //
    public static class TableRecordReader extends RecordReader<BytesWritable,List<KeyValue>> 
    {
        HBaseClient client;
        byte[] table;
        TableSplit split;
        Scanner scanner;
        int chunkSize;
        BytesWritable key;
        int index;
        ArrayList<ArrayList<KeyValue>> values;
        ArrayList<KeyValue> current;

        TableRecordReader(Configuration conf, TableSplit split) throws IOException
        {
            this.client = new HBaseClient(conf.get("hbase.async.cluster"));
            this.chunkSize = Integer.valueOf(conf.get("hbase.async.chunk.size", "1000"));
            this.table = Bytes.UTF8(conf.get("hbase.mapreduce.inputtable"));
            this.split = split;
            this.scanner = client.newScanner(table);
            this.key = new BytesWritable();
            // REMIND: set additional constraints
            if (conf.get("hbase.mapreduce.scan.family") != null) {
                for (String fam : conf.get("hbase.mapreduce.scan.families").split("[ ,]")) {
                    scanner.setFamily(fam);
                }
            }
            if (conf.get("hbase.mapreduce.scan.columns") != null) {
                for (String col : conf.get("hbase.mapreduce.scan.columns").split("[ ,]")) {
                    //scanner.addFilter(col);
                    scanner.setFamily(col.substring(0, col.indexOf(':')));
                    scanner.setQualifier(col.substring(col.indexOf(':')+1));
                }
            }
        }
        public @Override void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException 
        {
        }
        public @Override BytesWritable getCurrentKey() throws IOException
        {
            return key;
        }
        public @Override List<KeyValue> getCurrentValue() throws IOException, InterruptedException 
        {
            return current;
        }
        public @Override boolean nextKeyValue() throws IOException, InterruptedException 
        {
            if (values == null || index == values.size()) {
                try {
                    index = 0;
                    values = scanner.nextRows(chunkSize).join();
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
            if (values != null && index < values.size()) {
                current = values.get(index++);
                key = new BytesWritable(current.get(0).key());
                return true;
            }
            return false;
        }
        public @Override float getProgress() 
        {
            return 0f;
        }
        public @Override void close() 
        {
            try {
                client.shutdown().join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
