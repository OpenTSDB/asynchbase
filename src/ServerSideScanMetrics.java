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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ServerSideScanMetrics {

    /**
     * Hash to hold the String -&gt; Atomic Long mappings for each metric
     */
    private final Map<String, AtomicLong> counters = new HashMap<String, AtomicLong>();

    /**
     * Create a new counter with the specified name
     * @param counterName
     * @return {@link AtomicLong} instance for the counter with counterName
     */
    protected AtomicLong createCounter(String counterName) {
        AtomicLong c = new AtomicLong(0);
        counters.put(counterName, c);
        return c;
    }

    public static final String COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME = "ROWS_SCANNED";
    public static final String COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME = "ROWS_FILTERED";

    /**
     * number of rows filtered during scan RPC
     */
    public final AtomicLong countOfRowsFiltered = createCounter(COUNT_OF_ROWS_FILTERED_KEY_METRIC_NAME);

    /**
     * number of rows scanned during scan RPC. Not every row scanned will be returned to the client
     * since rows may be filtered.
     */
    public final AtomicLong countOfRowsScanned = createCounter(COUNT_OF_ROWS_SCANNED_KEY_METRIC_NAME);

    /**
     * @param counterName
     * @param value
     */
    public void setCounter(String counterName, long value) {
        AtomicLong c = this.counters.get(counterName);
        if (c != null) {
            c.set(value);
        }
    }

    /**
     * @param counterName
     * @return true if a counter exists with the counterName
     */
    public boolean hasCounter(String counterName) {
        return this.counters.containsKey(counterName);
    }

    /**
     * @param counterName
     * @return {@link AtomicLong} instance for this counter name, null if counter does not exist.
     */
    public AtomicLong getCounter(String counterName) {
        return this.counters.get(counterName);
    }

    /**
     * @param counterName
     * @param delta
     */
    public void addToCounter(String counterName, long delta) {
        AtomicLong c = this.counters.get(counterName);
        if (c != null) {
            c.addAndGet(delta);
        }
    }

    /**
     * Get all of the values since the last time this function was called. Calling this function will
     * reset all AtomicLongs in the instance back to 0.
     * @return A Map of String -&gt; Long for metrics
     */
    public Map<String, Long> getMetricsMap() {
        // Create a builder
        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
        // For every entry add the value and reset the AtomicLong back to zero
        for (Map.Entry<String, AtomicLong> e : this.counters.entrySet()) {
            builder.put(e.getKey(), e.getValue().getAndSet(0));
        }
        // Build the immutable map so that people can't mess around with it.
        return builder.build();
    }
}
