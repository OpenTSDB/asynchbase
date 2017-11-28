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


import java.util.concurrent.atomic.AtomicLong;

public class ScanMetrics extends ServerSideScanMetrics {

  public static final String RPC_CALLS_METRIC_NAME = "RPC_CALLS";
  public static final String MILLIS_BETWEEN_NEXTS_METRIC_NAME = "MILLIS_BETWEEN_NEXTS";
  public static final String NOT_SERVING_REGION_EXCEPTION_METRIC_NAME = "NOT_SERVING_REGION_EXCEPTION";
  public static final String BYTES_IN_RESULTS_METRIC_NAME = "BYTES_IN_RESULTS";
  public static final String REGIONS_SCANNED_METRIC_NAME = "REGIONS_SCANNED";
  public static final String RPC_RETRIES_METRIC_NAME = "RPC_RETRIES";

  /**
   * number of RPC calls
   */
  public final AtomicLong countOfRPCcalls = createCounter(RPC_CALLS_METRIC_NAME);

  /**
   * sum of milliseconds between sequential next calls
   */
  public final AtomicLong sumOfMillisSecBetweenNexts = createCounter(MILLIS_BETWEEN_NEXTS_METRIC_NAME);

  /**
   * number of NotServingRegionException caught
   */
  public final AtomicLong countOfNSRE = createCounter(NOT_SERVING_REGION_EXCEPTION_METRIC_NAME);

  /**
   * number of bytes in Result objects from region servers
   */
  public final AtomicLong countOfBytesInResults = createCounter(BYTES_IN_RESULTS_METRIC_NAME);

  /**
   * number of regions
   * Starts with 1 because it is incremented when a scanner switches to a next region.
   */
  public final AtomicLong countOfRegions = createCounter(REGIONS_SCANNED_METRIC_NAME, 1);

  /**
   * number of RPC retries
   */
  public final AtomicLong countOfRPCRetries = createCounter(RPC_RETRIES_METRIC_NAME);

  /**
   * constructor
   */
  public ScanMetrics() {
  }
}
