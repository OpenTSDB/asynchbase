/*
 * Copyright (C) 2014-2020  The Async HBase Authors.  All rights reserved.
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

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

/**
 * {@link RegionClient} usage statistics.
 * <p>
 * This is an immutable snapshot of usage statistics of the region client.
 * Please note that not all the numbers in the snapshot are collected
 * atomically, so although each individual number is up-to-date as of
 * the time this object is created, small inconsistencies between numbers
 * can arise.
 * @since 1.7
 */
public final class RegionClientStats {

  private final long rpcs_sent;
  private final int individual_rpcs_sent;
  private final int inflight_rpcs;
  private final int pending_rpcs;
  private final long rpcid;
  private final boolean dead;
  private final String remote_endpoint;
  private final int pending_batched_rpcs;
  private final long rpcs_timedout;
  private final int rpcs_success;
  private final int rpcs_retried;
  private final long writes_blocked;
  private final int writes_blocked_by_rate_limiter;
  private final long rpc_response_timedout;
  private final long rpc_response_unknown;
  private final long inflight_breached;
  private final long pending_breached;
  private final Double rate_limit;
  private final ExtendedStats extended_stats;
  
  /** Package-private constructor.  */
  RegionClientStats(
      final long rpcs_sent,
      final int individual_rpcs_sent,
      final int rpcs_inflight,
      final int pending_rpcs,
      final long rpcid,
      final boolean dead,
      final String remote_endpoint,
      final int pending_batched_rpcs,
      final long rpcs_timedout,
      final int rpcs_success,
      final int rpcs_retried,
      final long writes_blocked,
      final long rpc_response_timedout,
      final long rpc_response_unknown,
      final long inflight_breached,
      final long pending_breached,
      final int writes_blocked_by_rate_limiter,
      final Double rate_limit,
      final ExtendedStats extended_stats
      ) {
    this.rpcs_sent = rpcs_sent;
    this.individual_rpcs_sent = individual_rpcs_sent;
    this.inflight_rpcs = rpcs_inflight;
    this.pending_rpcs = pending_rpcs;
    this.rpcid = rpcid;
    this.dead = dead;
    this.remote_endpoint = remote_endpoint;
    this.pending_batched_rpcs = pending_batched_rpcs;
    this.writes_blocked = writes_blocked;
    this.rpcs_timedout = rpcs_timedout;
    this.rpcs_success = rpcs_success;
    this.rpcs_retried = rpcs_retried;
    this.rpc_response_timedout = rpc_response_timedout;
    this.rpc_response_unknown = rpc_response_unknown;
    this.inflight_breached = inflight_breached;
    this.pending_breached = pending_breached;
    this.rate_limit = rate_limit == null ? Double.POSITIVE_INFINITY : rate_limit;
    this.writes_blocked_by_rate_limiter = writes_blocked_by_rate_limiter;
    this.extended_stats = extended_stats;
  }

  /**
   * Represents the total number of RPCs sent from this client to the server, 
   * whether they were successful or not. This will count multiple RPCs in a 
   * single MultiAction RPC as 1 sent RPC. For the total number of RPCs, including
   * those within batches, see {@link individualRpcsSent}.
   * @return The number of RPCs written to the socket buffer.
   */
  public long rpcsSent() {
    return rpcs_sent;
  }
  
  /**
   * The total number of RPCs sent through the client, including those nested
   * within a MultiAction batch.
   * @return The total number of RPCs written to the socket buffer.
   */
  public int individualRpcsSent() {
    return individual_rpcs_sent;
  }
  
  /**
   * Represents the number of RPCs that have been sent to the region client
   * and are currently waiting for a response. If this value increases then the
   * region server is likely overloaded.
   * @return the number of RPCs sent to region client waiting for response.
   */
  public int inflightRPCs() {
    return inflight_rpcs;
  }
  
  /**
   * The number of RPCs that are queued up and ready to be sent to the region
   * server. When an RPC is sent, this number should be decremented and 
   * {@code rpcsInFlight} incremented.
   * @return the number of RPCs queued and ready to be sent to region server.
   */
  public int pendingRPCs() {
    return pending_rpcs;
  }
  
  /**
   * The current RPC ID. This can be used to track the number of RPC requests
   * sent from this region client. Note that the counter is initialized at -1
   * when a new client has been instantiated but no RPCs sent. It increments
   * from there and may rollover quickly, returning negative values. This is
   * acceptable as HBase doesn't care about the RPC ID as long as it's unique.
   * @return the current RPC ID.
   */
  public long rpcID() {
    return rpcid;
  }
  
  /**
   * Whether or not this region client has been marked as dead and should not
   * be used for requests. This likely means the host or server has failed or
   * we simply can't connect any more.
   * @return whether this region client has been marked as dead and should not
   * be used for requests.
   */
  public boolean isDead() {
    return dead;
  }
  
  /**
   * The remote endpoint of the region server this client is connecting to.
   * @return the host name (or IP address) of the region server this client is
   * connecting to.
   */
  public String remoteEndpoint() {
    return remote_endpoint;
  }
  
  /**
   * The number of batched RPCs waiting to be sent to the server.
   * @return the number of batched RPCs waiting to be sent to the server.
   */
  public int pendingBatchedRPCs() {
    return pending_batched_rpcs;
  }
  
  /**
   * The number of RPCs that timed out due to not receiving a response from 
   * HBase in a configurable amount of time.
   * @return The number of RPCs timedout
   */
  public long rpcsTimedout() {
    return rpcs_timedout;
  }
  
  /** The number of RPCs that received a valid, non-exception response, from 
   * HBase.
   * @return The number of successful RPC responses.
   */
  public int rpcsSuccess() {
    return rpcs_success;
  }
  
  /**
   * The number of RPCs that had a retry count of 1 or greater and were sent
   * to the region server.
   * @return The number of RPCs that were retried.
   */
  public int rpcsRetried() {
    return rpcs_retried;
  }
  
  /**
   * The number of times sending an RPC was blocked due to the socket send
   * buffer being full. This means HBase was not consuming RPCs fast enough.
   * @return The number of writes blocked due to a full buffer.
   */
  public long writesBlocked() {
    return writes_blocked;
  }
  
  /**
   * Represents the number of responses that were received from HBase for RPCs
   * that were already timed out by the client. A fairly high number means
   * HBase is busy but recovered.
   * @return The number of late responses received from HBase
   */
  public long rpcResponsesTimedout() {
    return rpc_response_timedout;
  }
  
  /**
   * Represents the number of responses that were received from HBase that 
   * were for RPCs the region client supposedly did not send. This means 
   * something really strange happened, i.e. we had a corrupt packet or 
   * HBase is sending responses for another connection.
   * @return The number of unknown responses from HBase
   */
  public long rpcResponsesUnknown() {
    return rpc_response_unknown;
  }

  /**
   * The number of times RPCs were rejected due to there being too many RPCs
   * in flight and waiting for responses from the HBase server. Failed RPCs
   * throw a {@link PleaseThrottleException}
   * @return The number of times we rejected RPCs due to a full inflight queue.
   */
  public long inflightBreached() {
    return inflight_breached;
  }
  
  /**
   * The number of times RPCs were rejected due to there being too many RPCs 
   * in the pending queue, i.e. while we were waiting for HBase to respond to
   * the connection request. Failed RPCs throw a 
   * {@link PleaseThrottleException}
   * @return The number of times we rejected RPCs due to a full pending queue.
   */
  public long pendingBreached() {
    return pending_breached;
  }

  /** The current RPC rate limit per second. */
  public double rateLimit() {
    return rate_limit;
  }

  /** The total number of writes blocked by the limiter. */
  public int writesBlockedByRateLimiter() {
    return writes_blocked_by_rate_limiter;
  }

  /** @return The extended stats. May be null if not set.
   * @since 1.9 */
  public ExtendedStats extendedStats() {
    return extended_stats;
  }
  
  /**
   * Extended stats around the client.
   * @since 1.9
   */
  public static class ExtendedStats {
    private final Map<String, Long> exception_counters;
    private final int decode_called;
    private final int replays;
    private final int nsre_exceptions;
    private final int probes_sent;
    private final int probes_succeeded;
    private final int probes_nsred;
    private final int probes_with_exception;
    private final int probes_timedout;
    private final int cqtbes;
    private final long bytes_read;
    private final long bytes_written;
    
    public ExtendedStats(final int decode_called,
                         final int replays,
                         final int nsre_exceptions,
                         final int probes_sent,
                         final int probes_succeeded,
                         final int probes_nsred,
                         final int probes_with_exception,
                         final int probes_timedout,
                         final int cqtbes,
                         final long bytes_read,
                         final long bytes_written,
                         final Map<Class<?>, Counter> exception_counters) {
      this.decode_called = decode_called;
      this.replays = replays;
      this.nsre_exceptions = nsre_exceptions;
      this.probes_sent = probes_sent;
      this.probes_succeeded = probes_succeeded;
      this.probes_nsred = probes_nsred;
      this.probes_with_exception = probes_with_exception;
      this.probes_timedout = probes_timedout;
      this.cqtbes = cqtbes;
      this.bytes_read = bytes_read;
      this.bytes_written = bytes_written;
      
      if (exception_counters != null) {
        this.exception_counters = Maps.newHashMapWithExpectedSize(exception_counters.size());
        for (final Entry<Class<?>, Counter> entry : exception_counters.entrySet()) {
          this.exception_counters.put(entry.getKey().getSimpleName(), 
              entry.getValue().get());
        }
      } else {
        this.exception_counters = null;
      }
    }
    
    /**
     * The number of times the decode method was called with full or partial
     * responses from HBase.
     * @return How often the decode method was called
     */
    public int decodeCalled() {
      return decode_called;
    }
    
    /**
     * How often the decode method was replayed because we didn't have the full
     * payload in the network buffer. This is normal and can be used to diagnose
     * large payloads.
     * @return How often the decode method was called with additional payload for
     * and RPC.
     */
    public int replays() {
      return replays;
    }
    
    /**
     * The number of NotServingRegionExceptions received.
     * @return The number of NotServingRegionExceptions received.
     */
    public int NSREExceptions() {
      return nsre_exceptions;
    }

    /**
     * The number of NSRE probes sent through this client.
     * @return The number of NSRE probes sent.
     */
    public int probesSent() {
      return probes_sent;
    }

    /**
     * The number of NSRE probes with a successful response from the region 
     * server.
     * @return The number of NSRe probes with a valid response.
     */
    public int probesSucceeded() {
      return probes_succeeded;
    }

    /**
     * The number of NSRE probes that were called back with another NSRE meaning
     * the region has most likely not been updated in the HBase meta table yet
     * so we kept sending the probe here. NSREs are not included in the probes
     * with exception count.
     * @return The total number of probes NSREd.
     */
    public int probesNsred() {
      return probes_nsred;
    }

    /**
     * The number of NSRE probes called back with a non-NSRE exception.
     * @return The number of probes with an exception.
     */
    public int probesWithException() {
      return probes_with_exception;
    }

    /**
     * The total number of NSRE probes that were timed out waiting for a response.
     * @return The total number of probes timed out.
     */
    public int probesTimedout() {
      return probes_timedout;
    }

    /**
     * The total number of CallQueueTooBigExceptions received from the region
     * server. These indicate the server is busy.
     * @return The number of call queue too big exceptions from the server.
     */
    public int CQTBEs() {
      return cqtbes;
    }

    /**
     * The total number of bytes read since the existence of this region client.
     * @return The total number of bytes read off the socket.
     */
    public long bytesRead() {
      return bytes_read;
    }
    
    /**
     * The total number of bytes written since the existence of this region client.
     * @return The total number of bytes written to the socket.
     */
    public long bytesWritten() {
      return bytes_written;
    }
    
    /** @return A map of exception class names to the total number of occurrences
     * since the client started. */
    public Map<String, Long> exceptionCounters() {
      return exception_counters;
    }
    
  }
}
