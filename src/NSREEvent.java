/*
 * Copyright (C) 2015  The Async HBase Authors.  All rights reserved.
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

import java.util.Arrays;

import com.google.common.base.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description of an NSRE that HBase resolved.
 * Given two NSRE metadata objects, this class determines how long the region
 * was unavailable and provides a likely reason for the unavailability.
 * @since 1.9
 */
public final class NSREEvent {
  // For investigate/informative purposes.
  private static final Logger LOG = LoggerFactory.getLogger(NSREEvent.class);

  /** Probable reason for NSRE. */
  public enum Reason {
    COMPACTED_OR_OFFLINE,
    MOVED,
    SPLIT,
    MOVED_AND_SPLIT, // likely due to region-server crash during split
    UNKNOWN;
  }

  // The name of the affected region.
  private final byte[] region_name;

  // Why the NSRE probably happened.
  private final Reason reason;

  // When, in milliseconds, the NSRE was first detected.
  private final long detection_time;

  // When, in milliseconds, the NSRE's resolution was detected.
  private final long resolution_time;
  
  // The original exception that triggered the NSRE
  private final RecoverableException original_exception;

  /**
   * Create a new NSRE event instance.
   * @param region_name The name of the region to which both instances of
   * metadata apply.
   * @param old_meta Data about the region immediately following the detection
   * of the NSRE.
   * @param new_meta Data about the region following the resolution of the
   * NSRE.
   */
  public NSREEvent(final byte[] region_name, final NSREMeta old_meta,
      final NSREMeta new_meta) {
    if (null == region_name) {
      throw new IllegalArgumentException("region name can't be null");
    }
    if (null == old_meta) {
      throw new IllegalArgumentException("old metadata can't be null");
    }
    if (null == new_meta) {
      throw new IllegalArgumentException("new metadata can't be null");
    }
    if (old_meta.getTime() > new_meta.getTime()) {
      throw new IllegalArgumentException("timestamp for old metadata can't " +
        "be greater than new-metadata timestamp");
    }

    this.region_name = region_name;
    reason = why(old_meta, new_meta);
    detection_time = old_meta.getTime();
    resolution_time = new_meta.getTime();
    original_exception = old_meta.getException();
  }

  /**
   * Given information gathered upon NSRE detection and after NSRE resolution,
   * attempt to determine why the exception occurred.
   * @param old_meta The region state when the NSRE was detected.
   * @param new_meta The new region state, now that the NSRE is resolved.
   * @return probable reason for NSRE.
   */
  static Reason why(final NSREMeta old_meta, final NSREMeta new_meta) {
    if (Arrays.equals(old_meta.getStartKey(), new_meta.getStartKey())) {
      if (Arrays.equals(old_meta.getStopKey(), new_meta.getStopKey())) {
        if (old_meta.getRemoteAddress().equals(new_meta.getRemoteAddress())) {
          // Same start key, stop key, and server.
          // Unfortunately, we can't easily disambiguate these two reasons.
          return Reason.COMPACTED_OR_OFFLINE;
        } else {
          // Same start and stop keys, but different server.
          return Reason.MOVED;
        }
      } else {
        if (old_meta.getRemoteAddress().equals(new_meta.getRemoteAddress())) {
          // Same server and start key, but different stop key.
          return Reason.SPLIT;
        } else {
          // Same start key, but different server and stop key.
          return Reason.MOVED_AND_SPLIT;
        }
      }
    } else {
      if (Arrays.equals(old_meta.getStopKey(), new_meta.getStopKey())) {
        if (old_meta.getRemoteAddress().equals(new_meta.getRemoteAddress())) {
          // Same server and stop key, but different start key.
          return Reason.SPLIT;
        } else {
          // Same stop key, but different server and start key.
          return Reason.MOVED_AND_SPLIT;
        }
      }
    }

    // Any other case should be impossible. In that case, we want to report an
    // unknown reason. If you see any of these, something is very wrong.
    {
      final StringBuilder oss = new StringBuilder();
      oss.append("Impossible unknown NSRE reason.\n")
         .append("Old metadata: ").append(old_meta).append("\n")
         .append("New metadata: ").append(new_meta).append("\n");
      LOG.warn(oss.toString());
    }
    return Reason.UNKNOWN; 
  }

  /** @return the name of the region affected by NSRE. */
  public String getRegionName() {
    return Bytes.pretty(region_name);
  }

  /** @return likely reason for the NSRE. */
  public Reason getReason() {
    return reason;
  }

  /** @return when, in milliseconds, the NSRE was first detected. */
  public long getDetectionTime() {
    return detection_time;
  }

  /** @return when, in milliseconds, the NSRE's resolution was detected. */
  public long getResolutionTime() {
    return resolution_time;
  }

  /** @return how long, in milliseconds, HBase took to resolve the NSRE. */
  public long getDuration() {
    return getResolutionTime() - getDetectionTime();
  }

  /** @return the original exception associated with this event */
  public String getOriginalException() {
    return original_exception.getClass().getCanonicalName() + " " + original_exception.getMessage();
  }
  
  @Override
  public int hashCode() {
    return Objects.hashCode(Arrays.hashCode(region_name), reason,
        detection_time, resolution_time);
  }

  @Override
  public boolean equals(final Object other) {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    final NSREEvent event = (NSREEvent)other;
    return Arrays.equals(region_name, event.region_name) &&
           Objects.equal(reason, event.reason) &&
           Objects.equal(detection_time, event.detection_time) &&
           Objects.equal(resolution_time, event.resolution_time);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("regionName=")
       .append(Bytes.pretty(region_name))
       .append(", reason=")
       .append(reason)
       .append(", detectionTime=")
       .append(detection_time)
       .append(", resolutionTime=")
       .append(resolution_time)
       .append(", duration=")
       .append(getDuration())
       .append(", originalException=")
       .append(original_exception);
    return buf.toString();
  }
}

