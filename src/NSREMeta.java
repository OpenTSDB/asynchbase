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

import com.google.common.base.Objects;

/**
 * Utility class to store information about an NSRE thrown by HBase.
 * @since 1.9
 */
final class NSREMeta {
  // UNIX timestamp in msec when NSRE first detected.
  private final long time;

  // HBase region server that owned region.
  private final String remote_address;

  // HBase key at which region started.
  private final byte[] start_key;

  // The original region info object
  private final RegionInfo original_region;
  
  // The original exception that triggered this NSRE. May be null if this is
  // for a resolution.
  private final RecoverableException original_exception; 

  /**
   * Create a new instance of the NSREMeta class.
   * @param time The time, in milliseconds, when the NSRE was first detected.
   * @param remote_address The hostname of the server on which the region lived.
   * @param original_region The original region info associated with this NSRE
   * @param original_exception The exception that triggered this NSRE
   * @throws IllegalArgumentException if time negative
   * @throws IllegalArgumentException if remote_address null or empty
   * @throws IllegalArgumentException if start_key null
   * @throws IllegalArgumentException if stop_key null
   */
  NSREMeta(final long time, final String remote_address, 
      final RegionInfo original_region, 
      final RecoverableException original_exception) {
    // No one in this world can you trust. Not men, not women, not beasts...
    // Not even other AsyncHBase programmers.
    if (time < 0L) {
      throw new IllegalArgumentException("timestamp can't be negative");
    }
    if (null == remote_address || remote_address.isEmpty()) {
      throw new IllegalArgumentException("remote address must not be null or" +
        " empty");
    }
    if (original_region == null) {
      throw new IllegalArgumentException("Region info cannot be null");
    }
    if (original_region.name() == null) {
      throw new IllegalArgumentException("Region name cannot be null");
    }
    this.time = time;
    this.remote_address = remote_address;
    this.original_region = original_region;
    this.original_exception = original_exception;
    
    final String[] name_parts = new String(original_region.name()).split(",");
    if (3 != name_parts.length) {
      // Region name format is borked.
      throw new IllegalArgumentException(
          "Region name not in expected format: " + original_region);
    }
    // We got what we expected.
    start_key = name_parts[1].getBytes();
  }

  /** @return the time in milliseconds at which the NSRE was first detected. */
  long getTime() {
    return time;
  }

  /** @return the hostname of the region server that owned the region. */
  String getRemoteAddress() {
    return remote_address;
  }

  /** @return the HBase key at which the region started. */
  byte[] getStartKey() {
    return start_key;
  }

  /** @return the HBase key at which the region ended. */
  byte[] getStopKey() {
    return original_region.stopKey();
  }
  
  /** @return the original region info object */
  RegionInfo getOriginalRegion() {
    return original_region;
  }
  
  /** @return the original exception. May be null. */
  RecoverableException getException() {
    return original_exception;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(time, remote_address,
        original_region, original_exception);
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

    final NSREMeta meta = (NSREMeta)other;
    return Objects.equal(time, meta.time) &&
           Objects.equal(remote_address, meta.remote_address) &&
           Objects.equal(original_region, meta.original_region) &&
           Objects.equal(original_exception, meta.original_exception);
  }

  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("time=")
       .append(time)
       .append(", remoteAddress=")
       .append(remote_address)
       .append(", regionInfo=")
       .append(original_region)
       .append(", originalException=")
       .append(original_exception);
    return buf.toString();
  }
}

