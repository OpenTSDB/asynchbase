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

/**
 * A public class that is used to return region hosting information to users
 * via the API. All objects in this class are copies so modifications will not
 * affect the HBase client.
 * @since 1.7
 */
public final class RegionLocation {
  
  private final byte[] table;
  private final byte[] region_name;
  private final String host;
  private final int port;
  private final byte[] start_key;
  private final byte[] stop_key;

  /**
   * Package private ctor as we want read-only information for the caller to play
   * with. We'll make copies of all of the arrays so that the user can't modify
   * anything used by AsyncHBase internally.
   * @param region_info The region info to pull the table, name and stop key from
   * @param start_key The start key of the region
   * @param host The region server hosting the region. May be null if the region
   * isn't hosted anywhere.
   * @param port The port for the region server. May be zero if the region isn't
   * hosted anywhere.
   */
  RegionLocation(RegionInfo region_info, byte[] start_key, String host, int port) {
    table = Arrays.copyOf(region_info.table(), region_info.table().length);
    region_name = Arrays.copyOf(region_info.name(), region_info.name().length);
    // may be an empty array but shouldn't be null
    stop_key = Arrays.copyOf(region_info.stopKey(), region_info.stopKey().length);
    this.start_key = Arrays.copyOf(start_key, start_key.length);
    this.host = host;
    this.port = port;
  }
  
  /**
   * The start key for this region. If this is the first (or only) region for 
   * the table then the start key may be empty. The key should never be null 
   * but may have a length of 0.
   * @return The start key for the region
   */
  public byte[] startKey() {
    return start_key;
  }
  
  /**
   * The stop key for this region. If this is the last (or only) region for 
   * the table then the stop key may be empty. The key should never be null 
   * but may have a length of 0.
   * @return The stop key for the region
   */
  public byte[] stopKey() {
    return stop_key;
  }

  /**
   * The name of the host currently serving this region.
   * NOTE: If the region is not hosted, due to a split, move, offline  or server 
   * failure, then the result may be null.
   * @return The name of the region server hosting the region or null if the
   * region is offline.
   */
  public String getHostname() {
    return host;
  }

  /**
   * The port the hosting region server is listening on for RPCs.
   * NOTE: If the region is not hosted, due to a split, move, offline or server
   * failure, then the result may be 0. See {@link getHostname}
   * @return The port of the hosting server or 0 if the region is offline
   */
  public int getPort() {
    return port;
  }
  
  /**
   * The name of the table that this region pertains to.
   * @return The table name for the region.
   */
  public byte[] getTable() {
    return table;
  }
  
  /**
   * Returns the full name or ID of the region as used in HBase and displayed
   * in the HBase GUI. The format is as follows:
   * [<namespace>:]<table>,<start_key>,<creation_timestamp>,<md5>
   * @return The name of the region
   */
  public byte[] getRegionName() {
    return region_name;
  }
  
  @Override
  public String toString() {
    return this.getClass().getCanonicalName() +
            "{hostport: " + host +
            ":" + port +
            ", startKey: " + Bytes.pretty(start_key) +
            ", stopKey:" + Bytes.pretty(stop_key) +
            ", table:" + Bytes.pretty(table) +
            ", name: " + Bytes.pretty(region_name) + 
            "}";
  }
}
