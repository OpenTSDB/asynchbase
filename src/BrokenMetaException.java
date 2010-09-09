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
package org.hbase.async;

/**
 * Indicates that the {@code .META.} or {@code -ROOT-} table is corrupted.
 */
public final class BrokenMetaException extends NonRecoverableException {

  private final byte[] table;

  /**
   * Constructor.
   * @param region The region we were looking up
   * (if known, can be {@code null} if not known).
   * @param msg A message describing as precisely as possible what's wrong
   * with the META table.
   */
  BrokenMetaException(final RegionInfo region, final String msg) {
    super("Your .META. table seems broken for "
          + (region == null ? "(unknown table)" : region)
          + ".  " + msg);
    this.table = region.table();
  }

  /**
   * Returns the name of the table for which we were trying to lookup a region.
   * @return A possibly {@code null} byte array.
   */
  public byte[] table() {
    return table;
  }

  /**
   * Helper to complain about a particular {@link KeyValue} found.
   * @param region The region we were looking up
   * (if known, can be {@code null} if not known).
   * @param msg A message describing as precisely as possible what's wrong
   * with the META table.
   * @param kv The {@link KeyValue} in which the problem was found.
   */
  static BrokenMetaException badKV(final RegionInfo region,
                                   final String msg,
                                   final KeyValue kv) {
    return new BrokenMetaException(region, "I found a row where " + msg
                                   + ".  KeyValue=" + kv);
  }

  private static final long serialVersionUID = 1280222742;

}
