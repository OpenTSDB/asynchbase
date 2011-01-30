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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.jboss.netty.buffer.ChannelBuffer;

import org.slf4j.LoggerFactory;

/**
 * Package-private class to group {@link PutRequest} into a multi-put.
 * <p>
 * This RPC is guaranteed to be sent atomically (but HBase doesn't guarantee
 * that it will apply it atomically).
 */
final class MultiPutRequest extends HBaseRpc {

  private static final byte[] MULTI_PUT = new byte[] {
    'm', 'u', 'l', 't', 'i', 'P', 'u', 't'
  };

  /**
   * All the edits in this multi-put.
   * We'll sort this list before serializing it.
   * @see MultiPutComparator
   */
  private final ArrayList<PutRequest> edits = new ArrayList<PutRequest>();

  /**
   * Constructor.
   */
  public MultiPutRequest() {
    super(MULTI_PUT);
  }

  /** Returns the number of edits in this multi-put.  */
  public int size() {
    return edits.size();
  }

  /**
   * Adds a "put" request in this multi-put.
   * <p>
   * @param request The edit to add in this multi-put.
   * This edit <b>must not</b> specify an explicit row lock.
   */
  public void add(final PutRequest request) {
    if (request.lockid() != RowLock.NO_LOCK) {
      throw new AssertionError("Should never happen!  We don't do multi-put"
        + " with RowLocks but we've been given an edit that has one!"
        + "  edit=" + request + ", this=" + this);
    }
    edits.add(request);
  }

  /** Returns the list of individual puts that make up this multi-put.  */
  ArrayList<PutRequest> edits() {
    return edits;
  }

  /**
   * Handles partial failures from a {@link MultiPutResponse}.
   * @param failures A map from region name to the index of the first edit that
   * failed.
   * @return A list of edits that need to be retried.
   */
  Iterable<PutRequest> handlePartialFailure(final Bytes.ByteMap<Integer> failures) {
    final ArrayList<PutRequest> retry =
      new ArrayList<PutRequest>(edits.size() >>> 2);  // Start size = 4x smaller.
    PutRequest prev = PutRequest.EMPTY_PUT;
    int edits_per_region = 0;
    int failed_index = -1;
    for (final PutRequest edit : edits) {
      final byte[] region_name = edit.getRegion().name();
      final boolean new_region = !Bytes.equals(prev.getRegion().name(),
                                               region_name);
      if (new_region) {
        edits_per_region = 0;
        final Integer i = failures.get(region_name);
        if (i == null) {  // Should never happen.
          LoggerFactory.getLogger(MultiPutRequest.class)
            .error("WTF?  Partial failures for " + this + " = " + failures
                   + ", no results for region=" + Bytes.pretty(region_name));
          prev = PutRequest.EMPTY_PUT;
          continue;
        }
        failed_index = i;
      } else {
        edits_per_region++;
      }

      if (edits_per_region < failed_index) {
        edit.callback(null);
      } else {
        retry.add(edit);
      }
      prev = edit;
    }
    if (retry.isEmpty()) {  // Sanity check.
      throw new AssertionError("Impossible, we attempted to retry a partially"
        + " applied multiPut but we didn't find anything to retry."
        + "  Original RPC = " + this + ", failures = " + failures
        + ", edits to retry = " + retry);
    }
    return retry;
  }

  /**
   * Predicts a lower bound on the serialized size of this RPC.
   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
   * Since we use a static buffer, if the prediction is wrong and turns out
   * to be less than what we need, there will be an exception which will
   * prevent the RPC from being serialized.  That'd be a severe bug.
   */
  private int predictSerializedSize() {
    // See the comment in serialize() about the for loop that follows.
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 1;  // byte: Type again (see HBASE-2877).
    size += 4;  // int:  How many regions do we want to edit?

    PutRequest prev = PutRequest.EMPTY_PUT;
    for (final PutRequest edit : edits) {
      final byte[] region_name = edit.getRegion().name();
      final boolean new_region = !Bytes.equals(prev.getRegion().name(),
                                               region_name);
      final boolean new_key = new_region || !Bytes.equals(prev.key, edit.key);
      final boolean new_family = new_key || !Bytes.equals(prev.family(),
                                                          edit.family());

      if (new_region) {
        size += 3;  // vint: region name length (3 bytes => max length = 32768).
        size += region_name.length;  // The region name.
        size += 4;  // int:  How many edits for this region.
      }

      final int key_length = edit.key.length;
      final int family_length = edit.family().length;

      if (new_key) {
        size += 1;  // byte: Version of Put.
        size += 3;  // vint: row key length (3 bytes => max length = 32768).
        size += key_length;  // The row key.
        size += 8;  // long: Timestamp.
        size += 8;  // long: Lock ID.
        size += 1;  // bool: Whether or not to write to the WAL.
        size += 4;  // int:  Number of families for which we have edits.
      }

      if (new_family) {
        size += 1;  // vint: Family length (guaranteed on 1 byte).
        size += family_length;  // The family.
        size += 4;  // int:  Number of KeyValues that follow.
        size += 4;  // int:  Total number of bytes for all those KeyValues.
      }

      size += 4;  // int:   Key + value length.
      size += 4;  // int:   Key length.
      size += 4;  // int:   Value length.
      size += 2;  // short: Row length.
      size += key_length;               // The row key (again!).
      size += 1;  // byte:  Family length.
      size += family_length;            // The family (again!).
      size += edit.qualifier().length;  // The qualifier.
      size += 8;  // long:  Timestamp (again!).
      size += 1;  // byte:  Type of edit.
      size += edit.value().length;

      prev = edit;
    }
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte unused_server_version) {
    // Due to the wire format expected by HBase, we need to group all the
    // edits by region, then by key, then by family.  HBase does this by
    // building a crazy map-of-map-of-map-of-list-of-edits, but this is
    // memory and time inefficient (lots of unnecessary references and
    // O(n log n) operations).  The approach we take here is to sort the
    // list and iterate on it.  Each time we find a different family or
    // row key or region, we start a new set of edits.  Because the RPC
    // format needs to know the number of edits or bytes that follows in
    // various places, we store a "0" value and then later monkey-patch it
    // once we cross a row key / family / region boundary, because we can't
    // efficiently tell ahead of time how many edits or bytes will follow
    // until we cross such boundaries.
    Collections.sort(edits, MULTIPUT_CMP);
    final ChannelBuffer buf = newBuffer(predictSerializedSize());
    buf.writeInt(1);  // Number of parameters.

    // 1st and only param: a MultiPut object.
    buf.writeByte(57);   // Code for a `MultiPut' parameter.
    buf.writeByte(57);   // Code again (see HBASE-2877).
    buf.writeInt(0);  // How many regions do we want to edit?
    //           ^------ We'll monkey patch this at the end.

    int nregions = 0;
    int nkeys_index = -1;
    int nkeys = 0;
    int nfamilies_index = -1;
    int nfamilies = 0;
    int nkeys_per_family_index = -1;
    int nkeys_per_family = 0;
    int nbytes_per_family = 0;
    PutRequest prev = PutRequest.EMPTY_PUT;
    for (final PutRequest edit : edits) {
      final byte[] region_name = edit.getRegion().name();
      final boolean new_region = !Bytes.equals(prev.getRegion().name(),
                                               region_name);
      final boolean new_key = new_region || !Bytes.equals(prev.key, edit.key);
      final boolean new_family = new_key || !Bytes.equals(prev.family(),
                                                          edit.family());
      if (new_region) {
        // Monkey-patch the number of edits of the previous region.
        if (nkeys_index > 0) {
          buf.setInt(nkeys_index, nkeys);
          nkeys = 0;
        }

        nregions++;
        writeByteArray(buf, region_name);  // The region name.
        nkeys_index = buf.writerIndex();
        // Number of keys for which we have edits for this region.
        buf.writeInt(0);  // We'll monkey patch this later.
      }

      final byte[] key = edit.key;
      if (new_key) {
        nkeys++;
        // Monkey-patch the number of families of the previous key.
        if (nfamilies_index > 0) {
          buf.setInt(nfamilies_index, nfamilies);
          nfamilies = 0;
        }

        buf.writeByte(1);    // Undocumented versioning of Put.
        writeByteArray(buf, key);  // The row key.

        // Timestamp.  We always set it to Long.MAX_VALUE, which means "unset".
        // The RegionServer will set it for us, right before writing to the WAL
        // (or to the Memstore if we're not using the WAL).
        buf.writeLong(Long.MAX_VALUE);

        buf.writeLong(RowLock.NO_LOCK);    // Lock ID.
        buf.writeByte(edit.durable() ? 0x01 : 0x00);  // Use the WAL?
        nfamilies_index = buf.writerIndex();
        // Number of families that follow.
        buf.writeInt(0);  // We'll monkey patch this later.
      }

      final byte[] family = edit.family();
      if (new_family) {
        nfamilies++;
        writeByteArray(buf, family);  // The column family.

        // Monkey-patch the number and size of edits for the previous family.
        if (nkeys_per_family_index > 0) {
          buf.setInt(nkeys_per_family_index, nkeys_per_family);
          nkeys_per_family = 0;
          buf.setInt(nkeys_per_family_index + 4, nbytes_per_family);
          nbytes_per_family = 0;
        }
        nkeys_per_family_index = buf.writerIndex();
        // Number of "KeyValues" that follow.
        buf.writeInt(0);  // We'll monkey patch this later.
        // Total number of bytes taken by those "KeyValues".
        buf.writeInt(0);  // We'll monkey patch this later.
      }
      nkeys_per_family++;

      final byte[] qualifier = edit.qualifier();
      final byte[] value = edit.value();
      final int key_length = edit.keyLength();
      nbytes_per_family += 4 + 4 + key_length + value.length;
      edit.serializeKeyValue(buf);
      prev = edit;
    }  // Yay, we made it!

    // Monkey-patch everything for the last set of edits.
    buf.setInt(nkeys_per_family_index, nkeys_per_family);
    buf.setInt(nkeys_per_family_index + 4, nbytes_per_family);
    buf.setInt(nfamilies_index, nfamilies);
    buf.setInt(nkeys_index, nkeys);

    // Monkey-patch the number of regions affected by this RPC.
    buf.setInt(4 + 4 + 2 + MULTI_PUT.length  // header length
               + 4 + 1 + 1, nregions);

    return buf;
  }

  public String toString() {
    return "MultiPutRequest(edits=" + edits + ')';
  }

  /**
   * Sorts {@link PutRequest}s appropriately for the multi-put RPC.
   * We sort by region, row key, column family.  No ordering is needed on the
   * column qualifier or value.
   */
  private static final MultiPutComparator MULTIPUT_CMP = new MultiPutComparator();

  /** Sorts {@link PutRequest}s appropriately for the multi-put RPC.  */
  private static final class MultiPutComparator implements Comparator<PutRequest> {

    private MultiPutComparator() {  // Can't instantiate outside of this class.
    }

    @Override
    public int compare(final PutRequest a, final PutRequest b) {
      int d;
      if ((d = Bytes.memcmp(a.getRegion().name(), b.getRegion().name())) != 0) {
        return d;
      } else if ((d = Bytes.memcmp(a.key, b.key)) != 0) {
        return d;
      }
      return Bytes.memcmp(a.family(), b.family());
    }

  }


}
