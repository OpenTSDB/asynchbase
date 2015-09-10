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

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.hbase.async.generated.ClientPB.Action;
import org.hbase.async.generated.ClientPB.MultiRequest;
import org.hbase.async.generated.ClientPB.MultiResponse;
import org.hbase.async.generated.ClientPB.RegionAction;
import org.hbase.async.generated.ClientPB.RegionActionResult;
import org.hbase.async.generated.ClientPB.ResultOrException;
import org.hbase.async.generated.HBasePB.NameBytesPair;

/**
 * Package-private class to batch multiple RPCs for a same region together.
 * <p>
 * This RPC is guaranteed to be sent atomically (but HBase doesn't guarantee
 * that it will apply it atomically).
 */
final class MultiAction extends HBaseRpc implements HBaseRpc.IsEdit {

  // NOTE: I apologize for the long methods and complex control flow logic,
  // with many nested loops and `if's.  `multiPut' and `multi' have always
  // been a gigantic mess in HBase, and despite my best efforts to provide
  // a reasonable implementation, it's obvious that the resulting code is
  // unwieldy.  I originally wrote support for `multi' as a separate class
  // from `multiPut', but this resulted in a lot of code duplication and
  // added complexity in other places that had to deal with either kind of
  // RPCs depending on the server version.  Hence this class supports both
  // RPCs, which does add a bit of complexity to the already unnecessarily
  // complex logic.  This was better than all the code duplication.  And
  // believe me, what's here is -- in my biased opinion -- significantly
  // better than the insane spaghetti mess in HBase's client and server.

  /** RPC method name for HBase before 0.92.  */
  private static final byte[] MULTI_PUT = {
    'm', 'u', 'l', 't', 'i', 'P', 'u', 't'
  };

  /** RPC method name for HBase 0.92 to 0.94.  */
  private static final byte[] MULTI = { 'm', 'u', 'l', 't', 'i' };

  /** RPC method name for HBase 0.95 and above.  */
  private static final byte[] MMULTI = { 'M', 'u', 'l', 't', 'i' };

  /** Template for NSREs.  */
  private static final NotServingRegionException NSRE =
    new NotServingRegionException("Region unavailable", null);

  /**
   * Protocol version from which we can use `multi' instead of `multiPut'.
   * Technically we could use `multi' with HBase 0.90.x, but as explained
   * in HBASE-5204, because of a change in HBASE-3335 this is harder than
   * necessary, so we don't support it.
   */
  static final byte USE_MULTI = RegionClient.SERVER_VERSION_092_OR_ABOVE;

  /**
   * All the RPCs in this batch.
   * We'll sort this list before serializing it.
   * @see MultiActionComparator
   */
  private final ArrayList<BatchableRpc> batch = new ArrayList<BatchableRpc>();

  @Override
  byte[] method(final byte server_version) {
    if (server_version >= RegionClient.SERVER_VERSION_095_OR_ABOVE) {
       return MMULTI;
    }
    return server_version >= USE_MULTI ? MULTI : MULTI_PUT;
  }

  /** Returns the number of RPCs in this batch.  */
  public int size() {
    return batch.size();
  }

  /**
   * Adds an RPC to this batch.
   * <p>
   * @param rpc The RPC to add in this batch.
   * Any edit added <b>must not</b> specify an explicit row lock.
   * @throws IllegalArgumentException if the region was missing from the RPC
   */
  public void add(final BatchableRpc rpc) {
    if (rpc.lockid != RowLock.NO_LOCK) {
      throw new AssertionError("Should never happen!  We don't do multi-put"
        + " with RowLocks but we've been given an edit that has one!"
        + "  edit=" + rpc + ", this=" + this);
    }
    if (rpc.region == null || rpc.region.name() == null) {
      throw new IllegalArgumentException("RPC " + rpc + 
          " is missing the region or region name");
    }
    batch.add(rpc);
  }

  /** Returns the list of individual RPCs that make up this batch.  */
  ArrayList<BatchableRpc> batch() {
    return batch;
  }

  /**
   * Predicts a lower bound on the serialized size of this RPC.
   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
   * Since we use a static buffer, if the prediction is wrong and turns out
   * to be less than what we need, there will be an exception which will
   * prevent the RPC from being serialized.  That'd be a severe bug.
   * @param server_version What RPC protocol version the server is running.
   */
  private int predictSerializedSize(final byte server_version) {
    // See the comment in serialize() about the for loop that follows.
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 1;  // byte: Type again (see HBASE-2877).
    size += 4;  // int:  How many regions do we want to affect?

    // Are we serializing a `multi' RPC, or `multiPut'?
    final boolean use_multi = method(server_version) == MULTI;

    BatchableRpc prev = PutRequest.EMPTY_PUT;
    for (final BatchableRpc rpc : batch) {
      final byte[] region_name = rpc.getRegion().name();
      final boolean new_region = !Bytes.equals(prev.getRegion().name(),
                                               region_name);
      final byte[] family = rpc.family();
      final boolean new_key = (new_region
                               || prev.code() != rpc.code()
                               || !Bytes.equals(prev.key, rpc.key)
                               || family == DeleteRequest.WHOLE_ROW);
      final boolean new_family = new_key || !Bytes.equals(prev.family(),
                                                          family);

      if (new_region) {
        size += 3;  // vint: region name length (3 bytes => max length = 32768).
        size += region_name.length;  // The region name.
        size += 4;  // int:  How many RPCs for this region.
      }

      final int key_length = rpc.key.length;

      if (new_key) {
        if (use_multi) {
          size += 4;  // int: Number of "attributes" for the last key (none).
          size += 4;  // Index of this `action'.
          size += 3;  // 3 bytes to serialize `null'.

          size += 1;  // byte: Type code for `Action'.
          size += 1;  // byte: Type code again (see HBASE-2877).

          size += 1;  // byte: Code for a `Row' object.
          size += 1;  // byte: Code for a `Put' object.
        }

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
        size += family.length;  // The family.
        size += 4;  // int:  Number of KeyValues that follow.
        if (rpc.code() == PutRequest.CODE) {
          size += 4;  // int:  Total number of bytes for all those KeyValues.
        }
      }

      size += rpc.payloadSize();
      prev = rpc;
    }
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize(final byte server_version) {
    if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
      return serializeOld(server_version);
    }

    Collections.sort(batch, SORT_BY_REGION);
    final MultiRequest.Builder req = MultiRequest.newBuilder();
    RegionAction.Builder actions = null;
    byte[] prev_region = HBaseClient.EMPTY_ARRAY;
    int i = 0;
    for (final BatchableRpc rpc : batch) {
      final RegionInfo region = rpc.getRegion();
      final boolean new_region = !Bytes.equals(prev_region, region.name());
      if (new_region) {
        if (actions != null) {  // If not the first iteration ...
          req.addRegionAction(actions.build());  // ... push actions thus far.
        }
        actions = RegionAction.newBuilder();
        actions.setRegion(rpc.getRegion().toProtobuf());
        prev_region = region.name();
      }
      final Action action = Action.newBuilder()
        .setIndex(i++)
        .setMutation(rpc.toMutationProto())
        .build();
      actions.addAction(action);
    }
    req.addRegionAction(actions.build());
    return toChannelBuffer(MMULTI, req.build());
  }

  /** Serializes this request for HBase 0.94 and before.  */
  private ChannelBuffer serializeOld(final byte server_version) {
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
    Collections.sort(batch, MULTI_CMP);
    final ChannelBuffer buf = newBuffer(server_version,
                                        predictSerializedSize(server_version));
    buf.writeInt(1);  // Number of parameters.

    // Are we serializing a `multi' RPC, or `multiPut'?
    final boolean use_multi = method(server_version) == MULTI;

    {  // 1st and only param.
      final int code = use_multi ? 66 : 57;  // `MultiAction' or `MultiPut'.
      buf.writeByte(code);  // Type code for the parameter.
      buf.writeByte(code);  // Type code again (see HBASE-2877).
    }

    buf.writeInt(0);  // How many regions do we want to affect?
    //           ^------ We'll monkey patch this at the end.

    int nregions = 0;
    int nkeys_index = -1;
    int nkeys = 0;
    int nfamilies_index = -1;
    int nfamilies = 0;
    int nkeys_per_family_index = -1;
    int nkeys_per_family = 0;
    int nbytes_per_family = 0;
    int nrpcs_per_key = 0;
    BatchableRpc prev = PutRequest.EMPTY_PUT;
    for (final BatchableRpc rpc : batch) {
      final byte[] region_name = rpc.getRegion().name();
      final boolean new_region = !Bytes.equals(prev.getRegion().name(),
                                               region_name);
      final byte[] family = rpc.family();
      final boolean new_key = (new_region
                               || prev.code() != rpc.code()
                               || !Bytes.equals(prev.key, rpc.key)
                               || family == DeleteRequest.WHOLE_ROW);
      final boolean new_family = new_key || !Bytes.equals(prev.family(),
                                                          family);

      if (new_key && use_multi && nkeys_index > 0) {
        buf.writeInt(0);  // Number of "attributes" for the last key (none).
        // Trailing useless junk from `Action'.  After serializing its
        // `action' (which implements an interface awesomely named `Row'),
        // HBase adds the index of the `action' as the server will sort
        // actions by key.  This is pretty useless as if the client was
        // sorting things before sending them off to the server, like we
        // do, then we wouldn't need to pass an index to be able to match
        // up requests and responses.  Since we don't need this index, we
        // instead use this field for another purpose: remembering how
        // many RPCs we sent for this particular key.  This will help us
        // when de-serializing the response.
        buf.writeInt(nrpcs_per_key);
        nrpcs_per_key = 0;
        // Then, because an `Action' can also contain a result, the code
        // also serializes that.  Of course for us end-users, this is stupid
        // because we never send a result along with our request.
        writeHBaseNull(buf);
      }

      if (new_region) {
        // Monkey-patch the number of edits of the previous region.
        if (nkeys_index > 0) {
          buf.setInt(nkeys_index, nkeys);
          nkeys = 0;
        }

        nregions++;
        writeByteArray(buf, region_name);  // The region name.
        nkeys_index = buf.writerIndex();
        // Number of keys for which we have RPCs for this region.
        buf.writeInt(0);  // We'll monkey patch this later.
      }

      final byte[] key = rpc.key;
      if (new_key) {
        // Monkey-patch the number of families of the previous key.
        if (nfamilies_index > 0) {
          buf.setInt(nfamilies_index, nfamilies);
          nfamilies = 0;
        }

        if (use_multi) {
          // Serialize an `Action' for the RPCs on this row.
          buf.writeByte(65);  // Code for an `Action' object.
          buf.writeByte(65);  // Code again (see HBASE-2877).

          // Inside the action, serialize a `Put' object.
          buf.writeByte(64);  // Code for a `Row' object.
          buf.writeByte(rpc.code());  // Code this object.
        }

        nkeys++;

        // Right now we only support batching puts.  In the future this part
        // of the code will have to change to also want to allow get/deletes.
        // The follow serializes a `Put'.
        buf.writeByte(rpc.version(server_version)); // Undocumented versioning.
        writeByteArray(buf, key);  // The row key.

        // This timestamp is unused, only the KeyValue-level timestamp is
        // used, except in one case: whole-row deletes.  In that case only,
        // this timestamp is used by the RegionServer to create per-family
        // deletes for that row.  See HRegion.prepareDelete() for more info.
        buf.writeLong(rpc.timestamp);  // Timestamp.

        buf.writeLong(RowLock.NO_LOCK);    // Lock ID.
        buf.writeByte(rpc.durable ? 0x01 : 0x00);  // Use the WAL?
        nfamilies_index = buf.writerIndex();
        // Number of families that follow.
        buf.writeInt(0);  // We'll monkey patch this later.
      }

      if (new_family) {
        // Monkey-patch the number and size of edits for the previous family.
        if (nkeys_per_family_index > 0) {
          buf.setInt(nkeys_per_family_index, nkeys_per_family);
          if (prev.code() == PutRequest.CODE) {
            buf.setInt(nkeys_per_family_index + 4, nbytes_per_family);
          }
          nkeys_per_family_index = -1;
          nkeys_per_family = 0;
          nbytes_per_family = 0;
        }

        if (family == DeleteRequest.WHOLE_ROW) {
          prev = rpc;  // Short circuit.  We have no KeyValue to write.
          continue;    // So loop again directly.
        }

        nfamilies++;
        writeByteArray(buf, family);  // The column family.

        nkeys_per_family_index = buf.writerIndex();
        // Number of "KeyValues" that follow.
        buf.writeInt(0);  // We'll monkey patch this later.
        if (rpc.code() == PutRequest.CODE) {
          // Total number of bytes taken by those "KeyValues".
          // This is completely useless and only done for `Put'.
          buf.writeInt(0);  // We'll monkey patch this later.
        }
      }
      nkeys_per_family += rpc.numKeyValues();
      nrpcs_per_key++;

      nbytes_per_family += rpc.payloadSize();
      rpc.serializePayload(buf);
      prev = rpc;
    }  // Yay, we made it!

    if (use_multi) {
      buf.writeInt(0);  // Number of "attributes" for the last key (none).
      // Trailing junk for the last `Action'.  See comment above.
      buf.writeInt(nrpcs_per_key);
      writeHBaseNull(buf);  // Useless.
    }

    // Note: the only case where nkeys_per_family_index remained -1 throughout
    // this whole ordeal is where we didn't have any KV to serialize because
    // every RPC was a `DeleteRequest.WHOLE_ROW'.
    if (nkeys_per_family_index > 0) {
      // Monkey-patch everything for the last set of edits.
      buf.setInt(nkeys_per_family_index, nkeys_per_family);
      if (prev.code() == PutRequest.CODE) {
        buf.setInt(nkeys_per_family_index + 4, nbytes_per_family);
      }
    }
    buf.setInt(nfamilies_index, nfamilies);
    buf.setInt(nkeys_index, nkeys);

    // Monkey-patch the number of regions affected by this RPC.
    int header_length = 4 + 4 + 2 + method(server_version).length;
    if (server_version >= RegionClient.SERVER_VERSION_092_OR_ABOVE) {
      header_length += 1 + 8 + 4;
    }
    buf.setInt(header_length + 4 + 1 + 1, nregions);

    return buf;
  }

  public String toString() {
    // Originally this was simply putting all the batch in the toString
    // representation, but that's way too expensive when we have large
    // batches.  So instead we toString each RPC until we hit some
    // hard-coded upper bound on how much data we're willing to put into
    // the toString.  If we have too many RPCs and hit that constant,
    // we skip all the remaining ones until the last one, as it's often
    // useful to see the last RPC added when debugging.
    final StringBuilder buf = new StringBuilder();
    buf.append("MultiAction(batch=[");
    final int nrpcs = batch.size();
    int i;
    for (i = 0; i < nrpcs; i++) {
      if (buf.length() >= 1024) {
        break;
      }
      buf.append(batch.get(i)).append(", ");
    }
    if (i < nrpcs) {
      if (i == nrpcs - 1) {
        buf.append("... 1 RPC not shown])");
      } else {
        buf.append("... ").append(nrpcs - 1 - i)
          .append(" RPCs not shown ..., ")
          .append(batch.get(nrpcs - 1))
          .append("])");
      }
    } else {
      buf.setLength(buf.length() - 2);  // Remove the last ", "
      buf.append("])");
    }
    return buf.toString();
  }

  /**
   * Sorts {@link BatchableRpc}s appropriately for the multi-put / multi-action RPC.
   * We sort by region, row key, column family.  No ordering is needed on the
   * column qualifier or value.
   */
  static final MultiActionComparator MULTI_CMP = new MultiActionComparator();

  /**
   * Sorts {@link BatchableRpc}s appropriately for the `multi' RPC.
   * Used with HBase 0.94 and earlier only.
   */
  private static final class MultiActionComparator
    implements Comparator<BatchableRpc> {

    private MultiActionComparator() {  // Can't instantiate outside of this class.
    }

    @Override
    /**
     * Compares two RPCs.
     */
    public int compare(final BatchableRpc a, final BatchableRpc b) {
      int d;
      if ((d = Bytes.memcmp(a.getRegion().name(), b.getRegion().name())) != 0) {
        return d;
      } else if ((d = a.code() - b.code()) != 0) {  // Sort by code before key.
        // Note that DeleteRequest.code() < PutRequest.code() and this matters
        // as the RegionServer processes deletes before puts, and we want to
        // send RPCs in the same order as they will be processed.
        return d;
      } else if ((d = Bytes.memcmp(a.key, b.key)) != 0) {
        return d;
      }
      return Bytes.memcmp(a.family(), b.family());
    }

  }

  /**
   * Sorts {@link BatchableRpc}s appropriately for HBase 0.95+ multi-action.
   */
  static final RegionComparator SORT_BY_REGION = new RegionComparator();

  /**
   * Sorts {@link BatchableRpc}s by region.
   * Used with HBase 0.95+ only.
   */
  private static final class RegionComparator
    implements Comparator<BatchableRpc> {

    private RegionComparator() {  // Can't instantiate outside of this class.
    }

    @Override
    /** Compares two RPCs.  */
    public int compare(final BatchableRpc a, final BatchableRpc b) {
      return Bytes.memcmp(a.getRegion().name(), b.getRegion().name());
    }

  }

  @Override
  Object deserialize(final ChannelBuffer buf, final int cell_size) {
    final MultiResponse resp = readProtobuf(buf, MultiResponse.PARSER);
    final int responses = resp.getRegionActionResultCount();
    final int nrpcs = batch.size();
    final Object[] resps = new Object[nrpcs];
    int n = 0;  // Index in `batch'.
    int r = 0;  // Index in `regionActionResult' in the PB.
    ArrayList<KeyValue> kvs = null;
    int kv_index = 0;
    
    while (n < nrpcs && r < responses) {
      final RegionActionResult results = resp.getRegionActionResult(r++);
      final int nresults = results.getResultOrExceptionCount();
      if (results.hasException()) {
        if (nresults != 0) {
          throw new InvalidResponseException("All edits in a batch failed yet"
                                             + " we found " + nresults
                                             + " results", results);
        }
        // All the edits for this region have failed, however the PB doesn't
        // tell us how many edits there were for that region, and we don't
        // keep track of this information except temporarily during
        // serialization.  So we need to go back through our list again to
        // re-count how many we did put together in this batch, so we can fail
        // all those RPCs.
        int last_edit = n + 1; // +1 because we have at least 1 edit.
        final byte[] region_name = batch.get(n).getRegion().name();
        while (last_edit < nrpcs
               && Bytes.equals(region_name,
                               batch.get(last_edit).getRegion().name())) {
          last_edit++;
        }
        final NameBytesPair pair = results.getException();
        for (int j = n; j < last_edit; j++) {
          resps[j] = RegionClient.decodeExceptionPair(batch.get(j), pair);
        }
        n = last_edit;
        continue;  // This batch failed, move on.
      } else if (nresults == 0) {
        /* WTF HBase? Why return nulls for appends when we don't ask for a 
         * response instead of at least returning an empty result like the 
         * puts? */
        if (!(batch.get(n) instanceof AppendRequest)) {
          throw new InvalidResponseException("No responses for a multiAction set"
              + " and the first RPC " + batch.get(n) 
              + " was not an append request", results);
        }
        int last_edit = n + 1; // +1 because we have at least 1 edit.
        final byte[] region_name = batch.get(n).getRegion().name();
        while (last_edit < nrpcs
               && Bytes.equals(region_name,
                               batch.get(last_edit).getRegion().name())) {
          last_edit++;
        }
        for (int j = n; j < last_edit; j++) {
          resps[j] = SUCCESS;
        }
        n = last_edit;
        continue;  // All the appends for this batch are successful
      }  // else: parse out the individual results:
      
      
      for (int j = 0; j < nresults; j++) {
        final ResultOrException roe = results.getResultOrException(j);
        final int index = roe.getIndex();
        if (index != n) {
          // More append fun! If we interleaved appends with puts or appends 
          // with and without result returns, we need to skip over appends
          // without results as they're not counted in {@code nresults}
          while (batch.get(n) instanceof AppendRequest && 
              !((AppendRequest)batch.get(n)).returnResult()) {
            resps[n++] = SUCCESS;
          }
          if (index != n) {
            // if we STILL don't match up on our indices then something is
            // foobar so toss an exception
            throw new InvalidResponseException("Expected result #" + n
                                               + " but got result #" + index,
                                               results);
          }
        }
        final Object result;
        if (roe.hasException()) {
          final NameBytesPair pair = roe.getException();
          // This RPC failed, get what the exception was.
          result = RegionClient.decodeExceptionPair(batch.get(n), pair);
        } else {
          // TODO - handle other types of RPCs if we start allowing them in
          // a batch
          if (batch.get(n) instanceof AppendRequest) {
            final AppendRequest append_request = (AppendRequest)(batch.get(n));
            if (kvs == null) { // only do this once
              kvs = GetRequest.convertResult(roe.getResult(), buf, cell_size);
            }
            if (append_request.returnResult()) {
              result = kvs.get(kv_index++);
            } else {
              result = SUCCESS;
            }
          } else {
            result = SUCCESS;  
          }
        }
        resps[n++] = result;
      }
    }
    if (n != nrpcs) {
      // handle trailing appends that don't have a response.
      while (n < nrpcs && batch.get(n) instanceof AppendRequest && 
          !((AppendRequest)batch.get(n)).returnResult()) {
        resps[n++] = SUCCESS;
      }
      if (n != nrpcs) {
        throw new InvalidResponseException("Expected " + nrpcs
                                           + " results but got " + n, resp);
      }
    }
    return new Response(resps);
  }

  /**
   * Response to a {@link MultiAction} RPC.
   */
  final class Response {

    /** Response for each Action that was in the batch, in the same order.  */
    private final Object[] resps;

    /** Constructor.  */
    Response(final Object[] resps) {
      assert resps.length == batch.size() : "Got " + resps.length
        + " responses but expected " + batch.size();
      this.resps = resps;
    }

    /** @return The number of objects in the response */
    public int size() {
      return resps.length;
    }
    
    /**
     * Returns the result number #i embodied in this response.
     * @throws IndexOutOfBoundsException if i is greater than batch.size()
     */
    public Object result(final int i) {
      return resps[i];
    }

    public String toString() {
      return "MultiAction.Response(" + Arrays.toString(resps) + ')';
    }

  }

  /**
   * De-serializes the response to a {@link MultiAction} RPC.
   * See HBase's {@code MultiResponse}.
   * Only used with HBase 0.94 and earlier.
   */
  Response responseFromBuffer(final ChannelBuffer buf) {
    switch (buf.readByte()) {
      case 58:
        return deserializeMultiPutResponse(buf);
      case 67:
        return deserializeMultiResponse(buf);
    }
    throw new NonRecoverableException("Couldn't de-serialize "
                                      + Bytes.pretty(buf));
  }

  /**
   * De-serializes a {@code MultiResponse}.
   * This is only used when talking to HBase 0.92.x to 0.94.x.
   */
  Response deserializeMultiResponse(final ChannelBuffer buf) {
    final int nregions = buf.readInt();
    HBaseRpc.checkNonEmptyArrayLength(buf, nregions);
    final Object[] resps = new Object[batch.size()];
    int n = 0;  // Index in `batch'.
    for (int i = 0; i < nregions; i++) {
      final byte[] region_name = HBaseRpc.readByteArray(buf);
      final int nkeys = buf.readInt();
      HBaseRpc.checkNonEmptyArrayLength(buf, nkeys);
      for (int j = 0; j < nkeys; j++) {
        final int nrpcs_per_key = buf.readInt();
        boolean error = buf.readByte() != 0x00;
        Object resp;  // Response for the current region.
        if (error) {
          final HBaseException e = RegionClient.deserializeException(buf, null);
          resp = e;
        } else {
          resp = RegionClient.deserializeObject(buf, this);
          // A successful response to a `Put' will be an empty `Result'
          // object, which we de-serialize as an empty `ArrayList'.
          // There's no need to waste memory keeping these around.
          if (resp instanceof ArrayList && ((ArrayList) resp).isEmpty()) {
            resp = SUCCESS;
          } else if (resp == null) {
            // Awesomely, `null' is used to indicate all RPCs for this region
            // have failed, and we're not told why.  Most of the time, it will
            // be an NSRE, so assume that.  What were they thinking when they
            // wrote that code in HBase?  Seriously WTF.
            resp = NSRE;
            error = true;
          }
        }
        if (error) {
          final HBaseException e = (HBaseException) resp;
          for (int k = 0; k < nrpcs_per_key; k++) {
            // We need to "clone" the exception for each RPC, as each RPC that
            // failed needs to have its own exception with a reference to the
            // failed RPC.  This makes significantly simplifies the callbacks
            // that do error handling, as they can extract the RPC out of the
            // exception.  The downside is that we don't have a perfect way of
            // cloning "e", so instead we just abuse its `make' factory method
            // slightly to duplicate it.  This mangles the message a bit, but
            // that's mostly harmless.
            resps[n + k] = e.make(e, batch.get(n + k));
          }
        } else {
          for (int k = 0; k < nrpcs_per_key; k++) {
            resps[n + k] = resp;
          }
        }
        n += nrpcs_per_key;
      }
    }
    return new Response(resps);
  }

  /**
   * De-serializes a {@code MultiPutResponse}.
   * This is only used when talking to old versions of HBase (pre 0.92).
   */
  Response deserializeMultiPutResponse(final ChannelBuffer buf) {
    final int nregions = buf.readInt();
    HBaseRpc.checkNonEmptyArrayLength(buf, nregions);
    final int nrpcs = batch.size();
    final Object[] resps = new Object[nrpcs];

    int n = 0;  // Index in `batch'.
    for (int i = 0; i < nregions; i++) {
      final byte[] region_name = HBaseRpc.readByteArray(buf);
      // Sanity check.  Response should give us regions back in same order.
      assert Bytes.equals(region_name, batch.get(n).getRegion().name())
        : ("WTF?  " + Bytes.pretty(region_name) + " != "
           + batch.get(n).getRegion().name());
      final int failed = buf.readInt();  // Index of the first failed edit.
      // Because of HBASE-2898, we don't know which RPCs exactly failed.  We
      // only now that for that region, the one at index `failed' wasn't
      // successful, so we can only assume that the subsequent ones weren't
      // either, and retry them.  First we need to find out how many RPCs
      // we originally had for this region.
      int edits_per_region = n;
      while (edits_per_region < nrpcs
             && Bytes.equals(region_name,
                             batch.get(edits_per_region).getRegion().name())) {
        edits_per_region++;
      }
      edits_per_region -= n;
      assert failed < edits_per_region : "WTF? Found more failed RPCs " + failed
        + " than sent " + edits_per_region + " to " + Bytes.pretty(region_name);

      // If `failed == -1' then we now that nothing has failed.  Otherwise, we
      // have that RPCs in [n; n + failed - 1] have succeeded, and RPCs in
      // [n + failed; n + edits_per_region] have failed.
      if (failed == -1) {  // Fast-path for the case with no failures.
        for (int j = 0; j < edits_per_region; j++) {
          resps[n + j] = SUCCESS;
        }
      } else {  // We had some failures.
        assert failed >= 0 : "WTF?  Found a negative failure index " + failed
          + " for region " + Bytes.pretty(region_name);
        for (int j = 0; j < failed; j++) {
          resps[n + j] = SUCCESS;
        }
        final String msg = "Multi-put failed on RPC #" + failed + "/"
          + edits_per_region + " on region " + Bytes.pretty(region_name);
        for (int j = failed; j < edits_per_region; j++) {
          resps[n + j] = new MultiPutFailedException(msg, batch.get(n + j));
        }
      }
      n += edits_per_region;
    }
    return new Response(resps);
  }

  /**
   * Used to represent the partial failure of a multi-put exception.
   * This is only used with older versions of HBase (pre 0.92).
   */
  private final class MultiPutFailedException extends RecoverableException
  implements HasFailedRpcException {

    final HBaseRpc failed_rpc;

    /**
     * Constructor.
     * @param msg The message of the exception.
     * @param failed_rpc The RPC that caused this exception.
     */
    MultiPutFailedException(final String msg, final HBaseRpc failed_rpc) {
      super(msg);
      this.failed_rpc = failed_rpc;
    }

    @Override
    public String getMessage() {
      return super.getMessage() + "\nCaused by RPC: " + failed_rpc;
    }

    public HBaseRpc getFailedRpc() {
      return failed_rpc;
    }

    @Override
    MultiPutFailedException make(final Object msg, final HBaseRpc rpc) {
      if (msg == this || msg instanceof MultiPutFailedException) {
        final MultiPutFailedException e = (MultiPutFailedException) msg;
        return new MultiPutFailedException(e.getMessage(), rpc);
      }
      return new MultiPutFailedException(msg.toString(), rpc);
    }

    private static final long serialVersionUID = 1326900942;

  }

  /** Singleton class returned to indicate success of a multi-put or append.  */
  private static final class MultiActionSuccess {

    private MultiActionSuccess() {
    }

    public String toString() {
      return "MultiActionSuccess";
    }

  }

  /** Indicates the results of the mutation were successful */
  static final MultiActionSuccess SUCCESS = new MultiActionSuccess();

}
