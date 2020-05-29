/*
 * Copyright (C) 2020  The Async HBase Authors.  All rights reserved.
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

import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.generated.ClientPB.Action;
import org.hbase.async.generated.ClientPB.MultiRequest;
import org.hbase.async.generated.ClientPB.RegionAction;

/**
 * Package-private class to batch multiple RPCs for a same region together.
 * <p>
 * This RPC is guaranteed to be sent atomically (but HBase doesn't guarantee
 * that it will apply it atomically).
 */
final class MultiActionAppend extends MultiAction {
  private static final Logger LOG = LoggerFactory.getLogger(MultiActionAppend.class);
  
  /** Serializes this request.  */
  ChannelBuffer serialize(final byte server_version) {
    if (server_version < RegionClient.SERVER_VERSION_095_OR_ABOVE) {
      return serializeOld(server_version);
    }

    try {
      // TODO - Deletes will happen before the puts. So a delete on the
      // same row would happen before the put if the timestamps are the
      // same.
      
      // first pass to bundle edits for the same key + qualifier
      final ByteMap<BatchableRpc> map = new ByteMap<BatchableRpc>();
      final Iterator<BatchableRpc> iterator = batch.iterator();
      while(iterator.hasNext()) {
        final BatchableRpc rpc = iterator.next();
        final byte[] qualifier = getQualifier(rpc);
        final byte[] type = rpc.getClass().getSimpleName().getBytes();
        final byte[] key = new byte[rpc.region.name().length + 
                                    type.length +
                                    rpc.key.length + 
                                    rpc.family.length + 
                                    qualifier.length];
        int idx = 0;
        System.arraycopy(rpc.region.name(), 0, key, idx, rpc.region.name().length);
        idx += rpc.region.name().length;
        System.arraycopy(type, 0, key, idx, type.length);
        idx += type.length;
        System.arraycopy(rpc.key, 0, key, idx, rpc.key.length);
        idx += rpc.key.length;
        System.arraycopy(rpc.family, 0, key, idx, rpc.family.length);
        idx += rpc.family.length;
        System.arraycopy(qualifier, 0, key, idx, qualifier.length);
        idx += qualifier.length;
        
        BatchableRpc extant = map.get(key);
        if (extant == null) {
          extant = rpc;
        } else {
          if (rpc instanceof PutRequest) {
            final byte[] value = new byte[((PutRequest) extant).value().length + ((PutRequest) rpc).value().length];
            idx = 0;
            System.arraycopy(((PutRequest) extant).value(), 0, value, idx, ((PutRequest) extant).value().length);
            idx += ((PutRequest) extant).value().length;
            System.arraycopy(((PutRequest) rpc).value(), 0, value, idx, ((PutRequest) rpc).value().length);
            
            final PutRequest new_put = new PutRequest(rpc.table, rpc.key, rpc.family, ((PutRequest) extant).qualifier(), value);
            new_put.setRegion(extant.region);
            
            // make sure to pass the results parsed for the new request down
            // to the original RPC.
            new_put.getDeferred()
              .chain(extant.getDeferred())
              .chain(rpc.getDeferred());
            
            extant = new_put;
          } else {
            LOG.warn("Existing non-PutRequest present: " + extant 
                + "  when adding " + rpc);
          }
        }
        map.put(key, extant);
      }
      
      // now we clear the batch array
      batch.clear();
      
      final MultiRequest.Builder req = MultiRequest.newBuilder();
      RegionAction.Builder actions = null;
      byte[] prev_region = HBaseClient.EMPTY_ARRAY;
      int i = 0;
      for (final BatchableRpc rpc : map.values()) {
        // add the RPC back so we match the deserialization arrays.
        
        batch.add(rpc);
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
        final Action.Builder action = Action.newBuilder().setIndex(i++);
        if (rpc instanceof GetRequest) {
          action.setGet(((GetRequest)rpc).getPB());
        } else {
          action.setMutation(rpc.toMutationProto());
        }
        actions.addAction(action);
      }
      if (actions != null) {
        req.addRegionAction(actions.build());
      }
      
      return toChannelBuffer(MMULTI, req.build());
    } catch (Throwable e) {
      LOG.error("Unexpected exception serializing multi-action-append.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper to get the qualifier of an RPC.
   * <b>WARN:</b> This is a fudge that works only with single value
   * puts and single value deletes. For Yahoo that's all we use.
   * @param rpc The non-null RPC to fetch the qualifier from.
   * @return The qualifier or an empty length array if no qualifier is
   * present.
   */
  static byte[] getQualifier(final HBaseRpc rpc) {
    if (rpc instanceof PutRequest) {
      if (((PutRequest) rpc).qualifier() != null) {
        return ((PutRequest) rpc).qualifier();
      } else if (((PutRequest) rpc).qualifiers() != null &&
          ((PutRequest) rpc).qualifiers().length > 0 && 
          ((PutRequest) rpc).qualifiers()[0] != null) {
        return ((PutRequest) rpc).qualifiers()[0];
      } else {
        return new byte[0];
      }
    }
    if (rpc instanceof DeleteRequest) {
      if (((DeleteRequest) rpc).qualifiers() != null && 
          ((DeleteRequest) rpc).qualifiers().length > 0 && 
          ((DeleteRequest) rpc).qualifiers()[0] != null) {
        return ((DeleteRequest) rpc).qualifiers()[0];
      } else {
        return new byte[0];
      }
    }
    return new byte[0];
  }
  
}