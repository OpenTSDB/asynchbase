/*
 * Copyright (C) 2015-2016  The Async HBase Authors.  All rights reserved.
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

import java.util.AbstractMap;
import java.util.List;

import org.hbase.async.generated.ClientPB;
import org.hbase.async.generated.ClientPB.Result;
import org.hbase.async.generated.RPCPB;
import org.hbase.async.generated.CellPB.Cell;
import org.hbase.async.generated.ClientPB.MultiResponse;
import org.hbase.async.generated.ClientPB.RegionActionResult;
import org.hbase.async.generated.ClientPB.ResultOrException;
import org.hbase.async.generated.ClientPB.RegionActionResult.Builder;
import org.hbase.async.generated.HBasePB.NameBytesPair;
import org.hbase.async.generated.RPCPB.ResponseHeader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Ignore;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessageLite;

@Ignore // ignore for test runners
public class PBufResponses {

  /**
   * Generate a full getRequest response with frame length and meta for decode() 
   * @param id The RPC ID to encode
   * @param kvs The KVs to serialize
   * @return A buffer to pass upstream
   */
  public static ChannelBuffer getRequest(final int id, final List<KeyValue> kvs)
    throws Exception {
    final ClientPB.Result.Builder result = ClientPB.Result.newBuilder();
    for (final KeyValue kv : kvs) {
      result.addCell(kvToCell(kv));
    }

    final ClientPB.GetResponse response = ClientPB.GetResponse.newBuilder()
        .setResult(result.build()).build();
    
    final RPCPB.ResponseHeader header = RPCPB.ResponseHeader.newBuilder()
        .setCallId(id)
        //.setCellBlockMeta(meta)
        .build();
    
    return writeToBuffer(header, response);
  }
  
  /**
   * Serializes a PBuf response with a frame length for upstream processing
   * @param header The header to encode
   * @param response The response to encode
   * @return The buffer to pass upstream
   */
  static ChannelBuffer writeToBuffer(final ResponseHeader header, 
      final GeneratedMessageLite response) throws Exception {
    final int hlen = header.getSerializedSize();
    final int vhlen = CodedOutputStream.computeRawVarint32Size(hlen);
    final int pblen = response != null ? response.getSerializedSize() : 0;
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[hlen + vhlen + vlen + pblen + 4];
    final CodedOutputStream out = CodedOutputStream.newInstance(buf, 4, 
        hlen + vhlen + vlen + pblen);
    
    out.writeMessageNoTag(header);
    if (response != null) {
      out.writeMessageNoTag(response);
    }
    
    Bytes.setInt(buf, buf.length - 4);
    return ChannelBuffers.wrappedBuffer(buf);
  }
  
  /**
   * Convert a key value pair to a cell for responses
   * @param kv The KV to convert
   * @return a cell
   */
  static Cell kvToCell(final KeyValue kv) {
    return Cell.newBuilder()
               .setRow(Bytes.wrap(kv.key()))
               .setFamily(Bytes.wrap(kv.family()))
               .setQualifier(Bytes.wrap(kv.qualifier()))
               .setValue(Bytes.wrap(kv.value()))
               .setTimestamp(kv.timestamp())
               .build();
  }
  
  /**
   * Generate a multi-action response from the list of results. All results are
   * considered part of a single region action.
   * @param responses The results or exceptions to serialize
   * @return A response PBuf object
   */
  static MultiResponse generateMultiActionResponse(
      final List<ResultOrException> responses) {
    return MultiResponse.newBuilder().addRegionActionResult(
        generateRegionActionResult(responses)).build();
  }
  
  /**
   * Generate a multi-action response from a list of region action results
   * @param responses The result sets to serialize
   * @return A response PBuf object
   */
  static MultiResponse generateMultiActionResponseFromRars(
      final List<RegionActionResult> responses) {
    
    MultiResponse.Builder builder = MultiResponse.newBuilder();
    for (RegionActionResult rar : responses) {
      builder.addRegionActionResult(rar);
    }
    return builder.build();
  }
  
  /**
   * Generate a region action result from a list of result/exceptions
   * @param responses The responses to encode
   * @return A region action result
   */
  static RegionActionResult generateRegionActionResult(
      final List<ResultOrException> responses) {
    final Builder rar = RegionActionResult.newBuilder();
    for (final ResultOrException roe : responses) {
      rar.addResultOrException(roe);
    }
    return rar.build();
  }
  
  /**
   * Convert a key value to a result for multi-actions
   * @param kv The column to encode
   * @param i The multi-action array index to encode
   * @return The result
   */
  static ResultOrException kvToROE(final KeyValue kv, final int i) {
    final Result result = Result.newBuilder().addCell(kvToCell(kv)).build();
    return ResultOrException.newBuilder()
        .setResult(result).setIndex(i).build();
  }
  
  /**
   * Convert the list of columns to a result set for a multi-action batched RPC
   * @param kvs The list of columns to encode
   * @param i The index of the parent RPC in the multi-action batch
   * @return The result
   */
  static ResultOrException kvsToROE(final List<KeyValue> kvs, final int i) {
    final Result.Builder result = Result.newBuilder();
    for (final KeyValue kv : kvs) {
      result.addCell(kvToCell(kv));
    } 
    return ResultOrException.newBuilder()
        .setResult(result).setIndex(i).build();
  }
  
  static ResultOrException generateAssociatedROE(final int associated_cell_cnt, 
      final int result_index) {
    final Result.Builder result = Result.newBuilder();
    result.setAssociatedCellCount(associated_cell_cnt);
    return ResultOrException.newBuilder().setResult(result).setIndex(result_index).build();
  }
  
  /**
   * Generates an empty result for an RPC such as returned from a PUT or DELETE
   * @param i The index of the parent RPC in the multi-action batch
   * @return The result
   */
  static ResultOrException generateEmptyResult(final int i) {
    return ResultOrException.newBuilder().setIndex(i).build();
  }
  
  /**
   * Return an exception for a multi-action batched RPC
   * @param ex The exception to encode
   * @param i The index of the parent RPC in the batch
   * @return The exception
   */
  static ResultOrException generateException(final Throwable ex, final int i) {
    return ResultOrException.newBuilder()
        .setException(buildException(ex)).setIndex(i).build();
  }
  
  /**
   * Override that returns an exception with the proper HBase class name for a 
   * multi action response.
   * @param class_name The name of the remote class
   * @param msg The message to encode
   * @param i The index of the parent RPC in the batch
   * @return The exception
   */
  static ResultOrException generateException(final String class_name, 
      final String msg, final int i) {
    return ResultOrException.newBuilder()
        .setException(buildException(class_name, msg)).setIndex(i).build();
  }
  
  /**
   * Encodes a response WITHOUT the frame length or header. Useful for 
   * individual parsing calls
   * @param response The response to serialize
   * @return A channel buffer to parse
   */
  static ChannelBuffer encodeResponse(final GeneratedMessageLite response) 
      throws Exception {
    final int pblen = response.getSerializedSize();
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[vlen + pblen];
    final CodedOutputStream out = CodedOutputStream.newInstance(buf, 0, 
        vlen + pblen);
    
    out.writeMessageNoTag(response);
    return ChannelBuffers.wrappedBuffer(buf);
  }
  
  /**
   * Encodes a response with associated cells, WITHOUT the frame length or header. It should be 
   * used for the {@link HBaseRpc} deserialize parsing.
   * @param response
   * @param associated_cells
   * @return
   * @throws Exception
   */
  static AbstractMap.SimpleEntry<ChannelBuffer, Integer> encodeResponseWithAssocaitedCells(
      final GeneratedMessageLite response, final List<KeyValue> associated_cells) throws Exception {
    final int pblen = response.getSerializedSize();
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[vlen + pblen];
    final CodedOutputStream out = CodedOutputStream.newInstance(buf, 0, vlen + pblen);

    out.writeMessageNoTag(response);
    ChannelBuffer cbuf = ChannelBuffers.dynamicBuffer();
    cbuf.writeBytes(buf);

    int rbyte_cnt = cbuf.readableBytes();

    for (KeyValue kv : associated_cells) {
      // we don't care the type, useless
      kv.serialize(cbuf, KeyValue.PUT);
    } // end for
    int cells_block_size = cbuf.readableBytes() - rbyte_cnt;
    return new AbstractMap.SimpleEntry<ChannelBuffer, Integer>(
        cbuf, cells_block_size);
  }
  
  /**
   * Shamelessly pulled from org.apache.hadoop.hbase.protobuf.ResponseConverter
   * to encode an exception as a PBuf response
   * @param t The exception to encode
   * @return A NameBytesPair to store in a response
   */
  static NameBytesPair buildException(final Throwable t) {
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(t.getClass().getName());
    parameterBuilder.setValue(
      //ByteString.copyFromUtf8(StringUtils.stringifyException(t))); // need HDP
      ByteString.copyFromUtf8(t.toString()));
    return parameterBuilder.build();
  }

  /**
   * Override that uses the proper remote class name
   * @param class_name The exception to encode
   * @param msg The message to encode
   * @return A NameBytesPair to store in a response
   */
  static NameBytesPair buildException(final String class_name, final String msg) {
    NameBytesPair.Builder parameterBuilder = NameBytesPair.newBuilder();
    parameterBuilder.setName(class_name);
    parameterBuilder.setValue(
      ByteString.copyFromUtf8(msg));
    return parameterBuilder.build();
  }
  
  /**
   * Generates a single PBuf with an exception instead of a response. For use,
   * e.g., in responding to a GetRequest with an NSRE
   * @param id The RPC ID
   * @param clazz The remote exception class name
   * @return A buffer you can pass to the Region Client
   */
  static ChannelBuffer generateException(final int id, final String clazz)
      throws Exception {
  
    final RPCPB.ExceptionResponse response = 
        RPCPB.ExceptionResponse.newBuilder()
        .setExceptionClassName(clazz)
        .setStackTrace("mock stack trace")
        .build();
    
    final RPCPB.ResponseHeader header = RPCPB.ResponseHeader.newBuilder()
        .setCallId(id)
        .setException(response)
        //.setCellBlockMeta(meta)
        .build();
    
    return writeToBuffer(header, null);
  }
}
