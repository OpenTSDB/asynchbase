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

import com.google.protobuf.CodedOutputStream;

import org.hbase.async.generated.RPCPB;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;

import java.io.IOException;
import java.net.SocketAddress;

/**
 * Implementation for security on HBase servers version 96 and up
 *
 * See {@link SecureRpcHelper} for configuration
 */
class SecureRpcHelper96 extends SecureRpcHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      SecureRpcHelper96.class);

  /** The array to send as the first hello */
  final byte[] connection_header;
  
  /**
   * Ctor that instantiates the authentication provider and attempts to 
   * authenticate at the same time.
   * @param hbase_client The Hbase client we belong to
   * @param region_client The region client we're dealing with
   * @param remote_endpoint The remote endpoint of the HBase Region server.
   */
  public SecureRpcHelper96(final HBaseClient hbase_client, 
      final RegionClient region_client, final SocketAddress remote_endpoint) {
    super(hbase_client, region_client, remote_endpoint);
    connection_header = new byte[] { 'H', 'B', 'a', 's',
          0,                                       // RPC version.
          client_auth_provider.getAuthMethodCode() //authmethod
        }; 
  }

  @Override
  public void sendHello(final Channel channel) {
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(connection_header);
    Channels.write(channel, buffer);

    // sasl_client is null for Simple Auth case
    if (sasl_client != null)  {
      byte[] challenge_bytes = null;
      if (sasl_client.hasInitialResponse()) {
        challenge_bytes = processChallenge(new byte[0]);
      }
      if (challenge_bytes != null) {
        final byte[] buf = new byte[4 + challenge_bytes.length];
        buffer = ChannelBuffers.wrappedBuffer(buf);
        buffer.clear();
        buffer.writeInt(challenge_bytes.length);
        buffer.writeBytes(challenge_bytes);

        //if (LOG.isDebugEnabled()) {
        //  LOG.debug("Sending initial SASL Challenge: " + Bytes.pretty(buf));
        //}
        Channels.write(channel, buffer);
      } else {
        // TODO - is this exception worthy? We'll never get back here
        LOG.error("Missing initial Sasl response on client " + region_client);
      }
    } else {
      sendRPCHeader(channel);
    }
  }

  @Override
  public ChannelBuffer handleResponse(final ChannelBuffer buf, 
      final Channel chan) {
    if (sasl_client == null) {
      return buf;
    }

    if (!sasl_client.isComplete()) {
      final int read_index = buf.readerIndex();
      final int state = buf.readInt();

      //0 is success
      //If unsuccessful let common exception handling do the work
      if (state != 0) {
        LOG.error("Sasl initialization failed with a status of [" + state + 
            "]: " + this);
        buf.readerIndex(read_index);
        return buf;
      } 

      final int len = buf.readInt();
      final byte[] b = new byte[len];
      buf.readBytes(b);
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("Got SASL challenge: "+Bytes.pretty(b));
      //}

      final byte[] challenge_bytes = processChallenge(b);

      if (challenge_bytes != null) {
        final byte[] out_bytes = new byte[4 + challenge_bytes.length];
        //if (LOG.isDebugEnabled()) {
        //  LOG.debug("Sending SASL response: "+Bytes.pretty(out_bytes));
        //}
        final ChannelBuffer out_buffer = ChannelBuffers.wrappedBuffer(out_bytes);
        out_buffer.clear();
        out_buffer.writeInt(challenge_bytes.length);
        out_buffer.writeBytes(challenge_bytes);
        Channels.write(chan, out_buffer);
      }

      if (sasl_client.isComplete()) {
        final String qop = (String)sasl_client.getNegotiatedProperty(Sasl.QOP);
        LOG.info("SASL client context established. Negotiated QoP: " + qop + 
            " on for: " + region_client);
        sendRPCHeader(chan);
      } else {
        // TODO is this an issue?
      }
      return null;
    }

    return unwrap(buf);
  }

  /**
   * Writes a header to the region server with the authenticated username.
   * It will also mark the region client as ready to process so that any
   * pending RPCs will be flushed to the server.
   * @param chan The channel to write to
   */
  private void sendRPCHeader(final Channel chan) {
    final RPCPB.UserInformation user = RPCPB.UserInformation.newBuilder()
      .setEffectiveUser(client_auth_provider.getClientUsername())
      .build();
    final RPCPB.ConnectionHeader pb = RPCPB.ConnectionHeader.newBuilder()
      .setUserInfo(user)
      .setServiceName("ClientService")
      .setCellBlockCodecClass("org.apache.hadoop.hbase.codec.KeyValueCodec")
      .build();
    final int pblen = pb.getSerializedSize();
    final byte[] buf = new byte[4 + pblen];
    final ChannelBuffer header = ChannelBuffers.wrappedBuffer(buf);
    header.clear();
    header.writeInt(pblen);  // 4 bytes
    try {
      final CodedOutputStream output =
        CodedOutputStream.newInstance(buf, 4, pblen);
      pb.writeTo(output);
      output.checkNoSpaceLeft();
    } catch (IOException e) {
      throw new RuntimeException("Should never happen", e);
    }
    // We wrote to the underlying buffer but Netty didn't see the writes,
    // so move the write index forward.
    header.writerIndex(buf.length);
    Channels.write(chan, header);
    region_client.becomeReady(chan, RegionClient.SERVER_VERSION_095_OR_ABOVE);
  }
}
