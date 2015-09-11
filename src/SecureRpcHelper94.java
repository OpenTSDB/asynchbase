/*
 * Copyright (C) 2015 The Async HBase Authors.  All rights reserved.
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

import java.net.SocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;

/**
 * Implementation for 0.94-security.
 * Enable it by setting the following system property:
 * <BR/>
 * <B>org.hbase.async.security.94</B>
 *
 * See {@link SecureRpcHelper} for configuration
 */
class SecureRpcHelper94 extends SecureRpcHelper {
  private static final Logger LOG = LoggerFactory.getLogger(
      SecureRpcHelper94.class);

  /** Code to switch to simple auth, sent by the region server */
  public static final int SWITCH_TO_SIMPLE_AUTH = -88;

  /**
   * Ctor that instantiates the authentication provider and attempts to 
   * authenticate at the same time.
   * @param hbase_client The Hbase client we belong to
   * @param region_client The region client we're dealing with
   * @param remote_endpoint The remote endpoint of the HBase Region server.
   */
  public SecureRpcHelper94(final HBaseClient hbase_client, 
      final RegionClient region_client, final SocketAddress remote_endpoint) {
    super(hbase_client, region_client, remote_endpoint);
  }

  @Override
  public void sendHello(final Channel channel) {
    final byte[] connectionHeader = {'s', 'r', 'p', 'c', 4};
    byte[] buf = new byte[4 + 1 + 1];
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buf);
    buffer.clear();
    buffer.writeBytes(connectionHeader);
    //code for Kerberos AuthMethod enum in HBaseRPC
    buffer.writeByte(client_auth_provider.getAuthMethodCode());
    Channels.write(channel, buffer);

    // sasl_client is null for Simple Auth case
    if(sasl_client != null)  {
      byte[] challengeBytes = null;
      if (sasl_client.hasInitialResponse()) {
        challengeBytes = processChallenge(new byte[0]);
      }
      if (challengeBytes != null) {
        buf = new byte[4 + challengeBytes.length];
        buffer = ChannelBuffers.wrappedBuffer(buf);
        buffer.clear();
        buffer.writeInt(challengeBytes.length);
        buffer.writeBytes(challengeBytes);

        //if (LOG.isDebugEnabled()) {
        //  LOG.debug("Sending initial SASL Challenge: "+Bytes.pretty(buf));
        //}
        Channels.write(channel, buffer);
      }
    } else {
      sendRPCHeader(channel);
      region_client.sendVersion(channel);
    }
  }

  @Override
  public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) {
    if (sasl_client == null) {
      return buf;
    }

    if (!sasl_client.isComplete()) {
      final int readIdx = buf.readerIndex();
      //RPCID is always -33 during SASL handshake
      final int rpcid = buf.readInt();
      if (rpcid != -33) {
        LOG.warn("Expected a value of -33 during SASL handshake, recieved " + rpcid);
      }

      //read rpc state
      final int state = buf.readInt();

      //0 is success
      //If unsuccessful let common exception handling do the work
      if (state != 0) {
        buf.readerIndex(readIdx);
        return buf;
      }

      //Get length
      //check for special case in length, for request to fallback simple auth
      //let's not support this if we don't have to; seems like a security loophole
      final int len = buf.readInt();
      if (len == SWITCH_TO_SIMPLE_AUTH) {
        throw new IllegalStateException("Server is requesting to fallback "
            + "to simple authentication");
      }

      final byte[] b = new byte[len];
      buf.readBytes(b);
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("Got SASL challenge: " + Bytes.pretty(b));
      //}

      final byte[] challengeBytes = processChallenge(b);

      if (challengeBytes != null) {
        final byte[] outBytes = new byte[4 + challengeBytes.length];
        //if (LOG.isDebugEnabled()) {
        //  LOG.debug("Sending SASL response: " + Bytes.pretty(outBytes));
        //}
        ChannelBuffer outBuffer = ChannelBuffers.wrappedBuffer(outBytes);
        outBuffer.clear();
        outBuffer.writeInt(challengeBytes.length);
        outBuffer.writeBytes(challengeBytes);
        Channels.write(chan, outBuffer);
      }

      if (sasl_client.isComplete()) {
        final String qop = (String) sasl_client.getNegotiatedProperty(Sasl.QOP);
        LOG.info("SASL client context established. Negotiated QoP: " + qop + 
            " on for: " + region_client);
        sendRPCHeader(chan);
        region_client.sendVersion(chan);
      }
      return null;
    }

    return unwrap(buf);
  }

  private void sendRPCHeader(final Channel channel) {
    final byte[] user_bytes = Bytes.UTF8(client_auth_provider.getClientUsername());
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    final byte[] class_bytes = Bytes.UTF8(klass);
    final byte[] buf = new byte[
               4 + 1 + class_bytes.length + 1 + 2 + user_bytes.length + 1];

    ChannelBuffer out_buffer = ChannelBuffers.wrappedBuffer(buf);
    out_buffer.clear();
    out_buffer.writerIndex(out_buffer.writerIndex() + 4);
    out_buffer.writeByte(class_bytes.length);              // 1
    out_buffer.writeBytes(class_bytes);      // 44
    //This is part of protocol header
    //true if a user field exists
    //1 is true in boolean
    out_buffer.writeByte(1);
    out_buffer.writeShort(user_bytes.length);
    out_buffer.writeBytes(user_bytes);
    //true if a realUser field exists
    out_buffer.writeByte(0);
    //write length
    out_buffer.setInt(0, out_buffer.writerIndex() - 4);
    out_buffer = wrap(out_buffer);
    //if(LOG.isDebugEnabled()) {
    //  LOG.debug("Sending RPC Header: " + Bytes.pretty(out_buffer));
    //}
    Channels.write(channel, out_buffer);
  }

}
