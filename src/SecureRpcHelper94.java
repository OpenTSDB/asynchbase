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
  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcHelper94.class);

  public static final int SWITCH_TO_SIMPLE_AUTH = -88;


  public SecureRpcHelper94(RegionClient regionClient, String ipHost) {
    super(regionClient, ipHost);
  }

  @Override
  public void sendHello(Channel channel) {
    byte[] connectionHeader = {'s', 'r', 'p', 'c', 4};
    byte[] buf = new byte[4 + 1 + 1];
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buf);
    buffer.clear();
    buffer.writeBytes(connectionHeader);
    //code for Kerberos AuthMethod enum in HBaseRPC
    buffer.writeByte(clientAuthProvider.getAuthMethodCode());
    Channels.write(channel, buffer);

    //SaslClient is null for Simple Auth case
    if(saslClient != null)  {
      byte[] challengeBytes = null;
      if (saslClient.hasInitialResponse()) {
        challengeBytes = processChallenge(new byte[0]);
      }
      if (challengeBytes != null) {
        buf = new byte[4 + challengeBytes.length];
        buffer = ChannelBuffers.wrappedBuffer(buf);
        buffer.clear();
        buffer.writeInt(challengeBytes.length);
        buffer.writeBytes(challengeBytes);

        LOG.debug("Sending initial SASL Challenge: "+Bytes.pretty(buf));
        Channels.write(channel, buffer);
      }
    } else {
      sendRPCHeader(channel);
      regionClient.sendVersion(channel);
    }
  }

  @Override
  public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) {
    if(saslClient == null) {
      return buf;
    }

    if (!saslClient.isComplete()) {
      final int readIdx = buf.readerIndex();
      //RPCID is always -33 during SASL handshake
      final int rpcid = buf.readInt();

      //read rpc state
      int state = buf.readInt();

      //0 is success
      //If unsuccessful let common exception handling do the work
      if (state != 0) {
        buf.readerIndex(readIdx);
        return buf;
      }

      //Get length
      //check for special case in length, for request to fallback simple auth
      //let's not support this if we don't have to seems like a security loophole
      int len = buf.readInt();
      if(len == SWITCH_TO_SIMPLE_AUTH) {
        throw new IllegalStateException("Server is requesting to fallback to simple " +
            "authentication");
      }

      LOG.debug("Got length: "+len);
      final byte[] b = new byte[len];
      buf.readBytes(b);
      LOG.debug("Got SASL challenge: "+Bytes.pretty(b));

      byte[] challengeBytes = processChallenge(b);

      if (challengeBytes != null) {
        byte[] outBytes = new byte[4 + challengeBytes.length];
        LOG.debug("Sending SASL response: "+Bytes.pretty(outBytes));
        ChannelBuffer outBuffer = ChannelBuffers.wrappedBuffer(outBytes);
        outBuffer.clear();
        outBuffer.writeInt(challengeBytes.length);
        outBuffer.writeBytes(challengeBytes);
        Channels.write(chan, outBuffer);
      }

      if (saslClient.isComplete()) {
        String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client context established. Negotiated QoP: " + qop);
        }
        sendRPCHeader(chan);
        regionClient.sendVersion(chan);
      }
      return null;
    }

    return unwrap(buf);
  }

  private void sendRPCHeader(Channel channel) {
    byte[] userBytes = Bytes.UTF8(clientAuthProvider.getClientUsername());
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    byte[] classBytes = Bytes.UTF8(klass);
    byte[] buf = new byte[4 + 1 + classBytes.length + 1 + 2 + userBytes.length + 1];

    ChannelBuffer outBuffer = ChannelBuffers.wrappedBuffer(buf);
    outBuffer.clear();
    outBuffer.writerIndex(outBuffer.writerIndex()+4);
    outBuffer.writeByte(classBytes.length);              // 1
    outBuffer.writeBytes(classBytes);      // 44
    //This is part of protocol header
    //true if a user field exists
    //1 is true in boolean
    outBuffer.writeByte(1);
    outBuffer.writeShort(userBytes.length);
    outBuffer.writeBytes(userBytes);
    //true if a realUser field exists
    outBuffer.writeByte(0);
    //write length
    outBuffer.setInt(0, outBuffer.writerIndex() - 4);
    outBuffer = wrap(outBuffer);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Sending RPC Header: "+Bytes.pretty(outBuffer));
    }
    Channels.write(channel, outBuffer);
  }

}
