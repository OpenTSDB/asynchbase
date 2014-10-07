/*
 * Copyright (C) 2014  The Async HBase Authors.  All rights reserved.
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

/**
 * Implementation for 0.96-security.
 *
 * See {@link SecureRpcHelper} for configuration
 */
class SecureRpcHelper96 extends SecureRpcHelper {
  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcHelper96.class);

  public SecureRpcHelper96(RegionClient regionClient, String ipHost) {
    super(regionClient, ipHost);
  }

  @Override
  public void sendHello(Channel channel) {
    byte[] connectionHeader = new byte[] { 'H', 'B', 'a', 's',
                                           0,     // RPC version.
                                           clientAuthProvider.getAuthMethodCode()}; //authmethod
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(connectionHeader);
    Channels.write(channel, buffer);

    //SaslClient is null for Simple Auth case
    if(saslClient != null)  {
      byte[] challengeBytes = null;
      if (saslClient.hasInitialResponse()) {
        challengeBytes = processChallenge(new byte[0]);
      }
      if (challengeBytes != null) {
        byte[] buf = new byte[4 + challengeBytes.length];
        buffer = ChannelBuffers.wrappedBuffer(buf);
        buffer.clear();
        buffer.writeInt(challengeBytes.length);
        buffer.writeBytes(challengeBytes);

        LOG.debug("Sending initial SASL Challenge: "+Bytes.pretty(buf));
        Channels.write(channel, buffer);
      }
    } else {
      sendRPCHeader(channel);
    }
  }

  //This is similar to 94 handshake except rpcid is no longer sent
  public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) {
    if(saslClient == null) {
      return buf;
    }

    if (!saslClient.isComplete()) {
      final int readIdx = buf.readerIndex();
      //read rpc state
      int state = buf.readInt();

      //0 is success
      //If unsuccessful let common exception handling do the work
      if (state != 0) {
        buf.readerIndex(readIdx);
        return buf;
      }

      //Get length
      int len = buf.readInt();
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
      }
      return null;
    }

    return unwrap(buf);
  }

  private void sendRPCHeader(Channel chan) {
    Channels.write(chan, header095());
    regionClient.becomeReady(chan, RegionClient.SERVER_VERSION_095_OR_ABOVE);
  }

  private ChannelBuffer header095() {
    final RPCPB.UserInformation user = RPCPB.UserInformation.newBuilder()
      .setEffectiveUser(clientAuthProvider.getClientUsername())
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
    return header;
  }
}
