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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * This base class provides the logic needed to interface
 * with SASL supported RPC versions. It is used by RegionClient
 * to perform RPC handshaking as well as wrapping/unwrapping
 * the rpc payload depending on the selected QOP level.
 * <BR/>
 *
 * For kerberos the following configurations need to be set as system properties.
 * <ul>
 *   <li>hbase.security.authentication=&lt;MECHANISM&gt;</li>
 *   <li>hbase.kerberos.regionserver.principal=&lt;REGIONSERVER PRINCIPAL&gt;</li>
 *   <li>hbase.rpc.protection=[authentication|integrity|privacy]</li>
 *   <li>hbase.sasl.clientconfig=&lt;JAAS Profile Name&gt;</li>
 *   <li>java.security.auth.login.config=&lt;Path to JAAS conf&gt;</li>
 * </ul>
 * ie
 * <BR/>
 * <ul>
 *   <li>hbase.security.authentication=kerberos</li>
 *   <li>hbase.kerberos.regionserver.principal=hbase/_HOST@MYREALM.COM</li>
 *   <li>hbase.rpc.protection=authentication</li>
 *   <li>hbase.sasl.clientconfig=Client</li>
 *   <li>java.security.auth.login.config=/path/to/jaas.conf</li>
 * </ul>
 */
public abstract class SecureRpcHelper {
  public static final String SECURITY_AUTHENTICATION_KEY = "hbase.security.authentication";
  public static final String RPC_QOP_KEY = "hbase.rpc.protection";

  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcHelper.class);

  protected final RegionClient regionClient;
  protected ClientAuthProvider clientAuthProvider;
  protected SaslClient saslClient;
  protected String ipHost;
  private boolean useWrap;

  public SecureRpcHelper(RegionClient regionClient, String ipHost) {
    this.ipHost = ipHost;
    this.regionClient = regionClient;
    initSecureClientProvider();
  }

  private void initSecureClientProvider() {
    String mech = System.getProperty(SECURITY_AUTHENTICATION_KEY, "simple");

    if("simple".equals(mech)) {
      clientAuthProvider = new SimpleClientAuthProvider();
      useWrap = false;
      return;
    }

    if("kerberos".equals(mech)) {
      clientAuthProvider = new KerberosClientAuthProvider();
    } else {
      try {
        Class clazz = Class.forName(mech);
        clientAuthProvider = (ClientAuthProvider)clazz.newInstance();
      } catch (Exception e) {
        throw new IllegalStateException("Failed to load specified SecureClientProvider: " + mech);
      }
    }

    //Get QOP
    String qop = parseQOP();
    useWrap = qop != null && !"auth".equalsIgnoreCase(qop);

    //sasl configuration
    final Map<String, String> props = new HashMap<String, String>();
    props.put(Sasl.QOP, parseQOP());
    props.put(Sasl.SERVER_AUTH, "true");

    saslClient = clientAuthProvider.newSaslClient(ipHost, props);
  }

  private String parseQOP() {
    String protection = System.getProperty(RPC_QOP_KEY, "authentication");

    if ("integrity".equals(protection)) {
      return "auth-int";
    }
    if ("privacy".equals(protection)) {
      return "auth-conf";
    }
    if ("authentication".equals(protection)) {
      return "auth";
    }
    throw new IllegalArgumentException("Unrecognized rpc protection level: "+protection);
  }

  /**
   * When QOP of auth-int or auth-conf is selected
   * This is used to unwrap the contents from the passed
   * buffer payload.
   */
  public ChannelBuffer unwrap(ChannelBuffer payload) {
    if(!useWrap) {
      return payload;
    }

    int len = payload.readInt();
    try {
      payload =
          ChannelBuffers.wrappedBuffer(saslClient.unwrap(payload.readBytes(len).array(), 0, len));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unwrapped payload: "+Bytes.pretty(payload));
      }
      return payload;
    } catch (SaslException e) {
      throw new IllegalStateException("Failed to unwrap payload", e);
    }
  }

  /**
   * When QOP of auth-int or auth-conf is selected
   * This is used to wrap the contents
   * into the proper payload (ie encryption, signature, etc)
   */
  public ChannelBuffer wrap(ChannelBuffer content) {
    if(!useWrap) {
      return content;
    }

    try {
      byte[] payload = new byte[content.writerIndex()];
      content.readBytes(payload);
      byte[] wrapped = saslClient.wrap(payload, 0, payload.length);
      ChannelBuffer ret = ChannelBuffers.wrappedBuffer(new byte[4 + wrapped.length]);
      ret.clear();
      ret.writeInt(wrapped.length);
      ret.writeBytes(wrapped);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Wrapped payload: "+Bytes.pretty(ret));
      }
      return ret;
    } catch (SaslException e) {
      throw new IllegalStateException("Failed to wrap payload", e);
    }
  }

  public abstract void sendHello(Channel channel);

  public abstract ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan);

  protected byte[] processChallenge(final byte[] b) {
    try {
      return Subject.doAs(clientAuthProvider.getClientSubject(),
          new PrivilegedExceptionAction<byte[]>() {
            @Override
            public byte[] run() {
              try {
                return saslClient.evaluateChallenge(b);
              } catch (SaslException e) {
                return null;
              }
            }
          });
    } catch (PrivilegedActionException e) {
      throw new IllegalStateException("Failed to send rpc hello", e);
    }
  }

}
