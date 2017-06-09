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

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.hbase.async.auth.ClientAuthProvider;
import org.hbase.async.auth.KerberosClientAuthProvider;
import org.hbase.async.auth.SimpleClientAuthProvider;
import org.hbase.async.SecureRpcHelper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This base class provides the logic needed to interface with SASL supported 
 * RPC versions. It is used by RegionClient to perform RPC handshaking as well 
 * as wrapping/unwrapping the rpc payload depending on the selected QOP level.
 * <br>
 *
 * For Kerberos the following configurations need to be set as system properties.
 * <ul>
 *   <li>hbase.security.authentication=&lt;MECHANISM&gt;</li>
 *   <li>hbase.kerberos.regionserver.principal=&lt;REGIONSERVER PRINCIPAL&gt;</li>
 *   <li>hbase.rpc.protection=[authentication|integrity|privacy]</li>
 *   <li>hbase.sasl.clientconfig=&lt;JAAS Profile Name&gt;</li>
 *   <li>java.security.auth.login.config=&lt;Path to JAAS conf&gt;</li>
 * </ul>
 * i.e.
 * <br>
 * <ul>
 *   <li>hbase.security.authentication=kerberos</li>
 *   <li>hbase.kerberos.regionserver.principal=hbase/_HOST@MYREALM.COM</li>
 *   <li>hbase.rpc.protection=authentication</li>
 *   <li>hbase.sasl.clientconfig=Client</li>
 *   <li>java.security.auth.login.config=/path/to/jaas.conf</li>
 * </ul>
 * @since 1.7
 */
public abstract class SecureRpcHelper {
  public static final String SECURITY_AUTHENTICATION_KEY = 
      "hbase.security.authentication";
  public static final String RPC_QOP_KEY = 
      "hbase.rpc.protection";

  private static final Logger LOG = LoggerFactory.getLogger(SecureRpcHelper.class);

  /** The HBaseClient config to pull values from */
  protected final Config config;
  
  /** The region client this helper will be working with */
  protected final RegionClient region_client;
  
  /** The IP of the region client */
  protected final String host_ip;
  
  /** The authentication provider, e.g. Simple or Kerberos */
  protected ClientAuthProvider client_auth_provider;
  
  /** The Sasl client if used */
  protected SaslClient sasl_client;
  
  /** Whether or not to encrypt/decrypt the payload on the socket */
  protected boolean use_wrap;

  /**
   * Ctor that instantiates the authentication provider and attempts to 
   * authenticate at the same time.
   * @param hbase_client The Hbase client we belong to
   * @param region_client The region client we're dealing with
   * @param remote_endpoint The remote endpoint of the HBase Region server.
   */
  public SecureRpcHelper(final HBaseClient hbase_client, 
      final RegionClient region_client, final SocketAddress remote_endpoint) {
    config = hbase_client.getConfig();
    this.host_ip = ((InetSocketAddress)remote_endpoint).getAddress()
        .getHostAddress();
    this.region_client = region_client;
    initSecureClientProvider(hbase_client);
  }

  /**
   * Instantiates the proper security provider based on the configuration. The
   * provider can be either "simple", "kerberos" or a canonical class name
   * to load from the class path. Note that this will attempt to authenticate
   * so it will throw an exception if the login failed.
   * @param hbase_client The hbase client we belong to.
   * @throws IllegalStateException if the config contained a class name and we
   * couldn't instantiate the class.
   */
  private void initSecureClientProvider(final HBaseClient hbase_client) {
    final String mechanism = config.hasProperty(SECURITY_AUTHENTICATION_KEY) ?
        config.getString(SECURITY_AUTHENTICATION_KEY) : "simple";

    if ("simple".equalsIgnoreCase(mechanism)) {
      client_auth_provider = new SimpleClientAuthProvider(hbase_client);
      use_wrap = false;
      return;
    }

    if ("kerberos".equalsIgnoreCase(mechanism)) {
      client_auth_provider = new KerberosClientAuthProvider(hbase_client);
    } else {
      try {
        final Class<?> clazz = Class.forName(mechanism);
        final Constructor<?> ctor = clazz.getConstructor(HBaseClient.class);
        client_auth_provider = (ClientAuthProvider)ctor.newInstance(hbase_client);
        LOG.info("Successfully instantiated a security provider of type: " + 
            clazz.getCanonicalName());
      } catch (Exception e) {
        throw new IllegalStateException(
            "Failed to load specified SecureClientProvider: " + mechanism, e);
      }
    }

    final String qop = parseQOP();
    use_wrap = qop != null && !"auth".equalsIgnoreCase(qop);

    final Map<String, String> props = new HashMap<String, String>(2);
    props.put(Sasl.QOP, parseQOP());
    props.put(Sasl.SERVER_AUTH, "true");

    sasl_client = client_auth_provider.newSaslClient(host_ip, props);
  }

  /**
   * Helper that checks to see if we should encrypt the packets between HBase
   * based on the client config. If the client has set "integrity" or "privacy"
   * then the packets will be encrypted. If set to "authentication" then no
   * encryption is used. Any other value will throw an exception
   * @return A string to compare against to see if we should wrap or not.
   * @throws IllegalArgumentException if the config doesn't contain one of 
   * the three strings above.
   */
  private String parseQOP() {
    final String protection = config.hasProperty(RPC_QOP_KEY) ? 
        config.getString(RPC_QOP_KEY) : "authentication";

    if ("integrity".equalsIgnoreCase(protection)) {
      return "auth-int";
    }
    if ("privacy".equalsIgnoreCase(protection)) {
      return "auth-conf";
    }
    if ("authentication".equalsIgnoreCase(protection)) {
      return "auth";
    }
    throw new IllegalArgumentException("Unrecognized rpc protection level: " 
        + protection);
  }

  /**
   * When QOP of auth-int or auth-conf is selected
   * This is used to unwrap the contents from the passed buffer payload. It
   * should be called as soon as it comes off the Netty channel.
   * Note that the ReplayingBufferDecoder may throw an error if we are unable
   * to read the full buffer and replay the data.
   * @param payload A wrapper around content.
   * @return The content extracted from the payload.
   * @throws IllegalStateException if the sasl client was unable to unwrap
   * the payload
   */
  public ChannelBuffer unwrap(final ChannelBuffer payload) {
    if (!use_wrap) {
      return payload;
    }

    final int len = payload.readInt();
    try {
      final ChannelBuffer unwrapped = ChannelBuffers.wrappedBuffer(
              sasl_client.unwrap(payload.readBytes(len).array(), 0, len));
      // If encryption was enabled, it's a good bet that you shouldn't log it
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("Unwrapped payload: " + Bytes.pretty(unwrapped));
      //}
      return unwrapped;
    } catch (SaslException e) {
      throw new IllegalStateException("Failed to unwrap payload", e);
    }
  }

  /**
   * When QOP of auth-int or auth-conf is selected
   * This is used to wrap the contents into the proper payload (ie encryption, 
   * signature, etc) and should be called just before sending the buffer to Netty.
   * @param content The content to be wrapped.
   * @return The wrapped payload.
   * @throws IllegalStateException if the sasl client was unable to wrap
   * the payload
   */
  public ChannelBuffer wrap(final ChannelBuffer content) {
    if (!use_wrap) {
      return content;
    }

    try {
      final byte[] payload = new byte[content.writerIndex()];
      content.readBytes(payload);
      final byte[] wrapped = sasl_client.wrap(payload, 0, payload.length);
      final ChannelBuffer ret = ChannelBuffers.wrappedBuffer(
          new byte[4 + wrapped.length]);
      ret.clear();
      ret.writeInt(wrapped.length);
      ret.writeBytes(wrapped);
      //if (LOG.isDebugEnabled()) {
      //  LOG.debug("Wrapped payload: " + Bytes.pretty(ret));
      //}
      return ret;
    } catch (SaslException e) {
      throw new IllegalStateException("Failed to wrap payload", e);
    }
  }

  /**
   * Sends the initial handshake and/or SASL challenge to the region server
   * @param channel The channel to write to
   */
  public abstract void sendHello(final Channel channel);

  /**
   * Handles a buffer received from the region server. It may be security 
   * related or it may be a wrapped packet if security has been negotiated. 
   * If wrapping is disabled, the original buffer is returned. If the data is
   * security related, a null should be returned.
   * NOTE: If the buffer IS security related, make sure to drain the entire
   * buffer or Netty will replay it until you're done consuming.
   * @param buf The buffer off the channel
   * @param chan The channel that we received the buffer from
   * @return The possibly unwrapped channel buffer for further decoding. If null
   * then the response was security related.
   */
  public abstract ChannelBuffer handleResponse(final ChannelBuffer buf, 
      final Channel chan);

  /**
   * Handles passing a buffer off to the sasl client for challenge evaluation
   * @param b The buffer to process
   * @return The results of the challenge if successful, or null if there was
   * a SaslException.
   * @throws IllegalStateException when processing of the action failed due to
   * a PrivilegedActionException
   */
  protected byte[] processChallenge(final byte[] b) {
    try {
      final class PrivilegedAction implements PrivilegedExceptionAction<byte[]> {
        @Override
        public byte[] run() {
          try {
            return sasl_client.evaluateChallenge(b);
          } catch (SaslException e) {
            LOG.error("Failed Sasl challenge", e);
            return null;
          }
        }
        @Override
        public String toString() {
          return "evaluate sasl challenge";
        }
      }
      
      return Subject.doAs(client_auth_provider.getClientSubject(),
          new PrivilegedAction());
    } catch (PrivilegedActionException e) {
      throw new IllegalStateException("Failed to process challenge", e);
    }
  }

}
