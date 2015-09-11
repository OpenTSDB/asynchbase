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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;

import org.hbase.async.auth.ClientAuthProvider;
import org.hbase.async.auth.KerberosClientAuthProvider;
import org.hbase.async.auth.Login;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, Login.class, RegionClient.class,
  SaslClient.class, KerberosClientAuthProvider.class, SecureRpcHelper.class,
  Subject.class })
public class BaseTestSecureRpcHelper {
  protected static byte[] unwrapped_payload = 
    { 'p', 't', 'r', 'a', 'c', 'i' };
  protected static byte[] wrapped_payload = 
    { 0, 0, 0, 10, 0, 0, 0, 6, 'p', 't', 'r', 'a', 'c', 'i'};
  
  protected HBaseClient client;
  protected Config config;
  protected RegionClient region_client;
  protected SocketAddress remote_endpoint;
  protected KerberosClientAuthProvider kerberos_provider;
  protected SaslClient sasl_client;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    config = new Config();
    client = mock(HBaseClient.class);
    region_client = mock(RegionClient.class);
    remote_endpoint = new InetSocketAddress("127.0.0.1", 50512);
    kerberos_provider = mock(KerberosClientAuthProvider.class);
    sasl_client = mock(SaslClient.class);
    
    when(client.getConfig()).thenReturn(config);
    PowerMockito.whenNew(KerberosClientAuthProvider.class).withAnyArguments()
      .thenReturn(kerberos_provider);
    when(kerberos_provider.newSaslClient(anyString(), anyMap()))
      .thenReturn(sasl_client);
  }
  
  /**
   * Super basic implementation of the SecureRpcHelper for unit testing
   */
  protected class UTHelper extends SecureRpcHelper {
    Channel chan;
    ChannelBuffer buffer;    
    public UTHelper(final HBaseClient hbase_client, final RegionClient region_client,
        final SocketAddress remote_endpoint) {
      super(hbase_client, region_client, remote_endpoint);
    }

    @Override
    public void sendHello(final Channel channel) {
      chan = channel;
    }

    @Override
    public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) {
      this.chan = chan;
      buffer = buf;
      return buf;
    }
    
    byte[] doProcessChallenge(final byte[] b) {
      return processChallenge(b);
    }
    
    ClientAuthProvider getProvider() {
      return client_auth_provider;
    }
    
    boolean useWrap() {
      return use_wrap;
    }
    
    String getHostIP() {
      return host_ip;
    }
    
    SaslClient getSaslClient() {
      return sasl_client;
    }
  }

  /**
   * Prepends a byte array with it's length and creates a wrapped channel buffer
   * @param payload The payload to wrap
   * @return A channel buffer for testing
   */
  protected ChannelBuffer getBuffer(final byte[] payload) {
    final byte[] buf = new byte[payload.length + 4];
    System.arraycopy(payload, 0, buf, 4, payload.length);
    Bytes.setInt(buf, payload.length);
    return ChannelBuffers.wrappedBuffer(buf);
  }
  
  /**
   * Helper to unwrap a wrapped buffer, pretending the sasl client simply 
   * prepends the length.
   * @throws Exception Exception it really shouldn't. Really.
   */
  protected void setupUnwrap() throws Exception {
    // TODO - figure out a way to use real wrapping. For now we just stick on
    // two bytes or take em off.
    when(sasl_client.unwrap(any(byte[].class), anyInt(), anyInt()))
      .thenAnswer(new Answer<byte[]>() {
        @Override
        public byte[] answer(final InvocationOnMock invocation)
            throws Throwable {
          final byte[] buffer = (byte[])invocation.getArguments()[0];
          final int length = (Integer)invocation.getArguments()[2];
          final byte[] unwrapped = new byte[length - 4];
          System.arraycopy(buffer, 4, unwrapped, 0, length - 4);
          return unwrapped;
        }
    });
  }
  
  /**
   * Helper to wrap a buffer, pretending the sasl client simply prepends the
   * length.
   * @throws Exception it really shouldn't. Really.
   */
  protected void setupWrap() throws Exception {
    when(sasl_client.wrap(any(byte[].class), anyInt(), anyInt()))
    .thenAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(final InvocationOnMock invocation)
          throws Throwable {
        final byte[] buffer = (byte[])invocation.getArguments()[0];
        final int length = (Integer)invocation.getArguments()[2];
        final byte[] wrapped = new byte[length + 4];
        System.arraycopy(buffer, 0, wrapped, 4, length);
        Bytes.setInt(wrapped, length);
        return wrapped;
      }
    });
  }

  @SuppressWarnings("unchecked")
  protected void setupChallenge() throws Exception {
    PowerMockito.mockStatic(Subject.class);
    PowerMockito.doAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(final InvocationOnMock invocation) throws Throwable {
        final PrivilegedExceptionAction<byte[]> cb = 
            (PrivilegedExceptionAction<byte[]>)invocation.getArguments()[1];
        return cb.run();
      }
    }).when(Subject.class);
    Subject.doAs(any(Subject.class), any(PrivilegedExceptionAction.class));
  }
}
