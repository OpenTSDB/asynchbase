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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.hbase.async.auth.ClientAuthProvider;
import org.hbase.async.auth.KerberosClientAuthProvider;
import org.hbase.async.auth.Login;
import org.hbase.async.auth.SimpleClientAuthProvider;
import org.hbase.async.generated.RPCPB;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.protobuf.CodedOutputStream;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, Login.class, RegionClient.class,
  SaslClient.class, KerberosClientAuthProvider.class, SecureRpcHelper.class,
  Subject.class, Channel.class, Channels.class })
public class TestSecureRpcHelper96 extends BaseTestSecureRpcHelper {

  private Channel channel;
  private List<ChannelBuffer> buffers;
  private SecureRpcHelper96 helper;

  @Before
  public void beforeLocal() throws Exception {
    channel = mock(Channel.class);

    when(kerberos_provider.getAuthMethodCode())
      .thenReturn(ClientAuthProvider.KEBEROS_CLIENT_AUTH_CODE);
    when(kerberos_provider.getClientUsername()).thenReturn("Eskarina");
    
    when(sasl_client.hasInitialResponse()).thenReturn(true);
    
    PowerMockito.mockStatic(Channels.class);
    PowerMockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) throws Throwable {
        if (buffers == null) {
          buffers = new ArrayList<ChannelBuffer>(2);
        }
        buffers.add((ChannelBuffer)invocation.getArguments()[1]);
        return null;
      }
    }).when(Channels.class);
    Channels.write(any(Channel.class), any(ChannelBuffer.class));
    
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    helper = new SecureRpcHelper96(client, region_client, remote_endpoint);
  }
  
  @Test
  public void ctorKerberos() throws Exception {
    assertTrue(helper.client_auth_provider instanceof KerberosClientAuthProvider);
    assertTrue(sasl_client == helper.sasl_client);
  }
  
  @Test
  public void sendHello() throws Exception {
    PowerMockito.doAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        return new byte[] { 42 };
      }
    }).when(sasl_client).evaluateChallenge(any(byte[].class));
    helper.sendHello(channel);
    assertEquals(2, buffers.size());
    assertArrayEquals(new byte[] { 'H', 'B', 'a', 's', 0, 81 }, 
        buffers.get(0).array());
    assertArrayEquals(new byte[] { 0, 0, 0, 1, 42 }, 
        buffers.get(1).array());
  }
  
  @Test
  public void sendHelloNoInitialResponse() throws Exception {
    when(sasl_client.hasInitialResponse()).thenReturn(false);
    PowerMockito.doAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        return new byte[] { 42 };
      }
    }).when(sasl_client).evaluateChallenge(any(byte[].class));
    helper.sendHello(channel);
    assertEquals(1, buffers.size());
    assertArrayEquals(new byte[] { 'H', 'B', 'a', 's', 0, 81 }, 
        buffers.get(0).array());
  }
  
  @Test
  public void sendHelloProcessException() throws Exception {
    PowerMockito.doThrow(new IllegalStateException("Boo!"))
      .when(sasl_client).evaluateChallenge(any(byte[].class));
    RuntimeException ex = null;
    try {
      helper.sendHello(channel);
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof IllegalStateException);
    assertEquals(1, buffers.size());
    assertArrayEquals(new byte[] { 'H', 'B', 'a', 's', 0, 81 }, 
        buffers.get(0).array());
  }
  
  @Test
  public void sendHelloSaslException() throws Exception {
    when(sasl_client.hasInitialResponse())
      .thenThrow(new RuntimeException("Boo!"));
    RuntimeException ex = null;
    try {
      helper.sendHello(channel);
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof RuntimeException);
    assertEquals(1, buffers.size());
    assertArrayEquals(new byte[] { 'H', 'B', 'a', 's', 0, 81 }, 
        buffers.get(0).array());
  }
  
  @Test
  public void sendHelloNoSasl() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "simple");
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, 
        "Cohen");
    helper = new SecureRpcHelper96(client, region_client, remote_endpoint);
    helper.sendHello(channel);
    
    assertArrayEquals(new byte[] { 'H', 'B', 'a', 's', 0, 80 }, 
        buffers.get(0).array());
    assertArrayEquals(header095("Cohen"), buffers.get(1).array());
    verify(region_client, times(1)).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
  }
  
  @Test
  public void handleResponseSimple() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "simple");
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, 
        "Cohen");
    helper = new SecureRpcHelper96(client, region_client, remote_endpoint);
    
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(new byte[] { 42 });
    assertTrue(buf == helper.handleResponse(buf, channel));
  }
  
  @Test
  public void handleResponseProcessChallenge() throws Exception {
    setupChallenge();

    // kinda fake in that we'll process it in one go
    when(sasl_client.isComplete()).thenReturn(false).thenReturn(true);
    final ChannelBuffer buf = getSaslBuffer(0, new byte[] { 42 });
    assertNull(helper.handleResponse(buf, channel));
    assertArrayEquals(header095("Eskarina"), buffers.get(0).array());
    verify(region_client, times(1)).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, times(1)).getNegotiatedProperty(Sasl.QOP);
  }
  
  @Test
  public void handleResponseProcessChallengeBadState() throws Exception {
    setupChallenge();

    // kinda fake in that we'll process it in one go
    when(sasl_client.isComplete()).thenReturn(false).thenReturn(true);
    final ChannelBuffer buf = getSaslBuffer(1, new byte[] { 42 });
    assertTrue(buf == helper.handleResponse(buf, channel));
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, never()).getNegotiatedProperty(Sasl.QOP);
  }
  
  @Test
  public void handleResponseProcessChallengeNotCompleted() throws Exception {
    setupChallenge();

    when(sasl_client.isComplete()).thenReturn(false).thenReturn(false);
    final ChannelBuffer buf = getSaslBuffer(0, new byte[] { 42 });
    assertNull(helper.handleResponse(buf, channel));
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, never()).getNegotiatedProperty(Sasl.QOP);
  }

  @Test
  public void handleResponseProcessChallengeNegotiaionException() throws Exception {
    setupChallenge();

    when(sasl_client.getNegotiatedProperty(Sasl.QOP))
      .thenThrow(new IllegalStateException("Boo!"));
    when(sasl_client.isComplete()).thenReturn(false).thenReturn(true);
    final ChannelBuffer buf = getSaslBuffer(0, new byte[] { 42 });
    RuntimeException ex = null;
    try {
      helper.handleResponse(buf, channel);
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof IllegalStateException);
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, times(1)).getNegotiatedProperty(Sasl.QOP);
  }
  
  @Test
  public void handleResponseProcessChallengeBadPacket() throws Exception {
    setupChallenge();

    when(sasl_client.isComplete()).thenReturn(false).thenReturn(true);
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(
        new byte[] { 0, 0, 0, 0, 0, 1 });
    RuntimeException ex = null;
    try {
      helper.handleResponse(buf, channel);
    } catch (RuntimeException e) {
      ex = e;
    }
    assertTrue(ex instanceof IndexOutOfBoundsException);
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, never()).getNegotiatedProperty(Sasl.QOP);
  }
  
  @Test
  public void handleResponseSaslComplete() throws Exception {
    setupChallenge();
    
    when(sasl_client.isComplete()).thenReturn(true);
    final ChannelBuffer buf = getSaslBuffer(0, new byte[] { 42 });
    assertTrue(buf == helper.handleResponse(buf, channel));
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, never()).getNegotiatedProperty(Sasl.QOP);
  }
  
  @Test
  public void handleResponseSaslCompleteWrapped() throws Exception {
    setupUnwrap();
    
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(wrapped_payload);
    Whitebox.setInternalState(helper, "use_wrap", true);
    when(sasl_client.isComplete()).thenReturn(true);
    final ChannelBuffer unwrapped = helper.handleResponse(buf, channel);
    assertArrayEquals(unwrapped.array(), unwrapped_payload);
    assertNull(buffers);
    verify(region_client, never()).becomeReady(channel, 
        RegionClient.SERVER_VERSION_095_OR_ABOVE);
    verify(sasl_client, never()).getNegotiatedProperty(Sasl.QOP);
  }
  
  /**
   * Creates a buffer with the sasl state at the top
   * @param state The state to encode
   * @param payload The pyalod to wrap
   * @return A channel buffer for testing
   */
  protected ChannelBuffer getSaslBuffer(final int state, final byte[] payload) {
    final byte[] buf = new byte[payload.length + 4 + 4];
    System.arraycopy(payload, 0, buf, 8, payload.length);
    System.arraycopy(Bytes.fromInt(payload.length), 0, buf, 4, 4);
    Bytes.setInt(buf, state);
    return ChannelBuffers.wrappedBuffer(buf);
  }
  
  /**
   * Pretty much a straight rip of the method but without the channel buffer
   * @param username The username to encode
   * @return The byte array to compare against in the unit test
   */
  private byte[] header095(final String username) {
    final RPCPB.UserInformation user = RPCPB.UserInformation.newBuilder()
      .setEffectiveUser(username)
      .build();
    final RPCPB.ConnectionHeader pb = RPCPB.ConnectionHeader.newBuilder()
      .setUserInfo(user)
      .setServiceName("ClientService")
      .setCellBlockCodecClass("org.apache.hadoop.hbase.codec.KeyValueCodec")
      .build();
    final int pblen = pb.getSerializedSize();
    final byte[] buf = new byte[4 + pblen];
    Bytes.setInt(buf, pblen);
    try {
      final CodedOutputStream output =
        CodedOutputStream.newInstance(buf, 4, pblen);
      pb.writeTo(output);
      output.checkNoSpaceLeft();
    } catch (IOException e) {
      throw new RuntimeException("Should never happen", e);
    }
    return buf;
  }
}
