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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.hbase.async.auth.KerberosClientAuthProvider;
import org.hbase.async.auth.MockProvider;
import org.hbase.async.auth.SimpleClientAuthProvider;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

public class TestSecureRpcHelper extends BaseTestSecureRpcHelper {
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNoConfigs() throws Exception {
    // we assume "simple" by default but haven't set a username
    new UTHelper(client, region_client, remote_endpoint);
  }
  
  @Test
  public void ctorSimple() throws Exception {
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, "Drumknott");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof SimpleClientAuthProvider);
    assertFalse(helper.useWrap());
    assertEquals("127.0.0.1", helper.getHostIP());
    assertNull(helper.getSaslClient());
  }
  
  @Test (expected = IllegalStateException.class)
  public void ctorUnknownImplementation() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "IDontExist");
    new UTHelper(client, region_client, remote_endpoint);
  }
  
  @Test
  public void ctorDynamic() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "org.hbase.async.auth.MockProvider");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof MockProvider);
    assertFalse(helper.useWrap());
  }
  
  @Test
  public void ctorKerberosNoQop() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof KerberosClientAuthProvider);
    assertFalse(helper.useWrap());
    assertTrue(sasl_client == helper.getSaslClient());
  }
  
  @Test
  public void ctorKerberosNoWrap() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Authentication");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof KerberosClientAuthProvider);
    assertFalse(helper.useWrap());
    assertTrue(sasl_client == helper.getSaslClient());
  }
  
  @Test
  public void ctorKerberosWithWrapPrivacy() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Privacy");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof KerberosClientAuthProvider);
    assertTrue(helper.useWrap());
    assertTrue(sasl_client == helper.getSaslClient());
  }
  
  @Test
  public void ctorKerberosWithWrapIntegrity() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Integrity");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertTrue(helper.getProvider() instanceof KerberosClientAuthProvider);
    assertTrue(helper.useWrap());
    assertTrue(sasl_client == helper.getSaslClient());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorKerberosUnknownQop() throws Exception {
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "derp");
    new UTHelper(client, region_client, remote_endpoint);
  }

  @Test
  public void unwrap() throws Exception {
    setupUnwrap();
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(wrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Integrity");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    final ChannelBuffer unwrapped = helper.unwrap(buf);
    assertArrayEquals(unwrapped.array(), unwrapped_payload);
    assertFalse(unwrapped == buf);
  }
  
  @Test
  public void unwrapNotWrapped() throws Exception {
    setupUnwrap();
    final ChannelBuffer buf = getBuffer(unwrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    final ChannelBuffer unwrapped = helper.unwrap(buf);
    assertArrayEquals(unwrapped.array(), getBuffer(unwrapped_payload).array());
    assertTrue(unwrapped == buf);
  }
  
  @Test (expected = IllegalStateException.class)
  public void unwrapException() throws Exception {
    when(sasl_client.unwrap(any(byte[].class), anyInt(), anyInt()))
      .thenThrow(new SaslException("Boo!"));
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Integrity");
    final ChannelBuffer buf = getBuffer(unwrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    helper.unwrap(buf);
  }
  
  @Test
  public void wrap() throws Exception {
    setupWrap();
    final ChannelBuffer buf = getBuffer(unwrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Integrity");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    final ChannelBuffer wrapped = helper.wrap(buf);
    assertArrayEquals(wrapped.array(), getBuffer(wrapped_payload).array());
    assertFalse(wrapped == buf);
  }
  
  @Test
  public void wrapNotWrapped() throws Exception {
    setupWrap();
    final ChannelBuffer buf = getBuffer(unwrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    final ChannelBuffer wrapped = helper.wrap(buf);
    assertArrayEquals(wrapped.array(), getBuffer(unwrapped_payload).array());
    assertTrue(wrapped == buf);
  }
  
  @Test (expected = IllegalStateException.class)
  public void wrapException() throws Exception {
    when(sasl_client.wrap(any(byte[].class), anyInt(), anyInt()))
      .thenThrow(new SaslException("Boo!"));
    config.overrideConfig(SecureRpcHelper.RPC_QOP_KEY, "Integrity");
    final ChannelBuffer buf = getBuffer(unwrapped_payload);
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    helper.wrap(buf);
  }
  
  @Test
  public void processChallenge() throws Exception {
    setupChallenge();
    final byte[] challenge = { 42 };
    
    PowerMockito.doAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        return challenge;
      }
    }).when(sasl_client).evaluateChallenge(any(byte[].class));
    
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertArrayEquals(challenge, helper.doProcessChallenge(challenge));
  }
  
  @Test
  public void processChallengeSaslException() throws Exception {
    final byte[] challenge = { 42 };
    setupChallenge();
    when(sasl_client.evaluateChallenge(any(byte[].class)))
      .thenThrow(new SaslException("Boo!"));
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    assertNull(helper.doProcessChallenge(challenge));
  }
  
  @Test (expected = RuntimeException.class)
  public void processChallengeOtherSaslException() throws Exception {
    final byte[] challenge = { 42 };
    setupChallenge();
    when(sasl_client.evaluateChallenge(any(byte[].class)))
      .thenThrow(new RuntimeException("Boo!"));
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    helper.doProcessChallenge(challenge);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = IllegalStateException.class)
  public void processChallengePrivilegedActionException() throws Exception {
    final byte[] challenge = { 42 };
    PowerMockito.mockStatic(Subject.class);
    PowerMockito.doThrow(new PrivilegedActionException(
        new RuntimeException("Boo!"))).when(Subject.class);
    Subject.doAs(any(Subject.class), any(PrivilegedExceptionAction.class));

    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    helper.doProcessChallenge(challenge);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = RuntimeException.class)
  public void processChallengeRuntimeException() throws Exception {
    final byte[] challenge = { 42 };
    PowerMockito.mockStatic(Subject.class);
    PowerMockito.doThrow(new RuntimeException("Boo!")).when(Subject.class);
    Subject.doAs(any(Subject.class), any(PrivilegedExceptionAction.class));
    
    config.overrideConfig(SecureRpcHelper.SECURITY_AUTHENTICATION_KEY, 
        "kerberos");
    final UTHelper helper = new UTHelper(client, region_client, remote_endpoint);
    helper.doProcessChallenge(challenge);
  }
  
}
