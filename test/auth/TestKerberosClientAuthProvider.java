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
package org.hbase.async.auth;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.zookeeper.server.auth.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.hbase.async.auth.KerberosClientAuthProvider.ClientCallbackHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
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
@PrepareForTest({ HBaseClient.class, Login.class, Subject.class, Sasl.class,
  SaslClient.class, KerberosName.class, KerberosClientAuthProvider.class })
public class TestKerberosClientAuthProvider {
  private HBaseClient client;
  private Config config;
  private Login login;
  private Subject subject;
  private SaslClient sasl_client;
  private Set<Principal> principals;
  private Principal principal;
  private KerberosName kerberos_name;
  
  // written when the callback is run to create a new SaslClient
  private String mechanism;
  private String service_name;
  private String service_hostname;
  private Map<String, String> properties;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    config = new Config();
    client = mock(HBaseClient.class);
    login = mock(Login.class);
    subject = mock(Subject.class);
    sasl_client = mock(SaslClient.class);
    principal = mock(Principal.class);
    kerberos_name = mock(KerberosName.class);
    
    config.overrideConfig(KerberosClientAuthProvider.PRINCIPAL_KEY, "ephebe");
    
    when(client.getConfig()).thenReturn(config);
    when(login.getSubject()).thenReturn(subject);
    
    principals = new HashSet<Principal>();
    principals.add(principal);
    when(subject.getPrincipals()).thenReturn(principals);
    
    PowerMockito.whenNew(KerberosName.class).withAnyArguments()
      .thenReturn(kerberos_name);
    when(kerberos_name.toString()).thenReturn("Aching");
    when(kerberos_name.getServiceName()).thenReturn("feegle");
    when(kerberos_name.getHostName()).thenReturn("ephebe");
    
    PowerMockito.mockStatic(Login.class);
    PowerMockito.when(Login.getCurrentLogin()).thenReturn(login);
    
    PowerMockito.mockStatic(Sasl.class);
    PowerMockito.when(Sasl.createSaslClient(any(String[].class), anyString(), 
        anyString(), anyString(), anyMap(), any(CallbackHandler.class)))
          .thenAnswer(new Answer<SaslClient>() {
            @Override
            public SaslClient answer(final InvocationOnMock invocation)
                throws Throwable {
              mechanism = ((String[])invocation.getArguments()[0])[0];
              service_name = (String)invocation.getArguments()[2];
              service_hostname = (String)invocation.getArguments()[3];
              properties = (Map<String, String>)invocation.getArguments()[4];
              return sasl_client;
            }
          });
    
    PowerMockito.mockStatic(Subject.class);
    PowerMockito.doAnswer(new Answer<SaslClient>() {
      @Override
      public SaslClient answer(final InvocationOnMock invocation) throws Throwable {
        final PrivilegedExceptionAction<SaslClient> cb = 
            (PrivilegedExceptionAction<SaslClient>)invocation.getArguments()[1];
        return cb.run();
      }
    }).when(Subject.class);
    Subject.doAs(eq(subject), any(PrivilegedExceptionAction.class));
  }
  
  @Test
  public void ctor() throws Exception {
    final KerberosClientAuthProvider provider = 
        new KerberosClientAuthProvider(client);
    assertEquals("Aching", provider.getClientUsername());
  }
  
  @Test (expected = IllegalStateException.class)
  public void ctorLoginFailure() throws Exception {
    PowerMockito.doThrow(new LoginException("Boo!")).when(Login.class);
    Login.initUserIfNeeded(any(Config.class), any(HashedWheelTimer.class), 
        anyString(), any(ClientCallbackHandler.class));
    new KerberosClientAuthProvider(client);
  }
  
  @Test (expected = RuntimeException.class)
  public void ctorOtherException() throws Exception {
    PowerMockito.doThrow(new RuntimeException("Boo!")).when(Login.class);
    Login.initUserIfNeeded(any(Config.class), any(HashedWheelTimer.class), 
        anyString(), any(ClientCallbackHandler.class));
    new KerberosClientAuthProvider(client);
  }
  
  @Test (expected = NullPointerException.class)
  public void nullClient() throws Exception {
    new KerberosClientAuthProvider(null);
  }
  
  @Test
  public void getAuthMethodCode() throws Exception {
    final KerberosClientAuthProvider provider = 
        new KerberosClientAuthProvider(client);
    assertEquals(ClientAuthProvider.KEBEROS_CLIENT_AUTH_CODE, 
        provider.getAuthMethodCode());
  }
  
  @Test
  public void newSaslClient() throws Exception {
    final KerberosClientAuthProvider provider = 
        new KerberosClientAuthProvider(client);
    final Map<String, String> props = new HashMap<String, String>(0);
    final SaslClient new_client = provider.newSaslClient("localhost", props);
    assertTrue(sasl_client == new_client);
    assertEquals("GSSAPI", mechanism);
    assertEquals("ephebe", service_hostname);
    assertEquals("feegle", service_name);
    assertTrue(properties == props);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = IllegalStateException.class)
  public void newSaslClientFailedSubject() throws Exception {
    PowerMockito.doThrow(new RuntimeException("Boo!")).when(Subject.class);
    Subject.doAs(eq(subject), any(PrivilegedExceptionAction.class));
    
    final KerberosClientAuthProvider provider = 
        new KerberosClientAuthProvider(client);
    final Map<String, String> props = new HashMap<String, String>(0);
    provider.newSaslClient("localhost", props);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = IllegalStateException.class)
  public void newSaslClientFailedCreation() throws Exception {
    PowerMockito.mockStatic(Sasl.class);
    PowerMockito.when(Sasl.createSaslClient(any(String[].class), anyString(), 
        anyString(), anyString(), anyMap(), any(CallbackHandler.class)))
        .thenThrow(new SaslException("Boo!"));
    
    final KerberosClientAuthProvider provider = 
        new KerberosClientAuthProvider(client);
    final Map<String, String> props = new HashMap<String, String>(0);
    provider.newSaslClient("localhost", props);
  }
  
  @Test
  public void clientCallbackHandlerName() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final NameCallback callback = new NameCallback("Enter a name", "Ogg");
    callbacks[0] = callback;
    assertNull(callback.getName());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertEquals("Ogg", callback.getName());
  }
  
  @Test
  public void clientCallbackHandlerNameNullDefault() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final NameCallback callback = new NameCallback("Enter a name");
    callbacks[0] = callback;
    assertNull(callback.getName());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertNull(callback.getName());
  }
  
  @Test
  public void clientCallbackHandlerPassword() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final PasswordCallback callback = 
        new PasswordCallback("Gimme a password", false);
    callbacks[0] = callback;
    assertNull(callback.getPassword());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler("Adora Belle");
    cch.handle(callbacks);
    
    assertArrayEquals("Adora Belle".toCharArray(), callback.getPassword());
  }
  
  @Test
  public void clientCallbackHandlerPasswordNoPassword() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final PasswordCallback callback = 
        new PasswordCallback("Gimme a password", false);
    callbacks[0] = callback;
    assertNull(callback.getPassword());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertNull(callback.getPassword());
  }
  
  @Test
  public void clientCallbackHandlerRealm() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final RealmCallback callback = new RealmCallback("Gimme a realm", "Buggarup");
    callbacks[0] = callback;
    assertNull(callback.getText());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertEquals("Buggarup", callback.getText());
  }
  
  @Test
  public void clientCallbackHandlerRealmNullDefault() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final RealmCallback callback = new RealmCallback("Gimme a realm");
    callbacks[0] = callback;
    assertNull(callback.getText());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertNull(callback.getText());
  }
  
  @Test
  public void clientCallbackHandlerAuthorize() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final AuthorizeCallback callback = new AuthorizeCallback("Roland", "Roland");
    callbacks[0] = callback;
    assertFalse(callback.isAuthorized());
    assertNull(callback.getAuthorizedID());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertTrue(callback.isAuthorized());
    assertEquals("Roland", callback.getAuthorizedID());
  }
  
  @Test
  public void clientCallbackHandlerAuthorizeNoMatch() throws Exception {
    final Callback[] callbacks = new Callback[1];
    final AuthorizeCallback callback = new AuthorizeCallback("Dean", "Ridcully");
    callbacks[0] = callback;
    assertFalse(callback.isAuthorized());
    assertNull(callback.getAuthorizedID());
    
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
    
    assertFalse(callback.isAuthorized());
    assertNull(callback.getAuthorizedID());
  }
  
  @Test (expected = UnsupportedCallbackException.class)
  public void clientCallbackHandlerUnrecognized() throws Exception {
    final Callback[] callbacks = new Callback[1];

    callbacks[0] = new UnknownCallback();

    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
  }
  
  @Test
  public void clientCallbackHandlerEmptyCallbacks() throws Exception {
    // shouldn't ever happen, but who knows?
    final Callback[] callbacks = new Callback[0];
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(callbacks);
  }
  
  @Test (expected = NullPointerException.class)
  public void clientCallbackHandlerNullCallbacks() throws Exception {
    // shouldn't ever happen, but who knows?
    final ClientCallbackHandler cch = new ClientCallbackHandler(null);
    cch.handle(null);
  }
  
  static class UnknownCallback implements Callback {
    // just a dummy class
  }
}
