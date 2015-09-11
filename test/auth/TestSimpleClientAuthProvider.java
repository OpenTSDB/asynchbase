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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.hbase.async.Config;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HBaseClient.class })
public class TestSimpleClientAuthProvider {
  private HBaseClient client;
  private Config config;
  
  @Before
  public void before() throws Exception {
    config = new Config();
    client = mock(HBaseClient.class);
    when(client.getConfig()).thenReturn(config);
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, "Weatherwax");
  }
  
  @Test
  public void ctor() throws Exception {
    final ClientAuthProvider provider = new SimpleClientAuthProvider(client);
    assertEquals("Weatherwax", provider.getClientUsername());
    assertNull(provider.newSaslClient("127.0.0.1", 
        Collections.<String, String> emptyMap()));
    assertEquals(ClientAuthProvider.SIMPLE_CLIENT_AUTH_CODE, 
        provider.getAuthMethodCode());
    assertNull(provider.getClientSubject());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMissingUsername() throws Exception {
    config = new Config();
    when(client.getConfig()).thenReturn(config);
    new SimpleClientAuthProvider(client);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullUsername() throws Exception {
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, null);
    new SimpleClientAuthProvider(client);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyUsername() throws Exception {
    config.overrideConfig(SimpleClientAuthProvider.USERNAME_KEY, "");
    new SimpleClientAuthProvider(client);
  }
  
  @Test (expected = NullPointerException.class)
  public void ctorNullClient() throws Exception {
    new SimpleClientAuthProvider(null);
  }
}
