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

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.sasl.SaslClient;

import org.hbase.async.HBaseClient;

/**
 * Class to extend to support other authentication mechanisms for Secure RPC 
 * versions. To select a provider fully qualified class names are passed to 
 * 'hbase.security.authentication' system property.
 * @since 1.7
 */
public abstract class ClientAuthProvider {

  public static final byte SIMPLE_CLIENT_AUTH_CODE = 80;
  public static final byte KEBEROS_CLIENT_AUTH_CODE = 81;
  
  /** The hbase client object for fetching settings */
  final HBaseClient hbase_client;
  
  /**
   * Default CTor that stores a reference to the AsyncHBase client
   * @param hbase_client The HBaseClient to fetch configuration and timers from
   */
  public ClientAuthProvider(final HBaseClient hbase_client) {
    this.hbase_client = hbase_client;
  }
  
  /**
   * Return a new SaslClient for target serviceIP host. Properties passed
   * are expected to be passed as part of the newSaslClient props parameter.
   * @param service_ip The IP of the target host.
   * @param props The set of properties with which to create a new client.
   * @return the newly-created client.
   */
  public abstract SaslClient newSaslClient(final String service_ip, 
      final Map<String, String> props);

  /**
   * @return The name of the user this provider is tied to.
   */
  public abstract String getClientUsername();

  /**
   * @return The RPC code identifying this authentication mechanism
   */
  public abstract byte getAuthMethodCode();

  /**
   * @return The subject identifying the client user this provider is tied to.
   */
  public abstract Subject getClientSubject();

}
