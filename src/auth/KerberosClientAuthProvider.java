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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
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

import org.apache.zookeeper.server.auth.KerberosName;
import org.hbase.async.HBaseClient;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles Kerberos based authentication using the Java Authentication and
 * Authorization Service (JAAS).
 * The class requires a few settings including:
 * asynchbase.security.auth.kerberos.regionserver.principal - a principal name
 * or template to use for authentication. The value can have the token "_HOST"
 * which will be replaced with the cannonical name of the localhost.
 * @since 1.7
 */
public class KerberosClientAuthProvider extends ClientAuthProvider {
  public static final String PASSWORD_KEY = "hbase.regionserver.kerberos.password";
  public static final String PRINCIPAL_KEY = "hbase.kerberos.regionserver.principal";
  private static final Logger LOG = LoggerFactory.getLogger(
      KerberosClientAuthProvider.class);

  /** The principal name */
  private String client_principal_name;

  /**
   * Default ctor that will attempt a login and setup the Login singleton
   * @param hbase_client The HBaseClient to fetch configuration and timers from
   * @throws IllegalArgumentException if the 
   * asynchbase.security.auth.simple.username is missing, null or empty.
   * @throws IllegalStateException if the login was unsuccessful
   */
  public KerberosClientAuthProvider(final HBaseClient hbase_client) {
    super(hbase_client);

    String password = null;
    if (hbase_client.getConfig().hasProperty(PASSWORD_KEY)) {
      password = hbase_client.getConfig().getString(PASSWORD_KEY);
    }
    try {
      Login.initUserIfNeeded(hbase_client.getConfig(), 
          (HashedWheelTimer)hbase_client.getTimer(),
          hbase_client.getConfig().getString(Login.LOGIN_CONTEXT_NAME_KEY),
          new ClientCallbackHandler(password));
    } catch (LoginException e) {
      throw new IllegalStateException("Failed to get login context", e);
    }

    //-- Prepare principals needed for SaslClient --
    final Login client_login = Login.getCurrentLogin();
    client_principal_name = getClientPrincipalName(client_login);
  }

  @Override
  public SaslClient newSaslClient(final String service_ip, 
      final Map<String, String> props) {
    final Login client_login = Login.getCurrentLogin();

    String server_principal = hbase_client.getConfig().getString(PRINCIPAL_KEY);
    if (server_principal.contains("_HOST")) {
      try {
        final String host = InetAddress.getByName(service_ip)
            .getCanonicalHostName();
        server_principal = server_principal.replaceAll("_HOST", host);
      } catch (UnknownHostException e) {
        throw new IllegalStateException("Failed to resolve hostname for: " + 
            service_ip, e);
      }
    }
    LOG.info("Connecting to " + server_principal);

    final KerberosName service_kerberos_name = new KerberosName(server_principal);
    final String service_name = service_kerberos_name.getServiceName();
    final String service_hostname = service_kerberos_name.getHostName();

    //-- create SaslClient --
    try {
      final class PriviledgedAction implements 
        PrivilegedExceptionAction<SaslClient> {
        @Override
        public SaslClient run() throws Exception {
          LOG.info("Client will use GSSAPI as SASL mechanism.");
          final String[] mechanism = { "GSSAPI" };
          LOG.debug("Creating sasl client: client=" + client_principal_name +
              ", service=" + service_name + ", serviceHostname=" + service_hostname);
          return Sasl.createSaslClient(
              mechanism, 
              null,              // authorization ID
              service_name,
              service_hostname, 
              props, 
              null);             // callback
        }
        @Override
        public String toString() {
          return "create sasl client";
        }
      }
      
      return Subject.doAs(client_login.getSubject(), new PriviledgedAction());
    } catch (Exception e) {
      LOG.error("Error creating SASL client", e);
      throw new IllegalStateException("Error creating SASL client", e);
    }
  }

  @Override
  public String getClientUsername() {
    return client_principal_name;
  }

  @Override
  public byte getAuthMethodCode() {
    return KEBEROS_CLIENT_AUTH_CODE;
  }

  @Override
  public Subject getClientSubject() {
    return Login.getCurrentLogin().getSubject();
  }

  /**
   * Return the principal name if set
   * @param login The login object to pull the name from
   * @return The name if found, null if not
   */
  private String getClientPrincipalName(final Login login) {
    if (login.getSubject() == null) {
      return null;
    }
    
    final Set<Principal> principals = login.getSubject().getPrincipals();
    if (principals == null || principals.isEmpty()) {
      return null;
    }
    final Principal principal = principals.iterator().next();
    final KerberosName name = new KerberosName(principal.getName());
    return name.toString();
  }

  /**
   * A callback executed on authentication to validate the name or password.
   * Right now the only way to set a password is via the config file, and that
   * isn't particularly secure. So use a key cache instead.
   */
  static class ClientCallbackHandler implements CallbackHandler {
    private String password;

    /** @param the password to use for auth */
    public ClientCallbackHandler(final String password) {
      this.password = password;
    }

    @Override
    public void handle(final Callback[] callbacks) 
        throws UnsupportedCallbackException {
      for (final Callback callback : callbacks) {
        LOG.debug("Processing callback: " + callback.getClass());
        if (callback instanceof NameCallback) {
          final NameCallback name_callback = (NameCallback) callback;
          name_callback.setName(name_callback.getDefaultName());
        } else if (callback instanceof PasswordCallback) {
          final PasswordCallback password_callback = (PasswordCallback)callback;
          if (password != null) {
            password_callback.setPassword(password.toCharArray());
          } else {
            LOG.warn("Could not login: the client is being asked for a password, but the " +
                " client code does not currently support obtaining a password from the user." +
                " Make sure that the client is configured to use a ticket cache (using" +
                " the JAAS configuration setting 'useTicketCache=true)' and restart the client. If" +
                " you still get this message after that, the TGT in the ticket cache has expired and must" +
                " be manually refreshed. To do so, first determine if you are using a password or a" +
                " keytab. If the former, run kinit in a Unix shell in the environment of the user who" +
                " is running this asynchbase client using the command" +
                " 'kinit <princ>' (where <princ> is the name of the client's Kerberos principal)." +
                " If the latter, do" +
                " 'kinit -k -t <keytab> <princ>' (where <princ> is the name of the Kerberos principal, and" +
                " <keytab> is the location of the keytab file). After manually refreshing your cache," +
                " restart this client. If you continue to see this message after manually refreshing" +
                " your cache, ensure that your KDC host's clock is in sync with this host's clock.");
          }
        } else if (callback instanceof RealmCallback) {
          final RealmCallback realm_callback = (RealmCallback) callback;
          realm_callback.setText(realm_callback.getDefaultText());
        } else if (callback instanceof AuthorizeCallback) {
          final AuthorizeCallback authorize_callback = 
              (AuthorizeCallback) callback;
          final String authid = authorize_callback.getAuthenticationID();
          final String authzid = authorize_callback.getAuthorizationID();
          if (authid.equals(authzid)) {
            authorize_callback.setAuthorized(true);
            authorize_callback.setAuthorizedID(authzid);
          } else {
            authorize_callback.setAuthorized(false);
          }
        } else {
          throw new UnsupportedCallbackException(callback, 
              "Unrecognized SASL ClientCallback: " + callback.getClass());
        }
      }
    }
  }

}
