/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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

import org.apache.zookeeper.server.auth.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

/**
 * Kerberos implementation for secure RPC authentication.
 * See {@link SecureRpcHelper94} for available configuration.
 */
final class KerberosClientAuthProvider extends ClientAuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosClientAuthProvider.class);

  public static final String REGIONSERVER_PRINCIPAL_KEY = "hbase.kerberos.regionserver.principal";

  private String clientPrincipalName;

  public KerberosClientAuthProvider() {
    //Login with jaas conf if needed
    try {
      Login.initUserIfNeeded(System.getProperty(Login.LOGIN_CONTEXT_NAME_KEY),
          new ClientCallbackHandler(null));
    } catch (LoginException e) {
      throw new IllegalStateException("Failed to get login context", e);
    }

    //-- Prepare principals needed for SaslClient --
    Login clientLogin = Login.getCurrLogin();
    clientPrincipalName = getClientPrincipalName(clientLogin);
  }

  @Override
  public SaslClient newSaslClient(final String serviceIP, final Map<String,
      String> props) {
    String hostname;
    SaslClient saslClient;
    Login clientLogin = Login.getCurrLogin();

    //We might need hostname for _HOST substitution in regionserver principal
    try {
      hostname = InetAddress.getByName(serviceIP).getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Failed to resolve hostname for: "+serviceIP, e);
    }

    final String serverPrincipal =
        System.getProperty(REGIONSERVER_PRINCIPAL_KEY).replaceAll("_HOST", hostname);

    LOG.debug("Connecting to "+serverPrincipal);

    final KerberosName serviceKerberosName = new KerberosName(serverPrincipal);
    final String serviceName = serviceKerberosName.getServiceName();
    final String serviceHostname = serviceKerberosName.getHostName();

    //-- create SaslClient --
    try {

      return Subject.doAs(clientLogin.getSubject(),
          new PrivilegedExceptionAction<SaslClient>() {
            public SaslClient run() throws SaslException {
              LOG.info("Client will use GSSAPI as SASL mechanism.");
              String[] mechs = {"GSSAPI"};
              LOG.debug("creating sasl client: client=" + clientPrincipalName +
                  ";service=" + serviceName + ";serviceHostname=" + serviceHostname);
              return Sasl.createSaslClient(mechs, null, serviceName,
                  serviceHostname, props, null);
            }
          });
    } catch (Exception e) {
      LOG.error("Error creating SASL client", e);
      throw new IllegalStateException("Error creating SASL client", e);
    }
  }

  @Override
  public String getClientUsername() {
    return clientPrincipalName;
  }

  @Override
  public byte getAuthMethodCode() {
    return 81;
  }

  @Override
  public Subject getClientSubject() {
    return Login.getCurrLogin().getSubject();
  }

  private String getClientPrincipalName(Login login) {
    final Principal clientPrincipal =
       (Principal)login.getSubject().getPrincipals().toArray()[0];
    final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
    return clientKerberosName.toString();
  }

  public static class ClientCallbackHandler implements CallbackHandler {
    private String password = null;

    public ClientCallbackHandler(String password) {
      this.password = password;
    }

    public void handle(javax.security.auth.callback.Callback[] callbacks) throws
        UnsupportedCallbackException {
      for (javax.security.auth.callback.Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          nc.setName(nc.getDefaultName());
        }
        else {
          if (callback instanceof PasswordCallback) {
            PasswordCallback pc = (PasswordCallback)callback;
            if (password != null) {
              pc.setPassword(this.password.toCharArray());
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
          }
          else {
            if (callback instanceof RealmCallback) {
              RealmCallback rc = (RealmCallback) callback;
              rc.setText(rc.getDefaultText());
            }
            else {
              if (callback instanceof AuthorizeCallback) {
                AuthorizeCallback ac = (AuthorizeCallback) callback;
                String authid = ac.getAuthenticationID();
                String authzid = ac.getAuthorizationID();
                if (authid.equals(authzid)) {
                  ac.setAuthorized(true);
                } else {
                  ac.setAuthorized(false);
                }
                if (ac.isAuthorized()) {
                  ac.setAuthorizedID(authzid);
                }
              }
              else {
                throw new UnsupportedCallbackException(callback,"Unrecognized SASL ClientCallback");
              }
            }
          }
        }
      }
    }
  }
}
