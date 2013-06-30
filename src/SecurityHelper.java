package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
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
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class provides the logic needed to interface
 * with SASL supported RPC versions. It is used by RegionClient
 * to perform RPC handshaking as well as wrapping/unwrapping
 * the rpc payload depending on the selected QOP level.
 * <BR/>
 * Presently only 0.94-security is currently supported.
 * Enable it by setting the following system property:
 * <BR/>
 * <B>org.hbase.async.security.94</B>
 *
 * The following configurations need to be set as system properties.
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
final class SecurityHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityHelper.class);
  public static final String SECURITY_AUTHENTICATION_KEY = "hbase.security.authentication";
  public static final String REGIONSERVER_PRINCIPAL_KEY = "hbase.kerberos.regionserver.principal";
  public static final String RPC_QOP_KEY = "hbase.rpc.protection";

  private final SaslClient saslClient;
  private final String clientPrincipalName;
  private final Login clientLogin;
  private final boolean useWrap;


  private final RegionClient regionClient;
  private final String hostname;


  public SecurityHelper(RegionClient regionClient, String iphost) {
    this.regionClient = regionClient;

    //Login with jaas conf if needed
    try {
      Login.initUserIfNeeded(System.getProperty(Login.LOGIN_CONTEXT_NAME_KEY),
          new ClientCallbackHandler(null));
    } catch (LoginException e) {
      throw new IllegalStateException("Failed to get login context", e);
    }

    //We might need hostname for _HOST substituation in regionserver principal
    try {
      hostname = InetAddress.getByName(iphost).getCanonicalHostName();
    } catch (UnknownHostException e) {
      throw new IllegalStateException("Failed to resolve hostname for: "+iphost, e);
    }


    //-- Prepare principals needed for SaslClient --
    clientLogin = Login.getCurrLogin();
    final Principal clientPrincipal =
       (Principal)clientLogin.getSubject().getPrincipals().toArray()[0];
    final String serverPrincipal =
        System.getProperty(REGIONSERVER_PRINCIPAL_KEY, "Client").replaceAll("_HOST", hostname);

    LOG.debug("Connecting to "+serverPrincipal);

    final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
    final KerberosName serviceKerberosName = new KerberosName(serverPrincipal);
    final String serviceName = serviceKerberosName.getServiceName();
    final String serviceHostname = serviceKerberosName.getHostName();
    clientPrincipalName = clientKerberosName.toString();

    //-- create SaslClient --
    try {
      //Get QOP
      String qop = parseQOP();
      useWrap = qop != null && !"auth".equalsIgnoreCase(qop);

      //sasl configuration
      final Map<String, String> props = new TreeMap<String, String>();
      props.put(Sasl.QOP, parseQOP());
      props.put(Sasl.SERVER_AUTH, "true");

      saslClient = Subject.doAs(clientLogin.getSubject(),
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


  public void sendHello(Channel channel) {
    byte[] connectionHeader = {'s', 'r', 'p', 'c', 4};
    byte[] buf = new byte[4 + 1 + 1];
    ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(buf);
    buffer.clear();
    buffer.writeBytes(connectionHeader);
    //code for Kerberos AuthMethod enum in HBaseRPC
    buffer.writeByte(81);
    Channels.write(channel, buffer);

    byte[] challengeBytes = null;
    if (saslClient.hasInitialResponse()) {
      challengeBytes = processChallenge(new byte[0]);
    }
    if (challengeBytes != null) {
      buf = new byte[4 + challengeBytes.length];
      buffer = ChannelBuffers.wrappedBuffer(buf);
      buffer.clear();
      buffer.writeInt(challengeBytes.length);
      buffer.writeBytes(challengeBytes);

      LOG.debug("Sending initial SASL Challenge: "+Bytes.pretty(buf));
      Channels.write(channel, buffer);
    }
  }

  public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) {
    if (!saslClient.isComplete()) {
      final int readIdx = buf.readerIndex();
      //RPCID is always -33 during SASL handshake
      final int rpcid = buf.readInt();

      //read rpc state
      int state = buf.readInt();

      //0 is success
      //If unsuccessful let common exception handling do the work
      if (state != 0) {
        buf.readerIndex(readIdx);
        return buf;
      }
      int len = buf.readInt();
      LOG.debug("handleSaslResponse:Got len="+len);
      final byte[] b = new byte[len];
      buf.readBytes(b);
      LOG.debug("Got SASL challenge: "+Bytes.pretty(b));

      byte[] challengeBytes = processChallenge(b);

      if (challengeBytes != null) {
        byte[] outBytes = new byte[4 + challengeBytes.length];
        LOG.debug("Sending SASL response: "+Bytes.pretty(outBytes));
        ChannelBuffer outBuffer = ChannelBuffers.wrappedBuffer(outBytes);
        outBuffer.clear();
        outBuffer.writeInt(challengeBytes.length);
        outBuffer.writeBytes(challengeBytes);
        Channels.write(chan, outBuffer);
      }
      if (saslClient.isComplete()) {
        String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client context established. Negotiated QoP: " + qop);
        }
        sendRPCHeader(chan);
        regionClient.sendVersion(chan);
      }
      return null;
    }

    return unwrap(buf);
  }

  private byte[] processChallenge(final byte[] b) {
    try {
      return Subject.doAs(clientLogin.getSubject(),
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

  private void sendRPCHeader(Channel channel) {
    byte[] userBytes = Bytes.UTF8(clientPrincipalName);
    final String klass = "org.apache.hadoop.hbase.ipc.HRegionInterface";
    byte[] classBytes = Bytes.UTF8(klass);
    byte[] buf = new byte[4 + 1 + classBytes.length + 1 + 2 + userBytes.length + 1];

    ChannelBuffer outBuffer = ChannelBuffers.wrappedBuffer(buf);
    outBuffer.clear();
    outBuffer.writerIndex(outBuffer.writerIndex()+4);
    outBuffer.writeByte(classBytes.length);              // 1
    outBuffer.writeBytes(classBytes);      // 44
    //This is part of protocol header
    //true if a user field exists
    //1 is true in boolean
    outBuffer.writeByte(1);
    outBuffer.writeShort(userBytes.length);
    outBuffer.writeBytes(userBytes);
    //true if a reaLuser field exists
    outBuffer.writeByte(0);
    //write length
    outBuffer.setInt(0, outBuffer.writerIndex() - 4);
    outBuffer = wrap(outBuffer);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Sending RPC Header: "+Bytes.pretty(outBuffer));
    }
    Channels.write(channel, outBuffer);
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

  public static boolean parseAuthentication() {
    return Boolean.valueOf(System.getProperty(SECURITY_AUTHENTICATION_KEY, "kerberos"));
  }

  // The CallbackHandler interface here refers to
  // javax.security.auth.callback.CallbackHandler.
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
