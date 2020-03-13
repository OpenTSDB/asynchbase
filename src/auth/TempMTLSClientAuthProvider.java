/*
 * Copyright (C) 2020  The Async HBase Authors.  All rights reserved.
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

import static org.jboss.netty.channel.Channels.pipeline;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.xml.bind.DatatypeConverter;

import org.hbase.async.HBaseClient;
import org.hbase.async.auth.RefreshingSSLContext.RefreshCallback;
import org.hbase.async.auth.RefreshingSSLContext.SourceType;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.stumbleupon.async.Deferred;

/**
 * This is a temporary class used at Yahoo. We're using the open source 
 * https://github.com/yahoo/athenz with short-lived X509 certs for authentication.
 * Until we have a full MTLS solution, a temporary work around is to use the
 * cert to authenticate to the HBase REST server, fetch a digest token, then
 * authenticate to the region server using that token. This class is meant to 
 * be a singleton that takes paths to the Athenz certificates and continuously
 * reloads them into an SSLContext that is used to periodically refresh the 
 * digest token for the region servers.
 * 
 * NOTE: Since we're still in Java 8 and don't have a dedicated HTTP client like
 * in JDK9+, AND we don't want to pull in extra JARs, we'll have to hack 
 * together a client with the Netty library. We'll use the HttpSnoop example from
 * Netty and make sure to set our SSL context in client mode.
 *
 * @since 1.9.0
 */
public class TempMTLSClientAuthProvider extends ClientAuthProvider
    implements RefreshCallback {
  private static Logger LOG = LoggerFactory.getLogger(TempMTLSClientAuthProvider.class);
  
  public static final String TOKEN_RENEWAL_KEY = "hbase.client.mtls.token.renewalPeriod";
  public static final String CERT_PATH_KEY = "hbase.client.mtls.certificate";
  public static final String CERT_KEY_KEY = "hbase.client.mtls.key";
  public static final String CERT_CA_KEY = "hbase.client.mtls.ca";
  public static final String CERT_KEYSTORE_KEY = "hbase.client.mtls.keystore";
  public static final String CERT_KEYSTORE_PASS_KEY = "hbase.client.mtls.keystore.pass";
  public static final String CERT_REFRESH_KEY = "hbase.client.mtls.refresh.interval";
  public static final String RETRY_INTERVAL_KEY = "hbase.client.mtls.retry.interval";
  public static final String TOKEN_REGISTRY_KEY = "hbase.client.mtls.token.registry";
  
  public static final Pattern TOKEN_REGEX = Pattern.compile("\\{\"token\":\"(.*)\"\\}");

  public static final String SASL_DEFAULT_REALM = "default";
  public static final String[] MECHANISM = { "DIGEST-MD5" };
  
  private final RefreshingSSLContext context;
  private final long token_renewal_interval;
  private volatile boolean initial_run;
  private volatile Throwable token_exception;
  private volatile String token_response;
  private volatile int token_status;
  private long last_attempt;
  
  /** The bits parsed from the token that we actually care about. */
  private volatile byte[] identifier;
  private volatile byte[] pass;
  
  /**
   * Ctor that will fetch the token on the first try.
   * @param hbase_client The non-null HBase client.
   */
  public TempMTLSClientAuthProvider(final HBaseClient hbase_client) {
    super(hbase_client);
    LOG.info("Initializing MTLS client auth with a ");
    initial_run = true;
    if (Strings.isNullOrEmpty(hbase_client.getConfig().getString(CERT_KEYSTORE_KEY))) {
      context = RefreshingSSLContext.newBuilder()
          .setCert(hbase_client.getConfig().getString(CERT_PATH_KEY))
          .setCa(hbase_client.getConfig().getString(CERT_CA_KEY))
          .setKey(hbase_client.getConfig().getString(CERT_KEY_KEY))
          .setInterval(hbase_client.getConfig().hasProperty(CERT_REFRESH_KEY) ?
              hbase_client.getConfig().getInt(CERT_REFRESH_KEY) :
                300000)
          .setTimer(hbase_client.getTimer())
          .setType(SourceType.FILES)
          .setCallback(this)
          .build();
    } else {
      context = RefreshingSSLContext.newBuilder()
          .setKeystore(hbase_client.getConfig().getString(CERT_KEYSTORE_KEY))
          .setKeystorePass(hbase_client.getConfig().getString(CERT_KEYSTORE_PASS_KEY))
          .setInterval(hbase_client.getConfig().hasProperty(CERT_REFRESH_KEY) ?
              hbase_client.getConfig().getInt(CERT_REFRESH_KEY) :
                300000)
          .setTimer(hbase_client.getTimer())
          .setType(SourceType.KEYSTORE)
          .setCallback(this)
          .build();
    }
    token_renewal_interval = hbase_client.getConfig().hasProperty(TOKEN_RENEWAL_KEY) ? 
        hbase_client.getConfig().getLong(TOKEN_RENEWAL_KEY) :
        3600000;
    
    try {
      fetchToken(true).join(30000);
    } catch (InterruptedException e) {
      LOG.error("Unexpected exception.", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch the token on startup.", e);
    }
  }
  
  @Override
  public SaslClient newSaslClient(final String service_ip,
                                  final Map<String, String> props) {
    try {
      String temp = hbase_client.getConfig().getString(
          KerberosClientAuthProvider.PRINCIPAL_KEY);
      final String server_principal;
      if (temp.contains("_HOST")) {
        try {
          final String host = InetAddress.getByName(service_ip)
              .getCanonicalHostName();
          server_principal = temp.replaceAll("_HOST", host);
        } catch (UnknownHostException e) {
          throw new IllegalStateException("Failed to resolve hostname for: " + 
              service_ip, e);
        }
      } else {
        server_principal = temp;
      }
      LOG.info("Connecting to " + server_principal);
      
      class SaslClientCallbackHandler implements CallbackHandler {

        // Cribbed from the Hadoop SASL code. 
        public void handle(final Callback[] callbacks)
            throws UnsupportedCallbackException {
          NameCallback nc = null;
          PasswordCallback pc = null;
          RealmCallback rc = null;
          for (Callback callback : callbacks) {
            if (callback instanceof RealmChoiceCallback) {
              continue;
            } else if (callback instanceof NameCallback) {
              nc = (NameCallback) callback;
            } else if (callback instanceof PasswordCallback) {
              pc = (PasswordCallback) callback;
            } else if (callback instanceof RealmCallback) {
              rc = (RealmCallback) callback;
            } else {
              throw new UnsupportedCallbackException(callback,
                  "Unrecognized SASL client callback");
            }
          }
          if (nc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting username: " + new String(identifier));
            nc.setName(DatatypeConverter.printBase64Binary(identifier));
          }
          if (pc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting userPassword");
            pc.setPassword(DatatypeConverter.printBase64Binary(pass).toCharArray());
          }
          if (rc != null) {
            if (LOG.isDebugEnabled())
              LOG.debug("SASL client callback: setting realm: "
                  + rc.getDefaultText());
            rc.setText(rc.getDefaultText());
          }
        }
      }
      
      final SaslClientCallbackHandler hndlr = new SaslClientCallbackHandler();
      final class PriviledgedAction implements 
          PrivilegedExceptionAction<SaslClient> {
        @Override
        public SaslClient run() throws Exception {
          LOG.info("Client will use " + MECHANISM[0] + " as SASL mechanism "
              + "with MTLS Temp to " + server_principal);
          return Sasl.createSaslClient(
              MECHANISM, 
              null,              // authorization ID
              null,
              "default", 
              props, 
              hndlr);             // callback
        }
        @Override
        public String toString() {
          return "create sasl client";
        }
      }

      final class SimplePrinciple implements Principal {
        final String user;
        
        SimplePrinciple(final String user) {
          this.user = user;
        }
        
        @Override
        public String getName() {
          return user;
        }
        
      }
      final Principal p = new SimplePrinciple("AsyncHBase");
      final Subject sub = new Subject(false,
          Sets.newHashSet(p), Sets.newHashSet(), Sets.newHashSet());
      return Subject.doAs(sub, new PriviledgedAction());
    } catch (Exception e) {
      LOG.error("Error creating SASL client", e);
      throw new IllegalStateException("Error creating SASL client", e);
    }
  }

  @Override
  public String getClientUsername() {
    // IMPORTANT: This MUST be null for the digest to complete properly.
    return null;
  }

  @Override
  public byte getAuthMethodCode() {
    return ClientAuthProvider.DIGEST_CLIENT_AUTH_CODE;
  }

  @Override
  public Subject getClientSubject() {
    return null;
  }
  
  @Override
  public void refresh(final SSLContext context) {
    // This is called back by the RefreshingSSLContext when we reload the
    // certificates.
    synchronized (this) {
      if (initial_run) {
        initial_run = false;
        return;
      }
    }
    
    try {
      fetchToken(true).join();
    } catch (InterruptedException e) {
      LOG.error("Failed to fetch the token", e);
    } catch (Exception e) {
      LOG.error("Failed to fetch the token", e);
    }
  }
  
  /**
   * Process that makes a call out to the token server and sets the local token
   * fields on success.
   * @param force Whether or not to force a refresh.
   * @return A non-null deferred resolving to true or false.
   */
  private Deferred<Boolean> fetchToken(final boolean force) {
    if (!force && (last_attempt > 0 && 
        System.currentTimeMillis() - last_attempt < token_renewal_interval) && 
          token_exception != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Waiting " + (System.currentTimeMillis() - last_attempt) 
          + " seconds until we try fetching another token.");
      }
      return Deferred.fromResult(false);
    }
    last_attempt = (int) System.currentTimeMillis() / 1000;
    
    final Deferred<Boolean> deferred = new Deferred<Boolean>();
    final URI uri = URI.create(hbase_client.getConfig().getString(TOKEN_REGISTRY_KEY));
    final String host = uri.getHost() == null? "127.0.0.1" : uri.getHost();
    final int port = uri.getPort() < 1 ? 443 : uri.getPort();
    
    final ClientBootstrap bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
            Executors.newCachedThreadPool(), 
            Executors.newCachedThreadPool()));
      bootstrap.setPipelineFactory(new HttpClientPipelineFactory(host, port));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetching token from REST server at: " + host + ":" + port);
      }
      final ChannelFuture future = bootstrap.connect(
          new InetSocketAddress(host, port));
      future.addListener(new ChannelFutureListener() {
        
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          // Prepare the HTTP request.
          final HttpRequest request = new DefaultHttpRequest(
                  HttpVersion.HTTP_1_1, HttpMethod.POST, uri.getRawPath());
          request.headers().set(HttpHeaders.Names.HOST, host);
          request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
          request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
          
          if (LOG.isDebugEnabled()) {
            LOG.debug("Requesting token from REST server at: " + uri);
          }
          
          // Send the HTTP request.
          future.getChannel().write(request);
          future.getChannel().getCloseFuture().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Completed REST token request with HTTP status code: " 
                    + token_status);
              }
              
              // now we can process the response
              if (token_status == 200) {
                final Matcher matcher = TOKEN_REGEX.matcher(token_response);
                if (matcher.matches()) {
                  token_response = matcher.group(1);
                  parseToken(token_response);
                  deferred.callback(true);
                } else {
                  token_exception = new RuntimeException(
                      "Failed to fetch token from REST server " + host + ":" 
                          + port + " with status code: " + token_status 
                          + " and content: " + token_response);
                  LOG.error("Failed to fetch token.", token_exception);
                  deferred.callback(token_exception);
                }
              }
            }
            
          });
        }
        
      });

    return deferred;
  }
  
  class HttpClientPipelineFactory implements ChannelPipelineFactory {
    
    private final String host;
    private final int port;

    public HttpClientPipelineFactory(final String host, final int port) {
      this.host = host;
      this.port = port;
    }

    public ChannelPipeline getPipeline() {
      // Create a default pipeline implementation.
      final ChannelPipeline pipeline = pipeline();
      final SSLEngine engine = context.context().createSSLEngine(host, port);
      engine.setUseClientMode(true);
      SslHandler hndlr = new SslHandler(engine);
      pipeline.addLast("ssl", hndlr);

      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("inflater", new HttpContentDecompressor());

      pipeline.addLast("handler", new HttpHandler());
      return pipeline;
    }
  }
  
  class HttpHandler extends SimpleChannelUpstreamHandler {

    private boolean readingChunks;
    private StringBuilder buffer;
    
    HttpHandler() {
      buffer = new StringBuilder();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, 
                                final ExceptionEvent e) throws Exception {
      LOG.error("Failed call", e.getCause());
      token_exception = e.getCause();
      ctx.getChannel().close();
    }
    
    @Override
    public void messageReceived(final ChannelHandlerContext ctx, 
                                final MessageEvent e) {
      if (!readingChunks) {
        final HttpResponse response = (HttpResponse) e.getMessage();
        token_status = response.getStatus().getCode();
        
        if (response.isChunked()) {
          readingChunks = true;
        } else {
          final ChannelBuffer content = response.getContent();
          if (content.readable()) {
            buffer.append(content.toString(CharsetUtil.UTF_8));
          }
          token_response = buffer.toString();
          buffer = null;
        }
      } else {
        final HttpChunk chunk = (HttpChunk) e.getMessage();
        if (chunk.isLast()) {
          readingChunks = false;
          token_response = buffer.toString();
          // TODO - verify if this last chunk is always empty.
        } else {
          buffer.append(chunk.getContent().toString(CharsetUtil.UTF_8));
        }
      }
    }
  }
  
  /**
   * Attempts to parse the token string.
   * @param token The token we want to parse.
   */
  private void parseToken(final String token) {
    try {
      // Note that there are plenty of other bits of information in the token
      // we could grab but we don't really care about them.
      //byte[] data = Base64.getUrlDecoder().decode(token);
      byte[] data = deocdeURLSafeBase64(token);
      
      // tracks the offset as we go. Ugly but... *shrug*
      int[] offset = new int[] { 0 };
      int len = TempMTLSClientAuthProvider.readVInt(data, offset);
      identifier = Arrays.copyOfRange(data, offset[0], offset[0] + len);
      offset[0] += len;

      // password
      len = TempMTLSClientAuthProvider.readVInt(data, offset);
      pass = Arrays.copyOfRange(data, offset[0], offset[0] + len);
    } catch (IOException e) {
      LOG.error("Unable to parse the token.", e);
    }
  }
  
  // Cribbed from Base64.java
  private static final char[] toBase64URL = {
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
      'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
      'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'
  };
  private static final int[] fromBase64URL = new int[256];
  static {
      Arrays.fill(fromBase64URL, -1);
      for (int i = 0; i < toBase64URL.length; i++)
          fromBase64URL[toBase64URL[i]] = i;
      fromBase64URL['='] = -2;
  }
  
  byte[] deocdeURLSafeBase64(final String token) {
    byte[] in = token.getBytes();
    int len = outLength(in);
    byte[] out = new byte[len];
    decodeBase64URLSafe(in, out);
    return out;
  }
  
  // cribbed from Base64 since we're still in Java 6.
  private int outLength(byte[] src) {
    int paddings = 0;
    int len = src.length;
    if (len == 0)
        return 0;
    if (len < 2) {
        throw new IllegalArgumentException(
            "Input byte[] should at least have 2 bytes for base64 bytes");
    }
    int i = src.length;
    if (src[i - 1] == '=') {
      paddings++;
      if (src[i - 2] == '=')
          paddings++;
    }
    if (paddings == 0 && (len & 0x3) !=  0)
        paddings = 4 - (len & 0x3);
    return 3 * ((len + 3) / 4) - paddings;
  }

  // cribbed from Base64 since we're still in Java 6.
  private int decodeBase64URLSafe(byte[] src, byte[] dst) {
    int[] base64 = fromBase64URL;
    int dp = 0;
    int bits = 0;
    int shiftto = 18;       // pos of first byte of 4-byte atom
    int i = 0;
    while (i < src.length) {
        int b = src[i++] & 0xff;
        if ((b = base64[b]) < 0) {
            if (b == -2) {         // padding byte '='
                // =     shiftto==18 unnecessary padding
                // x=    shiftto==12 a dangling single x
                // x     to be handled together with non-padding case
                // xx=   shiftto==6&&sp==sl missing last =
                // xx=y  shiftto==6 last is not =
                if (shiftto == 6 && (i == src.length || src[i++] != '=') ||
                    shiftto == 18) {
                    throw new IllegalArgumentException(
                        "Input byte array has wrong 4-byte ending unit");
                }
                break;
            }
            throw new IllegalArgumentException(
                "Illegal base64 character " +
                Integer.toString(src[i - 1], 16));
        }
        bits |= (b << shiftto);
        shiftto -= 6;
        if (shiftto < 0) {
            dst[dp++] = (byte)(bits >> 16);
            dst[dp++] = (byte)(bits >>  8);
            dst[dp++] = (byte)(bits);
            shiftto = 18;
            bits = 0;
        }
    }
    // reached end of byte array or hit padding '=' characters.
    if (shiftto == 6) {
        dst[dp++] = (byte)(bits >> 16);
    } else if (shiftto == 0) {
        dst[dp++] = (byte)(bits >> 16);
        dst[dp++] = (byte)(bits >>  8);
    } else if (shiftto == 12) {
        // dangling single "x", incorrectly encoded.
        throw new IllegalArgumentException(
            "Last unit does not have enough valid bits");
    }
    // anything left is invalid, if is not MIME.
    // if MIME, ignore all non-base64 character
    while (i < src.length) {
        throw new IllegalArgumentException(
            "Input byte array has incorrect ending byte at " + i);
    }
    return dp;
  }
  
  /** All of these methods are cribbed from Hadoop. */
  private static int readVInt(byte[] stream, int[] offset) throws IOException {
    long n = readVLong(stream, offset);
    if ((n > Integer.MAX_VALUE) || (n < Integer.MIN_VALUE)) {
      throw new IOException("value too long to fit in integer");
    }
    return (int)n;
  }
  
  private static long readVLong(byte[] stream, int[] offset) throws IOException {
    byte firstByte = stream[offset[0]++];
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = stream[offset[0]++];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }
  
  private static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }
  
  private static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }
}