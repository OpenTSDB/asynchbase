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

import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.zookeeper.Shell;
import org.hbase.async.Config;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for refreshing credentials for logins. This is a 
 * singleton class that will only allow authentication for one user against 
 * HBase per JVM.
 *
 * This class was culled from zookeeper which was culled from hadoop with some 
 * slight changes including using the HBaseClient's timer to refresh tokens
 * instead of firing up a separate thread and passing settings from the config
 * class. (and formatting to match AsyncHBase's code base)
 */
public class Login {
  private static final Logger LOG = LoggerFactory.getLogger(Login.class);
  
  /** Renewals won't occur until 80% of time from last refresh to
   * ticket's expiration has been reached, at which time it will wake
   * and try to renew the ticket. */
  private static final float TICKET_RENEW_WINDOW = 0.80f;

  /**
   * Percentage of random jitter added to the renewal time
   */
  private static final float TICKET_RENEW_JITTER = 0.05f;

  /** Regardless of TICKET_RENEW_WINDOW setting above and the ticket expiration time,
   * thread will not sleep between refresh attempts any less than 1 minute 
   * (60*1000 milliseconds = 1 minute). Change the '1' to e.g. 5, to change 
   * this to 5 minutes. */
  static final long MIN_TIME_BEFORE_RELOGIN = 1 * 60 * 1000L;
  
  /** Sasl config string for HBase */
  public static final String LOGIN_CONTEXT_NAME_KEY = 
      "hbase.sasl.clientconfig";
  
  /** Random number generator to generate jitter for refreshing tickets to 
   * avoid slamming the authentication system with requests from various clients
   * at the same time. */
  private static Random random = new Random(System.currentTimeMillis());
  
  /** A singleton that allows only one user per JVM to authenticate against
   * HBase. */
  private static Login current_login;
  
  /** The asynchbase config to pull settings from */
  private final Config config;
  
  /** A timer to use for renewing the tickets */
  private final HashedWheelTimer timer;
  
  /** The callback to trigger on login */
  private final CallbackHandler callback_handler;
  
  /** The name of the JAAS config section */
  private final String login_context_name;
  
  /** The authenticated subject */
  private final Subject subject;
  
  /** Whether or not this login is associated with a kerberos ticket */
  private final boolean is_kerberos_ticket;
  
  /** The login context created on login */
  private LoginContext login_context;
  
  /** Whether or not we need to use kinit for a ticket cache */
  private boolean using_ticket_cache;
  
  /** The authenticated principal */
  private String principal;

  /**
   * Attempts to set the singleton and authenticate using the given context and
   * callback. If we're already logged in then this is a no-op and the callback
   * is ignored.
   * @param config The AsyncHBase config to load settings from
   * @param timer A timer to use for renewing tickets
   * @param login_context_name Name of section in JAAS file that will be use to login.
   * Passed as first param to javax.security.auth.login.LoginContext().
   * @param callback_handler The callback to return results to.
   * Passed as second param to javax.security.auth.login.LoginContext().
   * @throws LoginException If login failed
   */
  public static synchronized void initUserIfNeeded(final Config config,
      final HashedWheelTimer timer, final String login_context_name, 
      final CallbackHandler callback_handler)
          throws LoginException {
    if (current_login == null)  {
      current_login = new Login(config, timer, login_context_name, callback_handler);
      LOG.info("Initialized kerberos login context");
    } else {
      LOG.debug("Already logged in");
    }
  }
  
  /** @return the current login. May be null if not set */
  static Login getCurrentLogin() {
    return current_login;
  }
  
  /**
   * Package private ctor to block instantiations outside of the singleton
   * @param config The AsyncHbase config to pull settings from
   * @param timer The client timer to use for renewing tokens
   * @param login_context_name The name of the JAAS context to load
   * @param callback_handler The callback to respond to
   * @throws LoginException if authentication fails
   */
  Login(final Config config, final HashedWheelTimer timer, 
      final String login_context_name, final CallbackHandler callback_handler)
      throws LoginException {
    this.config = config;
    this.timer = timer;
    this.login_context_name = login_context_name;
    this.callback_handler = callback_handler;

    login_context = login(login_context_name);
    subject = login_context.getSubject();
    is_kerberos_ticket = !subject
        .getPrivateCredentials(KerberosTicket.class).isEmpty();
    
    final AppConfigurationEntry entries[] = 
        Configuration.getConfiguration()
          .getAppConfigurationEntry(login_context_name);
    for (final AppConfigurationEntry entry : entries) {
      // there will only be a single entry, so this for() loop will only 
      // be iterated through once.
      if (entry.getOptions().get("useTicketCache") != null) {
        final String val = (String)entry.getOptions().get("useTicketCache");
        if (val.toLowerCase().equals("true")) {
          using_ticket_cache = true;
        }
      }
      if (entry.getOptions().get("keyTab") != null) {
        // TODO (cl) are these useful?
        //keytab_file = (String)entry.getOptions().get("keyTab");
        //using_keytab = true;
      }
      if (entry.getOptions().get("principal") != null) {
        principal = (String)entry.getOptions().get("principal");
      }
      break;
    }
    
    if (!is_kerberos_ticket) {
      return;
    }

    // Refresh the Ticket Granting Ticket (TGT) periodically. How often to 
    // refresh is determined by the TGT's existing expiration date and the 
    // configured MIN_TIME_BEFORE_RELOGIN. For testing and development,
    // you can decrease the interval of expiration of tickets 
    // (for example, to 3 minutes) by running :
    // "modprinc -maxlife 3mins <principal>" in kadmin.
    final long delay = getRefreshDelay(getTGT());
    timer.newTimeout(new TicketRenewalTask(), delay, TimeUnit.MILLISECONDS);
    LOG.info("Scheduled ticket renewal in " + delay + " ms");
  }
  
  /** @return the current subject */
  public Subject getSubject() {
    return subject;
  }
  
  /**
   * Attempts to login using the given context name from the JAAS config
   * @param login_context_name The JAAS file section
   * @return A login context if successful
   * @throws LoginException if authentication failed
   */
  private synchronized LoginContext login(final String login_context_name) 
      throws LoginException {
    if (login_context_name == null || login_context_name.isEmpty()) {
      throw new LoginException(
          "Login context name (JAAS file section header) was null or empty. " +
              "Please check your java.security.login.auth.config (=" +
              System.getProperty("java.security.auth.login.config") +
              ") and your " + LOGIN_CONTEXT_NAME_KEY + "(=" +
              login_context_name + ")");
    }
    LOG.debug("Constructing login context with context: " + login_context_name);
    final LoginContext login_context = new LoginContext(login_context_name, 
        callback_handler);
    login_context.login();
    LOG.info("Successfully logged in.");
    return login_context;
  }
  
  /**
   * Calculates a Unix epoch timestamp in milliseconds when we should attempt
   * another login. It will look at the expiration time of the ticket and 
   * set a time close to but before the expiration when we should try the renewal.
   * If a proper time can't be found, we'll try again in 
   * {@code MIN_TIME_BEFORE_RELOGIN} milliseconds.
   * c.f. org.apache.hadoop.security.UserGroupInformation.
   * @param tgt The ticket to parse the expiration time from
   * @return How long to wait, in milliseconds, before refreshing the ticket
   */
  private long getRefreshDelay(final KerberosTicket tgt) {
    final long now = System.currentTimeMillis();
    long next_refresh = MIN_TIME_BEFORE_RELOGIN;
    if (tgt == null) {
      LOG.warn("No TGT found: will try again at " + new Date(next_refresh));
      return next_refresh;
    }
    
    final long start = tgt.getStartTime().getTime();
    final long expires = tgt.getEndTime().getTime();
    LOG.info("TGT valid starting at:        " + tgt.getStartTime().toString());
    LOG.info("TGT expires:                  " + tgt.getEndTime().toString());
    next_refresh = (long) ((expires - start) *
            (TICKET_RENEW_WINDOW + (TICKET_RENEW_JITTER * random.nextDouble())));
    
    if ((using_ticket_cache) && (tgt.getEndTime().equals(tgt.getRenewTill()))) {
      LOG.error("The TGT cannot be renewed beyond the next expiration date: " 
          + new Date(expires) + ". This process will not be able to "
              + "authenticate new SASL connections after that time. Ask "
              + "your system administrator to either increase the 'renew "
              + "until' time by doing : 'modprinc -maxrenewlife " 
              + principal + "' within kadmin, or instead, to generate a "
              + "keytab for " + principal + ". Because the TGT's expiration "
              + "cannot be further extended by refreshing, exiting refresh "
              + "thread now.");
      return MIN_TIME_BEFORE_RELOGIN;
    }
    
    // Determine how long to sleep from looking at ticket's expiration.
    // We should not allow the ticket to expire, but we should take into 
    // consideration MIN_TIME_BEFORE_RELOGIN. Will not sleep less than 
    // MIN_TIME_BEFORE_RELOGIN, unless doing so would cause ticket expiration.
    if ((now + next_refresh) > expires || 
        (now + MIN_TIME_BEFORE_RELOGIN) > expires) {
      // expiration is before next scheduled refresh.
      LOG.info("Refreshing now because expiration " + new Date(expires) 
          + " is before next scheduled refresh time " + new Date(now + next_refresh) 
          + " or we are within " + MIN_TIME_BEFORE_RELOGIN + "ms of expiring.");
      next_refresh = 0;
    } else {
      if ((now + next_refresh) < (now + MIN_TIME_BEFORE_RELOGIN)) {
          // next scheduled refresh is sooner than (now + MIN_TIME_BEFORE_LOGIN).
          final Date until = new Date(now + next_refresh);
          final Date new_until = new Date(now + MIN_TIME_BEFORE_RELOGIN);
          LOG.warn("TGT refresh thread time adjusted from : " + until 
              + " to : " + new_until + " since "
              + "the former is sooner than the minimum refresh interval ("
              + MIN_TIME_BEFORE_RELOGIN / 1000 + " seconds) from now.");
      }
      next_refresh = Math.max(next_refresh, MIN_TIME_BEFORE_RELOGIN);
    }
    if ((now + next_refresh) > expires) {
      LOG.error("Next refresh: " + new Date(now + next_refresh) 
          + " is later than expiration " + new Date(expires) + 
          ". This may indicate a clock skew problem. "
          + "Check that this host and the KDC's hosts' clocks are in sync.");
      next_refresh = MIN_TIME_BEFORE_RELOGIN;
    }
    return next_refresh;
  }
  
  /**
   * Fetches the proper Kerberos ticket from the subject if we successfully
   * logged in.
   * @return The ticket found
   */
  private synchronized KerberosTicket getTGT() {
    final Set<KerberosTicket> tickets = 
        subject.getPrivateCredentials(KerberosTicket.class);
    for (final KerberosTicket ticket : tickets) {
      final KerberosPrincipal server = ticket.getServer();
      if (server.getName().equals(
          "krbtgt/" + server.getRealm() + "@" + server.getRealm())) {
        LOG.debug("Found tgt " + ticket + ".");
        return ticket;
      }
    }
    return null;
  }
  
  /**
   * Attempts to refresh the ticket by shelling out to the kinit utility
   */
  private void refreshTicketCache() {
    String cmd = "/usr/bin/kinit";
    if (config.hasProperty("asynchbase.security.auth.kinit")) {
      cmd = config.getString("asynchbase.security.auth.kinit");
    }
    final String args = "-R";
    try {
      LOG.info("Executing kinit command: " + cmd + " " + args);
      Shell.execCommand(cmd, args);
    } catch (Exception e) {
      throw new RuntimeException("Could not renew TGT due to problem "
          + "running shell command: '" + cmd + " " + args + "';", e);
    }
  }
  
  /**
   * Re-login a principal. This method assumes that {@link #login(String)} 
   * has happened already.
   * c.f. HADOOP-6559
   * @throws javax.security.auth.login.LoginException on a failure
   */
  private void reLogin() throws LoginException {
    if (!is_kerberos_ticket) {
      return;
    }
    if (login_context == null) {
      throw new LoginException("Login must be done first");
    }
    LOG.info("Initiating logout for " + principal);
    synchronized (Login.class) {
      //clear up the kerberos state. But the tokens are not cleared! As per
      //the Java kerberos login module code, only the kerberos credentials
      //are cleared
      login_context.logout();
      //login and also update the subject field of this instance to
      //have the new credentials (pass it to the LoginContext constructor)
      login_context = new LoginContext(login_context_name, subject);
      LOG.info("Initiating re-login for " + principal);
      login_context.login();
      LOG.info("Relogin was successful for " + principal);
    }
  }
  
  /**
   * Timer task that refreshes our ticket cache (if applicable) and attempts
   * a new login. If the login fails, we'll try again in at least 
   * {@code MIN_TIME_BEFORE_RELOGIN} milliseconds.
   */
  class TicketRenewalTask implements TimerTask {
    @Override
    public void run(final Timeout timeout) {
      // set a default for the next attempt
      long next_refresh = MIN_TIME_BEFORE_RELOGIN;
      try {
        // refresh and/or reattempt to login
        if (using_ticket_cache) {
          refreshTicketCache();
        }
        reLogin();
        
        // schedule our next renewal by getting the expiration time
        next_refresh = getRefreshDelay(getTGT());
      } catch (LoginException e) {
        LOG.error("Failed to renew ticket", e);
      } catch (Exception e) {
        LOG.error("Failed to renew ticket", e);
      } finally {
        LOG.debug("Scheduling next next login attempt in " + next_refresh + " ms");
        timer.newTimeout(this, next_refresh, TimeUnit.MILLISECONDS);
      }
    }
  }
}
