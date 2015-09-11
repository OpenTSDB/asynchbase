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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.hbase.async.auth.Login.TicketRenewalTask;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.TimerTask;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HashedWheelTimer.class, Configuration.class, Subject.class, 
  AppConfigurationEntry.class, LoginContext.class, Login.class, 
  KerberosTicket.class, KerberosPrincipal.class, System.class, Shell.class })
public class TestLogin {
  private final static String CONTEXT_NAME = "Uberwald"; 
  
  private HashedWheelTimer timer;
  private Config config;
  private CallbackHandler callback;
  private AppConfigurationEntry[] app_config;
  private AppConfigurationEntry app_config_entry;
  private Map<String, Object> app_config_options;
  private LoginContext login_context;
  private Subject subject;
  private KerberosTicket ticket;
  private Set<KerberosTicket> tickets;
  private KerberosPrincipal server;
  private Date start_time;
  private Date end_time;
  
  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    // always make sure to start the unit tests off by setting the login to null
    // since we can't gaurantee order.
    Whitebox.setInternalState(Login.class, "current_login", (Login)null);
    
    start_time = new Date(1388534400000L);
    end_time = new Date(1388538000000L);
    
    config = new Config();
    timer = mock(HashedWheelTimer.class);
    callback = mock(CallbackHandler.class);
    app_config_entry = mock(AppConfigurationEntry.class);
    app_config = new AppConfigurationEntry[] { app_config_entry };
    app_config_options = new HashMap<String, Object>(2);
    app_config_options.put("useTicketCache", "true");
    app_config_options.put("principal", "Vetinari");
    login_context = mock(LoginContext.class);
    subject = mock(Subject.class);
    ticket = PowerMockito.mock(KerberosTicket.class);
    server = mock(KerberosPrincipal.class);
    
    final Configuration app_conf = mock(Configuration.class);
    when(app_conf.getAppConfigurationEntry(anyString())).thenReturn(app_config);
    
    PowerMockito.mockStatic(Configuration.class);
    PowerMockito.when(Configuration.getConfiguration()).thenReturn(app_conf);
    
    PowerMockito.mockStatic(LoginContext.class);
    PowerMockito.whenNew(LoginContext.class)
      .withAnyArguments().thenReturn(login_context);
    when(login_context.getSubject()).thenReturn(subject);
    
    tickets = new HashSet<KerberosTicket>();
    tickets.add(ticket);
    when(subject.getPrivateCredentials(any(Class.class))).thenReturn(tickets);
    
    when(ticket.getServer()).thenReturn(server);
    when(ticket.getStartTime()).thenReturn(start_time);
    when(ticket.getEndTime()).thenReturn(end_time);
    
    when(server.getName()).thenReturn("krbtgt/Lancre@Lancre");
    when(server.getRealm()).thenReturn("Lancre");
    
    // do NOT shell out during a unit test!
    PowerMockito.mockStatic(Shell.class);

    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388534460000L);
  }
  
  @Test
  public void initUserIfNeeded() throws Exception {
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    // no-ops
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    Login.initUserIfNeeded(config, timer, CONTEXT_NAME, callback);
    
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, times(1)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void ctorWithKerberos() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    assertEquals(subject, login.getSubject());
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, times(1)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void ctorNotKerberos() throws Exception {
    when(subject.getPrivateCredentials(KerberosTicket.class))
      .thenReturn(Collections.<KerberosTicket>emptySet());
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    assertEquals(subject, login.getSubject());
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    verify(timer, never()).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
  }
  
  @Test (expected = LoginException.class)
  public void ctorNullContextName() throws Exception {
    new Login(config, timer, null, callback);
  }
  
  @Test (expected = LoginException.class)
  public void ctorEmptyContextName() throws Exception {
    new Login(config, timer, "", callback);
  }
  
  @Test (expected = LoginException.class)
  public void ctorFailed() throws Exception {
    doThrow(new LoginException("Boo!")).when(login_context).login();
    new Login(config, timer, CONTEXT_NAME, callback);
  }
  
  @Test
  public void getRefreshDelay() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    // should be within 80% of the end time
    assertTrue(delay < end_time.getTime());
    assertTrue(delay >= (end_time.getTime() - start_time.getTime()) * 0.80);
  }
  
  @Test
  public void getRefreshDelayNoTicket() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", 
        (KerberosTicket)null);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshDelayCantRenew() throws Exception {
    when(ticket.getRenewTill()).thenReturn(end_time);
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.setInternalState(login, "using_ticket_cache", true);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshPastExpiration() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388538060000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshWithinMinTime() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(1388537942000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(0, delay);
  }
  
  @Test
  public void getRefreshSuperShortExpiration() throws Exception {
    // I guess this prevents possible dos attacks if someone set the lifetime to
    // be less than a minute
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    start_time.setTime(1388537942000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshFlippedTicketTimes() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    // Friends don't let friend's KDC issue funky tickets like this
    end_time.setTime(1388534400000L);
    start_time.setTime(1388538000000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }
  
  @Test
  public void getRefreshSameTicketTimes() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    end_time.setTime(1388534400000L);
    final long delay = (Long)Whitebox.invokeMethod(login, "getRefreshDelay", ticket);
    assertEquals(Login.MIN_TIME_BEFORE_RELOGIN, delay);
  }

  @Test
  public void getTGT() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final KerberosTicket tgt = Whitebox.invokeMethod(login, "getTGT");
    assertEquals(tgt, ticket);
  }
  
  @Test
  public void getTGTNoMatch() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    when(server.getName()).thenReturn("quirm");
    final KerberosTicket tgt = Whitebox.invokeMethod(login, "getTGT");
    assertNull(tgt);
  }
  
  @Test
  public void getTGTNoTickets() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    when(subject.getPrivateCredentials(KerberosTicket.class))
      .thenReturn(Collections.<KerberosTicket>emptySet());
    final KerberosTicket tgt = Whitebox.invokeMethod(login, "getTGT");
    assertNull(tgt);
  }

  @Test
  public void refreshTicketCache() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.invokeMethod(login, "refreshTicketCache");
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
  
  @Test
  public void refreshTicketCacheConfigPath() throws Exception {
    config.overrideConfig("asynchbase.security.auth.kinit", 
        "/usr/local/bin/kinit");
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.invokeMethod(login, "refreshTicketCache");
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/local/bin/kinit", "-R");
  }
  
  @Test (expected = RuntimeException.class)
  public void refreshTicketCacheIOException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    PowerMockito.when(Shell.execCommand(anyString(), anyString()))
      .thenThrow(new IOException("Boo!"));
    Whitebox.invokeMethod(login, "refreshTicketCache");
  }
  
  @Test (expected = RuntimeException.class)
  public void refreshTicketCacheException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    PowerMockito.when(Shell.execCommand(anyString(), anyString()))
      .thenThrow(new Exception("Boo!"));
    Whitebox.invokeMethod(login, "refreshTicketCache");
  }
  
  @Test
  public void refreshTicketCacheEmptyCommand() throws Exception {
    config.overrideConfig("asynchbase.security.auth.kinit", "");
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.invokeMethod(login, "refreshTicketCache");
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }

  @Test
  public void reLogin() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.invokeMethod(login, "reLogin");
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
  }
  
  @Test
  public void reLoginNotKerberos() throws Exception {
    when(subject.getPrivateCredentials(KerberosTicket.class))
      .thenReturn(Collections.<KerberosTicket>emptySet());
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.invokeMethod(login, "reLogin");
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
  }
  
  @Test (expected = LoginException.class)
  public void reLoginNotLoggedInYet() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.setInternalState(login, "login_context", (LoginContext)null);
    Whitebox.invokeMethod(login, "reLogin");
  }
  
  @Test (expected = LoginException.class)
  public void reLoginLoginFailed() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    doThrow(new LoginException("Boo!")).when(login_context).login();
    Whitebox.invokeMethod(login, "reLogin");
  }

  @Test
  public void ticketRenewalTask() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
  
  @Test
  public void ticketRenewalTaskRefreshCache() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.setInternalState(login, "using_ticket_cache", true);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(times(1));
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
  
  @Test
  public void ticketRenewalTaskLoginException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    Whitebox.setInternalState(login, "login_context", (LoginContext)null);
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    // catch the default refresh rate
    verify(timer, times(1)).newTimeout((TimerTask)any(), 
        eq(Login.MIN_TIME_BEFORE_RELOGIN), eq(TimeUnit.MILLISECONDS));
    verify(login_context, never()).logout();
    verify(login_context, times(1)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
  
  @Test
  public void ticketRenewalTaskException() throws Exception {
    final Login login = new Login(config, timer, CONTEXT_NAME, callback);
    doThrow(new RuntimeException("Boo!")).when(login_context).login();
    final TicketRenewalTask task = login.new TicketRenewalTask();
    task.run(null);
    verify(timer, times(2)).newTimeout((TimerTask)any(), anyLong(), 
        eq(TimeUnit.MILLISECONDS));
    // catch the default refresh rate
    verify(timer, times(1)).newTimeout((TimerTask)any(), 
        eq(Login.MIN_TIME_BEFORE_RELOGIN), eq(TimeUnit.MILLISECONDS));
    verify(login_context, times(1)).logout();
    verify(login_context, times(2)).login();
    PowerMockito.verifyStatic(never());
    Shell.execCommand("/usr/bin/kinit", "-R");
  }
}
