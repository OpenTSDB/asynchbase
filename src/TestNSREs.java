/*
 * Copyright (c) 2011  StumbleUpon, Inc.  All rights reserved.
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

import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.runner.RunWith;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.mockito.Mockito.any;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
// "Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
// because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
                  "ch.qos.*", "org.slf4j.*",
                  "com.sum.*", "org.xml.*"})
@PrepareForTest({ HBaseClient.class, RegionClient.class })
final class TestNSREs {

  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 'k', 'e', 'y' };
  private static final byte[] FAMILY = { 'f' };
  private static final byte[] QUALIFIER = { 'q', 'u', 'a', 'l' };
  private static final byte[] VALUE = { 'v', 'a', 'l', 'u', 'e' };
  private static final KeyValue KV = new KeyValue(KEY, FAMILY, QUALIFIER, VALUE);
  private HBaseClient client = new HBaseClient("test-quorum-spec");

  @Before
  public void before() throws Exception {
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private static byte[] anyBytes() {
    return any(byte[].class);
  }

  /** Creates a new Deferred that's already called back.  */
  private static <T> Answer<Deferred<T>> newDeferred(final T result) {
    return new Answer<Deferred<T>>() {
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(result);
      }
    };
  }

}
