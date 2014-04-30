/*
 * Copyright (C) 2013  The Async HBase Authors.  All rights reserved.
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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;

import org.hbase.async.generated.ComparatorPB;
import org.hbase.async.generated.FilterPB;
import org.hbase.async.generated.HBasePB;

/**
 * Filters rows based on an expression applied to the row key.
 * <p>
 * The regular expression will be applied on the server-side, on the row
 * key.  Rows for which the key doesn't match will not be returned to the
 * scanner, which can be useful to carefully select which rows are matched
 * when you can't just do a prefix match, and cut down the amount of data
 * transfered on the network.
 * <p>
 * Don't use an expensive regular expression, because Java's implementation
 * uses backtracking and matching will happen on the server side, potentially
 * on many many row keys.
 * See <a href="http://swtch.com/~rsc/regexp/regexp1.html">Regular Expression
 * Matching Can Be Simple And Fast</a> for more details on regular expression
 * performance (or lack thereof) and what "backtracking" means.
 * <p>
 * This means you need to <strong>be careful</strong> about using regular
 * expressions supplied by users as that would allow them to easily DDoS
 * HBase by sending prohibitively expensive regexps that would consume all
 * CPU cycles and cause the entire HBase node to time out.
 * @since 1.5
 */
public final class KeyRegexpFilter extends ScanFilter {

  private static final byte[] ROWFILTER = Bytes.ISO88591("org.apache.hadoop"
    + ".hbase.filter.RowFilter");
  private static final byte[] REGEXSTRINGCOMPARATOR = Bytes.ISO88591("org.apache.hadoop"
    + ".hbase.filter.RegexStringComparator");
  private static final byte[] EQUAL = new byte[] { 'E', 'Q', 'U', 'A', 'L' };

  private final byte[] regexp;
  private final byte[] charset;

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This is equivalent to calling {@link #KeyRegexpFilter(String, Charset)}
   * with the ISO-8859-1 charset in argument.
   * @param regexp The regular expression with which to filter the row keys.
   */
  public KeyRegexpFilter(final String regexp) {
    this(regexp, CharsetUtil.ISO_8859_1);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * @param regexp The regular expression with which to filter the row keys.
   * @param charset The charset used to decode the bytes of the row key into a
   * string.  The RegionServer must support this charset, otherwise it will
   * unexpectedly close the connection the first time you attempt to use this
   * scanner.
   * @see #KeyRegexpFilter(byte[], Charset)
   */
  public KeyRegexpFilter(final String regexp, final Charset charset) {
    this(Bytes.UTF8(regexp), charset);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This is equivalent to calling {@link #KeyRegexpFilter(byte[], Charset)}
   * with the ISO-8859-1 charset in argument.
   * @param regexp The binary regular expression with which to filter
   * the row keys.
   */
  public KeyRegexpFilter(final byte[] regexp) {
    this(regexp, CharsetUtil.ISO_8859_1);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * @param regexp The regular expression with which to filter the row keys.
   * @param charset The charset used to decode the bytes of the row key into a
   * string.  The RegionServer must support this charset, otherwise it will
   * unexpectedly close the connection the first time you attempt to use this
   * scanner.
   */
  public KeyRegexpFilter(final byte[] regexp, final Charset charset) {
    this.regexp = regexp;
    this.charset = Bytes.UTF8(charset.name());
  }

  @Override
  byte[] serialize() {
    final ComparatorPB.Comparator.Builder comparator =
      ComparatorPB.Comparator.newBuilder();
    comparator.setNameBytes(Bytes.wrap(REGEXSTRINGCOMPARATOR));
    final byte[] regex_cmp = ComparatorPB.RegexStringComparator.newBuilder()
      .setPatternBytes(Bytes.wrap(regexp))
      .setPatternFlags(0)
      .setCharsetBytes(Bytes.wrap(charset))
      .build()
      .toByteArray();
    comparator.setSerializedComparator(Bytes.wrap(regex_cmp));
    final FilterPB.CompareFilter cmp = FilterPB.CompareFilter.newBuilder()
      .setCompareOp(HBasePB.CompareType.EQUAL)
      .setComparator(comparator.build())
      .build();
    return FilterPB.RowFilter.newBuilder()
      .setCompareFilter(cmp)
      .build()
      .toByteArray();
  }

  @Override
  byte[] name() {
    return ROWFILTER;
  }

  @Override
  void serializeOld(final ChannelBuffer buf) {
    buf.writeByte((byte) ROWFILTER.length);                     // 1
    buf.writeBytes(ROWFILTER);                                  // 40
    // writeUTF of the comparison operator
    buf.writeShort(5);                                          // 2
    buf.writeBytes(EQUAL);                                      // 5
    // The comparator: a RegexStringComparator
    buf.writeByte(54);  // Code for WritableByteArrayComparable // 1
    buf.writeByte(0);   // Code for "this has no code".         // 1
    buf.writeByte((byte) REGEXSTRINGCOMPARATOR.length);         // 1
    buf.writeBytes(REGEXSTRINGCOMPARATOR);                      // 52
    // writeUTF the regexp
    buf.writeShort(regexp.length);                              // 2
    buf.writeBytes(regexp);                                     // regexp.length
    // writeUTF the charset
    buf.writeShort(charset.length);                             // 2
    buf.writeBytes(charset);                                    // charset.length
  }

  @Override
  int predictSerializedSize() {
    return 1 + 40 + 2 + 5 + 1 + 1 + 1 + 52
        + 2 + regexp.length + 2 + charset.length;
  }

  public String toString() {
    return "KeyRegexpFilter(\"" + new String(regexp, CharsetUtil.UTF_8)
      + "\", " + new String(charset, CharsetUtil.UTF_8) + ')';
  }

}
