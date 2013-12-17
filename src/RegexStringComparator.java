/*
 * Copyright (C) 2014  The Async HBase Authors.  All rights reserved.
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

import java.nio.charset.Charset;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.ComparatorPB;

/**
 * A regular expression comparator used in comparison filters such as RowFilter,
 * QualifierFilter, and ValueFilter.
 * <p>
 * Don't use an expensive regular expression, because Java's implementation
 * uses backtracking and matching will happen on the server side, potentially
 * on many many row keys, columns, or values.
 * See <a href="http://swtch.com/~rsc/regexp/regexp1.html">Regular Expression
 * Matching Can Be Simple And Fast</a> for more details on regular expression
 * performance (or lack thereof) and what "backtracking" means.
 * <p>
 * This means you need to <strong>be careful</strong> about using regular
 * expressions supplied by users as that would allow them to easily DDoS
 * HBase by sending prohibitively expensive regexps that would consume all
 * CPU cycles and cause the entire HBase node to time out.
 * <p>
 * Only EQUAL and NOT_EQUAL comparisons are valid with this comparator.
 * @since 1.6
 */
public final class RegexStringComparator extends FilterComparator {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.RegexStringComparator");

  private final String expr;
  private final Charset charset;

  /**
   * Create a regular expression filter with the specified regular expression.
   * <p>
   * This is equivalent to calling {@link #RegexStringComparator(String, Charset)}
   * with the UTF-8 charset in argument.
   *
   * @param expr The regular expression with which to filter.
   */
  public RegexStringComparator(String expr) {
    this(expr, Charsets.UTF_8);
  }

  /**
   * Create a regular expression filter with the specified regular expression
   * and charset.
   *
   * @param expr The regular expression with which to filter.
   */
  public RegexStringComparator(String expr, Charset charset) {
    this.expr = expr;
    this.charset = charset;
  }

  public String expression() {
    return expr;
  }

  public Charset charset() {
    return charset;
  }

  @Override
  byte[] name() {
    return NAME;
  }

  @Override
  ComparatorPB.Comparator toProtobuf() {
    ByteString byte_string = ComparatorPB
      .RegexStringComparator
      .newBuilder()
      .setPattern(expr)
      .setPatternFlags(Pattern.DOTALL)
      .setCharset(charset.name())
      .build()
      .toByteString();
    return super.toProtobuf(byte_string);
  }

  @Override
  void serializeOld(ChannelBuffer buf) {
    super.serializeOld(buf);                            // super.predictSerializedSize()
    // Write code
    buf.writeByte(0);                                   // 1
    buf.writeByte((byte) NAME.length);                  // 1
    buf.writeBytes(NAME);                               // NAME.length
    // writeUTF the expr
    byte[] expr_bytes = Bytes.UTF8(expr);
    buf.writeShort(expr_bytes.length);                  // 2
    buf.writeBytes(expr_bytes);                         // expr.length
    // writeUTF the charset
    byte[] charset_bytes = Bytes.UTF8(charset.name());
    buf.writeShort(charset_bytes.length);               // 2
    buf.writeBytes(charset_bytes);                      // charset.length
  }

  @Override
  int predictSerializedSize() {
    return super.predictSerializedSize()
        + 1 + 1 + NAME.length
        + 2 + Bytes.UTF8(expr).length
        + 2 + Bytes.UTF8(charset.name()).length;
  }

  @Override
  public String toString() {
    return String.format("%s(%s, %s)",
        getClass().getSimpleName(),
        expr,
        charset.name());
  }
}
