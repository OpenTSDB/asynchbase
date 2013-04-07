package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;

public class RowFilter extends ScanFilter {

  private static final String FILTERNAME = "org.apache.hadoop" +
      ".hbase.filter.RowFilter";

  private static final byte[] REGEXSTRINGCOMPARATOR = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.RegexStringComparator");

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private static final byte[] EQUAL = new byte[] { 'E', 'Q', 'U', 'A', 'L' };

  private final byte[] regexp;
  private final byte[] charset;

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This is equivalent to calling {@link #RowFilter(String, Charset)}
   * with the ISO-8859-1 charset in argument.
   * @param regexp The regular expression with which to filter the row keys.
   */
  public RowFilter(final String regexp) {
    this(regexp, CharsetUtil.ISO_8859_1);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This regular expression will be applied on the server-side, on the row
   * key.  Rows for which the key doesn't match will not be returned to this
   * scanner, which can be useful to carefully select which rows are matched
   * when you can't just do a prefix match, and cut down the amount of data
   * transfered on the network.
   * <p>
   * Don't use an expensive regular expression, because Java's implementation
   * uses backtracking and matching will happen on the server side, potentially
   * on many many row keys.  See <a href="su.pr/2xaY8D">Regular Expression
   * Matching Can Be Simple And Fast</a> for more details on regular expression
   * performance (or lack thereof) and what "backtracking" means.
   * @param regexp The regular expression with which to filter the row keys.
   * @param charset The charset used to decode the bytes of the row key into a
   * string.  The RegionServer must support this charset, otherwise it will
   * unexpectedly close the connection the first time you attempt to use this
   * scanner.
   */
  public RowFilter(final String regexp, final Charset charset) {
    this(Bytes.UTF8(regexp), charset);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This is equivalent to calling {@link #RowFilter(byte[], Charset)}
   * with the ISO-8859-1 charset in argument.
   * @param regexp The binary regular expression with which to filter the row keys.
   */
  public RowFilter(final byte[] regexp) {
    this(regexp, CharsetUtil.ISO_8859_1);
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This regular expression will be applied on the server-side, on the row
   * key.  Rows for which the key doesn't match will not be returned to this
   * scanner, which can be useful to carefully select which rows are matched
   * when you can't just do a prefix match, and cut down the amount of data
   * transfered on the network.
   * <p>
   * Don't use an expensive regular expression, because Java's implementation
   * uses backtracking and matching will happen on the server side, potentially
   * on many many row keys.  See <a href="su.pr/2xaY8D">Regular Expression
   * Matching Can Be Simple And Fast</a> for more details on regular expression
   * performance (or lack thereof) and what "backtracking" means.
   * @param regexp The regular expression with which to filter the row keys.
   * @param charset The charset used to decode the bytes of the row key into a
   * string.  The RegionServer must support this charset, otherwise it will
   * unexpectedly close the connection the first time you attempt to use this
   * scanner.
   */
  public RowFilter(final byte[] regexp, final Charset charset) {
    this.regexp = regexp;
    this.charset = Bytes.UTF8(charset.name());
  }

  @Override
  String getName() {
    return FILTERNAME;
  }

  @Override
  byte[] getNameBytes() {
    return FILTERBYTES;
  }

  @Override
  void serialize(ChannelBuffer buf) {
    buf.writeByte((byte) FILTERBYTES.length);                   // 1
    buf.writeBytes(FILTERBYTES);                                // 40
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
    buf.writeBytes(regexp);                                     // regex.length
    // writeUTF the charset
    buf.writeShort(charset.length);                             // 2
    buf.writeBytes(charset);                                    // chars.length
  }

  @Override
  int predictSerializedSize() {
    return 1 + 40 + 2 + 5 + 1 + 1 + 1 + 52
        + 2 + regexp.length + 2 + charset.length;
  }
}
