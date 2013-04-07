package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.CharsetUtil;

import java.nio.charset.Charset;

public class QualifierFilter extends ScanFilter {
  private static final String FILTERNAME = "org.apache.hadoop" +
      ".hbase.filter.QualifierFilter";

  private static final byte[] REGEXSTRINGCOMPARATOR = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.RegexStringComparator");

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private static final byte[] EQUAL = new byte[] { 'E', 'Q', 'U', 'A', 'L' };

  private final byte[] regexp;
  private final byte[] charset;

  public QualifierFilter(String regexp) {
    this(regexp, CharsetUtil.ISO_8859_1);
  }

  public QualifierFilter(final String regexp, final Charset charset) {
    this(Bytes.UTF8(regexp), charset);
  }

  public QualifierFilter(final byte[] regexp, final Charset charset) {
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
    buf.writeBytes(FILTERBYTES);                                // 46
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
    return 1 + 46 + 2 + 5 + 1 + 1 + 1 + 52
        + 2 + regexp.length + 2 + charset.length;
  }
}
