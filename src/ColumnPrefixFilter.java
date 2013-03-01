package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public class ColumnPrefixFilter extends ScanFilter {

  private static final String FILTERNAME = "org.apache.hadoop.hbase"
      + ".filter.ColumnPrefixFilter";

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private final byte[] prefix;

  /**
   * Sets a binary prefix to filter results based on the column qualifier prefix.
   * <p>
   * Very fast comparisons, compares the column qualifier bytes up to the length of the
   * @param prefix to see if it matches. Useful for when your column qualifer are not strings
   * and are written as compressed form bytes.
   * Only settings this filter will return all rows that match the criteria but at the same will cost
   * a full table scan. Be sure of what you are doing when using this.
   */
  public ColumnPrefixFilter(byte[] prefix) {
    this.prefix = prefix;
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
    //org.apache.hadoop.hbase.filter.ColumnPrefixFilter
    buf.writeByte((byte)FILTERBYTES.length);     //1
    buf.writeBytes(FILTERBYTES);                 //49
    //write the bytes of the prefix
    buf.writeByte((byte)prefix.length);          //1
    buf.writeBytes(prefix);                      //prefix.length
  }

  @Override
  int predictSerializedSize() {
    return 1 + FILTERBYTES.length + 1 + prefix.length;
  }
}
