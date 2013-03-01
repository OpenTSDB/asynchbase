package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public class ColumnRangeFilter extends ScanFilter {

  private static final String FILTERNAME = "org.apache.hadoop.hbase"
      + ".filter.ColumnRangeFilter";

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private final byte[] minColumn;
  private final boolean minColumnInclusive;
  private final byte[] maxColumn;
  private final boolean maxColumnInclusive;

  public ColumnRangeFilter(final byte[] minColumn, final byte[] maxColumn) {
    this(minColumn, true, maxColumn, true);
  }

  public ColumnRangeFilter(final byte[] minColumn, final boolean minColumnInclusive,
                           final byte[] maxColumn, final boolean maxColumnInclusive) {
    this.minColumn = minColumn;
    this.minColumnInclusive = minColumnInclusive;
    this.maxColumn = maxColumn;
    this.maxColumnInclusive = false;
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
    //org.apache.hadoop.hbase.filter.ColumnRangeFilter
    buf.writeByte((byte)FILTERBYTES.length);         //1
    buf.writeBytes(FILTERBYTES);                    //48
    //min column null
    buf.writeByte(minColumn == null ? 1 : 0);       //1
    //min column
    buf.writeByte(minColumn.length);                //1 (length of minColumn)
    buf.writeBytes(minColumn);                      //minColumn.length
    //min column inclusive
    buf.writeByte(minColumnInclusive ? 1 : 0);      //1
    //max column null
    buf.writeByte(maxColumn == null ? 1 : 0);       //1
    //max column
    buf.writeByte(maxColumn.length);                //1
    buf.writeBytes(maxColumn);                      //maxColumn.length
    //max column inclusive
    buf.writeByte(maxColumnInclusive ? 1: 0);       //1
  }

  @Override
  int predictSerializedSize() {
    return 1 + FILTERBYTES.length + 1 + 1 + minColumn.length
        + 1 + 1 + 1 + maxColumn.length + 1;
  }
}
