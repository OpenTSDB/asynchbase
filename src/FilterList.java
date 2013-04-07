package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FilterList extends ScanFilter {
  private static final String FILTERNAME = "org.apache.hadoop" +
      ".hbase.filter.FilterList";

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private final List<ScanFilter> filters;
  private final Operator operator;

  public FilterList(List<ScanFilter> filters) {
    this(filters, Operator.MUST_PASS_ALL);
  }

  public FilterList(List<ScanFilter> filters, Operator operator) {
    this.filters = filters;
    this.operator = operator;
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
    buf.writeByte((byte)FILTERBYTES.length);          //1
    buf.writeBytes(FILTERBYTES);                      //41
    //Operator i.e. MUST_PASS_ALL or MUST_PASS_ONE
    buf.writeByte(operator == Operator.MUST_PASS_ALL
        ? (byte)0 : (byte)1);                         //1
    //number of filters in list
    buf.writeInt(filters.size());                     //4
    //append all filters
    for(ScanFilter scanFilter : filters) {
      buf.writeByte(54);                              //1 : code for WritableByteArrayComparable
      buf.writeByte(0);                               //1 : code for NOT_ENCODED
      scanFilter.serialize(buf);                      //size of filter binary representation
    }
  }

  @Override
  int predictSerializedSize() {
    int totalBytesForFilters = 0;
    for(ScanFilter filter : filters) {
      //we need this extra byte to avoid NullPointerException when readObject() is invoked
      //we will use the WritableByteArrayComparable used in setKeyRegexp
      //though I believe that byte value is wrong, since we use 53 while HbaseObjectWritable says its 54
      totalBytesForFilters += 1;
      //we need this extra byte since we will be using the NOT_ENCODED for doing Class.forName in HBASE code
      //see org.apache.hadoop.hbase.io.HbaseObjectWritable.java
      totalBytesForFilters += 1;
      totalBytesForFilters += filter.predictSerializedSize();
    }

    return 1 + FILTERBYTES.length + 1 + 4 + totalBytesForFilters;
  }

  public enum Operator {
    MUST_PASS_ALL,
    MUST_PASS_ONE
  }
}
