package org.hbase.async;

import com.google.common.collect.Lists;
import org.jboss.netty.buffer.ChannelBuffer;
import java.util.List;

public class FuzzyRowFilter extends ScanFilter {
  private static final String FILTERNAME = "org.apache.hadoop.hbase.filter.FuzzyRowFilter";

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private List<List<byte[]>> fuzzyKeys;

  public FuzzyRowFilter() { }

  public FuzzyRowFilter(byte[] rowKey, byte[] fuzzyKey) {
    addPair(rowKey, fuzzyKey);
  }

  public void addPair(byte[] rowKey, byte[] fuzzyKey) {
    if (fuzzyKeys == null) {
      fuzzyKeys = Lists.newArrayList();
    }
    fuzzyKeys.add(Lists.newArrayList(rowKey, fuzzyKey));
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
    buf.writeByte((byte)FILTERBYTES.length);    //1
    buf.writeBytes(FILTERBYTES);                //45
    buf.writeInt(fuzzyKeys.size());             //4
    for (List<byte[]> k: fuzzyKeys) {
      for (byte[] b: k) {
        HBaseRpc.writeByteArray(buf, b);
      }
    }
  }

  @Override
  int predictSerializedSize() {
    int size = 1 + FILTERBYTES.length + 4;
    for (List<byte[]> k: fuzzyKeys) {
      for (byte[] b: k) {
        size += HBaseRpc.getVIntSize(b.length);
        size += b.length;
      }
    }
    return size;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("FuzzyRowFilter");
    sb.append("{fuzzyKeysData=");
    for (List<byte[]> fuzzyData: fuzzyKeys) {
      sb.append('{').append(Bytes.pretty(fuzzyData.get(0))).append(":");
      sb.append(Bytes.pretty(fuzzyData.get(1))).append('}');
    }
    sb.append("}");
    return sb.toString();
  }
}
