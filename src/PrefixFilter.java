package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public class PrefixFilter extends ScanFilter {

  private static final String FILTERNAME = "org.apache.hadoop"
      + ".hbase.filter.PrefixFilter";

  private static final byte[] FILTERBYTES = Bytes.ISO88591(FILTERNAME);

  private final byte[] prefix;

  public PrefixFilter(final byte[] prefix) {
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
  void serialize(final ChannelBuffer buf) {
    //TODO: how to set startKey ?? either we set it or leave it to the user ?
    // prefix filter
    buf.writeByte((byte) FILTERBYTES.length);       // 1
    buf.writeBytes(FILTERBYTES);                   // 43
    // write the bytes of the prefix
    //buf.writeByte((byte)prefix.length);             // 1
    //buf.writeBytes(prefix);                         // prefix.length
    HBaseRpc.writeByteArray(buf, prefix);
  }

  @Override
  int predictSerializedSize() {
    return 1 + 43 + 1 + prefix.length;
  }
}