package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class PrefixFilter extends ScanFilter {

  private static final byte[] PREFIXFILTER = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.PrefixFilter");

  private final byte[] prefix;

  public PrefixFilter(final byte[] prefix) {
    this.prefix = prefix;
  }

  @Override
  byte[] getNameBytes() {
    return PREFIXFILTER;
  }

  @Override
  void serialize(final ChannelBuffer buf) {
    //TODO: how to set startKey ?? either we set it or leave it to the user ?
    // prefix filter
    buf.writeByte((byte)PREFIXFILTER.length);       // 1
    buf.writeBytes(PREFIXFILTER);                   // 43
    // write the bytes of the prefix
    buf.writeByte((byte)prefix.length);             // 1
    buf.writeBytes(prefix);                         // prefix.length
  }

  @Override
  int predictSerializedSize() {
    return 1 + 43 + 1 + prefix.length;
  }

  @Override
  public byte[] getFilterByteArray() {
    byte[] ret = new byte[predictSerializedSize()];
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(ret);
    buf.clear();
    serialize(buf);
    return ret;
  }
}