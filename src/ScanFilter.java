package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public abstract class ScanFilter {

  abstract byte[] getNameBytes();

  abstract void serialize(ChannelBuffer buf);

  abstract int predictSerializedSize();

  public abstract byte[] getFilterByteArray();
}
