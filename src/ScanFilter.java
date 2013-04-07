package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

public abstract class ScanFilter {

  /**
   * Returns the name of this ScanFilter (useful while debugging)
   * @return filter name
   */
  abstract String getName();

  /**
   * Returns the binary representation of the underlying Filter
   * eg: Bytes.ISO88591("org.apache.hadoop.hbase.filter.PrefixFilter")
   * <h1>do not modify this byte array</h1>
   * @return a binary representation of the underlying Filter type
   */
  abstract byte[] getNameBytes();

  /**
   * Serializes the byte representation to the RPC channel buffer.
   * @param buf The RPC channel buffer to which the byte array is serialized
   */
  abstract void serialize(ChannelBuffer buf);

  /**
   * returns the number of bytes that it will write to the RPC channel buffer when {@code serialize}
   * is called. This method helps predict the initial size of the byte array
   * @return A strictly positive integer
   */
  abstract int predictSerializedSize();
}
