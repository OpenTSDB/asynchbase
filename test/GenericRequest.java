package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

class GenericRequest extends HBaseRpc implements 
  HBaseRpc.HasTable, HBaseRpc.HasKey,
  HBaseRpc.HasFamily, HBaseRpc.HasQualifiers {

  GenericRequest(final byte[] table, final byte[] key) {
    super(table, key);
  }
  
  @Override
  ChannelBuffer serialize(byte server_version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  Object deserialize(ChannelBuffer buf, int cell_size) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  byte[] method(byte server_version) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[][] qualifiers() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] family() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] key() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] table() {
    // TODO Auto-generated method stub
    return null;
  }

}
