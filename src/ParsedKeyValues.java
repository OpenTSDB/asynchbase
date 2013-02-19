package org.hbase.async;

public class ParsedKeyValues {
  public byte[] key;
  public byte[][] families;
  public byte[][][] qualifiers;
  public byte[][][] values;

  public ParsedKeyValues(byte[] key, byte[][] families, byte[][][] qualifiers, byte[][][] values) {
    this.key = key;
    this.families = families;
    this.qualifiers = qualifiers;
    this.values = values;
  }
}
