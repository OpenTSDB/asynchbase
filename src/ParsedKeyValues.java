package org.hbase.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ParsedKeyValues {
  private static final Logger LOG = LoggerFactory.getLogger(ParsedKeyValues.class);

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

  public static ParsedKeyValues parse(List<KeyValue> keyValues) {
    int uniqueFamilyIndex = 0;
    HashMap<String, byte[]> familyIndex = new HashMap<String, byte[]>();
    HashMap<String, List<byte[]>> familyToQualifierIndex = new HashMap<String, List<byte[]>>();
    HashMap<String, List<byte[]>> familyToValueIndex = new HashMap<String, List<byte[]>>();

    String uniqueKeyStr = null;
    byte[] uniqueKey = null;

    for (KeyValue keyValue : keyValues) {
      String keyStr = Bytes.pretty(keyValue.key());

      if (uniqueKeyStr != null && !keyStr.equalsIgnoreCase(uniqueKeyStr)) {
        throw new IllegalArgumentException("MultiPut only supported for unique keys.");
      }

      if(uniqueKeyStr == null) {
        uniqueKeyStr = keyStr;
        uniqueKey = keyValue.key();
      }

      String family = Bytes.pretty(keyValue.family());
      //check if we have already seen the family, if not use the global counter
      int currFamilyIndex;
      if (!familyIndex.containsKey(family)) {
        LOG.debug("adding family " + family);
        familyIndex.put(family, keyValue.family());
      }

      //check if we have seen a qualifier before, if not use index as 0 or else get previous value
      //int currQualifierIndex;
      if (!familyToQualifierIndex.containsKey(family)) {
        familyToQualifierIndex.put(family, new ArrayList<byte[]>());
        familyToValueIndex.put(family, new ArrayList<byte[]>());
      }

      //currQualifierIndex = familyToQualifierIndex.get(keyValue.key()).size();
      familyToQualifierIndex.get(family).add(keyValue.qualifier());
      familyToValueIndex.get(family).add(keyValue.value());
    }

    byte[][] families = new byte[familyIndex.size()][];
    byte[][][] qualifiers = new byte[familyIndex.size()][][];
    byte[][][] values = new byte[familyIndex.size()][][];

    int currFamilyIndex = 0;
    for (byte[] family : familyIndex.values()) {
      String familyStr = Bytes.pretty(family);
      LOG.debug("family : " + familyStr);
      families[currFamilyIndex] = family;
      qualifiers[currFamilyIndex] = new byte[familyToQualifierIndex.get(familyStr).size()][];
      values[currFamilyIndex] = new byte[familyToValueIndex.get(familyStr).size()][];

      for (int i = 0; i < familyToQualifierIndex.get(familyStr).size(); i++) {
        LOG.debug("qualifier : " + Bytes.pretty(familyToQualifierIndex.get(familyStr).get(i)));
        qualifiers[currFamilyIndex][i] = familyToQualifierIndex.get(familyStr).get(i);
        LOG.debug("value : " + Bytes.pretty(familyToValueIndex.get(familyStr).get(i)));
        values[currFamilyIndex][i] = familyToValueIndex.get(familyStr).get(i);
      }
      currFamilyIndex++;
    }

    return new ParsedKeyValues(uniqueKey, families, qualifiers, values);
  }
}
