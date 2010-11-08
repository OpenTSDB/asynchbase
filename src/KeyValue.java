/*
 * Copyright (c) 2010  StumbleUpon, Inc.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A "cell" in an HBase table.
 * <p>
 * This represents one unit of HBase data, one "record".
 *
 * <h1>A note {@code byte} arrays</h1>
 * This class will never copy any {@code byte[]} that's given to it, neither
 * will it create a copy before returning one to you.
 * <strong>Changing a byte array get from or pass to this class will have
 * <em>unpredictable</em> consequences</strong>.  In particular, multiple
 * {@link KeyValue} instances may share the same byte arrays, so changing
 * one instance may also unexpectedly affect others.
 */
public final class KeyValue implements Comparable<KeyValue> {
  /*
   * This class deliberately does not give access to the timestamp of a
   * KeyValue.  I think exposing timestamps in Bigtable (and HBase) was
   * a mistake -- at least giving users the opportunity write them was.
   *
   * We don't support versions for simplicity, but this can be added.
   */

  //private static final Logger LOG = LoggerFactory.getLogger(KeyValue.class);

  private final byte[] key;     // Max length: Short.MAX_VALUE = 32768
  private final byte[] family;  // Max length: Byte.MAX_VALUE  =   128
  private final byte[] qualifier;
  private final byte[] value;
  //private final long timestamp; // TODO(tsuna): Do I care about those?
  //private final byte type;      //              Will I need them?  Not sure.

  // Note: type can be one of:
  //   -  4  0b00000100  Put
  //   -  8  0b00001000  Delete
  static final byte DELETE = 8;
  //   - 12  0b00001100  DeleteColumn  (??)
  //   - 14  0b01110010  DeleteFamily  (delete all cells within a family)
  // (Not sure how those have been assigned...  Randomly maybe?)

  /**
   * Constructor.
   * @param key The row key.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @param value The value, the contents of the cell.
   */
  public KeyValue(final byte[] key,
                  final byte[] family, final byte[] qualifier,
                  final byte[] value//,
                  //final long timestamp, final byte type
                  ) {
    this.key = key;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    //this.timestamp = timestamp;
    //this.type = type;
  }

  /** Returns the row key.  */
  public byte[] key() {
    return key;
  }

  /** Returns the column family.  */
  public byte[] family() {
    return family;
  }

  /** Returns the column qualifier.  */
  public byte[] qualifier() {
    return qualifier;
  }

  //public long timestamp() {
  //  return timestamp;
  //}

  //public byte type() {
  //  return type;
  //}

  /** Returns the value, the contents of the cell.  */
  public byte[] value() {
    return value;
  }

  @Override
  public int compareTo(final KeyValue other) {
    int d;
    if ((d = Bytes.memcmp(key, other.key)) != 0) {
      return d;
    } else if ((d = Bytes.memcmp(family, other.family)) != 0) {
      return d;
    } else if ((d = Bytes.memcmp(qualifier, other.qualifier)) != 0) {
      return d;
    //} else if ((d = Bytes.memcmp(value, other.value)) != 0) {
    //  return d;
    //} else if ((d = Long.signum(timestamp - other.timestamp)) != 0) {
    //  return d;
    } else {
    //  d = type - other.type;
      d = Bytes.memcmp(value, other.value);
    }
    return d;
  }

  public boolean equals(final Object other) {
    if (other == null || !(other instanceof KeyValue)) {
      return false;
    }
    return compareTo((KeyValue) other) == 0;
  }

  public int hashCode() {
    return Arrays.hashCode(key)
      ^ Arrays.hashCode(family)
      ^ Arrays.hashCode(qualifier)
      ^ Arrays.hashCode(value)
      //^ (int) (timestamp ^ (timestamp >>> 32))
      //^ type
      ;
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(84  // Boilerplate + timestamp
      // the row key is likely to contain non-ascii characters, so
      // let's multiply its length by 2 to avoid re-allocations.
      + key.length * 2 + family.length + qualifier.length + value.length);
    buf.append("KeyValue(key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(", value=");
    Bytes.pretty(buf, value);
    //buf.append(", timestamp=").append(timestamp)
    //  .append(", type=").append(type);
    buf.append(')');
    return buf.toString();
  }

  /**
   * De-serializes {@link KeyValue} from a buffer.
   * @param buf The buffer to de-serialize from.
   * @param prev Another {@link KeyValue} previously de-serialized from the
   * same buffer.  Can be {@code null}.  The idea here is that KeyValues
   * often come in a sorted batch, and often share a number of byte arrays
   * (e.g.  they all have the same row key and/or same family...).  When
   * you specify another KeyValue, its byte arrays will be re-used in order
   * to avoid having too much duplicate data in memory.  This costs a little
   * bit of CPU time to compare the arrays but saves memory (which in turns
   * saves CPU time later).
   * @return a new instance (guaranteed non-{@code null}).
   * @throws IllegalArgumentException if the buffer seems to contain a
   * malformed {@link KeyValue}.
   */
  public static KeyValue fromBuffer(final ChannelBuffer buf,
                                    final KeyValue prev) {
    final int rowkey_length = buf.readInt();  // Total length of the row key.
    //LOG.debug("rowkey_length="+rowkey_length);
    HBaseRpc.checkNonEmptyArrayLength(buf, rowkey_length);
    final int value_length = buf.readInt();
    //LOG.debug("value_length="+value_length);
    HBaseRpc.checkArrayLength(buf, value_length);
    final short key_length = buf.readShort();
    //LOG.debug("key_length="+key_length);
    HBaseRpc.checkArrayLength(buf, value_length);
    final byte[] key = new byte[key_length];
    buf.readBytes(key);
    //LOG.debug("key="+Bytes.pretty(key));
    final byte family_length = buf.readByte();
    if (key_length + family_length + 2 + 1 + 8 + 1 > rowkey_length) {
      invalid("rowkey_length="
              + key_length + " doesn't match key_length + family_length ("
              + key_length + " + " + family_length + " +12) in " + buf + '='
              + Bytes.pretty(buf));
    }
    final byte[] family = new byte[family_length];
    buf.readBytes(family);
    final int qual_length = (rowkey_length - key_length - family_length
                             - 2 - 1 - 8 - 1);
    HBaseRpc.checkArrayLength(buf, qual_length);
    final byte[] qualifier = (qual_length > 0 ? new byte[qual_length]
                              : HBaseClient.EMPTY_ARRAY);
    buf.readBytes(qualifier);
    final long timestamp = buf.readLong();
    final byte key_type = buf.readByte();
    final byte[] value = (value_length > 0 ? new byte[value_length]
                          : HBaseClient.EMPTY_ARRAY);
    buf.readBytes(value);
    if (2 + key_length + 1 + family_length + qual_length + 8 + 1
        != rowkey_length) {  // XXX TMP DEBUG
      invalid("2 + rl:" + key_length + " + 1 + fl:" + family_length + " + ql:"
              + qual_length + " + 8 + 1" + " != kl:" + rowkey_length);
    }
    if (prev == null) {
      return new KeyValue(key, family, qualifier, /*timestamp, key_type,*/
                          value);
    } else {
      return new KeyValue(Bytes.deDup(prev.key, key),
                          Bytes.deDup(prev.family, family),
                          Bytes.deDup(prev.qualifier, qualifier),
                          /*timestamp, key_type,*/ value);
    }
  }

  private static void invalid(final String errmsg) {
    throw new IllegalArgumentException(errmsg);
  }

  // ------------------------------------------------------------ //
  // Misc helper functions to validate some aspects of KeyValues. //
  // ------------------------------------------------------------ //

  // OK this isn't technically part of a KeyValue but since all the similar
  // functions are here, let's keep things together in one place.
  /**
   * Validates a table name.
   * @throws IllegalArgumentException if the table name is too big or
   * malformed.
   * @throws NullPointerException if the table name is {@code null}.
   */
  static void checkTable(final byte[] table) {
    if (table.length > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Table name too long: "
        + table.length + " bytes long " + Bytes.pretty(table));
    } else if (table.length == 0) {
      throw new IllegalArgumentException("empty table name");
    }
  }

  /**
   * Validates a row key.
   * @throws IllegalArgumentException if the key is too big.
   * @throws NullPointerException if the key is {@code null}.
   */
  static void checkKey(final byte[] key) {
    if (key.length > Short.MAX_VALUE) {
      throw new IllegalArgumentException("row key too long: "
        + key.length + " bytes long " + Bytes.pretty(key));
    }
  }

  /**
   * Validates a column family.
   * @throws IllegalArgumentException if the family name is too big.
   * @throws NullPointerException if the family is {@code null}.
   */
  static void checkFamily(final byte[] family) {
    if (family.length > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("column family too long: "
        + family.length + " bytes long " + Bytes.pretty(family));
    }
  }

  /**
   * Validates a column qualifier.
   * @throws IllegalArgumentException if the qualifier name is too big.
   * @throws NullPointerException if the qualifier is {@code null}.
   */
  static void checkQualifier(final byte[] qualifier) {
    HBaseRpc.checkArrayLength(qualifier);
  }

  /**
   * Validates a value (the contents of an HBase cell).
   * @throws IllegalArgumentException if the value is too big.
   * @throws NullPointerException if the value is {@code null}.
   */
  static void checkValue(final byte[] value) {
    HBaseRpc.checkArrayLength(value);
  }

}
