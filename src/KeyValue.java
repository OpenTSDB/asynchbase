/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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

import org.hbase.async.generated.CellPB;

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

  /**
   * Timestamp value to let the server set the timestamp at processing time.
   * When this value is used as a timestamp on a {@code KeyValue}, the server
   * will substitute a real timestamp at the time it processes it.  HBase uses
   * current UNIX time in milliseconds.
   */
  public static final long TIMESTAMP_NOW = Long.MAX_VALUE;

  //private static final Logger LOG = LoggerFactory.getLogger(KeyValue.class);

  private final byte[] key;     // Max length: Short.MAX_VALUE = 32768
  private final byte[] family;  // Max length: Byte.MAX_VALUE  =   128
  private final byte[] qualifier;
  private final byte[] value;
  private final long timestamp;
  //private final byte type;  // Not needed for us ATM.

  // Note: type can be one of:
  //   -  4  0b00000100  Put
  static final byte PUT = 4;
  //   -  8  0b00001000  Delete        (delete the specified version of a cell)
  static final byte DELETE = 8;
  //   - 12  0b00001100  DeleteColumn  (delete all previous versions of a cell)
  static final byte DELETE_COLUMN = 12;
  //   - 14  0b01110010  DeleteFamily  (delete all cells within a family)
  static final byte DELETE_FAMILY = 14;
  // (Not sure how those have been assigned...  Randomly maybe?)

  /**
   * Constructor.
   * @param key The row key.  Length must fit in 16 bits.
   * @param family The column family.  Length must fit in 8 bits.
   * @param qualifier The column qualifier.
   * @param timestamp Timestamp on the value.  This timestamp can be set to
   * guarantee ordering of values or operations.  It is strongly advised to
   * use a UNIX timestamp in milliseconds, e.g. from a source such as
   * {@link System#currentTimeMillis}.  This value must be strictly positive.
   * @param value The value, the contents of the cell.
   * @throws IllegalArgumentException if any argument is invalid (e.g. array
   * size is too long) or if the timestamp is negative.
   * @since 1.2
   */
  public KeyValue(final byte[] key,
                  final byte[] family, final byte[] qualifier,
                  final long timestamp,
                  //final byte type,
                  final byte[] value) {
    checkKey(key);
    checkFamily(family);
    checkQualifier(qualifier);
    checkTimestamp(timestamp);
    checkValue(value);
    this.key = key;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    this.timestamp = timestamp;
    //this.type = type;
  }

  /**
   * Constructor.
   * <p>
   * This {@code KeyValue} will be timestamped by the server at the time
   * the server processes it.
   * @param key The row key.  Length must fit in 16 bits.
   * @param family The column family.  Length must fit in 8 bits.
   * @param qualifier The column qualifier.
   * @param value The value, the contents of the cell.
   * @throws IllegalArgumentException if any argument is invalid (e.g. array
   * size is too long).
   * @see #TIMESTAMP_NOW
   */
  public KeyValue(final byte[] key,
                  final byte[] family, final byte[] qualifier,
                  final byte[] value) {
    this(key, family, qualifier, TIMESTAMP_NOW, value);
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

  /**
   * Returns the timestamp stored in this {@code KeyValue}.
   * @see #TIMESTAMP_NOW
   */
  public long timestamp() {
    return timestamp;
  }

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
    } else if ((d = Long.signum(timestamp - other.timestamp)) != 0) {
      return d;
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
      ^ (int) (timestamp ^ (timestamp >>> 32))
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
    buf.append(", timestamp=").append(timestamp);
    //  .append(", type=").append(type);
    buf.append(')');
    return buf.toString();
  }

  /**
   * De-serializes {@link KeyValue} from a buffer (HBase 0.94 and before).
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
    final byte[] qualifier;
    if (qual_length > 0) {
      qualifier = new byte[qual_length];
      buf.readBytes(qualifier);
    } else {
      qualifier = HBaseClient.EMPTY_ARRAY;
    }
    final long timestamp = buf.readLong();
    final byte key_type = buf.readByte();
    final byte[] value;
    if (value_length > 0) {
      value = new byte[value_length];
      buf.readBytes(value);
    } else {
      value = HBaseClient.EMPTY_ARRAY;
    }
    if (2 + key_length + 1 + family_length + qual_length + 8 + 1
        != rowkey_length) {  // XXX TMP DEBUG
      invalid("2 + rl:" + key_length + " + 1 + fl:" + family_length + " + ql:"
              + qual_length + " + 8 + 1" + " != kl:" + rowkey_length);
    }
    if (prev == null) {
      return new KeyValue(key, family, qualifier, timestamp, /*key_type,*/
                          value);
    } else {
      return new KeyValue(Bytes.deDup(prev.key, key),
                          Bytes.deDup(prev.family, family),
                          Bytes.deDup(prev.qualifier, qualifier),
                          timestamp, /*key_type,*/ value);
    }
  }

  private static void invalid(final String errmsg) {
    throw new IllegalArgumentException(errmsg);
  }

  /**
   * Transforms a protobuf Cell message into a KeyValue (HBase 0.95+).
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
   */
  static KeyValue fromCell(final CellPB.Cell cell, final KeyValue prev) {
    final byte[] key = Bytes.get(cell.getRow());
    final byte[] family = Bytes.get(cell.getFamily());
    final byte[] qualifier = Bytes.get(cell.getQualifier());
    final long timestamp = cell.getTimestamp();
    final byte[] value = Bytes.get(cell.getValue());
    if (prev == null) {
      return new KeyValue(key, family, qualifier, timestamp, /*key_type,*/
                          value);
    } else {
      return new KeyValue(Bytes.deDup(prev.key, key),
                          Bytes.deDup(prev.family, family),
                          Bytes.deDup(prev.qualifier, qualifier),
                          timestamp, /*key_type,*/ value);
    }
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
   * Validates a timestamp.
   * @throws IllegalArgumentException if the timestamp is zero or negative.
   */
  static void checkTimestamp(final long timestamp) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("Negative timestamp: " + timestamp);
    }
  }

  /**
   * Validates a value (the contents of an HBase cell).
   * @throws IllegalArgumentException if the value is too big.
   * @throws NullPointerException if the value is {@code null}.
   */
  static void checkValue(final byte[] value) {
    HBaseRpc.checkArrayLength(value);
  }

  // ---------------------- //
  // Serialization helpers. //
  // ---------------------- //

  /**
   * Serializes this KeyValue.
   * @param buf The buffer into which to write the serialized form.
   * @param type What kind of KV (e.g. {@link #PUT} or {@link DELETE_FAMILY}).
   */
  void serialize(final ChannelBuffer buf, final byte type) {
    serialize(buf, type, timestamp, key, family, qualifier, value);
  }

  /**
   * Returns the serialized length of a KeyValue.
   */
  int predictSerializedSize() {
    return predictSerializedSize(key, family, qualifier, value);
  }

  /**
   * Returns the serialized length of a KeyValue.
   */
  static int predictSerializedSize(final byte[] key,
                                   final byte[] family,
                                   final byte[] qualifier,
                                   final byte[] value) {
    return
      + 4  // int: Total length of the whole KeyValue.
      + 4  // int: Total length of the key part of the KeyValue.
      + 4  // int: Total length of the value part of the KeyValue.
      + 2                 // short: Row key length.
      + key.length        // The row key.
      + 1                 // byte: Family length.
      + family.length     // The family.
      + qualifier.length  // The qualifier.
      + 8                 // long: The timestamp.
      + 1                 // byte: The type of KeyValue.
      + (value == null ? 0 : value.length);
  }

  /**
   * Serializes a KeyValue.
   * @param buf The buffer into which to write the serialized form.
   * @param type What kind of KV (e.g. {@link #PUT} or {@link DELETE_FAMILY}).
   * @param timestamp The timestamp to put on the KV.
   */
  static void serialize(final ChannelBuffer buf,
                        final byte type,
                        final long timestamp,
                        final byte[] key,
                        final byte[] family,
                        final byte[] qualifier,
                        final byte[] value) {
    final int val_length = value == null ? 0 : value.length;
    final int key_length = 2 + key.length + 1 + family.length
      + qualifier.length + 8 + 1;

    // Write the length of the whole KeyValue again (this is so useless...).
    buf.writeInt(4 + 4 + key_length + val_length);   // Total length.
    buf.writeInt(key_length);                        // Key length.
    buf.writeInt(val_length);                        // Value length.

    // Then the whole key.
    buf.writeShort(key.length);           // Row length.
    buf.writeBytes(key);                  // The row key (again!).
    buf.writeByte((byte) family.length);  // Family length.
    buf.writeBytes(family);               // Write the family (again!).
    buf.writeBytes(qualifier);            // The qualifier.
    buf.writeLong(timestamp);             // The timestamp (again!).
    buf.writeByte(type);                  // Type of edit
    if (value != null) {
      buf.writeBytes(value);              // Finally, the value (if any).
    }
  }

}
