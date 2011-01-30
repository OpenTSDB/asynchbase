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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import com.stumbleupon.async.Deferred;

/**
 * Abstract base class for all RPC requests going out to HBase.
 * <p>
 * Implementations of this class are <b>not</b> expected to be synchronized.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * If you change the contents of any byte array you give to an instance of
 * this class, you <em>may</em> affect the behavior of the request in an
 * <strong>unpredictable</strong> way.  If you need to change the byte array,
 * {@link Object#clone() clone} it before giving it to this class.  For those
 * familiar with the term "defensive copy", we don't do it in order to avoid
 * unnecessary memory copies when you know you won't be changing (or event
 * holding a reference to) the byte array, which is frequently the case.
 */
public abstract class HBaseRpc {
  /*
   * This class, although it's part of the public API, is mostly here to make
   * it easier for this library to manipulate the HBase RPC protocol.
   *
   *
   * Unofficial Hadoop / HBase RPC protocol documentation
   * ****************************************************
   *
   * HBase uses a modified version of the Hadoop RPC protocol.  They took
   * Hadoop's RPC code, copy-pasted it into HBase, and tweaked it a little
   * bit (mostly in a desperate attempt to try to make it less inefficient).
   *
   * RPCs are numbered with an arbitrary 32-bit ID.  It is customary, but not
   * mandatory, to start at 0 and increment by 1 every time you send out an
   * RPC.  The ID is allowed to wrap around and become negative.  As long as
   * no 2 RPCs share the same ID at the same time, we're fine.
   *
   * When requests are written out to the wire, they're framed.  Meaning, a
   * 4 byte integer value is first written in order to specify how many bytes
   * are in the request (excluding the first 4 bytes themselves).  The size -1
   * is special.  The client uses it to send a "ping" to the server at regular
   * intervals, and the server specifically ignores any RPC with this ID.  We
   * don't do this in this client, because it's mostly useless, and we rely on
   * TCP keepalive instead.
   *
   * Then the RPC ID is written (4 bytes).  BTW, all integer values are
   * encoded in big endian, as it's the default in Java world (Sun, SPARC...).
   *
   * Then the length of the method name is written on 2 bytes (I guess 1 byte
   * wasn't enough in case you wanted to have 32768 byte long method names).
   *
   * Then the method name itself is written as-is (as a byte array).
   *
   * The last 4 fields are what constitute the "RPC header".  The remaining
   * bytes are the parameters of the request.  First, there is a 4-byte int
   * that specifies how many parameters follow (this way you can have up to
   * 2 147 483 648 parameters, which may come in handy in a few centuries).
   *
   * In Hadoop RPC, the name of the class is first serialized (2 bytes
   * specifying the length of the string, followed by that number of bytes
   * of a UTF-8 encoded string in case you name your classes with Kanjis).
   * In HBase RPC, a 1-byte ID representing the class name is written instead
   * of writing the full class name.  Those IDs are hard-coded in a central
   * location (`HbaseObjectWritable', HBase's copy-pasted-hacked version of
   * Hadoop's `ObjectWritable').
   *
   * The way each parameter is serialized depends on the object type being
   * serialized.  Since Hadoop doesn't use any automatic serialization
   * framework, every class is free to serialize itself however it wants.
   * The way it works is that for built-in types, they'll handle the
   * serialization manually, and for other objects, they require that those
   * objects implement their `Writable' interface which requires that a method
   * named `readFields' and a method named `write' be implemented to
   * de-serialize / serialize the object.  So since the RPC layer knows the
   * name of the class of the parameter, it will grab its `Class' using the
   * Java Classloader and then `newInstance' it and then use `readFields' to
   * populate the newly created instance.  Thankfully most objects use a
   * common library to serialize their own fields recursively, however things
   * aren't always consistent, particularly when HBase chose to diverge from
   * Hadoop in certain (but not all) code paths.
   *
   * The way RPC responses are encoded is as follows.  First comes the 4-byte
   * RPC ID.  Then 1 byte which is a boolean indicating whether or not the
   * request failed on the remote side (if the byte is zero = false).  If
   * the request failed, the rest of the response is just 2 Hadoop-encoded
   * strings (2-byte length, followed by a UTF-8 string).  The first string is
   * the name of the class of the exception and the second is the message of
   * the exception (which typically includes some of the server-side stack
   * trace).  Note that the response is NOT framed, so it's not easy to tell
   * ahead of time how many bytes to expect or where the next response starts.
   *
   * If the RPC was successful, the remaining of the payload is serialized
   * using the same method as the RPC parameters are serialized (see above).
   *
   * Before the very first RPC, the server expects a "hello" message that
   * starts with 4-byte magic number, followed by the RPC version (1 byte).
   * Then comes 4 bytes to specify the rest of the length of the "hello"
   * message.  The remaining is a `Writable' instance serialized that
   * specifies which authentication provider to use and give our credentials.
   * I have a hunch that this is going to change imminently in Hadoop when
   * they roll out the security stuff they've been working on for a while,
   * and HBase will probably have to adopt that (unless they decide to stop
   * using Hadoop RPC and switch to something more modern and reasonable).
   * The "hello" message is implemented in `RegionClient.SayHelloFirstRpc'.
   */

  /**
   * To be implemented by the concrete sub-type.
   * This method is expected to instantiate a {@link ChannelBuffer} using
   * either {@link #newBuffer} or {@link #newDynamicBuffer} and return it
   * properly populated so it's ready to be written out to the wire (except
   * for the "RPC header" that contains the RPC ID and method name and such,
   * which is going to be populated automatically just before sending the RPC
   * out, see {@link RegionClient#encode}.
   *
   * Notice that this method is package-private, so only classes within this
   * package can use this as a base class.
   *
   * @param server_version The RPC protocol version of the server this RPC is
   * going to.  If the version of the server is unknown, this will be -1.  If
   * this RPC cares a lot about the version of the server (due to backwards
   * incompatible changes in the RPC serialization), the concrete class should
   * override {@link #versionSensitive} to make sure it doesn't get -1, as the
   * version is lazily fetched.
   */
  abstract ChannelBuffer serialize(byte server_version);

  /**
   * Name of the method to invoke on the server side.
   * This is the name of a method of {@code HRegionInterface}.
   */
  private final byte[] method;

  /**
   * The Deferred that will be invoked when this RPC completes or fails.
   * In case of a successful completion, this Deferred's first callback
   * will be invoked with an {@link Object} containing the de-serialized
   * RPC response in argument.
   * Once an RPC has been used, we create a new Deferred for it, in case
   * the user wants to re-use it.
   */
  private Deferred<Object> deferred;

  // The next 3 fields are package-private so subclasses can access them
  // without them being part of the interface (unlike with `protected').

  /**
   * The table for which this RPC is.
   * {@code null} if this RPC isn't for a particular table.
   * Invariants:
   *   table == null  =>  key == null
   *   table != null  =>  key != null
   */
  final byte[] table;  // package-private for subclasses, not other classes.

  /**
   * The row key for which this RPC is.
   * {@code null} if this RPC isn't for a particular row key.
   * Invariants:
   *   table == null  =>  key == null
   *   table != null  =>  key != null
   */
  final byte[] key;  // package-private for subclasses, not other classes.

  /**
   * The region for which this RPC is.
   * {@code null} if this RPC isn't for a single specific region.
   * Invariants:
   *   table == null  =>  region == null
   *   table != null  =>  region != null before {@link #serialize} gets called
   */
  RegionInfo region;  // package-private for subclasses, not other classes.

  /**
   * How many times have we retried this RPC?.
   * Only used by the low-level retry logic in {@link RegionClient} in order
   * to detect broken META tables (e.g. we keep getting an NSRE but META keeps
   * sending us to the same RegionServer that sends us the NSREs, or we keep
   * looking up the same row in META because of a "hole" in META).
   * <p>
   * Proper synchronization is required, although in practice most of the code
   * that access this attribute will have a happens-before relationship with
   * the rest of the code, due to other existing synchronization.
   */
  byte attempt;  // package-private for RegionClient and HBaseClient only.

  /**
   * Package private constructor for RPCs that aren't for any region.
   * @param method The name of the method to invoke on the RegionServer.
   */
  HBaseRpc(final byte[] method) {
    this.method = method;
    table = null;
    key = null;
  }

  /**
   * Package private constructor for RPCs that are for a region.
   * @param method The name of the method to invoke on the RegionServer.
   * @param table The name of the table this RPC is for.
   * @param row The name of the row this RPC is for.
   */
  HBaseRpc(final byte[] method, final byte[] table, final byte[] key) {
    KeyValue.checkTable(table);
    KeyValue.checkKey(key);
    this.method = method;
    this.table = table;
    this.key = key;
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  /** Package private way of getting the table this RPC is for (if any).  */
  final byte[] table() {
    return table;
  }

  /** Package private way of getting the row key this RPC is for (if any).  */
  final byte[] key() {
    return key;
  }

  /** Package private way of getting the name of the RPC method.  */
  final byte[] method() {
    return method;
  }

  /**
   * Sets the region this RPC is going to.
   * <p>
   * This method is invoked by {@link HBaseClient} once the user give it
   * their {@code HBaseRpc}, because {@link HBaseClient} is the only thing
   * that knows about and keeps track of regions.  Users don't care about
   * regions.
   * @param region The target region of this RPC.
   */
  final void setRegion(final RegionInfo region) {
    if (table == null) {
      throw new AssertionError("Can't use setRegion if no table was given.");
    }
    this.region = region;
  }

  /**
   * Returns the region this RPC is supposed to go to (can be {@code null}).
   */
  final RegionInfo getRegion() {
    return region;
  }

  /** Package private way of accessing / creating the Deferred of this RPC.  */
  final Deferred<Object> getDeferred() {
    if (deferred == null) {
      deferred = new Deferred<Object>();
    }
    return deferred;
  }

  /**
   * Package private way of making an RPC complete by giving it its result.
   * If this RPC has no {@link Deferred} associated to it, nothing will
   * happen.  This may happen if the RPC was already called back.
   * <p>
   * Once this call to this method completes, this object can be re-used to
   * re-send the same RPC, provided that no other thread still believes this
   * RPC to be in-flight (guaranteeing this may be hard in error cases).
   */
  final void callback(final Object result) {
    final Deferred<Object> d = deferred;
    if (d == null) {
      return;
    }
    deferred = null;
    attempt = 0;
    d.callback(result);
  }

  /** Checks whether or not this RPC has a Deferred without creating one.  */
  final boolean hasDeferred() {
    return deferred != null;
  }

  /**
   * Is the encoding of this RPC sensitive to the RPC protocol version?.
   * Override this method to return {@code true} in order to guarantee that
   * the version of the remote server this RPC is going to will be known by
   * the time this RPC gets serialized.
   */
  boolean versionSensitive() {
    return false;
  }

  public String toString() {
    return "HBaseRpc(method=" + Bytes.pretty(method)
      + ", table=" + Bytes.pretty(table)
      + ", key=" + Bytes.pretty(key)
      + ", region=" + region
      + ", attempt=" + attempt
      + ')';
  }

  // --------------------- //
  // RPC utility functions //
  // --------------------- //

  /*
   * The remaining of this file is just a whole bunch of functions to make
   * it easier to deal with the absolutely horrible Hadoop RPC protocol.
   *
   * One should assume that all the following functions can throw an
   * IndexOutOfBoundsException when reading past the end of a buffer
   * or writing past the end of a fixed-length buffer.
   *
   * A number of functions, particularly those reading something, will
   * throw an IllegalArgumentException if the buffer they're asked to
   * parse contains junk or otherwise corrupted or suspicious data.
   */

  /**
   * Creates a new fixed-length buffer on the heap.
   * @param max_payload_size A good approximation of the size of the payload.
   * The approximation must be an upper bound on the expected size of the
   * payload as trying to store more than {@code max_payload_size} bytes in
   * the buffer returned will cause an {@link ArrayIndexOutOfBoundsException}.
   * <p>
   * When no reasonable upper bound on the payload size can be easily
   * estimated ahead of time, you can use {@link #newDynamicBuffer} instead.
   */
  final ChannelBuffer newBuffer(final int max_payload_size) {
    // Add extra bytes for the RPC header:
    //   4 bytes: Payload size.
    //   4 bytes: RPC ID.
    //   2 bytes: Length of the method name.
    //   N bytes: The method name.
    final int header = 4 + 4 + 2 + method.length;
    final ChannelBuffer buf = ChannelBuffers.buffer(header + max_payload_size);
    buf.setIndex(0, header);  // Advance the writerIndex past the header.
    return buf;
  }

  /**
   * Creates a new dynamic-length buffer on the heap.
   * @param max_payload_size A good approximation of the size of the payload.
   * The approximation should be an upper bound on the expected size of the
   * payload.  Trying to store more than {@code max_payload_size} bytes in
   * this buffer will be automatically resized, which involves doubling the
   * size of the buffer (or more) and copying the contents of the old buffer
   * to the new one.
   * <p>
   * Whenever possible, {@link #newBuffer} should be used instead.  In a
   * benchmark I did, writing to a dynamic-length buffer was about 16% slower
   * and that's without ever re-sizing the buffer!
   */
  final ChannelBuffer newDynamicBuffer(final int max_payload_size) {
    // See the comment in newBuffer above.
    final int header = 4 + 4 + 2 + method.length;
    final ChannelBuffer buf = ChannelBuffers.dynamicBuffer(header
                                                           + max_payload_size);
    buf.setIndex(0, header);  // Advance the writerIndex past the header.
    return buf;
  }

  /**
   * Writes a {@link Boolean boolean} as an HBase RPC parameter.
   * @param buf The buffer to serialize the string to.
   * @param b The boolean value to serialize.
   */
  static void writeHBaseBool(final ChannelBuffer buf, final boolean b) {
    buf.writeByte(1);  // Code for Boolean.class in HbaseObjectWritable
    buf.writeByte(b ? 0x01 : 0x00);
  }

  /**
   * Writes an {@link Integer int} as an HBase RPC parameter.
   * @param buf The buffer to serialize the string to.
   * @param v The value to serialize.
   */
  static void writeHBaseInt(final ChannelBuffer buf, final int v) {
    buf.writeByte(5);  // Code for Integer.class in HbaseObjectWritable
    buf.writeInt(v);
  }

  /**
   * Writes a {@link Long long} as an HBase RPC parameter.
   * @param buf The buffer to serialize the string to.
   * @param v The value to serialize.
   */
  static void writeHBaseLong(final ChannelBuffer buf, final long v) {
    buf.writeByte(6);  // Code for Long.class in HbaseObjectWritable
    buf.writeLong(v);
  }

  /**
   * Writes a {@link String} as an HBase RPC parameter.
   * @param buf The buffer to serialize the string to.
   * @param s The string to serialize.
   */
  static void writeHBaseString(final ChannelBuffer buf, final String s) {
    buf.writeByte(10);  // Code for String.class in HbaseObjectWritable
    final byte[] b = s.getBytes(CharsetUtil.UTF_8);
    writeVLong(buf, b.length);
    buf.writeBytes(b);
  }

  /**
   * Writes a byte array as an HBase RPC parameter.
   * @param buf The buffer to serialize the string to.
   * @param b The byte array to serialize.
   */
  static void writeHBaseByteArray(final ChannelBuffer buf, final byte[] b) {
    buf.writeByte(11);     // Code for byte[].class in HbaseObjectWritable
    writeByteArray(buf, b);
  }

  /**
   * Writes a byte array.
   * @param buf The buffer to serialize the string to.
   * @param b The byte array to serialize.
   */
  static void writeByteArray(final ChannelBuffer buf, final byte[] b) {
    writeVLong(buf, b.length);
    buf.writeBytes(b);
  }

  /**
   * Upper bound on the size of a byte array we de-serialize.
   * This is to prevent HBase from OOM'ing us, should there be a bug or
   * undetected corruption of an RPC on the network, which would turn a
   * an innocuous RPC into something allocating a ton of memory.
   * The Hadoop RPC protocol doesn't do any checksumming as they probably
   * assumed that TCP checksums would be sufficient (they're not).
   */
  private static final long MAX_BYTE_ARRAY_MASK =
    0xFFFFFFFFF0000000L;  // => max = 256MB

  /**
   * Verifies that the given length looks like a reasonable array length.
   * This method accepts 0 as a valid length.
   * @param buf The buffer from which the length was read.
   * @param length The length to validate.
   * @throws IllegalArgumentException if the length is negative or
   * suspiciously large.
   */
  static void checkArrayLength(final ChannelBuffer buf, final long length) {
    // 2 checks in 1.  If any of the high bits are set, we know the value is
    // either too large, or is negative (if the most-significant bit is set).
    if ((length & MAX_BYTE_ARRAY_MASK) != 0) {
      if (length < 0) {
        throw new IllegalArgumentException("Read negative byte array length: "
          + length + " in buf=" + buf + '=' + Bytes.pretty(buf));
      } else {
        throw new IllegalArgumentException("Read byte array length that's too"
          + " large: " + length + " > " + ~MAX_BYTE_ARRAY_MASK + " in buf="
          + buf + '=' + Bytes.pretty(buf));
      }
    }
  }

  /**
   * Verifies that the given array looks like a reasonably big array.
   * This method accepts empty arrays.
   * @param array The array to check.
   * @throws IllegalArgumentException if the length of the array is
   * suspiciously large.
   * @throws NullPointerException if the array is {@code null}.
   */
  static void checkArrayLength(final byte[] array) {
    if ((array.length & MAX_BYTE_ARRAY_MASK) != 0) {
      if (array.length < 0) {  // Not possible unless there's a JVM bug.
        throw new AssertionError("Negative byte array length: "
                                 + array.length + ' ' + Bytes.pretty(array));
      } else {
        throw new IllegalArgumentException("Byte array length too big: "
          + array.length + " > " + ~MAX_BYTE_ARRAY_MASK);
        // Don't dump the gigantic byte array in the exception message.
      }
    }
  }

  /**
   * Verifies that the given length looks like a reasonable array length.
   * This method does not accept 0 as a valid length.
   * @param buf The buffer from which the length was read.
   * @param length The length to validate.
   * @throws IllegalArgumentException if the length is zero, negative or
   * suspiciously large.
   */
  static void checkNonEmptyArrayLength(final ChannelBuffer buf,
                                       final long length) {
    if (length == 0) {
      throw new IllegalArgumentException("Read zero-length byte array "
        + " in buf=" + buf + '=' + Bytes.pretty(buf));
    }
    checkArrayLength(buf, length);
  }

  /**
   * Reads a byte array.
   * @param buf The buffer from which to read the array.
   * @return A possibly empty but guaranteed non-{@code null} byte array.
   * @throws IllegalArgumentException if the length we read for the byte array
   * is out of reasonable bounds.
   */
  static byte[] readByteArray(final ChannelBuffer buf) {
    final long length = readVLong(buf);
    checkArrayLength(buf, length);
    final byte[] b = new byte[(int) length];
    buf.readBytes(b);
    return b;
  }

  /**
   * Reads a string encoded by {@code hadoop.io.WritableUtils#readString}.
   * @throws IllegalArgumentException if the length we read for the string
   * is out of reasonable bounds.
   */
  static String readHadoopString(final ChannelBuffer buf) {
    final int length = buf.readInt();
    checkArrayLength(buf, length);
    final byte[] s = new byte[length];
    buf.readBytes(s);
    return new String(s, CharsetUtil.UTF_8);
  }

  // -------------------------------------- //
  // Variable-length integer value encoding //
  // -------------------------------------- //

  /*
   * Unofficial documentation of the Hadoop VLong encoding
   * *****************************************************
   *
   * The notation used to refer to binary numbers here is `0b' followed by
   * the bits, as is printed by Python's built-in `bin' function for example.
   *
   * Values between
   *   -112 0b10010000
   * and
   *    127 0b01111111
   * (inclusive) are encoded on a single byte using their normal
   * representation.  The boundary "-112" sounds weird at first (and it is)
   * but it'll become clearer once you understand the format.
   *
   * Values outside of the boundaries above are encoded by first having
   * 1 byte of meta-data followed by a variable number of bytes that make up
   * the value being encoded.
   *
   * The "meta-data byte" always starts with the prefix 0b1000.  Its format
   * is as follows:
   *   1 0 0 0 | S | L L L
   * The bit `S' is the sign bit (1 = positive value, 0 = negative, yes
   * that's weird, I would've done it the other way around).
   * The 3 bits labeled `L' indicate how many bytes make up this variable
   * length value.  They're encoded like so:
   *   1 1 1 = 1 byte follows
   *   1 1 0 = 2 bytes follow
   *   1 0 1 = 3 bytes follow
   *   1 0 0 = 4 bytes follow
   *   0 1 1 = 5 bytes follow
   *   0 1 0 = 6 bytes follow
   *   0 0 1 = 7 bytes follow
   *   0 0 0 = 8 bytes follow
   * Yes, this is weird too, it goes backwards, requires more operations to
   * convert the length into something human readable, and makes sorting the
   * numbers unnecessarily complicated.
   * Notice that the prefix wastes 3 bits.  Also, there's no "VInt", all
   * variable length encoded values are eventually transformed to `long'.
   *
   * The remaining bytes are just the original number, as-is, without the
   * unnecessary leading bytes (that are all zeroes).
   *
   * Examples:
   *   42 is encoded as                   00101010 (as-is, 1 byte)
   *  127 is encoded as                   01111111 (as-is, 1 bytes)
   *  128 is encoded as          10001111 10000000 (2 bytes)
   *  255 is encoded as          10001111 11111111 (2 bytes)
   *  256 is encoded as 10001110 00000001 00000000 (3 bytes)
   *   -1 is encoded as                   11111111 (as-is, 1 byte)
   *  -42 is encoded as                   11010110 (as-is, 1 byte)
   * -112 is encoded as                   10010000 (as-is, 1 byte)
   * -113 is encoded as          10000111 01110000 (2 bytes)
   * -256 is encoded as          10000111 11111111 (2 bytes)
   * -257 is encoded as 10000110 00000001 00000000 (3 bytes)
   *
   * The implementations of writeVLong and readVLong below are on average
   * 14% faster than Hadoop's implementation given a uniformly distributed
   * input (lots of values of all sizes), and can be up to 40% faster on
   * certain input sizes (e.g. big values that fit on 8 bytes).  This is due
   * to two main things: fewer arithmetic and logic operations, and processing
   * multiple bytes together when possible.
   * Reading is about 6% faster than writing (negligible difference).
   * My MacBook Pro with a 2.66 GHz Intel Core 2 Duo easily does 5000 calls to
   * readVLong or writeVLong per millisecond.
   *
   * However, since we use Netty, we don't have to deal with the stupid Java
   * I/O library, so unlike Hadoop we don't use DataOutputStream and
   * ByteArrayOutputStream, instead we use ChannelBuffer.  This gives us a
   * significant extra performance boost over Hadoop.  The 14%-60% difference
   * above becomes a 70% to 80% difference!  Yes, that's >4 times faster!  With
   * the code below my MacBook Pro with a 2.66 GHz Intel Core 2 Duo easily
   * does 11000 writeVLong/ms or 13500 readVLong/ms (notice that reading is
   * 18% faster) when using a properly sized dynamicBuffer.  When using a
   * fixed-size buffer, writing (14200/s) is almost as fast as reading
   * (14500/s).
   *
   * So there's really no reason on Earth to use java.io.  Its API is horrible
   * and so is its performance.
   */

  /**
   * Writes a variable-length {@link Long} value.
   * @param buf The buffer to write to.
   * @param n The value to write.
   */
  @SuppressWarnings("fallthrough")
  static void writeVLong(final ChannelBuffer buf, long n) {
    // All those values can be encoded on 1 byte.
    if (n >= -112 && n <= 127) {
      buf.writeByte((byte) n);
      return;
    }

    // Set the high bit to indicate that more bytes are to come.
    // Both 0x90 and 0x88 have the high bit set (and are thus negative).
    byte b = (byte) 0x90; // 0b10010000
    if (n < 0) {
      n = ~n;
      b = (byte) 0x88;    // 0b10001000
    }

    {
      long tmp = n;
      do {
        tmp >>>= 8;
        // The first time we decrement `b' here, it's going to move the
        // rightmost `1' in `b' to the right, due to the way 2's complement
        // representation works.  So if `n' is positive, and we started with
        // b = 0b10010000, now we'll have b = 0b10001111, which correctly
        // indicates that `n' is positive (5th bit set) and has 1 byte so far
        // (last 3 bits are set).  If `n' is negative, and we started with
        // b = 0b10001000, now we'll have b = 0b10000111, which correctly
        // indicates that `n' is negative (5th bit not set) and has 1 byte.
        // Each time we keep decrementing this value, the last remaining 3
        // bits are going to change according to the format described above.
        b--;
      } while (tmp != 0);
    }

    buf.writeByte(b);
    switch (b & 0x07) {  // Look at the low 3 bits (the length).
      case 0x00:
        buf.writeLong(n);
        break;
      case 0x01:
        buf.writeInt((int) (n >>> 24));
        buf.writeMedium((int) n);
        break;
      case 0x02:
        buf.writeMedium((int) (n >>> 24));
        buf.writeMedium((int) n);
        break;
      case 0x03:
        buf.writeByte((byte) (n >>> 32));
      case 0x04:
        buf.writeInt((int) n);
        break;
      case 0x05:
        buf.writeMedium((int) n);
        break;
      case 0x06:
        buf.writeShort((short) n);
        break;
      case 0x07:
        buf.writeByte((byte) n);
    }
  }

  /**
   * Reads a variable-length {@link Long} value.
   * @param buf The buffer to read from.
   * @return The value read.
   */
  @SuppressWarnings("fallthrough")
  static long readVLong(final ChannelBuffer buf) {
    byte b = buf.readByte();
    // Unless the first half of the first byte starts with 0xb1000, we're
    // dealing with a single-byte value.
    if ((b & 0xF0) != 0x80) {  // 0xF0 = 0b11110000, 0x80 = 0b10000000
      return b;
    }

    // The value is negative if the 5th bit is 0.
    final boolean negate = (b & 0x08) == 0;    // 0x08 = 0b00001000
    long result = 0;
    switch (b & 0x07) {  // Look at the low 3 bits (the length).
      case 0x00:
        result = buf.readLong();
        break;
      case 0x01:
        result = buf.readUnsignedInt();
        result <<= 32;
        result |= buf.readUnsignedMedium();
        break;
      case 0x02:
        result = buf.readUnsignedMedium();
        result <<= 24;
        result |= buf.readUnsignedMedium();
        break;
      case 0x03:
        b = buf.readByte();
        result <<= 8;
        result |= b & 0xFF;
      case 0x04:
        result <<= 32;
        result |= buf.readUnsignedInt();
        break;
      case 0x05:
        result |= buf.readUnsignedMedium();
        break;
      case 0x06:
        result |= buf.readUnsignedShort();
        break;
      case 0x07:
        b = buf.readByte();
        result <<= 8;
        result |= b & 0xFF;
    }
    return negate ? ~result : result;
  }

}
