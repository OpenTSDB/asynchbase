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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(HBaseRpc.class);
  
  /**
   * An RPC from which you can get a table name.
   * @since 1.1
   */
  public interface HasTable {
    /**
     * Returns the name of the table this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the name of the table this RPC is for.
     */
    public byte[] table();
  }

  /**
   * An RPC from which you can get a row key name.
   * @since 1.1
   */
  public interface HasKey {
    /**
     * Returns the row key this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the row key this RPC is for.
     */
    public byte[] key();
  }

  /**
   * An RPC from which you can get a family name.
   * @since 1.1
   */
  public interface HasFamily {
    /**
     * Returns the family this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the family this RPC is for.
     */
    public byte[] family();
  }

  /**
   * An RPC from which you can get a column qualifier name.
   * @since 1.1
   */
  public interface HasQualifier {
    /**
     * Returns the column qualifier this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the column qualifier this RPC is for.
     */
    public byte[] qualifier();
  }

  /**
   * An RPC from which you can get multiple column qualifier names.
   * @since 1.1
   */
  public interface HasQualifiers {
    /**
     * Returns the column qualifiers this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the column qualifiers this RPC is for.
     */
    public byte[][] qualifiers();
  }

  /**
   * An RPC from which you can get a value.
   * @since 1.1
   */
  public interface HasValue {
    /**
     * Returns the value contained in this RPC.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the value contained in this RPC.
     */
    public byte[] value();
  }

  /**
   * An RPC from which you can get multiple values.
   * @since 1.3
   */
  public interface HasValues {
    /**
     * Returns the values contained in this RPC.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     * @return the values contained in this RPC.
     */
    public byte[][] values();
  }

  /**
   * An RPC from which you can get a timestamp.
   * @since 1.2
   */
  public interface HasTimestamp {
    /**
     * Returns the strictly positive timestamp contained in this RPC.
     * @return the strictly positive timestamp contained in this RPC.
     */
    public long timestamp();
  }

  /**
   * Package-private interface to mark RPCs that are changing data in HBase.
   * @since 1.4
   */
  interface IsEdit {
    /** RPC method name to use with HBase 0.95+.  */
    static final byte[] MUTATE = { 'M', 'u', 't', 'a', 't', 'e' };
  }
  private boolean trace_rpc;

  public boolean isTraceRPC() {
    return trace_rpc;
  }

  public void setTraceRPC(boolean trace_rpc) {
    this.trace_rpc = trace_rpc;
  }
  
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
   * intervals, and the server specifically ignores any RPC with size -1.  We
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
   * In HBase 0.92 and above, 3 more fields have been added in the header as
   * previously described.  The first is a one byte version number that comes
   * right before the method name, indicating how the parameters of the RPC
   * have been serialized.  Then there is a 8 byte (!) client version that's
   * right after the method name, followed by a 4 byte "fingerprint", which
   * is a sort of hash code of the method's signature (name, return type, and
   * parameters types).  Note that the client version seems to be always set
   * to zero...
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
   * RPC ID.  Then 1 byte containing flags indicating whether or not the
   * request failed (0x01) on the remote side, and whether the response is
   * framed (0x02).  If flags are only 0x00, this is an old-style (pre 0.92)
   * successful response that is not framed.  Framed responses contain a
   * 4-byte integer with the length of the entire response, including the
   * leading RPC ID, flags, and the length itself.  If there is a length, it
   * is always followed by a 4-byte integer with the state of the RPC follows.
   * As of 0.92, this state mostly useless.  If the request failed (flag 0x01
   * is set), the rest of the response is just 2 Hadoop-encoded
   * strings (2-byte length, followed by a UTF-8 string).  The first string is
   * the name of the class of the exception and the second is the message of
   * the exception (which typically includes some of the server-side stack
   * trace).  Note that if the response is NOT framed, it's not easy to tell
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
   * In HBase 0.92 and above, the `Writable' should represent what protocol
   * the client wants to speak, which should be the name of an interface.
   * "org.apache.hadoop.hbase.ipc.HRegionInterface" should be used.
   * The "hello" message is implemented in `RegionClient#helloRpc'.  In order
   * to support HBase 0.92, we always piggy back a `getProtocolVersion' RPC
   * right after the header, so we can tell what version the server is using
   * and how to serialize RPCs and read its responses.
   */

  // ------ //
  // Flags. //
  // ------ //
  // 5th byte into the response.
  // See ipc/ResponseFlag.java in HBase's source code.

  static final byte RPC_SUCCESS = 0x00;
  static final byte RPC_ERROR = 0x01;
  /**
   * Indicates that the next byte is an integer with the length of the response.
   * This can be found on both successful ({@link RPC_SUCCESS}) or failed
   * ({@link RPC_ERROR}) responses.
   * @since HBase 0.92
   */
  static final byte RPC_FRAMED = 0x02;

  // ----------- //
  // RPC Status. //
  // ----------- //
  // 4 byte integer (on wire), located 9 byte into the response, only if
  // {@link RPC_FRAMED} is set.
  // See ipc/Status.java in HBase's source code.

  /**
   * Indicates that an error prevented the RPC from being executed.
   * This is a somewhat misleading name.  It indicates that the RPC couldn't
   * be executed, typically because of a protocol version mismatch, an
   * incorrectly encoded RPC (or possibly corrupted on-wire such that the
   * server couldn't deserialize it), or an authentication error (unsure about
   * that one).
   */
  static final byte RPC_FATAL = -1;

  /**
   * To be implemented by the concrete sub-type.
   * This method is expected to instantiate a {@link ChannelBuffer} using
   * either {@link #newBuffer} and return it
   * properly populated so it's ready to be written out to the wire (except
   * for the "RPC header" that contains the RPC ID and method name and such,
   * which is going to be populated automatically just before sending the RPC
   * out, see {@link RegionClient#encode}.
   *
   * Notice that this method is package-private, so only classes within this
   * package can use this as a base class.
   *
   * @param server_version What RPC protocol version the server is running.
   */
  abstract ChannelBuffer serialize(byte server_version);

  /**
   * To be implemented by the concrete sub-type.
   * This method is expected to de-serialize a response received for the
   * current RPC, when communicating with HBase 0.95 and newer.
   *
   * Notice that this method is package-private, so only classes within this
   * package can use this as a base class.
   *
   * @param buf The buffer from which to de-serialize the response.
   * @param cell_size The size, in bytes, of the "cell block" that follows the
   * protobuf of the RPC response.  If 0, then there is just the protobuf.
   * The value is guaranteed to be both positive and of a "reasonable" size.
   */
   abstract Object deserialize(ChannelBuffer buf, int cell_size);

  /**
   * Throws an exception if the argument is non-zero.
   */
  static void ensureNoCell(final int cell_size) {
    if (cell_size != 0) {
      throw new InvalidResponseException(
        "Should not have gotten any cell blocks, yet there are "
        + cell_size + " bytes that follow the protobuf response."
        + "  This should never happen."
        + "  Are you using an incompatible version of HBase?", null);
    }
  }

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

  /** An optional timeout in milliseconds for the RPC. -1 means use the 
   * default from the config. 0 means don't timeout. */
  private int timeout = -1;
  
  /** If the RPC has a timeout set this will be set on submission to the 
   * timer thread. */
  Timeout timeout_handle; // package-private for RegionClient and HBaseClient only.
  
  /** Task set if a timeout has been requested. The task will be executed only
   * if the RPC did timeout, then we mark {@link #has_timedout} as true and
   * remove the RPC from the region client as well as calling it back with
   * a {@link RpcTimedoutException}
   */
  private TimerTask timeout_task;
  
  /** Whether or not this RPC has timed out already */
  private boolean has_timedout;
  
  /**
   * If true, this RPC should fail-fast as soon as we know we have a problem.
   */
  boolean failfast = false;

  /** The ID of this RPC as set by the last region client that handled it */
  int rpc_id;
  
  /** A reference to the last region client that handled this RPC */
  private RegionClient region_client;
  
  /**
   * Set whether the RPC not be retried upon encountering a problem.
   * <p>
   * RPCs can be retried for various legitimate reasons (e.g. NSRE due to a
   * region moving), but under certain failure circumstances (such as a node
   * going down) we want to give up and be alerted as soon as possible.
   * @param failfast If {@code true}, this RPC should fail-fast as soon as
   * we know we have a problem.
   * @return whether the RPC not be retried upon encountering a problem.
   * @since 1.5
   */
  public final boolean setFailfast(final boolean failfast) {
    return this.failfast = failfast;
  }

  /**
   * Returns whether the RPC not be retried upon encountering a problem.
   * @return whether the RPC not be retried upon encountering a problem.
   * @see #setFailfast
   * @since 1.5
   */
  public final boolean failfast() {
    return failfast;
  }

  /**
   * If true, this RPC is a probe which checks if the destination region is
   * online.
   */
  private boolean probe = false;

  boolean isProbe() {
    return probe;
  }

  void setProbe(boolean probe) {
    this.probe = probe;
  }

  /**
   * Whether or not if this RPC is a probe that is suspended by an NSRE
   */
  private boolean suspended_probe = false;

  boolean isSuspendedProbe() {
    return suspended_probe;
  }

  void setSuspendedProbe(boolean suspended_probe) {
    this.suspended_probe = suspended_probe;
  }

  /**
   * Package private constructor for RPCs that aren't for any region.
   */
  HBaseRpc() {
    table = null;
    key = null;
  }

  /**
   * Package private constructor for RPCs that are for a region.
   * @param table The name of the table this RPC is for.
   * @param row The name of the row this RPC is for.
   */
  HBaseRpc(final byte[] table, final byte[] key) {
    KeyValue.checkTable(table);
    KeyValue.checkKey(key);
    this.table = table;
    this.key = key;
  }

  /**
   * A timeout, in milliseconds, to set for this RPC. If the RPC cannot be 
   * sent and processed by HBase within this period then a 
   * {@link RpcTimedOutException} will be returned in the deferred.
   * <b>
   * If no timeout is set, then "hbase.rpc.timeout" will be used by default.
   * However if a value of "0" is supplied as the timeout, then the RPC will
   * not be timed out.
   * @param timeout The timeout in milliseconds.
   * @throws IllegalArgumentException if the value is less than zero
   * @since 1.7
   */
  public void setTimeout(final int timeout) {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout cannot be negative");
    }
    this.timeout = timeout;
  }
  
  /** An optional timeout for the RPC in milliseconds.
   * Note that the initial value is -1, meaning the RPC will use the default
   * value configured in hbase.rpc.timeout.
   * @return The timeout value in milliseconds.
   */
  public int getTimeout() {
    return timeout;
  }
  
  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  /**
   * Package private way of getting the name of the RPC method.
   * @param server_version What RPC protocol version the server is running.
   */
  abstract byte[] method(byte server_version);

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
   * A timeout task that is schedule as soon as the RPC is about to go out on
   * the wire. If this is called then the RPC has timed out and we return a
   * {@link RpcTimedOutException} to the user. This class is also responsible
   * for removing the RPC from the proper region client map and incrementing 
   * it's timeout counter.
   * <p>
   * If this run method throws an exception or the Deferred callback does, then
   * it will be caught and logged by Netty's timer executor.
   */
  private final class TimeoutTask implements TimerTask { 
    @Override
    public void run(final Timeout time_out) throws Exception {
      synchronized (HBaseRpc.this) {
        if (has_timedout) {
          throw new IllegalStateException(
              "This RPC has already timed out " + HBaseRpc.this);
        }
        has_timedout = true;
      }
      
      if (timeout_handle == null) {
        LOG.error("Received a timeout handle " + time_out 
            + " but this RPC did not have one " + this);
      }
      if (time_out != timeout_handle) {
        LOG.error("Receieved a timeout handle " + time_out + 
            " that doesn't match our own " + this);
      }
      if (region_client == null) {
        LOG.error("Somehow the region client was null when timing out RPC " 
            + this);
      } else {
        region_client.removeRpc(HBaseRpc.this, true);
      }
      
      callback(new RpcTimedOutException("RPC ID [" + rpc_id + 
          "] timed out waiting for response from HBase on region client [" + 
          region_client + " ] for over " + timeout + "ms"));
      timeout_task = null;
      timeout_handle = null;
    }
  }
  
  /**
   * Schedules the RPC with the HBaseClient rpc timeout timer with the given
   * timeout interval. If the timeout is set to zero then no timeout is 
   * scheduled.
   * If the RPC has already been timed out then we won't allow another attempt.
   * If the timer has shutdown (due to the client shutting down) then we 
   * don't do anything and let the region client expire the RPCs in it's queue.
   * @param region_client The region client that sent the RPC over the wire.
   * @throws IllegalStateException if the RPC has already timed out.
   */
  void enqueueTimeout(final RegionClient region_client) {
    // TODO - it's possible that we may actually retry a timed out RPC in which
    // case we want to allow this.
    if (has_timedout) {
      throw new IllegalStateException("This RPC has already timed out " + this);
    }
    if (timeout == -1) {
      timeout = region_client.getHBaseClient().getDefaultRpcTimeout();
    }
    if (timeout > 0) {
      this.region_client = region_client;
      if (timeout_task == null) {
        // we can re-use the task if this RPC is sent to another region server
        timeout_task = new TimeoutTask();
      }
      try {
        if (timeout_handle != null) {
          LOG.warn("RPC " + this + " had a previous timeout task");
        }
        timeout_handle = region_client.getHBaseClient().getRpcTimeoutTimer()
            .newTimeout(timeout_task, timeout, TimeUnit.MILLISECONDS);
      } catch (IllegalStateException e) {
        // This can happen if the timer fires just before shutdown()
        // is called from another thread, and due to how threads get
        // scheduled we tried to schedule a timeout after timer.stop().
        // Region clients will handle the RPCs on shutdown so we don't need 
        // to here.
        LOG.warn("Failed to schedule RPC timeout: " + this
                 + "  Ignore this if we're shutting down.", e);
        timeout_handle = null;
      }
    }
  }
  
  /** @return Whether or not this particular RPC has timed out and should not
   * be retried */
  final synchronized boolean hasTimedOut() {
    return has_timedout;
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
    if (timeout_handle != null) {
      timeout_handle.cancel();
      timeout_task = null;
      timeout_handle = null;
    }
    
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

  public String toString() {
    // Try to rightsize the buffer.
    final String method = new String(this.method((byte) 0));
    final StringBuilder buf = new StringBuilder(16 + method.length() + 2
      + 8 + (table == null ? 4 : table.length + 2)  // Assumption: ASCII => +2
      + 6 + (key == null ? 4 : key.length * 2)      // Assumption: binary => *2
      + 9 + (region == null ? 4 : region.stringSizeHint())
      + 10 + 1 + 1);
    buf.append("HBaseRpc(method=");
    buf.append(method);
    buf.append(", table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", region=");
    if (region == null) {
      buf.append("null");
    } else {
      region.toStringbuf(buf);
    }
    buf.append(", attempt=").append(attempt)
       .append(", timeout=").append(timeout)
       .append(", hasTimedout=").append(has_timedout);
    buf.append(')');
    return buf.toString();
  }

  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifiers A non-empty list of qualifiers or null.
   */
  final String toStringWithQualifiers(final String classname,
                                      final byte[] family,
                                      final byte[][] qualifiers) {
    return toStringWithQualifiers(classname, family, qualifiers, null, "");
  }


  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifiers A non-empty list of qualifiers or null.
   * @param values A non-empty list of values or null.
   * @param fields Additional fields to include in the output.
   */
  final String toStringWithQualifiers(final String classname,
                                      final byte[] family,
                                      final byte[][] qualifiers,
                                      final byte[][] values,
                                      final String fields) {
    final StringBuilder buf = new StringBuilder(256  // min=182
                                                + fields.length());
    buf.append(classname).append("(table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifiers=");
    Bytes.pretty(buf, qualifiers);
    if (values != null) {
      buf.append(", values=");
      Bytes.pretty(buf, values);
    }
    buf.append(fields);
    buf.append(", attempt=").append(attempt)
      .append(", region=");
    if (region == null) {
      buf.append("null");
    } else {
      region.toStringbuf(buf);
    }
    buf.append(')');
    return buf.toString();
  }

  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifier A possibly null column qualifier.
   * @param fields Additional fields to include in the output.
   */
  final String toStringWithQualifier(final String classname,
                                     final byte[] family,
                                     final byte[] qualifier,
                                     final String fields) {
    final StringBuilder buf = new StringBuilder(256  // min=181
                                                + fields.length());
    buf.append(classname).append("(table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(fields);
    buf.append(", attempt=").append(attempt)
      .append(", region=");
    if (region == null) {
      buf.append("null");
    } else {
      region.toStringbuf(buf);
    }
    buf.append(')');
    return buf.toString();
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
   * @param server_version What RPC protocol version the server is running.
   * @param max_payload_size A good approximation of the size of the payload.
   * The approximation must be an upper bound on the expected size of the
   * payload as trying to store more than {@code max_payload_size} bytes in
   * the buffer returned will cause an {@link ArrayIndexOutOfBoundsException}.
   */
  final ChannelBuffer newBuffer(final byte server_version,
                                final int max_payload_size) {
    // Add extra bytes for the RPC header:
    //   4 bytes: Payload size (always present, even in HBase 0.95+).
    //   4 bytes: RPC ID.
    //   2 bytes: Length of the method name.
    //   N bytes: The method name.
    final int header = 4 + 4 + 2 + method(server_version).length
      // Add extra bytes for the RPC header used in HBase 0.92 and above:
      //   1 byte:  RPC header version.
      //   8 bytes: Client version.  Yeah, 8 bytes, WTF seriously.
      //   4 bytes: Method fingerprint.
      + (server_version < RegionClient.SERVER_VERSION_092_OR_ABOVE ? 0
         : 1 + 8 + 4);
    // Note: with HBase 0.95 and up, the size of the protobuf header varies.
    // It is currently made of (see RequestHeader in RPC.proto):
    //   - uint32 callId: varint 1 to 5 bytes
    //   - RPCTInfo traceInfo: two uint64 varint so 4 to 20 bytes.
    //   - string methodName: varint length (1 byte) and method name.
    //   - bool requestParam: 1 byte
    //   - CellBlockMeta cellBlockMeta: one uint32 varint so 2 to 6 bytes.
    // Additionally each field costs an extra 1 byte, and there is a varint
    // prior to the header for the size of the header.  We don't set traceInfo
    // right now so that leaves us with 4 fields for a total maximum size of
    // 1 varint + 4 fields + 5 + 1 + N + 1 + 6 = 18 bytes max + method name.
    // Since for HBase 0.92 we reserve 19 bytes, we're good, we over-allocate
    // at most 1 bytes.  So the logic above doesn't need to change for 0.95+.
    final ChannelBuffer buf = ChannelBuffers.buffer(header + max_payload_size);
    buf.setIndex(0, header);  // Advance the writerIndex past the header.
    return buf;
  }

  /**
   * Serializes the given protobuf object into a Netty {@link ChannelBuffer}.
   * @param method The name of the method of the RPC we're going to send.
   * @param pb The protobuf to serialize.
   * @return A new channel buffer containing the serialized protobuf, with
   * enough free space at the beginning to tack on the RPC header.
   */
  static final ChannelBuffer toChannelBuffer(final byte[] method,
                                             final AbstractMessageLite pb) {
    final int pblen = pb.getSerializedSize();
    final int vlen = CodedOutputStream.computeRawVarint32Size(pblen);
    final byte[] buf = new byte[4 + 19 + method.length + vlen + pblen];
    try {
      final CodedOutputStream out = CodedOutputStream.newInstance(buf, 4 + 19 + method.length,
                                                                  vlen + pblen);
      out.writeRawVarint32(pblen);
      pb.writeTo(out);
      out.checkNoSpaceLeft();
    } catch (IOException e) {
      throw new RuntimeException("Should never happen", e);
    }
    return ChannelBuffers.wrappedBuffer(buf);
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
   * Serializes a `null' reference.
   * @param buf The buffer to write to.
   */
  static void writeHBaseNull(final ChannelBuffer buf) {
    buf.writeByte(14);  // Code type for `Writable'.
    buf.writeByte(17);  // Code type for `NullInstance'.
    buf.writeByte(14);  // Code type for `Writable'.
  }

  /**
   * Upper bound on the size of a byte array we de-serialize.
   * This is to prevent HBase from OOM'ing us, should there be a bug or
   * undetected corruption of an RPC on the network, which would turn a
   * an innocuous RPC into something allocating a ton of memory.
   * The Hadoop RPC protocol doesn't do any checksumming as they probably
   * assumed that TCP checksums would be sufficient (they're not).
   */
  static final long MAX_BYTE_ARRAY_MASK =
    0xFFFFFFFFF0000000L;  // => max = 256MB == 268435455

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

  /**
   * De-serializes a protobuf from the given buffer.
   * <p>
   * The protobuf is assumed to be prefixed by a varint indicating its size.
   * @param buf The buffer to de-serialize the protobuf from.
   * @param parser The protobuf parser to use for this type of protobuf.
   * @return An instance of the de-serialized type.
   * @throws InvalidResponseException if the buffer contained an invalid
   * protobuf that couldn't be de-serialized.
   */
  static <T> T readProtobuf(final ChannelBuffer buf, final Parser<T> parser) {
    final int length = HBaseRpc.readProtoBufVarint(buf);
    HBaseRpc.checkArrayLength(buf, length);
    final byte[] payload;
    final int offset;
    if (buf.hasArray()) {  // Zero copy.
      payload = buf.array();
      offset = buf.arrayOffset() + buf.readerIndex();
      buf.readerIndex(buf.readerIndex() + length);
    } else {  // We have to copy the entire payload out of the buffer :(
      payload = new byte[length];
      buf.readBytes(payload);
      offset = 0;
    }
    try {
      return parser.parseFrom(payload, offset, length);
    } catch (InvalidProtocolBufferException e) {
      final String msg = "Invalid RPC response: length=" + length
        + ", payload=" + Bytes.pretty(payload);
      LOG.error("Invalid RPC from buffer: " + buf);
      throw new InvalidResponseException(msg, e);
    }
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

  /**
   * Reads a 32-bit variable-length integer value as used in Protocol Buffers.
   * @param buf The buffer to read from.
   * @return The integer read.
   */
  static int readProtoBufVarint(final ChannelBuffer buf) {
    int result = buf.readByte();
    if (result >= 0) {
      return result;
    }
    result &= 0x7F;
    result |= buf.readByte() << 7;
    if (result >= 0) {
      return result;
    }
    result &= 0x3FFF;
    result |= buf.readByte() << 14;
    if (result >= 0) {
      return result;
    }
    result &= 0x1FFFFF;
    result |= buf.readByte() << 21;
    if (result >= 0) {
      return result;
    }
    result &= 0x0FFFFFFF;
    final byte b = buf.readByte();
    result |= b << 28;
    if (b >= 0) {
      return result;
    }
    throw new IllegalArgumentException("Not a 32 bit varint: " + result
                                       + " (5th byte: " + b + ")");
  }

}
