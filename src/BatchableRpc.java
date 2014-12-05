/*
 * Copyright (C) 2012-2012  The Async HBase Authors.  All rights reserved.
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

import org.hbase.async.generated.ClientPB.MutationProto;

/**
 * An intermediate abstract class for all RPC requests that can be batched.
 * <p>
 * This class is internal only and doesn't provide any user-facing API other
 * than guaranteeing that the RPC has a family and a timestamp.
 */
abstract class BatchableRpc extends HBaseRpc
  implements HBaseRpc.HasFamily, HBaseRpc.HasTimestamp {

  // Attributes should have `protected' visibility, but doing so exposes
  // them as part of the public API and in Javadoc, which we don't want.
  // So instead we make them package-private so that subclasses can still
  // access them directly.

  /** Family affected by this RPC.  */
  /*protected*/ final byte[] family;

  /** The timestamp to use for {@link KeyValue}s of this RPC.  */
  /*protected*/ final long timestamp;

  /**
   * Explicit row lock to use, if any.
   * @see RowLock
   */
  /*protected*/ final long lockid;

  /**
   * Whether or not this batchable RPC can be buffered on the client side.
   * Please call {@link #canBuffer} to check if this RPC can be buffered,
   * don't test this field directly.
   */
  /*protected*/ boolean bufferable = true;

  /**
   * Whether or not the RegionServer must write to its WAL (Write Ahead Log).
   */
  /*protected*/ boolean durable = true;

  /**
   * Package private constructor.
   * @param table The name of the table this RPC is for.
   * @param row The name of the row this RPC is for.
   * @param family The column family to edit in that table.  Subclass must
   * validate, this class doesn't perform any validation on the family.
   * @param timestamp The timestamp to use for {@link KeyValue}s of this RPC.
   * @param lockid Explicit row lock to use, or {@link RowLock#NO_LOCK}.
   */
  BatchableRpc(final byte[] table,
               final byte[] key, final byte[] family,
               final long timestamp, final long lockid) {
    super(table, key);
    this.family = family;
    this.timestamp = timestamp;
    this.lockid = lockid;
  }

  /**
   * Sets whether or not this RPC is can be buffered on the client side.
   * The default is {@code true}.
   * <p>
   * Setting this to {@code false} bypasses the client-side buffering, which
   * is used to send RPCs in batches for greater throughput, and causes this
   * RPC to be sent directly to the server.
   * @param bufferable Whether or not this RPC can be buffered (i.e. delayed)
   * before being sent out to HBase.
   * @see HBaseClient#setFlushInterval
   */
  public final void setBufferable(final boolean bufferable) {
    this.bufferable = bufferable;
  }

  /**
   * Changes the durability setting of this edit.
   * The default is {@code true}.
   * Make sure you've read and understood the
   * <a href="HBaseClient.html#durability">data durability</a> section before
   * setting this to {@code false}.
   * @param durable Whether or not this edit should be stored with data
   * durability guarantee.
   */
  public final void setDurable(final boolean durable) {
    this.durable = durable;
  }

  @Override
  public final byte[] family() {
    return family;
  }

  @Override
  public final long timestamp() {
    return timestamp;
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  /** Returns whether or not it's OK to buffer this RPC on the client side. */
  final boolean canBuffer() {
    // Don't buffer edits that have a row-lock, we want those to
    // complete ASAP so as to not hold the lock for too long.
    return lockid == RowLock.NO_LOCK && bufferable;
  }

  /**
   * Transforms this edit into a MutationProto for HBase 0.95+.
   */
  abstract MutationProto toMutationProto();

  // ----------------------------------------------- //
  // Serialization helpers for HBase 0.94 and before //
  // ----------------------------------------------- //

  /**
   * Serialization version for this RPC.
   * Only used when this RPC is serialized as part of a {@link MultiAction}.
   * @param server_version What RPC protocol version the server is running.
   */
  abstract byte version(final byte server_version);

  /** HBase code type for this kind of serialized RPC.  */
  abstract byte code();

  /**
   * How many {@link KeyValue}s will be serialized by {@link #serializePayload}.
   */
  abstract int numKeyValues();

  /**
   * An estimate of the number of bytes needed for {@link #serializePayload}.
   * The estimate is conservative.
   */
  abstract int payloadSize();

  /**
   * Serialize the part of this RPC for a {@link MultiAction}.
   */
  abstract void serializePayload(final ChannelBuffer buf);

}
