/*
 * Copyright (c) 2010, 2011  StumbleUpon, Inc.  All rights reserved.
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

/**
 * Atomically checks if a row/family/qualifier value match the expected value,
 * if it does, it adds the put.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class CheckAndPutRequest extends HBaseRpc
    implements HBaseRpc.HasTable, HBaseRpc.HasKey,
               HBaseRpc.HasFamily, HBaseRpc.HasQualifier, HBaseRpc.HasValue {

    private static final byte[] CHECKANDPUT = new byte[] {
        'c', 'h', 'e', 'c', 'k',
        'A', 'n', 'd',
        'P', 'u', 't'
    };

    private final byte[] family;
    private final byte[] qualifier;
    private final byte[] value;
    private final PutRequest put;

    /**
     * Constructor.
     * <strong>These byte arrays will NOT be copied.</strong>
     * @param table
     *          The non-empty name of the table to use.
     * @param key
     *          The row key of the value to check.
     * @param family
     *          The column family of the value to check.
     * @param qualifier
     *          The column qualifier of the value to check.
     * @param value
     *          The expected value of a row/family/qualifier to check.
     * @param put
     *          Put request to execute if value matches.
     */
    public CheckAndPutRequest(final byte[] table,
                              final byte[] key,
                              final byte[] family,
                              final byte[] qualifier,
                              final byte[] value,
                              final PutRequest put) {
        super(CHECKANDPUT, table, key);
        KeyValue.checkFamily(family);
        KeyValue.checkQualifier(qualifier);
        KeyValue.checkValue(value);
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.put = put;
    }

    @Override
    public byte[] table() {
        return table;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte[] family() {
        return family;
    }

    @Override
    public byte[] qualifier() {
        return qualifier;
    }

    @Override
    public byte[] value() {
        return value;
    }

    /**
     * Get putRequest to execute if value matches.
     *
     * @return put request.
     */
    PutRequest putRequest() {
        return put;
    }

    // ---------------------- //
    // Package private stuff. //
    // ---------------------- //

    private int predictSerializedSize() {
        int size = 0;
        size += 4; // int: Number of parameters.
        size += 1; // byte: Type of the 1st parameter.
        size += 3; // vint: region name length (3 bytes => max length = 32768).
        size += region.name().length; // The region name.

        size += 1; // byte: Type of the 2nd parameter.
        size += 3; // vint: check row key length (3 bytes => max length =
                   // 32768).
        size += key.length; // The check row key.

        size += 1; // byte: Type of the 3rd parameter.
        size += 3; // vint: family length (3 bytes => max length = 32768).
        size += family.length; // The check family name.

        size += 1; // byte: Type of the 4th parameter.
        size += 3; // vint: check qualifier length (3 bytes => max length =
                   // 32768).
        size += qualifier.length; // The check qualifier key.

        size += 1; // byte: Type of the 5th parameter.
        size += 3; // vint: check data length (3 bytes => max length = 32768).
        size += value.length; // The check data.

        size += 1; // byte: Type of the 6th parameter.
        size += 1; // byte: Type again (see HBASE-2877).

        size += 1; // byte: Version of Put.
        size += 3; // vint: row key length (3 bytes => max length = 32768).
        size += put.key.length; // The row key.
        size += 8; // long: Timestamp.
        size += 8; // long: Lock ID.
        size += 1; // bool: Whether or not to write to the WAL.
        size += 4; // int: Number of families for which we have edits.

        size += 1; // vint: Family length (guaranteed on 1 byte).
        size += put.family().length; // The family.
        size += 4; // int: Number of KeyValues that follow.
        size += 4; // int: Total number of bytes for all those KeyValues.

        size += 4; // int: Key + value length.
        size += 4; // int: Key length.
        size += 4; // int: Value length.
        size += 2; // short: Row length.
        size += put.key.length; // The row key (again!).
        size += 1; // byte: Family length.
        size += put.family().length; // The family (again!).
        size += put.qualifier().length; // The qualifier.
        size += 8; // long: Timestamp (again!).
        size += 1; // byte: Type of edit.
        size += put.value().length;

        return size;
    }

    @Override
    ChannelBuffer serialize(byte unused_server_version) {
        final ChannelBuffer buf = newBuffer(predictSerializedSize());
        buf.writeInt(6); // Number of parameters.

        // 1st param: byte array containing region name
        writeHBaseByteArray(buf, region.name());

        // 2nd param: byte array row to check
        writeHBaseByteArray(buf, key);

        // 3rd param: byte array column family
        writeHBaseByteArray(buf, family);

        // 4th param: byte array qualifier
        writeHBaseByteArray(buf, qualifier);

        // 5th param: byte array check value
        writeHBaseByteArray(buf, value);

        // 6th param: Put object
        buf.writeByte(35); // Code for a `Put' parameter.
        buf.writeByte(35); // Code again (see HBASE-2877).
        buf.writeByte(1); // Put#PUT_VERSION. Undocumented versioning of Put.
        writeByteArray(buf, put.key); // The row key.

        // Timestamp. We always set it to Long.MAX_VALUE, which means "unset".
        // The RegionServer will set it for us, right before writing to the WAL
        // (or to the Memstore if we're not using the WAL).
        buf.writeLong(Long.MAX_VALUE);

        buf.writeLong(put.lockid()); // Lock ID.
        buf.writeByte(put.durable() ? 0x01 : 0x00); // Whether or not to use the
                                                    // WAL.

        buf.writeInt(1); // Number of families that follow.
        writeByteArray(buf, put.family()); // The column family.

        buf.writeInt(1); // Number of "KeyValues" that follow.
        buf.writeInt(put.kv().predictSerializedSize()); // Size of the KV that follows.
        put.kv().serialize(buf, KeyValue.PUT);
        return buf;
    }

}
