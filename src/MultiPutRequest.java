/*
 * Copyright 2010 StumbleUpon, Inc.
 * This file is part of Async HBase.
 * Async HBase is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hbase.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.netty.buffer.ChannelBuffer;

import static org.hbase.async.Bytes.ByteMap;

/**
 * Package-private class to group {@link PutRequest} into a multi-put.
 * <p>
 * This RPC is guaranteed to be sent atomically (but HBase doesn't guarantee
 * that it will apply it atomically).
 */
final class MultiPutRequest extends HBaseRpc {

  /*
   * TODO(tsuna): This class is totally insane.  The deeply nested generics
   * (map-of-maps-of-maps-of-list-of-edits) hurt my head and are hard to read
   * and probably not space / time efficient.  I did it this way to stay close
   * to the "wire format", which is ridiculously complicated and inefficient.
   *
   * Maybe we should just stuff all the PutRequest together in an array and
   * then sort it kinda like what PutRequest.ROW_TABLE_CMP does, and then
   * walk the sorted array as we do the serialization kinda like what
   * HBaseClient#doPut does.  This would probably be both faster (no need to
   * rebalance the RB-trees of TreeMaps and do O(n log n) operations) and take
   * less memory (no crazy nesting of maps which create several Entry objects
   * per edit).
   */

  private static final byte[] MULTI_PUT = new byte[] {
    'm', 'u', 'l', 't', 'i', 'P', 'u', 't'
  };

  /**
   * Whether or not the server should use its WAL (Write Ahead Log).
   * Setting this to {@code false} makes the operation complete faster
   * (sometimes significantly faster) but that's in exchange of data
   * durability: if the server crashes before its next flush, this edit
   * will be lost.  This is typically good only for batch imports where
   * in case of a server failure, the whole import can be done again.
   */
  private final boolean wal;

  /** ID of the explicit {@link RowLock} to use for these edits, if any.  */
  private final long lockid;

  /** How many times have we attempted to retry those edits?  */
  private final byte attempt;

  /**
   * Maps a region name to all the edits for that region.
   * This makes my head hurt too.  This unnecessary complication is inherent
   * to how the MultiPut has to be written to the wire.  Basically, here's
   * how the maps are nested:
   * <pre>  region --> row key --> family --> list of (qualifier, value)</pre>
   * Get ready for some serious generic nesting.
   */
  private final ByteMap<ByteMap<ByteMap<ArrayList<Cell>>>> region2edits =
    new ByteMap<ByteMap<ByteMap<ArrayList<Cell>>>>();  // OMG

  /** How many edits are in the crazy map above.  */
  private int size;

  /** A (qualifier, value) pair, which thus uniquely identifies a cell.  */
  private static final class Cell {
    final byte[] qualifier;
    final byte[] value;

    public Cell(final byte[] qualifier, final byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }
  }

  /**
   * Constructor.
   */
  public MultiPutRequest(final boolean durable) {
    super(MULTI_PUT);
    this.wal = durable;
    this.lockid = RowLock.NO_LOCK;
    this.attempt = 0;
  }

  MultiPutRequest(final boolean durable, final long lockid) {
    super(MULTI_PUT);
    this.wal = durable;
    this.lockid = lockid;
    this.attempt = 0;
  }

  private MultiPutRequest(final boolean durable,
                          final long lockid,
                          final byte attempt) {
    super(MULTI_PUT);
    this.wal = durable;
    this.lockid = lockid;
    this.attempt = attempt;
  }

  /** Returns the number of edits in this multi-put.  */
  public int size() {
    return size;
  }

  /**
   * Adds a number of "put" requests in this multi-put.
   * @param requests A list of puts that are guaranteed to be sent
   * out together at the same time, in the same single RPC.
   * @param region_name The name of the region all those edits belongs to.
   */
  void add(final List<PutRequest> requests, final byte[] region_name) {
    ByteMap<ByteMap<ArrayList<Cell>>> region = region2edits.get(region_name);
    if (region == null) {
      region = new ByteMap<ByteMap<ArrayList<Cell>>>();
      region2edits.put(region_name, region);
    }

    for (final PutRequest request : requests) {
      ByteMap<ArrayList<Cell>> put = region.get(request.key());
      if (put == null) {
        put = new ByteMap<ArrayList<Cell>>();
        region.put(request.key(), put);
      }

      ArrayList<Cell> cells = put.get(request.family());
      if (cells == null) {
        cells = new ArrayList<Cell>(1);
        put.put(request.family(), cells);
      }
      cells.add(new Cell(request.qualifier(), request.value()));
      size++;
    }
  }

  /**
   * Builds an RPC of edits that need to be retried.
   * @param request The original request that failed (partially or entirely).
   * @param failures A map of region name to the index of the first edit that
   * failed.
   * @return A multi-put that contains all the edits after (and including) the
   * index of the first failure, for each region that had failures.
   */
  static MultiPutRequest retry(MultiPutRequest request,
                               final ByteMap<Integer> failures) {
    final MultiPutRequest retry =
      new MultiPutRequest(request.wal, request.lockid,
                          (byte) (request.attempt + 1));
    final ByteMap<ByteMap<ByteMap<ArrayList<Cell>>>> region2edits =  // OMG
      request.region2edits;
    request = null;

    for (final Map.Entry<byte[], Integer> fail : failures) {
      final byte[] region_name = fail.getKey();
      final int failed_index = fail.getValue();
      // If failed_index is 0, all edits for this region have failed, so let's
      // just re-use the same object instead of copying all of that crazy map.
      final ByteMap<ByteMap<ArrayList<Cell>>> region =
        failed_index == 0 ? region2edits.get(region_name)
        : new ByteMap<ByteMap<ArrayList<Cell>>>();
      retry.region2edits.put(region_name, region);
      if (failed_index == 0) {
        continue;
      }

      // Iterate over each individual edit for this region, until we reach the
      // index of the edit that failed.  All edits after (and including) that
      // index are then added to the retry RPC.
      int i = 0;
      for (final Map.Entry<byte[], ByteMap<ArrayList<Cell>>> put
           : region2edits.get(region_name)) {
        final ByteMap<ArrayList<Cell>> families = put.getValue();
        for (Map.Entry<byte[], ArrayList<Cell>> edits : families) {
          final ArrayList<Cell> cells = edits.getValue();
          for (final Cell cell : cells) {
            i++;
            if (i < failed_index) {
              continue;
            }
            // Retry this edit.
            ByteMap<ArrayList<Cell>> retryput = region.get(put.getKey());
            if (retryput == null) {
              retryput = new ByteMap<ArrayList<Cell>>();
              region.put(put.getKey(), retryput);
            }
            ArrayList<Cell> retrycells = retryput.get(edits.getKey());
            if (retrycells == null) {
              retrycells = new ArrayList<Cell>(1);
              retryput.put(edits.getKey(), retrycells);
            }
            retrycells.add(cell);
          }
        }
      }
    }

    if (retry.attempt >= 5) {  // XXX don't hardcode
      // We're going to throw away all our hard work above, but at least we'll
      // log only the edits that failed.
      // XXX use a more specific exception type..?
      throw new NonRecoverableException("Edits failed: " + retry);
    }
    return retry;
  }

  /**
   * Returns a set of all the region names this multi-put request involves.
   */
  Set<byte[]> regions() {
    return region2edits.keySet();
  }

  /**
   * Transforms this multi-put back into a list of individual puts.
   */
  public ArrayList<PutRequest> toPuts() {
    int nedits = 0;
    for (final Map.Entry<byte[], ByteMap<ByteMap<ArrayList<Cell>>>> region
         : region2edits) {
      nedits += region.getValue().size();  // TreeMap's size() is in O(1).
    }

    final ArrayList<PutRequest> puts = new ArrayList<PutRequest>(nedits);
    for (final Map.Entry<byte[], ByteMap<ByteMap<ArrayList<Cell>>>> region
         : region2edits) {
      final byte[] table = RegionInfo.tableFromRegionName(region.getKey());
      for (Map.Entry<byte[], ByteMap<ArrayList<Cell>>> put : region.getValue())
      {
        final byte[] key = put.getKey();
        for (Map.Entry<byte[], ArrayList<Cell>> edits : put.getValue()) {
          final byte[] family = edits.getKey();
          for (final Cell cell : edits.getValue()) {
            // TODO(tsuna): opportunistically de-dup byte arrays?
            puts.add(new PutRequest(table, key, family,
                                    cell.qualifier, cell.value));
          }
        }
      }
    }

    return puts;
  }

  /**
   * Predicts a lower bound on the serialized size of this RPC.
   * This is to avoid using a dynamic buffer, to avoid re-sizing the buffer.
   * Since we use a static buffer, if the prediction is wrong and turns out
   * to be less than what we need, there will be an exception which will
   * prevent the RPC from being serialized.  That'd be a severe bug.
   */
  private int predictSerializedSize() {
    int size = 0;
    size += 4;  // int:  Number of parameters.
    size += 1;  // byte: Type of the 1st parameter.
    size += 1;  // byte: Type again (see HBASE-2877).
    size += 4;  // int:  How many regions do we want to edit?

    for (final Map.Entry<byte[], ByteMap<ByteMap<ArrayList<Cell>>>> region
         : region2edits) {
      size += 3;  // vint: region name length (3 bytes => max length = 32768).
      size += region.getKey().length;  // The region name.
      size += 4;  // int:  How many edits for this region.

      for (Map.Entry<byte[], ByteMap<ArrayList<Cell>>> put : region.getValue())
      {
        final int key_length = put.getKey().length;
        final ByteMap<ArrayList<Cell>> families = put.getValue();
        put = null;

        size += 1;  // byte: Version of Put.
        size += 3;  // vint: row key length (3 bytes => max length = 32768).
        size += key_length;  // The row key.
        size += 8;  // long: Timestamp.
        size += 8;  // long: Lock ID.
        size += 1;  // bool: Whether or not to write to the WAL.
        size += 4;  // int:  Number of families for which we have edits.

        for (Map.Entry<byte[], ArrayList<Cell>> edits : families) {
          final int family_length = edits.getKey().length;
          final ArrayList<Cell> cells = edits.getValue();
          edits = null;

          size += 1;  // vint: Family length (guaranteed on 1 byte).
          size += family_length;  // The family.
          size += 4;  // int:  Number of KeyValues that follow.
          size += 4;  // int:  Total number of bytes for all those KeyValues.
          for (final Cell cell : cells) {
            size += 4;  // int:   Key + value length.
            size += 4;  // int:   Key length.
            size += 4;  // int:   Value length.
            size += 2;  // short: Row length.
            size += key_length;             // The row key (again!).
            size += 1;  // byte:  Family length.
            size += family_length;          // The family (again!).
            size += cell.qualifier.length;  // The qualifier.
            size += 8;  // long:  Timestamp (again!).
            size += 1;  // byte:  Type of edit.
            size += cell.value.length;
          }
        }
      }
    }
    return size;
  }

  /** Serializes this request.  */
  ChannelBuffer serialize() {
    final ChannelBuffer buf = newBuffer(predictSerializedSize());
    buf.writeInt(1);  // Number of parameters.

    // 1st and only param: a MultiPut object.
    buf.writeByte(57);   // Code for a `MultiPut' parameter.
    buf.writeByte(57);   // Code again (see HBASE-2877).
    buf.writeInt(region2edits.size());  // How many regions do we want to edit?

    for (final Map.Entry<byte[], ByteMap<ByteMap<ArrayList<Cell>>>> region
         : region2edits) {
      writeByteArray(buf, region.getKey());  // The region name.

      // Number of edits for this region that follow.
      buf.writeInt(region.getValue().size());

      for (Map.Entry<byte[], ByteMap<ArrayList<Cell>>> put : region.getValue())
      {
        buf.writeByte(1);    // Undocumented versioning of Put.
        final byte[] key = put.getKey();
        writeByteArray(buf, key);  // The row key.

        final ByteMap<ArrayList<Cell>> families = put.getValue();
        put = null;

        // Timestamp.  We always set it to Long.MAX_VALUE, which means "unset".
        // The RegionServer will set it for us, right before writing to the WAL
        // (or to the Memstore if we're not using the WAL).
        buf.writeLong(Long.MAX_VALUE);

        buf.writeLong(lockid);             // Lock ID.
        buf.writeByte(wal ? 0x01 : 0x00);  // Whether or not to use the WAL.
        buf.writeInt(families.size());  // Number of families that follow.
        for (Map.Entry<byte[], ArrayList<Cell>> edits : families) {
          final byte[] family = edits.getKey();
          final ArrayList<Cell> cells = edits.getValue();
          edits = null;
          writeByteArray(buf, family);  // The column family.
          buf.writeInt(cells.size());   // Number of "KeyValues" that follow.

          // Total number of bytes taken by those "KeyValues".
          int size = 0;
          final int size_pos = buf.writerIndex();
          buf.writeInt(0);  // We'll monkey patch this later.

          for (final Cell cell : cells) {
            final int key_length = (2 + key.length + 1 + family.length
                                    + cell.qualifier.length + 8 + 1);
            size += 4 + 4 + key_length + cell.value.length;
            // Write the length of the whole KeyValue (this is so useless...).
            buf.writeInt(4 + 4 + key_length + cell.value.length);
            buf.writeInt(key_length);             // Key length.
            buf.writeInt(cell.value.length);      // Value length.

            // Then the whole key.
            buf.writeShort(key.length);      // Row length.
            buf.writeBytes(key);             // The row key (again!).
            buf.writeByte((byte) family.length);  // Family length.
            buf.writeBytes(family);               // Write the family (again!).
            buf.writeBytes(cell.qualifier);       // The qualifier.
            buf.writeLong(Long.MAX_VALUE);        // The timestamp (again!).
            buf.writeByte(0x04);                  // Type of edit (4 = Put).

            buf.writeBytes(cell.value);           // Finally, the value.
          }
          buf.setInt(size_pos, size);
        }
      }
    }  // Yay, we made it!

    return buf;
  }

}
