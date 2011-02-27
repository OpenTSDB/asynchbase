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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.util.CharsetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import static org.hbase.async.HBaseClient.EMPTY_ARRAY;

/**
 * Creates a scanner to read data sequentially from HBase.
 * <p>
 * This class is <strong>not synchronized</strong> as it's expected to be
 * used from a single thread at a time.  It's rarely (if ever?) useful to
 * scan concurrently from a shared scanner using multiple threads.  If you
 * want to optimize large table scans using extra parallelism, create a few
 * scanners and give each of them a partition of the table to scan.  Or use
 * MapReduce.
 * <p>
 * Unlike HBase's traditional client, there's no method in this class to
 * explicitly open the scanner.  It will open itself automatically when you
 * start scanning by calling {@link #nextRows()}.  Also, the scanner will
 * automatically call {@link #close} when it reaches the end key.  If, however,
 * you would like to stop scanning <i>before reaching the end key</i>, you
 * <b>must</b> call {@link #close} before disposing of the scanner.  Note that
 * it's always safe to call {@link #close} on a scanner.
 * <p>
 * If you keep your scanner open and idle for too long, the RegionServer will
 * close the scanner automatically for you after a timeout configured on the
 * server side.  When this happens, you'll get an
 * {@link UnknownScannerException} when you attempt to use the scanner again.
 * Also, if you scan too slowly (e.g. you take a long time between each call
 * to {@link #nextRows()}), you may prevent HBase from splitting the region if
 * the region is also actively being written to while you scan.  For heavy
 * processing you should consider using MapReduce.
 * <p>
 * A {@code Scanner} is not re-usable.  Should you want to scan the same rows
 * or the same table again, you must create a new one.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class Scanner {

  private static final Logger LOG = LoggerFactory.getLogger(Scanner.class);

  /**
   * The default maximum number of {@link KeyValue}s the server is allowed
   * to return in a single RPC response to a {@link Scanner}.
   * <p>
   * This default value is exposed only as a hint but the value itself
   * is not part of the API and is subject to change without notice.
   * @see #setMaxNumKeyValues
   */
  public static final int DEFAULT_MAX_NUM_KVS = 4096;

  /**
   * The default maximum number of rows to scan per RPC.
   * <p>
   * This default value is exposed only as a hint but the value itself
   * is not part of the API and is subject to change without notice.
   * @see #setMaxNumRows
   */
  public static final int DEFAULT_MAX_NUM_ROWS = 128;

  /** Special reference we use to indicate we're done scanning.  */
  private static final RegionInfo DONE =
    new RegionInfo(EMPTY_ARRAY, EMPTY_ARRAY, EMPTY_ARRAY);

  private final HBaseClient client;
  private final byte[] table;

  /**
   * The key to start scanning from.  An empty array means "start from the
   * first key in the table".  This key is updated each time we move on to
   * another row, so that in the event of a failure, we know what was the
   * last key previously returned.  Note that this doesn't entail that the
   * full row was returned.  Depending on the failure, we may not know if
   * the last key returned was only a subset of a row or a full row, so it
   * may not be possible to gracefully recover from certain errors without
   * re-scanning and re-returning the same data twice.
   */
  private byte[] start_key = EMPTY_ARRAY;

  /**
   * The last key to scan up to (exclusive).
   * An empty array means "scan until the last key in the table".
   */
  private byte[] stop_key = EMPTY_ARRAY;

  private byte[] family;     // TODO(tsuna): Handle multiple families?
  private byte[] qualifier;  // TODO(tsuna): Handle multiple qualifiers?

  /** Pre-serialized filter to apply on the scanner.  */
  private byte[] filter;

  /** @see #setServerBlockCache  */
  private boolean populate_blockcache = true;

  /**
   * Maximum number of rows to fetch at a time.
   * @see #setMaxNumRows
   */
  private int max_num_rows = DEFAULT_MAX_NUM_ROWS;

  /**
   * Maximum number of KeyValues to fetch at a time.
   * @see #setMaxNumKeyValues
   */
  private int max_num_kvs = DEFAULT_MAX_NUM_KVS;

  /**
   * The region currently being scanned.
   * If null, we haven't started scanning.
   * If == DONE, then we're done scanning.
   * Otherwise it contains a proper region name, and we're currently scanning.
   */
  private RegionInfo region;

  /**
   * This is the scanner ID we got from the RegionServer.
   * It's generated randomly so any {@code long} value is possible.
   */
  private long scanner_id;

  /**
   * Request object we re-use to avoid generating too much garbage.
   * @see #getNextRowsRequest
   */
  private GetNextRowsRequest get_next_rows_request;

  /**
   * Constructor.
   * <strong>This byte array will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   */
  Scanner(final HBaseClient client, final byte[] table) {
    KeyValue.checkTable(table);
    this.client = client;
    this.table = table;
  }

  /**
   * Returns the row key this scanner is currently at.
   * <strong>Do not modify the byte array returned.</strong>
   */
  public byte[] getCurrentKey() {
    return start_key;
  }

  /**
   * Specifies from which row key to start scanning (inclusive).
   * @param start_key The row key to start scanning from.  If you don't invoke
   * this method, scanning will begin from the first row key in the table.
   * <strong>This byte array will NOT be copied.</strong>
   * @throws IllegalStateException if scanning already started.
   */
  public void setStartKey(final byte[] start_key) {
    KeyValue.checkKey(start_key);
    checkScanningNotStarted();
    this.start_key = start_key;
  }

  /**
   * Specifies from which row key to start scanning (inclusive).
   * @see #setStartKey(byte[])
   * @throws IllegalStateException if scanning already started.
   */
  public void setStartKey(final String start_key) {
    setStartKey(start_key.getBytes());
  }

  /**
   * Specifies up to which row key to scan (exclusive).
   * @param stop_key The row key to scan up to.  If you don't invoke
   * this method, or if the array is empty ({@code stop_key.length == 0}),
   * every row up to and including the last one will be scanned.
   * <strong>This byte array will NOT be copied.</strong>
   * @throws IllegalStateException if scanning already started.
   */
  public void setStopKey(final byte[] stop_key) {
    KeyValue.checkKey(stop_key);
    checkScanningNotStarted();
    this.stop_key = stop_key;
  }

  /**
   * Specifies up to which row key to scan (exclusive).
   * @see #setStopKey(byte[])
   * @throws IllegalStateException if scanning already started.
   */
  public void setStopKey(final String stop_key) {
    setStopKey(stop_key.getBytes());
  }

  /**
   * Specifies a particular column family to scan.
   * @param family The column family.
   * <strong>This byte array will NOT be copied.</strong>
   * @throws IllegalStateException if scanning already started.
   */
  public void setFamily(final byte[] family) {
    KeyValue.checkFamily(family);
    checkScanningNotStarted();
    this.family = family;
  }

  /** Specifies a particular column family to scan.  */
  public void setFamily(final String family) {
    setFamily(family.getBytes());
  }

  /**
   * Specifies a particular column qualifier to scan.
   * @param qualifier The column qualifier.
   * <strong>This byte array will NOT be copied.</strong>
   * @throws IllegalStateException if scanning already started.
   */
  public void setQualifier(final byte[] qualifier) {
    KeyValue.checkQualifier(qualifier);
    checkScanningNotStarted();
    this.qualifier = qualifier;
  }

  /** Specifies a particular column qualifier to scan.  */
  public void setQualifier(final String qualifier) {
    setQualifier(qualifier.getBytes());
  }

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This is equivalent to calling {@link #setKeyRegexp(String, Charset)}
   * with the ISO-8859-1 charset in argument.
   * @param regexp The regular expression with which to filter the row keys.
   */
  public void setKeyRegexp(final String regexp) {
    setKeyRegexp(regexp, CharsetUtil.ISO_8859_1);
  }

  private static final byte[] ROWFILTER = Bytes.ISO88591("org.apache.hadoop"
    + ".hbase.filter.RowFilter");
  private static final byte[] REGEXSTRINGCOMPARATOR = Bytes.ISO88591("org.apache.hadoop"
    + ".hbase.filter.RegexStringComparator");
  private static final byte[] EQUAL = new byte[] { 'E', 'Q', 'U', 'A', 'L' };

  /**
   * Sets a regular expression to filter results based on the row key.
   * <p>
   * This regular expression will be applied on the server-side, on the row
   * key.  Rows for which the key doesn't match will not be returned to this
   * scanner, which can be useful to carefully select which rows are matched
   * when you can't just do a prefix match, and cut down the amount of data
   * transfered on the network.
   * <p>
   * Don't use an expensive regular expression, because Java's implementation
   * uses backtracking and matching will happen on the server side, potentially
   * on many many row keys.  See <a href="su.pr/2xaY8D">Regular Expression
   * Matching Can Be Simple And Fast</a> for more details on regular expression
   * performance (or lack thereof) and what "backtracking" means.
   * @param regexp The regular expression with which to filter the row keys.
   * @param charset The charset used to decode the bytes of the row key into a
   * string.  The RegionServer must support this charset, otherwise it will
   * unexpectedly close the connection the first time you attempt to use this
   * scanner.
   */
  public void setKeyRegexp(final String regexp, final Charset charset) {
    final byte[] regex = Bytes.UTF8(regexp);
    final byte[] chars = Bytes.UTF8(charset.name());
    filter = new byte[(1 + 40 + 2 + 5 + 1 + 1 + 1 + 52
                       + 2 + regex.length + 2 + chars.length)];
    final ChannelBuffer buf = ChannelBuffers.wrappedBuffer(filter);
    buf.clear();  // Set the writerIndex to 0.

    buf.writeByte((byte) ROWFILTER.length);                     // 1
    buf.writeBytes(ROWFILTER);                                  // 40
    // writeUTF of the comparison operator
    buf.writeShort(5);                                          // 2
    buf.writeBytes(EQUAL);                                      // 5
    // The comparator: a RegexStringComparator
    buf.writeByte(53);  // Code for WritableByteArrayComparable // 1
    buf.writeByte(0);   // Code for "this has no code".         // 1
    buf.writeByte((byte) REGEXSTRINGCOMPARATOR.length);         // 1
    buf.writeBytes(REGEXSTRINGCOMPARATOR);                      // 52
    // writeUTF the regexp
    buf.writeShort(regex.length);                               // 2
    buf.writeBytes(regex);                                      // regex.length
    // writeUTF the charset
    buf.writeShort(chars.length);                               // 2
    buf.writeBytes(chars);                                      // chars.length
  }

  /**
   * Sets whether or not the server should populate its block cache.
   * @param populate_blockcache if {@code false}, the block cache of the server
   * will not be populated as the rows are being scanned.  If {@code true} (the
   * default), the blocks loaded by the server in order to feed the scanner
   * <em>may</em> be added to the block cache, which will make subsequent read
   * accesses to the same rows and other neighbouring rows faster.  Whether or
   * not blocks will be added to the cache depend on the table's configuration.
   * <p>
   * If you scan a sequence of keys that is unlikely to be accessed again in
   * the near future, you can help the server improve its cache efficiency by
   * setting this to {@code false}.
   * @throws IllegalStateException if scanning already started.
   */
  public void setServerBlockCache(final boolean populate_blockcache) {
    checkScanningNotStarted();
    this.populate_blockcache = populate_blockcache;
  }

  /**
   * Sets the maximum number of rows to scan per RPC (for better performance).
   * <p>
   * Every time {@link #nextRows()} is invoked, up to this number of rows may
   * be returned.  The default value is {@link #DEFAULT_MAX_NUM_ROWS}.
   * <p>
   * <b>This knob has a high performance impact.</b>  If it's too low, you'll
   * do too many network round-trips, if it's too high, you'll spend too much
   * time and memory handling large amounts of data.  The right value depends
   * on the size of the rows you're retrieving.
   * <p>
   * If you know you're going to be scanning lots of small rows (few cells, and
   * each cell doesn't store a lot of data), you can get better performance by
   * scanning more rows by RPC.  You probably always want to retrieve at least
   * a few dozen kilobytes per call.
   * <p>
   * If you want to err on the safe side, it's better to use a value that's a
   * bit too high rather than a bit too low.  Avoid extreme values (such as 1
   * or 1024) unless you know what you're doing.
   * <p>
   * Note that unlike many other methods, it's fine to change this value while
   * scanning.  Changing it will take affect all the subsequent RPCs issued.
   * This can be useful you want to dynamically adjust how much data you want
   * to receive at once (provided that you can estimate the size of your rows).
   * @param max_num_rows A strictly positive integer.
   * @throws IllegalArgumentException if the argument is zero or negative.
   */
  public void setMaxNumRows(final int max_num_rows) {
    if (max_num_rows <= 0) {
      throw new IllegalArgumentException("zero or negative argument: "
                                         + max_num_rows);
    }
    this.max_num_rows = max_num_rows;
  }

  /**
   * Sets the maximum number of {@link KeyValue}s the server is allowed to
   * return in a single RPC response.
   * <p>
   * If you're dealing with wide rows, in which you have many cells, you may
   * want to limit the number of cells ({@code KeyValue}s) that the server
   * returns in a single RPC response.
   * <p>
   * The default is {@link #DEFAULT_MAX_NUM_KVS}, unlike in HBase's client
   * where the default is {@code -1}.  If you set this to a negative value,
   * the server will always return full rows, no matter how wide they are.  If
   * you request really wide rows, this may cause increased memory consumption
   * on the server side as the server has to build a large RPC response, even
   * if it tries to avoid copying data.  On the client side, the consequences
   * on memory usage are worse due to the lack of framing in RPC responses.
   * The client will have to buffer a large RPC response and will have to do
   * several memory copies to dynamically grow the size of the buffer as more
   * and more data comes in.
   * @param max_num_kvs A non-zero value.
   * @throws IllegalArgumentException if the argument is zero.
   * @throws IllegalStateException if scanning already started.
   */
  public void setMaxNumKeyValues(final int max_num_kvs) {
    if (max_num_kvs == 0) {
      throw new IllegalArgumentException("batch size can't be zero");
    }
    checkScanningNotStarted();
    this.max_num_kvs = max_num_kvs;
  }

  /**
   * Scans a number of rows.  Calling this method is equivalent to:
   * <pre>
   *   this.{@link #setMaxNumRows setMaxNumRows}(nrows);
   *   this.{@link #nextRows() nextRows}();
   * </pre>
   * @param nrows The maximum number of rows to retrieve.
   * @return A deferred list of rows.
   * @see #setMaxNumRows
   * @see #nextRows()
   */
  public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows(final int nrows) {
    setMaxNumRows(nrows);
    return nextRows();
  }

  /**
   * Scans a number of rows.
   * <p>
   * The last row returned may be partial if it's very wide and
   * {@link #setMaxNumKeyValues} wasn't called with a negative value in
   * argument.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return A deferred list of rows.  Each row is a list of {@link KeyValue}
   * and each element in the list returned represents a different row.  Rows
   * are returned in sequential order.  {@code null} is returned if there are
   * no more rows to scan.  Otherwise its {@link ArrayList#size size} is
   * guaranteed to be less than or equal to the value last given to
   * {@link #setMaxNumRows}.
   * @see #setMaxNumRows
   * @see #setMaxNumKeyValues
   */
  public Deferred<ArrayList<ArrayList<KeyValue>>> nextRows() {
    if (region == DONE) {  // We're already done scanning.
      return Deferred.fromResult(null);
    } else if (region == null) {  // We need to open the scanner first.
      return client.openScanner(this).addCallbackDeferring(
        new Callback<Deferred<ArrayList<ArrayList<KeyValue>>>, Long>() {
          public Deferred<ArrayList<ArrayList<KeyValue>>> call(final Long arg) {
            scanner_id = arg;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Scanner " + Bytes.hex(arg) + " opened on " + region);
            }
            return nextRows();  // Restart the call.
          }
          public String toString() {
            return "scanner opened";
          }
        });
    }

    // Need to silence this warning because the callback `got_next_row'
    // declares its return type to be Object, because its return value
    // may or may not be deferred.
    @SuppressWarnings("unchecked")
    final Deferred<ArrayList<ArrayList<KeyValue>>> d = (Deferred)
      client.scanNextRows(this).addCallbacks(got_next_row, nextRowErrback());
    return d;
  }

  /**
   * Singleton callback to handle responses of "next" RPCs.
   * This returns an {@code ArrayList<ArrayList<KeyValue>>} (possibly inside a
   * deferred one).
   */
  private final Callback<Object, Object> got_next_row =
    new Callback<Object, Object>() {
      public Object call(final Object response) {
        if (response == null) {  // We're done scanning this region.
          final byte[] region_stop_key = region.stopKey();
          // Check to see if this region is the last we should scan (either
          // because (1) it's the last region or (3) because its stop_key is
          // greater than or equal to the stop_key of this scanner provided
          // that (2) we're not trying to scan until the end of the table).
          if (region_stop_key == EMPTY_ARRAY                           // (1)
              || (stop_key != EMPTY_ARRAY                              // (2)
                  && Bytes.memcmp(stop_key, region_stop_key) <= 0)) {  // (3)
            get_next_rows_request = null;        // free();
            family = qualifier = null;           // free();
            start_key = stop_key = EMPTY_ARRAY;  // free() but mustn't be null.
            return close()  // Auto-close the scanner.
              .addCallback(new Callback<ArrayList<ArrayList<KeyValue>>, Object>() {
                public ArrayList<ArrayList<KeyValue>> call(final Object arg) {
                  return null;  // Tell the user there's nothing more to scan.
                }
                public String toString() {
                  return "auto-close scanner " + Bytes.hex(scanner_id);
                }
              });
          }
          return continueScanOnNextRegion();
        } else if (!(response instanceof ArrayList)) {
          throw new InvalidResponseException(ArrayList.class, response);
        }
        @SuppressWarnings("unchecked")  // I 3>> generics.
        final ArrayList<ArrayList<KeyValue>> rows = (ArrayList<ArrayList<KeyValue>>) response;
        final ArrayList<KeyValue> lastrow = rows.get(rows.size() - 1);
        start_key = lastrow.get(0).key();
        return rows;
      }
      public String toString() {
        return "get nextRows response";
      }
    };

  /**
   * Creates a new errback to handle errors while trying to get more rows.
   */
  private final Callback<Object, Object> nextRowErrback() {
    return new Callback<Object, Object>() {
      public Object call(final Object error) {
        final RegionInfo old_region = region;  // Save before invalidate().
        invalidate();  // If there was an error, don't assume we're still OK.
        if (error instanceof NotServingRegionException) {
          // We'll resume scanning on another region, and we want to pick up
          // right after the last key we successfully returned.  Padding the
          // last key with an extra 0 gives us the next possible key.
          // TODO(tsuna): If we get 2 NSRE in a row, well pad the key twice!
          start_key = Arrays.copyOf(start_key, start_key.length + 1);
          return nextRows();  // XXX dangerous endless retry
        } else if (error instanceof UnknownScannerException) {
          // This can happen when our scanner lease expires.  Unfortunately
          // there's no way for us to distinguish between an expired lease
          // and a real problem, for 2 reasons: the server doesn't keep track
          // of recently expired scanners and the lease time is only known by
          // the server and never communicated to the client.  The normal
          // HBase client assumes that the client will share the same
          // hbase-site.xml configuration so that both the client and the
          // server will know the same lease time, but this assumption is bad
          // as nothing guarantees that the client's configuration will be in
          // sync with the server's.  This unnecessarily increases deployment
          // complexity and it's brittle.
          LOG.warn(old_region + " pretends to not know " + this + ".  I will"
            + " retry to open a scanner but this is typically because you've"
            + " been holding the scanner open and idle for too long (possibly"
            + " due to a long GC pause on your side or in the RegionServer)",
            error);
          // Let's re-open ourselves and keep scanning.
          return nextRows();  // XXX dangerous endless retry
        }
        return error;  // Let the error propagate.
      }
      public String toString() {
        return "NextRow errback";
      }
    };
  }

  /**
   * Closes this scanner (don't forget to call this when you're done with it!).
   * <p>
   * Closing a scanner already closed has no effect.  The deferred returned
   * will be called back immediately.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
   */
  public Deferred<Object> close() {
    if (region == null) {
      return Deferred.fromResult(null);
    }
    return client.closeScanner(this).addBoth(closedCallback());
  }

  /** Callback+Errback invoked when the RegionServer closed our scanner.  */
  private Callback<Object, Object> closedCallback() {
    return new Callback<Object, Object>() {
      public Object call(Object arg) {
        if (arg instanceof Exception) {
          final Exception error = (Exception) arg;
          // NotServingRegionException:
          //   If the region isn't serving, then our scanner is already broken
          //   somehow, because while it's open it holds a read lock on the
          //   region, which prevents it from splitting (among other things).
          //   So if we get this error, our scanner is already dead anyway.
          // UnknownScannerException:
          //   If this region doesn't know anything about this scanner then we
          //   don't have anything to do to close it!
          if (error instanceof NotServingRegionException
              || error instanceof UnknownScannerException) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring exception when closing " + Scanner.this,
                        error);
            }
            arg = null;  // Clear the error.
          }  // else: the `return arg' below will propagate the error.
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner " + Bytes.hex(scanner_id) + " closed on "
                    + region);
        }
        region = DONE;
        scanner_id = 0xDEAD000CC000DEADL;   // Make debugging easier.
        return arg;
      }
      public String toString() {
        return "scanner closed";
      }
    };
  }

  /**
   * Continues scanning on the next region.
   * <p>
   * This method is called when we tried to get more rows but we reached the
   * end of the current region and need to move on to the next region.
   * <p>
   * This method closes the scanner on the current region, updates the start
   * key of this scanner and resumes scanning on the next region.
   * @return The deferred results from the next region.
   */
  private Deferred<ArrayList<ArrayList<KeyValue>>> continueScanOnNextRegion() {
    // Copy those into local variables so we can still refer to them in the
    // "closure" below even after we've changed them.
    final long old_scanner_id = scanner_id;
    final RegionInfo old_region = region;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanner " + Bytes.hex(old_scanner_id) + " done scanning "
                + old_region);
    }
    client.closeScanner(this).addCallback(new Callback<Object, Object>() {
      public Object call(final Object arg) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner " + Bytes.hex(old_scanner_id) + " closed on "
                    + old_region);
        }
        return arg;
      }
      public String toString() {
        return "scanner moved";
      }
    });
    // Continue scanning from the next region's start key.
    start_key = region.stopKey();
    scanner_id = 0xDEAD000AA000DEADL;   // Make debugging easier.
    invalidate();
    return nextRows();
  }

  public String toString() {
    final String region = this.region == null ? "null"
      : this.region == DONE ? "none" : this.region.toString();
    final StringBuilder buf = new StringBuilder(14 + 1 + table.length + 1 + 12
      + 1 + start_key.length + 1 + 1 + stop_key.length + 1
      + 9 + 1 + (family == null ? 4 : family.length) + 1
      + 12 + 1 + (qualifier == null ? 4 : qualifier.length) + 1
      + 22 + 5 + 15 + 5 + 14 + 6
      + 14 + 1 + region.length() + 1
      + 13 + 18 + 1);
    buf.append("Scanner(table=");
    Bytes.pretty(buf, table);
    buf.append(", start_key=");
    Bytes.pretty(buf, start_key);
    buf.append(", stop_key=");
    Bytes.pretty(buf, stop_key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(", populate_blockcache=").append(populate_blockcache)
      .append(", max_num_rows=").append(max_num_rows)
      .append(", max_num_kvs=").append(max_num_kvs)
      .append(", region=").append(region);
    buf.append(", scanner_id=").append(Bytes.hex(scanner_id))
      .append(')');
    return buf.toString();
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  byte[] table() {
    return table;
  }

  byte[] startKey() {
    return start_key;
  }

  /**
   * Sets the name of the region that's hosting {@code this.start_key}.
   * @param region The region we're currently supposed to be scanning.
   */
  void setRegionName(final RegionInfo region) {
    this.region = region;
  }

  /**
   * Invalidates this scanner and makes it assume it's no longer opened.
   * When a RegionServer goes away while we're scanning it, or some other type
   * of access problem happens, this method should be called so that the
   * scanner will have to re-locate the RegionServer and re-open itself.
   */
  void invalidate() {
    region = null;
  }

  /**
   * Returns the region currently being scanned, if any.
   */
  RegionInfo currentRegion() {
    return region;
  }

  /**
   * Returns an RPC to fetch the next rows.
   */
  HBaseRpc getNextRowsRequest() {
    if (get_next_rows_request == null) {
      get_next_rows_request = new GetNextRowsRequest();
    }
    return get_next_rows_request;
  }

  /**
   * Returns an RPC to open this scanner.
   */
  HBaseRpc getOpenRequest() {
    return new OpenScannerRequest();
  }

  /**
   * Returns an RPC to close this scanner.
   */
  HBaseRpc getCloseRequest() {
    return new CloseScannerRequest(scanner_id);
  }

  /**
   * Throws an exception if scanning already started.
   * @throws IllegalStateException if scanning already started.
   */
  private void checkScanningNotStarted() {
    if (region != null) {
      throw new IllegalStateException("scanning already started");
    }
  }

  private static final byte[] OPEN_SCANNER = new byte[] {
    'o', 'p', 'e', 'n', 'S', 'c', 'a', 'n', 'n', 'e', 'r'
  };

  /**
   * RPC sent out to open a scanner on a RegionServer.
   */
  private final class OpenScannerRequest extends HBaseRpc {

    public OpenScannerRequest() {
      super(OPEN_SCANNER, Scanner.this.table, start_key);
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
      size += 3;  // vint: region name length (3 bytes => max length = 32768).
      size += super.region.name().length;  // The region name.

      size += 1;  // byte: Type of the 2nd parameter.
      size += 1;  // byte: Type again (see HBASE-2877).
      size += 1;  // byte: Version of Scan.
      size += 3;  // vint: start key length (3 bytes => max length = 32768).
      size += start_key.length;  // The start key.
      size += 3;  // vint: stop key length (3 bytes => max length = 32768).
      size += stop_key.length;  // The stop key.
      size += 4;  // int:  Max number of versions to return.
      size += 4;  // int:  Max number of KeyValues to get per RPC.
      size += 4;  // int:  Unused field only used by HBase's client.
      size += 1;  // bool: Whether or not to populate the blockcache.
      size += 1;  // byte: Whether or not to use a filter.
      if (filter != null) {
        size += filter.length;
      }
      size += 8;  // long: Minimum timestamp.
      size += 8;  // long: Maximum timestamp.
      size += 1;  // byte: Boolean: "all time".
      size += 4;  // int:  Number of families.
      if (family != null) {
        size += 1;  // vint: Family length (guaranteed on 1 byte).
        size += family.length;  // The family.
        size += 4;  // int:  How many qualifiers follow?
        if (qualifier != null) {
          size += 3;  // vint: Qualifier length.
          size += qualifier.length;  // The qualifier.
        }
      }
      return size;
    }

    /** Serializes this request.  */
    ChannelBuffer serialize(final byte unused_server_version) {
      final ChannelBuffer buf = newBuffer(predictSerializedSize());
      buf.writeInt(2);  // Number of parameters.

      // 1st param: byte array containing region name
      writeHBaseByteArray(buf, super.region.name());

      // 2nd param: Scan object
      buf.writeByte(39);   // Code for a `Scan' parameter.
      buf.writeByte(39);   // Code again (see HBASE-2877).
      buf.writeByte(1);    // Manual versioning of Scan.
      writeByteArray(buf, start_key);
      writeByteArray(buf, stop_key);
      buf.writeInt(1);     // Max number of versions to return.

      // Max number of KeyValues to get per RPC.
      buf.writeInt(max_num_kvs);

      // Unused field, only used by HBase's client.  This value should represent
      // how many rows per call the client will fetch, but the server doesn't
      // care about this value, neither do we, because we use a different API.
      buf.writeInt(0xDEADBA5E);

      // Whether or not to populate the blockcache.
      buf.writeByte(populate_blockcache ? 0x01 : 0x00);

      if (filter == null) {
        buf.writeByte(0x00); // boolean (false): don't use a filter.
      } else {
        buf.writeByte(0x01); // boolean (true): use a filter.
        buf.writeBytes(filter);
      }

      // TimeRange
      buf.writeLong(0);               // Minimum timestamp.
      buf.writeLong(Long.MAX_VALUE);  // Maximum timestamp.
      buf.writeByte(0x01);            // Boolean: "all time".
      // The "all time" boolean indicates whether or not this time range covers
      // all possible times.  Not sure why it's part of the serialized RPC...

      // Families.
      buf.writeInt(family != null ? 1 : 0);  // Number of families that follow.

      if (family != null) {
        // Each family is then written like so:
        writeByteArray(buf, family);  // Column family name.
        buf.writeInt(qualifier == null ? 0 : 1);  // How many qualifiers do we want?
        if (qualifier != null) {
          writeByteArray(buf, qualifier);  // Column qualifier name.
        }
      }

      // Save the region in the Scanner.  This kind of a kludge but it really
      // is the easiest way to give the Scanner the RegionInfo it needs.
      Scanner.this.region = super.region;
      return buf;
    }

  }

  private static final byte[] NEXT = new byte[] { 'n', 'e', 'x', 't' };

  /**
   * RPC sent out to fetch the next rows from the RegionServer.
   */
  private final class GetNextRowsRequest extends HBaseRpc {

    public GetNextRowsRequest() {
      super(NEXT);  // "next"...  Great method name!
    }

    /** Serializes this request.  */
    ChannelBuffer serialize(final byte unused_server_version) {
      final ChannelBuffer buf = newBuffer(4 + 1 + 8 + 1 + 4);
      buf.writeInt(2);  // Number of parameters.
      writeHBaseLong(buf, scanner_id);
      writeHBaseInt(buf, max_num_rows);
      return buf;
    }

  }

  /**
   * RPC sent out to close a scanner on a RegionServer.
   */
  private static final class CloseScannerRequest extends HBaseRpc {

    private static final byte[] CLOSE = new byte[] { 'c', 'l', 'o', 's', 'e' };

    private final long scanner_id;

    public CloseScannerRequest(final long scanner_id) {
      super(CLOSE);  // "close"...  Great method name!
      this.scanner_id = scanner_id;
    }

    /** Serializes this request.  */
    ChannelBuffer serialize(final byte unused_server_version) {
      final ChannelBuffer buf = newBuffer(4 + 1 + 8);
      buf.writeInt(1);  // Number of parameters.
      writeHBaseLong(buf, scanner_id);
      return buf;
    }

  }

}
