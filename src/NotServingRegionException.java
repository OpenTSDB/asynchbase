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

/**
 * Exception thrown when we attempted to use a region that wasn't serving from
 * that particular RegionServer.  It probably moved somewhere else.
 */
public final class NotServingRegionException extends RecoverableException {

  static final String REMOTE_CLASS =
    "org.apache.hadoop.hbase.NotServingRegionException";

  private final byte[] region;

  /**
   * Constructor.
   * @param region The name of the region that caused this exception.
   */
  NotServingRegionException(final byte[] region) {
    super(Bytes.pretty(region));
    this.region = region;
  }

  /**
   * Returns the name of the region that caused this exception.
   */
  public byte[] getRegion() {
    return region;
  }

  @Override
  NotServingRegionException make(final Object region) {
    return new NotServingRegionException((byte[]) region);
  }

  private static final long serialVersionUID = 1281000942;

}
