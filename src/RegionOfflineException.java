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
 * Exception thrown when we attempted to find a region, but it wasn't being
 * served by any RegionServer (it's "offline" in HBase speak).
 * XXX verify those claims...  Is "offline" = "disabled"?  Or the region is in
 * transition?  Or what?
 */
public final class RegionOfflineException extends RecoverableException {

  private final byte[] region;

  /**
   * Constructor.
   * @param region The region that was found to be offline.
   */
  RegionOfflineException(final byte[] region) {
    super(Bytes.pretty(region));
    this.region = region;
  }

  /**
   * Returns the name of the region that's offline.
   */
  public byte[] getRegion() {
    return region;
  }

  private static final long serialVersionUID = 1280641842;

}
