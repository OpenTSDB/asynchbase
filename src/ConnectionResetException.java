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

import org.jboss.netty.channel.Channel;

/**
 * Exception thrown when an RPC was in flight while we got disconnected.
 */
public final class ConnectionResetException extends RecoverableException {

  private final Channel chan;

  /**
   * Constructor.
   * @param table The table that wasn't found.
   */
  ConnectionResetException(final Channel chan) {
    super(chan + " got disconnected");
    this.chan = chan;
  }

  /**
   * Returns the name of the region that's offline.
   */
  public Channel getChannel() {
    return chan;
  }

  private static final long serialVersionUID = 1280644142;

}
