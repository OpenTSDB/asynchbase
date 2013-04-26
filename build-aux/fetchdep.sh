#!/bin/bash
# Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
# This file is part of Async HBase.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the StumbleUpon nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

set -e
MKDIR_P="mkdir -p"
for i in md5sum md5 "$MD5"; do
  sum=`echo a | $i 2>/dev/null` || continue
  case $sum in
   (*60b725f10c9c85c70d97880dfe8191b3*) MD5=$i; break;;
   (*) :;;  # continue
  esac
done
test -n "$MD5" || {
  echo >&2 "couldn't find a working md5sum command"
  exit 1
}
if wget --version >/dev/null 2>&1; then
  WGET=wget
elif curl --version >/dev/null 2>&1; then
  CURL=curl
else
  echo >&2 "couldn't find either curl or wget"
  exit 1
fi
srcdir=`dirname "$0"`
# The ".." is because we need to go out of the build-aux directory.
srcdir=`cd "$srcdir"; cd ..; pwd -P`

f=`basename "$2"`
d=`dirname "$2"`

validate_checksum() {
  cksum=`$MD5 "$1" | sed 's/.*\([0-9a-fA-F]\{32\}\).*/\1/'`
  valid=`< "$srcdir/${1%-t}.md5"`
  test "x$cksum" = "x$valid"
}

# Don't re-download if we happen to have the right file already.
test -f "$2" && validate_checksum "$2" && touch "$2" && rm -f "$2-t" && exit

rm -f "$2"
$MKDIR_P "$d"

if test -n "$WGET"; then
  $WGET "$1/$f" -O "$2-t"
elif test -n "$CURL"; then
  $CURL "$1/$f" -o "$2-t"
else
  echo >&2 "cannot find a tool to download $1/$f"
  exit 1
fi

if validate_checksum "$2-t"; then
  mv "$2-t" "$2"
  # wget sets the timestamp to whatever is the Last-Modified header, so
  # make sure we have a recent mtime on the file to keep `make' happy.
  touch "$2"
else
  echo >&2 "error: invalid checksum for $2"
  echo >&2 "error: expected: $valid"
  echo >&2 "error:    found: $cksum"
  rm -f "$2-t"
  exit 1
fi
