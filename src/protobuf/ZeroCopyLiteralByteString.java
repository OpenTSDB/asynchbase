/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.protobuf;  // This is a lie.

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Helper class to extract byte arrays from {@link com.google.protobuf.ByteString} without copy.
 * <p>
 * Without this protobufs would force us to copy every single byte array out of the objects
 * de-serialized from the wire (which already do one copy, on top of the copies the JVM does to go
 * from kernel buffer to C buffer and from C buffer to JVM buffer).
 *
 * Pulled from the Google Bigtable Javaclient. This class should really make it
 * into the Protobuf project. 
 * https://github.com/GoogleCloudPlatform/cloud-bigtable-client/
 *
 * @author sduskis
 * @since 1.8
 */
public class ZeroCopyLiteralByteString {

  /**
   * Wraps a byte array in a {@link com.google.protobuf.ByteString} without copying it.
   *
   * @param array an array of byte.
   * @return a {@link com.google.protobuf.ByteString} object.
   */
  public static ByteString wrap(final byte[] array) {
    return UnsafeByteOperations.unsafeWrap(ByteBuffer.wrap(array));
  }

  /**
   * Extracts the byte array from the given {@link com.google.protobuf.ByteString} without copy.
   *
   * @param byteString A {@link ByteString} from which to extract the array.
   * @return an array of byte.
   */
  public static byte[] get(final ByteString byteString) {
    try {
      ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
      UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
      return byteOutput.bytes;
    } catch (IOException e) {
      return byteString.toByteArray();
    }
  }

  private static final class ZeroCopyByteOutput extends ByteOutput {
    private byte[] bytes;

    @Override
    public void writeLazy(byte[] value, int offset, int length) throws IOException {
      if (offset != 0) {
        throw new UnsupportedOperationException();
      }
      bytes = value;
    }

    @Override
    public void write(byte value) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer value) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

}
