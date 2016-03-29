/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Variable-length encoding for integers.
 *
 * <p>Handles, in a common encoding format, signed bytes, shorts, ints, and longs.
 * Takes between 1 and 10 bytes.
 * Less efficient than BigEndian{Int,Long} coder for negative or large numbers.
 * All negative ints are encoded using 5 bytes, longs take 10 bytes.
 */
public class VarInt {

  private static long convertIntToLongNoSignExtend(int v) {
    return v & 0xFFFFFFFFL;
  }

  /**
   * Encodes the given value onto the stream.
   */
  public static void encode(int v, OutputStream stream) throws IOException {
    encode(convertIntToLongNoSignExtend(v), stream);
  }

  /**
   * Encodes the given value onto the stream.
   */
  public static void encode(long v, OutputStream stream) throws IOException {
    do {
      // Encode next 7 bits + terminator bit
      long bits = v & 0x7F;
      v >>>= 7;
      byte b = (byte) (bits | ((v != 0) ? 0x80 : 0));
      stream.write(b);
    } while (v != 0);
  }

  /**
   * Decodes an integer value from the given stream.
   */
  public static int decodeInt(InputStream stream) throws IOException {
    long r = decodeLong(stream);
    if (r < 0 || r >= 1L << 32) {
      throw new IOException("varint overflow " + r);
    }
    return (int) r;
  }

  /**
   * Decodes a long value from the given stream.
   */
  public static long decodeLong(InputStream stream) throws IOException {
    long result = 0;
    int shift = 0;
    int b;
    do {
      // Get 7 bits from next byte
      b = stream.read();
      if (b < 0) {
        if (shift == 0) {
          throw new EOFException();
        } else {
          throw new IOException("varint not terminated");
        }
      }
      long bits = b & 0x7F;
      if (shift >= 64 || (shift == 63 && bits > 1)) {
        // Out of range
        throw new IOException("varint too long");
      }
      result |= bits << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return result;
  }

  /**
   * Returns the length of the encoding of the given value (in bytes).
   */
  public static int getLength(int v) {
    return getLength(convertIntToLongNoSignExtend(v));
  }

  /**
   * Returns the length of the encoding of the given value (in bytes).
   */
  public static int getLength(long v) {
    int result = 0;
    do {
      result++;
      v >>>= 7;
    } while (v != 0);
    return result;
  }
}
