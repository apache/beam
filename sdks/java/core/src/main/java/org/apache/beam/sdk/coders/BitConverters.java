/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.coders;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class BitConverters {
  private BitConverters() {}

  static long readBigEndianLong(InputStream in) throws IOException {
    byte[] buf = new byte[8];
    int n = in.read(buf, 0, 8);
    if (n != 8) {
      readRemaining(in, buf, n);
    }
    return ((buf[0] & 0xFFL) << 56)
        | ((buf[1] & 0xFFL) << 48)
        | ((buf[2] & 0xFFL) << 40)
        | ((buf[3] & 0xFFL) << 32)
        | ((buf[4] & 0xFFL) << 24)
        | ((buf[5] & 0xFFL) << 16)
        | ((buf[6] & 0xFFL) << 8)
        | (buf[7] & 0xFFL);
  }

  private static void readRemaining(InputStream in, byte[] buf, int readSoFar) throws IOException {
    if (readSoFar < 0) {
      throw new EOFException();
    }
    int total = readSoFar;
    while (total < 8) {
      int n = in.read(buf, total, 8 - total);
      if (n < 0) {
        throw new EOFException();
      }
      total += n;
    }
  }

  static int readBigEndianInt(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();

    if ((b1 | b2 | b3 | b4) < 0) {
      throw new EOFException();
    }

    return (b1 & 255) << 24 | (b2 & 255) << 16 | (b3 & 255) << 8 | (b4 & 255);
  }

  static short readBigEndianShort(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();

    if ((b1 | b2) < 0) {
      throw new EOFException();
    }

    return (short) ((b1 & 255) << 8 | (b2 & 255));
  }

  static void writeBigEndianLong(long value, OutputStream out) throws IOException {
    out.write((int) (value >>> 56) & 0xFF);
    out.write((int) (value >>> 48) & 0xFF);
    out.write((int) (value >>> 40) & 0xFF);
    out.write((int) (value >>> 32) & 0xFF);
    out.write((int) (value >>> 24) & 0xFF);
    out.write((int) (value >>> 16) & 0xFF);
    out.write((int) (value >>> 8) & 0xFF);
    out.write((int) value & 0xFF);
  }

  static void writeBigEndianInt(int value, OutputStream out) throws IOException {
    out.write((byte) (value >>> 24) & 0xFF);
    out.write((byte) (value >>> 16) & 0xFF);
    out.write((byte) (value >>> 8) & 0xFF);
    out.write((byte) value);
  }

  static void writeBigEndianShort(short value, OutputStream out) throws IOException {
    out.write((byte) (value >> 8));
    out.write((byte) value);
  }
}
