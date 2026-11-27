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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;

class BitConverters {
  private BitConverters() {}

  static long readBigEndianLong(InputStream in) throws IOException {
    byte[] buf = new byte[8];
    ByteStreams.readFully(in, buf);

    return Longs.fromByteArray(buf);
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

  // Unrolls the 8 big-endian byte stores into a local byte[8] and invokes out.write(buf, 0, 8)
  // directly (avoiding Guava Longs.toByteArray's loop and OutputStream.write(byte[]) indirection).
  // This allows HotSpot C2 Escape Analysis to scalar-replace buf to 0 B/op on monomorphic streams
  // like ByteStringOutputStream and ByteArrayOutputStreaml.
  static void writeBigEndianLong(long value, OutputStream out) throws IOException {
    byte[] buf = new byte[8];
    buf[0] = (byte) (value >>> 56);
    buf[1] = (byte) (value >>> 48);
    buf[2] = (byte) (value >>> 40);
    buf[3] = (byte) (value >>> 32);
    buf[4] = (byte) (value >>> 24);
    buf[5] = (byte) (value >>> 16);
    buf[6] = (byte) (value >>> 8);
    buf[7] = (byte) value;
    out.write(buf, 0, 8);
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
