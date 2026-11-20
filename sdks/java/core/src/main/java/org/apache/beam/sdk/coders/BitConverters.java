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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class BitConverters {
  private static final class ScratchHolder {
    byte @Nullable [] buf = new byte[8];
  }

  @SuppressWarnings("type.argument") // ScratchHolder guaranteed to be non-null
  private static final ThreadLocal<@NonNull ScratchHolder> SCRATCH_HOLDER =
      ThreadLocal.withInitial(ScratchHolder::new);

  private BitConverters() {}

  // Deliberately allocates a local byte[8] instead of reusing SCRATCH_HOLDER like
  // writeBigEndianLong does: JMH benchmarks show that C2 scalar-replaces the local array on
  // monomorphic InputStreams (making ThreadLocal lookup ~18-20% slower), and even under
  // megamorphic InputStream dispatch the ThreadLocal checkout overhead outweighs the small young-GC
  // savings. On the encode path, by contrast, Longs.toByteArray() escapes through
  // OutputStream.write() even on ByteStringOutputStream, so SCRATCH_HOLDER improves both throughput
  // and allocation rate.
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

  // Uses a checked-out thread-local 8-byte buffer (with new byte[8] fallback on reentrant
  // OutputStream.write callbacks) to issue a single out.write(buf, 0, 8) call without allocating.
  // See readBigEndianLong for why the decode path intentionally does not use SCRATCH_HOLDER.
  static void writeBigEndianLong(long value, OutputStream out) throws IOException {
    ScratchHolder holder = SCRATCH_HOLDER.get();
    byte[] buf = holder.buf;
    if (buf != null) {
      holder.buf = null;
      try {
        writeBigEndianLong(value, out, buf);
      } finally {
        holder.buf = buf;
      }
      return;
    }
    writeBigEndianLong(value, out, new byte[8]);
  }

  private static void writeBigEndianLong(long value, OutputStream out, byte[] buf)
      throws IOException {
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
