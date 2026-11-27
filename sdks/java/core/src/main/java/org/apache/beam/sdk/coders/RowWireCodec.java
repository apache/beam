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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.UUID;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Utf8;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/**
 * Leaf wire-format encoding, decoding, and byte-size helpers matching {@code beam:coder:row:v1}
 * bit-for-bit.
 *
 * <p>Provides buffered {@code pack*} writers and {@code read*(in, buf)} readers that accept a
 * caller-checked-out per-thread scratch buffer ({@link ScratchPool}) so a full row is packed into a
 * single {@link OutputStream#write(byte[], int, int)} flush with one {@link ThreadLocal} lookup per
 * row. Checkout/release ownership prevents silent buffer clobbering across re-entrant user getters,
 * custom streams, lazy collections, or nested {@link Coder}s; callers must still call {@link
 * #flush(byte[], int, OutputStream)} before invoking any nested {@link Coder} so bytes arrive in
 * wire order.
 */
@Internal
public final class RowWireCodec {
  private static final int SCRATCH_SIZE = 512;
  // Every fixed-width / varint / logical-type / header write emits at most 24 bytes.
  private static final int FLUSH_LIMIT = SCRATCH_SIZE - 32;
  // Two VarInt headers (fieldCount + nullByteLen) take at most 10 bytes.
  private static final int MAX_HEADER_VARINT_BYTES = 10;
  private static final int MAX_ASCII_STRING_FAST = 254;
  private static final int MAX_INLINE_BYTES_FAST = 64;
  private static final byte[] EMPTY_BYTES = new byte[0];

  private static final VarHandle SHORT_BE =
      MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_BE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
  private static final VarHandle INT_LE =
      MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
  private static final VarHandle LONG_BE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

  private static final ThreadLocal<@Nullable ScratchPool> SCRATCH_POOL =
      ThreadLocal.withInitial(ScratchPool::new);
  private static final ThreadLocal<@Nullable RefScratchPool> REF_SCRATCH_POOL =
      ThreadLocal.withInitial(RefScratchPool::new);

  private static final int[] TIMESTAMP_SCALING_FACTORS = {
    1_000_000_000, 100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1
  };
  private static final int[] TIMESTAMP_MAX_SUBSECONDS = {
    0, 9, 99, 999, 9_999, 99_999, 999_999, 9_999_999, 99_999_999, 999_999_999
  };

  /**
   * Per-thread re-entrancy-safe pool of 512-byte scratch buffers.
   *
   * <p>Each {@link #acquire()} removes a buffer from the thread-local stack (allocating a fresh
   * buffer only if re-entrancy exceeds {@link #MAX_DEPTH}), guaranteeing nested or re-entrant
   * encoders/decoders on the same thread never alias an active caller's buffer.
   */
  @Internal
  public static final class ScratchPool {
    private static final int MAX_DEPTH = 8;
    private final byte[] @Nullable [] stack = new byte[MAX_DEPTH][];
    private int depth;

    ScratchPool() {}

    public byte[] acquire() {
      int d = depth;
      if (d > 0) {
        depth = d - 1;
        byte @Nullable [] buf = stack[d - 1];
        stack[d - 1] = null;
        if (buf != null) {
          return buf;
        }
      }
      return new byte[SCRATCH_SIZE];
    }

    public void release(byte[] buf) {
      int d = depth;
      if (d < MAX_DEPTH) {
        stack[d] = buf;
        depth = d + 1;
      }
    }
  }

  /**
   * Per-thread re-entrancy-safe pool of {@code Object[]} reference buffers for wide nullable
   * JavaBean encoders so getters are evaluated once per record without per-record heap allocation.
   * Callers pass {@code usedLen} to {@link #release(Object[], int)} so references are cleared
   * before pooling and never retained for GC.
   */
  @Internal
  public static final class RefScratchPool {
    private static final int MAX_DEPTH = 8;
    private static final int MAX_POOLED_SIZE = 512;
    private final @Nullable Object[] @Nullable [] stack = new Object[MAX_DEPTH][];
    private int depth;

    RefScratchPool() {}

    public @Nullable Object[] acquire(int minSize) {
      int d = depth;
      if (d > 0) {
        depth = d - 1;
        @Nullable Object @Nullable [] buf = stack[d - 1];
        stack[d - 1] = null;
        if (buf != null && buf.length >= minSize) {
          return buf;
        }
      }
      return new Object[minSize];
    }

    public void release(@Nullable Object[] buf, int usedLen) {
      int d = depth;
      if (d < MAX_DEPTH && buf.length <= MAX_POOLED_SIZE) {
        Arrays.fill(buf, 0, usedLen, null);
        stack[d] = buf;
        depth = d + 1;
      }
    }
  }

  @FunctionalInterface
  private interface ElementReader<T> {
    T read(InputStream in, byte[] buf) throws IOException;
  }

  private RowWireCodec() {}

  /** Returns the current thread's re-entrancy-safe scratch buffer pool. */
  public static ScratchPool scratchPool() {
    ScratchPool pool = SCRATCH_POOL.get();
    if (pool == null) {
      pool = new ScratchPool();
      SCRATCH_POOL.set(pool);
    }
    return pool;
  }

  /** Returns the current thread's re-entrancy-safe reference array pool. */
  public static RefScratchPool refScratchPool() {
    RefScratchPool pool = REF_SCRATCH_POOL.get();
    if (pool == null) {
      pool = new RefScratchPool();
      REF_SCRATCH_POOL.set(pool);
    }
    return pool;
  }

  /** Flushes any buffered bytes in {@code buf[0..pos)} to {@code out} and returns {@code 0}. */
  public static int flush(byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    return 0;
  }

  private static int ensureRoom(byte[] buf, int pos, int needed, OutputStream out)
      throws IOException {
    if (pos + needed > SCRATCH_SIZE) {
      out.write(buf, 0, pos);
      return 0;
    }
    return pos;
  }

  private static int putVarInt(byte[] buf, int pos, int v) {
    return putVarLong(buf, pos, v & 0xFFFFFFFFL);
  }

  private static int putVarLong(byte[] buf, int pos, long v) {
    while ((v & ~0x7FL) != 0L) {
      buf[pos++] = (byte) (((int) v & 0x7F) | 0x80);
      v >>>= 7;
    }
    buf[pos++] = (byte) v;
    return pos;
  }

  private static int putBigEndianInt(byte[] buf, int pos, int v) {
    INT_BE.set(buf, pos, v);
    return pos + 4;
  }

  private static int putBigEndianLong(byte[] buf, int pos, long v) {
    LONG_BE.set(buf, pos, v);
    return pos + 8;
  }

  private static void readExact(InputStream in, byte[] buf, int len) throws IOException {
    if (len == 0) {
      return;
    }
    int n = in.read(buf, 0, len);
    if (n != len) {
      readExactSlow(in, buf, n, len);
    }
  }

  private static void readExactSlow(InputStream in, byte[] buf, int firstRead, int len)
      throws IOException {
    if (firstRead < 0) {
      throw new CoderException(new EOFException());
    }
    int off = firstRead;
    while (off < len) {
      int n = in.read(buf, off, len - off);
      if (n < 0) {
        throw new CoderException(new EOFException());
      }
      off += n;
    }
  }

  public static void skipExact(InputStream in, int len) throws IOException {
    if (len <= 0) {
      return;
    }
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int remaining = len;
      while (remaining > 0) {
        int chunk = Math.min(remaining, SCRATCH_SIZE);
        int n = in.read(buf, 0, chunk);
        if (n < 0) {
          throw new CoderException(new EOFException());
        }
        remaining -= n;
      }
    } finally {
      pool.release(buf);
    }
  }

  /**
   * Throws the exact runtime exception message emitted by {@code RowWithGetters} on null fields.
   */
  public static RuntimeException nullNonNullableField(Schema schema, int fieldIndex) {
    return new RuntimeException(
        "Null value set on non-nullable field " + schema.getField(fieldIndex));
  }

  // ---- Headers and null masks ----

  public static int packHeader(int fieldCount, long nullMask, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putVarInt(buf, pos, fieldCount);
    if (nullMask == 0L) {
      buf[pos++] = 0;
    } else {
      int numBytes = ((64 - Long.numberOfLeadingZeros(nullMask)) + 7) >>> 3;
      buf[pos++] = (byte) numBytes;
      for (int i = 0; i < numBytes; i++) {
        buf[pos++] = (byte) (nullMask >>> (i << 3));
      }
    }
    return pos;
  }

  /**
   * Packs a {@code beam:coder:row:v1} header with a precomputed byte-array null bitmap into {@code
   * buf}, falling back to direct stream writes only if the bitmap exceeds {@code buf} capacity.
   */
  public static int packWideNullHeader(
      int fieldCount, byte[] nullBytes, int nullByteLen, byte[] buf, OutputStream out)
      throws IOException {
    if (nullByteLen < 0) {
      throw new CoderException("Invalid null bitset length: " + nullByteLen);
    }
    if (nullByteLen <= buf.length - MAX_HEADER_VARINT_BYTES) {
      int pos = putVarInt(buf, 0, fieldCount);
      pos = putVarInt(buf, pos, nullByteLen);
      if (nullByteLen > 0) {
        System.arraycopy(nullBytes, 0, buf, pos, nullByteLen);
      }
      return pos + nullByteLen;
    }
    writeVarInt(fieldCount, out);
    writeVarInt(nullByteLen, out);
    if (nullByteLen > 0) {
      out.write(nullBytes, 0, nullByteLen);
    }
    return 0;
  }

  /**
   * Writes a 32-bit chunk null bitmask directly into {@code buf} at {@code hdrLen + chunkIdx * 4}
   * in little-endian byte order and returns the updated highest non-zero null-bitmap byte length.
   * Callers must guarantee {@code hdrLen + (chunkIdx + 1) * 4 <= buf.length}.
   */
  public static int putChunkNullMask32(
      byte[] buf, int hdrLen, int chunkIdx, int chunkMask, int highestNullByteLen) {
    int base = chunkIdx << 2;
    INT_LE.set(buf, hdrLen + base, chunkMask);
    if (chunkMask != 0) {
      return base + ((32 - Integer.numberOfLeadingZeros(chunkMask) + 7) >>> 3);
    }
    return highestNullByteLen;
  }

  /**
   * Patches the {@code beam:coder:row:v1} header prefix ({@code VarInt(fieldCount)} followed by a
   * 1-byte {@code nullByteLen} in {@code [0, 127]}) in-place at the start of {@code buf} and
   * returns the buffer position immediately after the null bitmap.
   */
  public static int patchWideNullHeaderPrefix(byte[] buf, int fieldCount, int nullByteLen) {
    int pos = putVarInt(buf, 0, fieldCount);
    buf[pos++] = (byte) nullByteLen;
    return pos + nullByteLen;
  }

  /**
   * Reads {@code nullByteLen} null-bitmap bytes into {@code scratchBuf} with zero allocation when
   * {@code nullByteLen <= scratchBuf.length}, or into a freshly allocated array for >4096 fields.
   */
  public static byte[] readNullBytes(InputStream in, int nullByteLen, byte[] scratchBuf)
      throws IOException {
    if (nullByteLen < 0) {
      throw new CoderException("Invalid null bitset length: " + nullByteLen);
    }
    if (nullByteLen == 0) {
      return scratchBuf;
    }
    byte[] target = nullByteLen <= scratchBuf.length ? scratchBuf : new byte[nullByteLen];
    readExact(in, target, nullByteLen);
    return target;
  }

  /**
   * Extracts the 32-bit null bitmask for chunk {@code chunkIdx} (fields {@code [chunkIdx*32 ..
   * chunkIdx*32 + 31]}) from {@code nullBytes[0..nullByteLen)}.
   */
  public static int extractChunkNullMask32(byte[] nullBytes, int nullByteLen, int chunkIdx) {
    int base = chunkIdx << 2;
    if (base <= nullByteLen - 4) {
      return (int) INT_LE.get(nullBytes, base);
    }
    if (base >= nullByteLen) {
      return 0;
    }
    return extractChunkNullMask32Tail(nullBytes, nullByteLen, base);
  }

  private static int extractChunkNullMask32Tail(byte[] nullBytes, int nullByteLen, int base) {
    int m = nullBytes[base] & 0xFF;
    if (base + 1 < nullByteLen) {
      m |= (nullBytes[base + 1] & 0xFF) << 8;
      if (base + 2 < nullByteLen) {
        m |= (nullBytes[base + 2] & 0xFF) << 16;
      }
    }
    return m;
  }

  /** Returns whether bit {@code pos} is set in {@code nullBytes[0..nullByteLen)}. */
  public static boolean isNullBitSet(byte[] nullBytes, int nullByteLen, int pos) {
    int byteIdx = pos >>> 3;
    return byteIdx < nullByteLen && (nullBytes[byteIdx] & (1 << (pos & 7))) != 0;
  }

  private static int fillNullBitmapFromArray(
      Object[] arr,
      int[] map,
      boolean identityPositions,
      int count,
      byte[] target,
      int targetOffset) {
    int lastNonZero = -1;
    int fullBytes = count >>> 3;
    if (identityPositions) {
      for (int bi = 0; bi < fullBytes; bi++) {
        int p = bi << 3;
        int b = 0;
        if (arr[p] == null) {
          b |= 1;
        }
        if (arr[p + 1] == null) {
          b |= 2;
        }
        if (arr[p + 2] == null) {
          b |= 4;
        }
        if (arr[p + 3] == null) {
          b |= 8;
        }
        if (arr[p + 4] == null) {
          b |= 16;
        }
        if (arr[p + 5] == null) {
          b |= 32;
        }
        if (arr[p + 6] == null) {
          b |= 64;
        }
        if (arr[p + 7] == null) {
          b |= 128;
        }
        target[targetOffset + bi] = (byte) b;
        if (b != 0) {
          lastNonZero = bi;
        }
      }
      int tailStart = fullBytes << 3;
      if (tailStart < count) {
        int b = 0;
        for (int p = tailStart, bit = 1; p < count; p++, bit <<= 1) {
          if (arr[p] == null) {
            b |= bit;
          }
        }
        target[targetOffset + fullBytes] = (byte) b;
        if (b != 0) {
          lastNonZero = fullBytes;
        }
      }
    } else {
      for (int bi = 0; bi < fullBytes; bi++) {
        int p = bi << 3;
        int b = 0;
        if (arr[map[p]] == null) {
          b |= 1;
        }
        if (arr[map[p + 1]] == null) {
          b |= 2;
        }
        if (arr[map[p + 2]] == null) {
          b |= 4;
        }
        if (arr[map[p + 3]] == null) {
          b |= 8;
        }
        if (arr[map[p + 4]] == null) {
          b |= 16;
        }
        if (arr[map[p + 5]] == null) {
          b |= 32;
        }
        if (arr[map[p + 6]] == null) {
          b |= 64;
        }
        if (arr[map[p + 7]] == null) {
          b |= 128;
        }
        target[targetOffset + bi] = (byte) b;
        if (b != 0) {
          lastNonZero = bi;
        }
      }
      int tailStart = fullBytes << 3;
      if (tailStart < count) {
        int b = 0;
        for (int p = tailStart, bit = 1; p < count; p++, bit <<= 1) {
          if (arr[map[p]] == null) {
            b |= bit;
          }
        }
        target[targetOffset + fullBytes] = (byte) b;
        if (b != 0) {
          lastNonZero = fullBytes;
        }
      }
    }
    return lastNonZero + 1;
  }

  private static int fillNullBitmapFromList(
      List<Object> list,
      int[] map,
      boolean identityPositions,
      int count,
      byte[] target,
      int targetOffset) {
    int lastNonZero = -1;
    int fullBytes = count >>> 3;
    if (identityPositions) {
      for (int bi = 0; bi < fullBytes; bi++) {
        int p = bi << 3;
        int b = 0;
        if (list.get(p) == null) {
          b |= 1;
        }
        if (list.get(p + 1) == null) {
          b |= 2;
        }
        if (list.get(p + 2) == null) {
          b |= 4;
        }
        if (list.get(p + 3) == null) {
          b |= 8;
        }
        if (list.get(p + 4) == null) {
          b |= 16;
        }
        if (list.get(p + 5) == null) {
          b |= 32;
        }
        if (list.get(p + 6) == null) {
          b |= 64;
        }
        if (list.get(p + 7) == null) {
          b |= 128;
        }
        target[targetOffset + bi] = (byte) b;
        if (b != 0) {
          lastNonZero = bi;
        }
      }
      int tailStart = fullBytes << 3;
      if (tailStart < count) {
        int b = 0;
        for (int p = tailStart, bit = 1; p < count; p++, bit <<= 1) {
          if (list.get(p) == null) {
            b |= bit;
          }
        }
        target[targetOffset + fullBytes] = (byte) b;
        if (b != 0) {
          lastNonZero = fullBytes;
        }
      }
    } else {
      for (int bi = 0; bi < fullBytes; bi++) {
        int p = bi << 3;
        int b = 0;
        if (list.get(map[p]) == null) {
          b |= 1;
        }
        if (list.get(map[p + 1]) == null) {
          b |= 2;
        }
        if (list.get(map[p + 2]) == null) {
          b |= 4;
        }
        if (list.get(map[p + 3]) == null) {
          b |= 8;
        }
        if (list.get(map[p + 4]) == null) {
          b |= 16;
        }
        if (list.get(map[p + 5]) == null) {
          b |= 32;
        }
        if (list.get(map[p + 6]) == null) {
          b |= 64;
        }
        if (list.get(map[p + 7]) == null) {
          b |= 128;
        }
        target[targetOffset + bi] = (byte) b;
        if (b != 0) {
          lastNonZero = bi;
        }
      }
      int tailStart = fullBytes << 3;
      if (tailStart < count) {
        int b = 0;
        for (int p = tailStart, bit = 1; p < count; p++, bit <<= 1) {
          if (list.get(map[p]) == null) {
            b |= bit;
          }
        }
        target[targetOffset + fullBytes] = (byte) b;
        if (b != 0) {
          lastNonZero = fullBytes;
        }
      }
    }
    return lastNonZero + 1;
  }

  /**
   * Computes the null bitmap for a wide {@code Object[]} row (>64 fields) in a single forward pass
   * and packs the header + bitmap into {@code buf} with zero heap allocation for schemas up to
   * ~4000 fields.
   */
  public static int packWideNullHeaderFromArray(
      Object[] arr, int[] map, boolean identityPositions, int count, byte[] buf, OutputStream out)
      throws IOException {
    int maxBitmapBytes = (count + 7) >>> 3;
    if (maxBitmapBytes > buf.length - MAX_HEADER_VARINT_BYTES) {
      return packHugeNullHeaderFromArray(arr, map, identityPositions, count, maxBitmapBytes, out);
    }
    int countVarLen = VarInt.getLength(count);
    int hdrReserve = countVarLen + VarInt.getLength(maxBitmapBytes);
    int numBytes = fillNullBitmapFromArray(arr, map, identityPositions, count, buf, hdrReserve);
    int hdrPos = putVarInt(buf, 0, count);
    hdrPos = putVarInt(buf, hdrPos, numBytes);
    if (hdrPos != hdrReserve && numBytes > 0) {
      System.arraycopy(buf, hdrReserve, buf, hdrPos, numBytes);
    }
    return hdrPos + numBytes;
  }

  private static int packHugeNullHeaderFromArray(
      Object[] arr,
      int[] map,
      boolean identityPositions,
      int count,
      int maxBitmapBytes,
      OutputStream out)
      throws IOException {
    byte[] bitmap = new byte[maxBitmapBytes];
    int numBytes = fillNullBitmapFromArray(arr, map, identityPositions, count, bitmap, 0);
    writeVarInt(count, out);
    writeVarInt(numBytes, out);
    if (numBytes > 0) {
      out.write(bitmap, 0, numBytes);
    }
    return 0;
  }

  /**
   * Computes the null bitmap for a wide {@code List<Object>} row (>64 fields) in a single forward
   * pass and packs the header + bitmap into {@code buf} with zero heap allocation for schemas up to
   * ~4000 fields.
   */
  public static int packWideNullHeaderFromList(
      List<Object> list,
      int[] map,
      boolean identityPositions,
      int count,
      byte[] buf,
      OutputStream out)
      throws IOException {
    int maxBitmapBytes = (count + 7) >>> 3;
    if (maxBitmapBytes > buf.length - MAX_HEADER_VARINT_BYTES) {
      return packHugeNullHeaderFromList(list, map, identityPositions, count, maxBitmapBytes, out);
    }
    int countVarLen = VarInt.getLength(count);
    int hdrReserve = countVarLen + VarInt.getLength(maxBitmapBytes);
    int numBytes = fillNullBitmapFromList(list, map, identityPositions, count, buf, hdrReserve);
    int hdrPos = putVarInt(buf, 0, count);
    hdrPos = putVarInt(buf, hdrPos, numBytes);
    if (hdrPos != hdrReserve && numBytes > 0) {
      System.arraycopy(buf, hdrReserve, buf, hdrPos, numBytes);
    }
    return hdrPos + numBytes;
  }

  private static int packHugeNullHeaderFromList(
      List<Object> list,
      int[] map,
      boolean identityPositions,
      int count,
      int maxBitmapBytes,
      OutputStream out)
      throws IOException {
    byte[] bitmap = new byte[maxBitmapBytes];
    int numBytes = fillNullBitmapFromList(list, map, identityPositions, count, bitmap, 0);
    writeVarInt(count, out);
    writeVarInt(numBytes, out);
    if (numBytes > 0) {
      out.write(bitmap, 0, numBytes);
    }
    return 0;
  }

  public static void writeHeader(int fieldCount, long nullMask, OutputStream out)
      throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packHeader(fieldCount, nullMask, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static long readNullMask(InputStream in, int nullByteLen, byte[] buf) throws IOException {
    if (nullByteLen < 0 || nullByteLen > 8) {
      throw new CoderException("Invalid 64-bit null bitset length: " + nullByteLen);
    }
    if (nullByteLen == 0) {
      return 0L;
    }
    if (buf.length < nullByteLen) {
      throw new CoderException("Scratch buffer too small for null bitset: " + buf.length);
    }
    readExact(in, buf, nullByteLen);
    long mask = 0L;
    for (int i = 0; i < nullByteLen; i++) {
      mask |= (buf[i] & 0xFFL) << (i << 3);
    }
    return mask;
  }

  // ---- Primitives (buffered pack* + buf-accepting read*) ----

  public static int packByte(byte v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= SCRATCH_SIZE) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = v;
    return pos;
  }

  public static byte readByte(InputStream in) throws IOException {
    int b = in.read();
    if (b >= 0) {
      return (byte) b;
    }
    throw eofException();
  }

  private static CoderException eofException() {
    return new CoderException(new EOFException());
  }

  private static IOException varIntNotTerminated() {
    return new IOException("varint not terminated");
  }

  private static IOException varIntTooLong() {
    return new IOException("varint too long");
  }

  public static int packBoolean(boolean v, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (pos >= SCRATCH_SIZE) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = (byte) (v ? 1 : 0);
    return pos;
  }

  public static boolean readBoolean(InputStream in) throws IOException {
    int bv = in.read();
    if ((bv & ~1) == 0) {
      return bv != 0;
    }
    throw invalidBoolean(bv);
  }

  private static IOException invalidBoolean(int bv) {
    if (bv < 0) {
      return new CoderException(new EOFException());
    }
    return new IOException(String.format("Expected 0 or 1, got %d", (byte) bv));
  }

  public static int packInt16(short v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    SHORT_BE.set(buf, pos, v);
    return pos + 2;
  }

  public static short readInt16(InputStream in, byte[] buf) throws IOException {
    readExact(in, buf, 2);
    return (short) SHORT_BE.get(buf, 0);
  }

  private static int readBigEndianInt(InputStream in, byte[] buf) throws IOException {
    readExact(in, buf, 4);
    return (int) INT_BE.get(buf, 0);
  }

  public static long readBigEndianLong(InputStream in, byte[] buf) throws IOException {
    readExact(in, buf, 8);
    return (long) LONG_BE.get(buf, 0);
  }

  public static int packVarInt(int v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    if ((v & ~0x7F) == 0) {
      buf[pos++] = (byte) v;
      return pos;
    }
    return putVarInt(buf, pos, v);
  }

  public static void writeVarInt(int v, OutputStream out) throws IOException {
    if ((v & ~0x7F) == 0) {
      out.write(v);
      return;
    }
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int len = putVarInt(buf, 0, v);
      out.write(buf, 0, len);
    } finally {
      pool.release(buf);
    }
  }

  public static int readVarInt(InputStream in) throws IOException {
    int b0 = in.read();
    if ((b0 & ~0x7F) == 0) {
      return b0;
    }
    return readVarIntSlow(b0, in);
  }

  private static int readVarIntSlow(int b0, InputStream in) throws IOException {
    if (b0 < 0) {
      throw eofException();
    }
    int b1 = in.read();
    if ((b1 & ~0x7F) == 0) {
      return (b0 & 0x7F) | (b1 << 7);
    }
    return readVarInt3to5(b0, b1, in);
  }

  private static int readVarInt3to5(int b0, int b1, InputStream in) throws IOException {
    if (b1 < 0) {
      throw varIntNotTerminated();
    }
    int r = (b0 & 0x7F) | ((b1 & 0x7F) << 7);
    int b2 = in.read();
    if ((b2 & ~0x7F) == 0) {
      return r | (b2 << 14);
    }
    if (b2 < 0) {
      throw varIntNotTerminated();
    }
    r |= (b2 & 0x7F) << 14;
    int b3 = in.read();
    if ((b3 & ~0x7F) == 0) {
      return r | (b3 << 21);
    }
    if (b3 < 0) {
      throw varIntNotTerminated();
    }
    r |= (b3 & 0x7F) << 21;
    int b4 = in.read();
    if ((b4 & ~0x0F) == 0) {
      return r | (b4 << 28);
    }
    return readVarIntOverflowOrError(r, b4, in);
  }

  private static int readVarIntOverflowOrError(int low28, int b4, InputStream in)
      throws IOException {
    long r;
    if ((b4 & ~0x7F) == 0) {
      r = (low28 & 0xFFFFFFFFL) | ((long) b4 << 28);
    } else {
      r = readVarLongTail(low28 & 0xFFFFFFFFL, b4, in);
    }
    if (r < 0L || r >= (1L << 32)) {
      throw new IOException("varint overflow " + r);
    }
    return (int) r;
  }

  public static int packVarLong(long v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    if ((v & ~0x7FL) == 0L) {
      buf[pos++] = (byte) v;
      return pos;
    }
    return putVarLong(buf, pos, v);
  }

  private static void writeVarLong(long v, OutputStream out) throws IOException {
    if ((v & ~0x7FL) == 0L) {
      out.write((int) v);
      return;
    }
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int len = putVarLong(buf, 0, v);
      out.write(buf, 0, len);
    } finally {
      pool.release(buf);
    }
  }

  public static long readVarLong(InputStream in) throws IOException {
    int b0 = in.read();
    if ((b0 & ~0x7F) == 0) {
      return b0;
    }
    return readVarLongSlow(b0, in);
  }

  private static long readVarLongSlow(int b0, InputStream in) throws IOException {
    if (b0 < 0) {
      throw eofException();
    }
    int b1 = in.read();
    if ((b1 & ~0x7F) == 0) {
      return (b0 & 0x7F) | (b1 << 7);
    }
    if (b1 < 0) {
      throw varIntNotTerminated();
    }
    int lo = (b0 & 0x7F) | ((b1 & 0x7F) << 7);
    int b2 = in.read();
    if ((b2 & ~0x7F) == 0) {
      return lo | (b2 << 14);
    }
    if (b2 < 0) {
      throw varIntNotTerminated();
    }
    lo |= (b2 & 0x7F) << 14;
    int b3 = in.read();
    if ((b3 & ~0x7F) == 0) {
      return lo | (b3 << 21);
    }
    if (b3 < 0) {
      throw varIntNotTerminated();
    }
    long res = lo | ((b3 & 0x7F) << 21);
    int b4 = in.read();
    if ((b4 & ~0x7F) == 0) {
      return res | ((long) b4 << 28);
    }
    return readVarLongTail(res, b4, in);
  }

  private static long readVarLongTail(long res, int b4, InputStream in) throws IOException {
    if (b4 < 0) {
      throw varIntNotTerminated();
    }
    res |= (b4 & 0x7FL) << 28;
    int shift = 35;
    int b;
    do {
      b = in.read();
      if (b < 0) {
        throw varIntNotTerminated();
      }
      long bits = b & 0x7FL;
      if (shift >= 64 || (shift == 63 && bits > 1L)) {
        throw varIntTooLong();
      }
      res |= bits << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return res;
  }

  public static int packFloat(float v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    return putBigEndianInt(buf, pos, Float.floatToIntBits(v));
  }

  public static float readFloat(InputStream in, byte[] buf) throws IOException {
    return Float.intBitsToFloat(readBigEndianInt(in, buf));
  }

  public static int packDouble(double v, byte[] buf, int pos, OutputStream out) throws IOException {
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    return putBigEndianLong(buf, pos, Double.doubleToLongBits(v));
  }

  public static double readDouble(InputStream in, byte[] buf) throws IOException {
    return Double.longBitsToDouble(readBigEndianLong(in, buf));
  }

  public static int packDateTime(@Nullable ReadableInstant v, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (v == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    return putBigEndianLong(buf, pos, v.getMillis() - Long.MIN_VALUE);
  }

  public static Instant readDateTime(InputStream in, byte[] buf) throws IOException {
    return new Instant(readBigEndianLong(in, buf) + Long.MIN_VALUE);
  }

  // ---- Variable-length scalars ----

  public static int packString(@Nullable String s, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (s == null) {
      throw new CoderException("cannot encode a null String");
    }
    int len = s.length();
    if (len == 0) {
      if (pos >= SCRATCH_SIZE) {
        out.write(buf, 0, pos);
        pos = 0;
      }
      buf[pos++] = 0;
      return pos;
    }
    if (len <= MAX_ASCII_STRING_FAST) {
      int hdr = len < 128 ? 1 : 2;
      pos = ensureRoom(buf, pos, hdr + len, out);
      int savedPos = pos;
      if (hdr == 1) {
        buf[pos] = (byte) len;
      } else {
        buf[pos] = (byte) ((len & 0x7F) | 0x80);
        buf[pos + 1] = (byte) (len >> 7);
      }
      int start = pos + hdr;
      for (int i = 0; i < len; i++) {
        char c = s.charAt(i);
        if (c >= 0x80) {
          if (savedPos > 0) {
            out.write(buf, 0, savedPos);
          }
          writeStringUtf8Slow(s, buf, out);
          return 0;
        }
        buf[start + i] = (byte) c;
      }
      return start + len;
    }
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    writeStringUtf8Slow(s, buf, out);
    return 0;
  }

  private static void writeStringUtf8Slow(String s, byte[] buf, OutputStream out)
      throws IOException {
    byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
    int n = putVarInt(buf, 0, utf8.length);
    out.write(buf, 0, n);
    out.write(utf8);
  }

  public static String readString(InputStream in, byte[] buf) throws IOException {
    int len = readVarInt(in);
    if (len > 0 && len <= SCRATCH_SIZE) {
      readExact(in, buf, len);
      return new String(buf, 0, len, StandardCharsets.UTF_8);
    }
    return readStringSlow(in, len);
  }

  private static String readStringSlow(InputStream in, int len) throws IOException {
    if (len == 0) {
      return "";
    }
    if (len < 0) {
      throw new CoderException("Invalid encoded string length: " + len);
    }
    byte[] bytes = new byte[len];
    readExact(in, bytes, len);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static int packBytes(byte @Nullable [] bytes, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (bytes == null) {
      throw new CoderException("cannot encode a null byte[]");
    }
    int len = bytes.length;
    if (len == 0) {
      if (pos >= SCRATCH_SIZE) {
        out.write(buf, 0, pos);
        pos = 0;
      }
      buf[pos++] = 0;
      return pos;
    }
    if (len <= MAX_INLINE_BYTES_FAST) {
      pos = ensureRoom(buf, pos, len + 2, out);
      pos = putVarInt(buf, pos, len);
      System.arraycopy(bytes, 0, buf, pos, len);
      return pos + len;
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putVarInt(buf, pos, len);
    out.write(buf, 0, pos);
    out.write(bytes);
    return 0;
  }

  public static byte[] readBytes(InputStream in) throws IOException {
    int len = readVarInt(in);
    if (len > 0) {
      byte[] bytes = new byte[len];
      readExact(in, bytes, len);
      return bytes;
    }
    return readEmptyOrInvalidBytes(len);
  }

  private static byte[] readEmptyOrInvalidBytes(int len) throws IOException {
    if (len == 0) {
      return EMPTY_BYTES;
    }
    throw new CoderException("Invalid encoded byte[] length: " + len);
  }

  public static int packDecimal(@Nullable BigDecimal bd, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (bd == null) {
      throw new NullPointerException();
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    int scale = bd.scale();
    BigInteger unscaled = bd.unscaledValue();
    int bitLen = unscaled.bitLength();
    if (bitLen <= 63) {
      long v = unscaled.longValue();
      int byteLen = (bitLen >>> 3) + 1;
      pos = putVarInt(buf, pos, scale);
      buf[pos++] = (byte) byteLen;
      for (int i = byteLen - 1; i >= 0; i--) {
        buf[pos++] = (byte) (v >>> (i << 3));
      }
      return pos;
    }
    byte[] mag = unscaled.toByteArray();
    pos = putVarInt(buf, pos, scale);
    pos = putVarInt(buf, pos, mag.length);
    out.write(buf, 0, pos);
    out.write(mag);
    return 0;
  }

  public static BigDecimal readDecimal(InputStream in, byte[] buf) throws IOException {
    int scale = readVarInt(in);
    int len = readVarInt(in);
    if (len < 0) {
      throw new IOException("invalid length " + len);
    }
    if (len >= 1 && len <= 8) {
      readExact(in, buf, len);
      long unscaled = buf[0];
      for (int i = 1; i < len; i++) {
        unscaled = (unscaled << 8) | (buf[i] & 0xFFL);
      }
      return BigDecimal.valueOf(unscaled, scale);
    }
    byte[] bytes = len == 0 ? EMPTY_BYTES : new byte[len];
    if (len > 0) {
      readExact(in, bytes, len);
    }
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  // ---- Built-in logical types ----

  private static void expectTwoNonNullFields(
      InputStream in, String typeName, boolean illegalStateOnMissing) throws IOException {
    int b0 = in.read();
    if (b0 == 2) {
      int b1 = in.read();
      if (b1 == 0) {
        return;
      }
      if (b1 < 0) {
        throw new CoderException(new EOFException());
      }
      int nullLen = (b1 & 0x80) == 0 ? b1 : readVarIntSlow(b1, in);
      checkTwoFieldNullBitmap(in, nullLen, typeName, illegalStateOnMissing);
      return;
    }
    if (b0 < 0) {
      throw new CoderException(new EOFException());
    }
    int fieldCount = (b0 & 0x80) == 0 ? b0 : readVarIntSlow(b0, in);
    int nullLen = readVarInt(in);
    checkTwoFieldNullBitmap(in, nullLen, typeName, illegalStateOnMissing);
    if (fieldCount < 2) {
      String msg =
          "While trying to convert to " + typeName + ": Row expected 2 fields, got " + fieldCount;
      if (illegalStateOnMissing) {
        throw new IllegalStateException(msg);
      }
      throw new IllegalArgumentException(msg);
    }
  }

  private static void checkTwoFieldNullBitmap(
      InputStream in, int nullLen, String typeName, boolean illegalStateOnMissing)
      throws IOException {
    if (nullLen < 0) {
      throw new CoderException("Negative null bitset length: " + nullLen);
    }
    if (nullLen == 0) {
      return;
    }
    int firstNullByte = in.read();
    if (firstNullByte < 0) {
      throw new CoderException(new EOFException());
    }
    if (nullLen > 1) {
      skipExact(in, nullLen - 1);
    }
    if ((firstNullByte & 0x03) != 0) {
      String msg = "While trying to convert to " + typeName + ": Row missing required field";
      if (illegalStateOnMissing) {
        throw new IllegalStateException(msg);
      }
      throw new IllegalArgumentException(msg);
    }
  }

  public static int packLocalDate(@Nullable LocalDate value, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null LocalDate");
    }
    return packVarLong(value.toEpochDay(), buf, pos, out);
  }

  public static void writeLocalDate(@Nullable LocalDate value, OutputStream out)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null LocalDate");
    }
    writeVarLong(value.toEpochDay(), out);
  }

  public static LocalDate readLocalDate(InputStream in) throws IOException {
    return LocalDate.ofEpochDay(readVarLong(in));
  }

  public static int packLocalTime(@Nullable LocalTime value, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null LocalTime");
    }
    return packVarLong(value.toNanoOfDay(), buf, pos, out);
  }

  public static void writeLocalTime(@Nullable LocalTime value, OutputStream out)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null LocalTime");
    }
    writeVarLong(value.toNanoOfDay(), out);
  }

  public static LocalTime readLocalTime(InputStream in) throws IOException {
    return LocalTime.ofNanoOfDay(readVarLong(in));
  }

  public static int packLocalDateTime(
      @Nullable LocalDateTime value, byte[] buf, int pos, OutputStream out) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null LocalDateTime");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = 2;
    buf[pos++] = 0;
    pos = putVarLong(buf, pos, value.toLocalDate().toEpochDay());
    return putVarLong(buf, pos, value.toLocalTime().toNanoOfDay());
  }

  public static void writeLocalDateTime(@Nullable LocalDateTime value, OutputStream out)
      throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packLocalDateTime(value, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static LocalDateTime readLocalDateTime(InputStream in) throws IOException {
    expectTwoNonNullFields(in, "LocalDateTime", false);
    long epochDay = readVarLong(in);
    long nanoOfDay = readVarLong(in);
    return LocalDateTime.of(LocalDate.ofEpochDay(epochDay), LocalTime.ofNanoOfDay(nanoOfDay));
  }

  public static int packUuid(@Nullable UUID value, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null UUID");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = 2;
    buf[pos++] = 0;
    pos = putVarLong(buf, pos, value.getMostSignificantBits());
    return putVarLong(buf, pos, value.getLeastSignificantBits());
  }

  public static void writeUuid(@Nullable UUID value, OutputStream out) throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packUuid(value, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static UUID readUuid(InputStream in) throws IOException {
    expectTwoNonNullFields(in, "UUID", true);
    long msb = readVarLong(in);
    long lsb = readVarLong(in);
    return new UUID(msb, lsb);
  }

  public static int packNanosInstant(
      java.time.@Nullable Instant value, byte[] buf, int pos, OutputStream out) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = 2;
    buf[pos++] = 0;
    pos = putVarLong(buf, pos, value.getEpochSecond());
    return putVarInt(buf, pos, value.getNano());
  }

  public static void writeNanosInstant(java.time.@Nullable Instant value, OutputStream out)
      throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packNanosInstant(value, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static java.time.Instant readNanosInstant(InputStream in) throws IOException {
    expectTwoNonNullFields(in, "Instant", false);
    long seconds = readVarLong(in);
    int nanos = readVarInt(in);
    return java.time.Instant.ofEpochSecond(seconds, nanos);
  }

  public static int packMicrosInstant(
      java.time.@Nullable Instant value, byte[] buf, int pos, OutputStream out) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    int nano = value.getNano();
    if (nano % 1000 != 0) {
      throw new AssertionError(
          "micros_instant logical type encountered a Java "
              + "Instant with greater than microsecond precision.");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = 2;
    buf[pos++] = 0;
    pos = putVarLong(buf, pos, value.getEpochSecond());
    return putVarInt(buf, pos, nano / 1000);
  }

  public static void writeMicrosInstant(java.time.@Nullable Instant value, OutputStream out)
      throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packMicrosInstant(value, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static java.time.Instant readMicrosInstant(InputStream in) throws IOException {
    expectTwoNonNullFields(in, "Instant", false);
    long seconds = readVarLong(in);
    int micros = readVarInt(in);
    return java.time.Instant.ofEpochSecond(seconds, micros * 1000L);
  }

  public static int packNanosDuration(
      @Nullable Duration value, byte[] buf, int pos, OutputStream out) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Duration");
    }
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    buf[pos++] = 2;
    buf[pos++] = 0;
    pos = putVarLong(buf, pos, value.getSeconds());
    return putVarInt(buf, pos, value.getNano());
  }

  public static void writeNanosDuration(@Nullable Duration value, OutputStream out)
      throws IOException {
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      int pos = packNanosDuration(value, buf, 0, out);
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static Duration readNanosDuration(InputStream in) throws IOException {
    expectTwoNonNullFields(in, "Duration", false);
    long seconds = readVarLong(in);
    int nanos = readVarInt(in);
    return Duration.ofSeconds(seconds, nanos);
  }

  public static void writeTimestamp(
      java.time.@Nullable Instant value, int precision, OutputStream out) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null Instant");
    }
    int scalingFactor = TIMESTAMP_SCALING_FACTORS[precision];
    int nano = value.getNano();
    int subseconds;
    if (scalingFactor == 1) {
      subseconds = nano;
    } else {
      if (nano % scalingFactor != 0) {
        throw new IllegalStateException(
            String.format(
                "Timestamp logical type was configured with precision %s, but encountered "
                    + "a Java Instant with %s nanoseconds (not evenly divisible by scaling factor %s).",
                precision, nano, scalingFactor));
      }
      subseconds = nano / scalingFactor;
    }
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      buf[0] = 2;
      buf[1] = 0;
      int pos = putVarLong(buf, 2, value.getEpochSecond());
      if (precision < 5) {
        short s = (short) subseconds;
        buf[pos++] = (byte) (s >>> 8);
        buf[pos++] = (byte) s;
      } else {
        pos = putVarInt(buf, pos, subseconds);
      }
      out.write(buf, 0, pos);
    } finally {
      pool.release(buf);
    }
  }

  public static java.time.Instant readTimestamp(int precision, InputStream in) throws IOException {
    expectTwoNonNullFields(in, "Instant", false);
    long seconds = readVarLong(in);
    long subseconds;
    if (precision < 5) {
      ScratchPool pool = scratchPool();
      byte[] buf = pool.acquire();
      try {
        subseconds = readInt16(in, buf);
      } finally {
        pool.release(buf);
      }
    } else {
      subseconds = readVarInt(in);
    }
    if (subseconds < 0) {
      throw new IllegalArgumentException(
          String.format(
              "While trying to convert to Instant: subseconds field must be non-negative, "
                  + "but was %s. This likely indicates data corruption.",
              subseconds));
    }
    int maxSubseconds = TIMESTAMP_MAX_SUBSECONDS[precision];
    if (subseconds > maxSubseconds) {
      throw new IllegalArgumentException(
          String.format(
              "While trying to convert to Instant: subseconds field must be <= %s for precision %s, "
                  + "but was %s. This likely indicates data corruption or precision mismatch.",
              maxSubseconds, precision, subseconds));
    }
    return java.time.Instant.ofEpochSecond(
        seconds, subseconds * TIMESTAMP_SCALING_FACTORS[precision]);
  }

  // ---- Wire-exact non-nullable container codecs (match ListCoder / MapCoder bit-for-bit) ----

  private static <T> List<T> readChunkedList(
      InputStream in, byte[] buf, ElementReader<T> elementReader) throws IOException {
    ArrayList<T> out = new ArrayList<>();
    long count = readVarLong(in);
    while (count > 0L) {
      out.add(elementReader.read(in, buf));
      --count;
      if (count == 0L) {
        count = readVarLong(in);
      }
    }
    if (count != 0L) {
      throw new IllegalStateException(
          String.format(
              "List does not support non zero terminator values. Received stream with terminator %s.",
              count));
    }
    return out;
  }

  public static int packStringCollection(
      @Nullable Collection<String> col, byte[] buf, int pos, OutputStream out) throws IOException {
    if (col == null) {
      throw new CoderException("cannot encode a null List");
    }
    int size = col.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    if (col instanceof List && col instanceof RandomAccess) {
      List<String> list = (List<String>) col;
      for (int i = 0; i < size; i++) {
        pos = packString(list.get(i), buf, pos, out);
      }
    } else {
      for (String s : col) {
        pos = packString(s, buf, pos, out);
      }
    }
    return pos;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int packStringIterable(
      Iterable<String> iter, byte[] buf, int pos, OutputStream out, Coder fallback)
      throws IOException {
    if (iter instanceof Collection) {
      return packStringCollection((Collection<String>) iter, buf, pos, out);
    }
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    fallback.encode(iter, out);
    return 0;
  }

  public static List<String> readStringList(InputStream in, byte[] buf) throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size >= 0) {
      ArrayList<String> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(readString(in, buf));
      }
      return out;
    }
    return readChunkedList(in, buf, RowWireCodec::readString);
  }

  public static int packInt32Collection(
      @Nullable Collection<Integer> col, byte[] buf, int pos, OutputStream out) throws IOException {
    if (col == null) {
      throw new CoderException("cannot encode a null List");
    }
    int size = col.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    if (col instanceof List && col instanceof RandomAccess) {
      List<Integer> list = (List<Integer>) col;
      for (int i = 0; i < size; i++) {
        Integer v = list.get(i);
        if (v == null) {
          throw new CoderException("cannot encode a null Integer");
        }
        pos = packVarInt(v, buf, pos, out);
      }
    } else {
      for (Integer v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Integer");
        }
        pos = packVarInt(v, buf, pos, out);
      }
    }
    return pos;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int packInt32Iterable(
      Iterable<Integer> iter, byte[] buf, int pos, OutputStream out, Coder fallback)
      throws IOException {
    if (iter instanceof Collection) {
      return packInt32Collection((Collection<Integer>) iter, buf, pos, out);
    }
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    fallback.encode(iter, out);
    return 0;
  }

  public static List<Integer> readInt32List(InputStream in, byte[] buf) throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size >= 0) {
      ArrayList<Integer> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(readVarInt(in));
      }
      return out;
    }
    return readChunkedList(in, buf, (stream, b) -> readVarInt(stream));
  }

  public static int packInt64Collection(
      @Nullable Collection<Long> col, byte[] buf, int pos, OutputStream out) throws IOException {
    if (col == null) {
      throw new CoderException("cannot encode a null List");
    }
    int size = col.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    if (col instanceof List && col instanceof RandomAccess) {
      List<Long> list = (List<Long>) col;
      for (int i = 0; i < size; i++) {
        Long v = list.get(i);
        if (v == null) {
          throw new CoderException("cannot encode a null Long");
        }
        pos = packVarLong(v, buf, pos, out);
      }
    } else {
      for (Long v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Long");
        }
        pos = packVarLong(v, buf, pos, out);
      }
    }
    return pos;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int packInt64Iterable(
      Iterable<Long> iter, byte[] buf, int pos, OutputStream out, Coder fallback)
      throws IOException {
    if (iter instanceof Collection) {
      return packInt64Collection((Collection<Long>) iter, buf, pos, out);
    }
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    fallback.encode(iter, out);
    return 0;
  }

  public static List<Long> readInt64List(InputStream in, byte[] buf) throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size >= 0) {
      ArrayList<Long> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(readVarLong(in));
      }
      return out;
    }
    return readChunkedList(in, buf, (stream, b) -> readVarLong(stream));
  }

  public static int packDoubleCollection(
      @Nullable Collection<Double> col, byte[] buf, int pos, OutputStream out) throws IOException {
    if (col == null) {
      throw new CoderException("cannot encode a null List");
    }
    int size = col.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    if (col instanceof List && col instanceof RandomAccess) {
      List<Double> list = (List<Double>) col;
      for (int i = 0; i < size; i++) {
        Double v = list.get(i);
        if (v == null) {
          throw new CoderException("cannot encode a null Double");
        }
        pos = packDouble(v, buf, pos, out);
      }
    } else {
      for (Double v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Double");
        }
        pos = packDouble(v, buf, pos, out);
      }
    }
    return pos;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static int packDoubleIterable(
      Iterable<Double> iter, byte[] buf, int pos, OutputStream out, Coder fallback)
      throws IOException {
    if (iter instanceof Collection) {
      return packDoubleCollection((Collection<Double>) iter, buf, pos, out);
    }
    if (pos > 0) {
      out.write(buf, 0, pos);
    }
    fallback.encode(iter, out);
    return 0;
  }

  public static List<Double> readDoubleList(InputStream in, byte[] buf) throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size >= 0) {
      ArrayList<Double> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(readDouble(in, buf));
      }
      return out;
    }
    return readChunkedList(in, buf, RowWireCodec::readDouble);
  }

  /** Encodes any {@link Collection} with an element {@link Coder}, avoiding iterator allocation. */
  public static <T> void writeElementCollection(
      @Nullable Collection<T> col, Coder<T> elemCoder, OutputStream out) throws IOException {
    if (col == null) {
      throw new CoderException("cannot encode a null List");
    }
    int size = col.size();
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      putBigEndianInt(buf, 0, size);
      out.write(buf, 0, 4);
    } finally {
      pool.release(buf);
    }
    if (size == 0) {
      return;
    }
    if (col instanceof List && col instanceof RandomAccess) {
      List<T> list = (List<T>) col;
      for (int i = 0; i < size; i++) {
        elemCoder.encode(list.get(i), out);
      }
    } else {
      for (T elem : col) {
        elemCoder.encode(elem, out);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> void writeElementIterable(
      Iterable<T> iter, Coder<T> elemCoder, OutputStream out, Coder fallback) throws IOException {
    if (iter instanceof Collection) {
      writeElementCollection((Collection<T>) iter, elemCoder, out);
    } else {
      fallback.encode(iter, out);
    }
  }

  public static <T> List<T> readElementList(InputStream in, Coder<T> elemCoder) throws IOException {
    int size;
    ScratchPool pool = scratchPool();
    byte[] buf = pool.acquire();
    try {
      size = readBigEndianInt(in, buf);
    } finally {
      pool.release(buf);
    }
    if (size >= 0) {
      ArrayList<T> out = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        out.add(elemCoder.decode(in));
      }
      return out;
    }
    return readChunkedList(in, EMPTY_BYTES, (stream, ignored) -> elemCoder.decode(stream));
  }

  public static int packStringLongMap(
      @Nullable Map<String, Long> map, byte[] buf, int pos, OutputStream out) throws IOException {
    if (map == null) {
      throw new CoderException("cannot encode a null Map");
    }
    int size = map.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    for (Map.Entry<String, Long> e : map.entrySet()) {
      pos = packString(e.getKey(), buf, pos, out);
      Long v = e.getValue();
      if (v == null) {
        throw new CoderException("cannot encode a null Long");
      }
      pos = packVarLong(v, buf, pos, out);
    }
    return pos;
  }

  public static Map<String, Long> readStringLongMap(InputStream in, byte[] buf) throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size == 0) {
      return Collections.emptyMap();
    }
    if (size < 0) {
      throw new CoderException("Invalid map size: " + size);
    }
    Map<String, Long> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      String k = readString(in, buf);
      long v = readVarLong(in);
      map.put(k, v);
    }
    return map;
  }

  public static int packStringStringMap(
      @Nullable Map<String, String> map, byte[] buf, int pos, OutputStream out) throws IOException {
    if (map == null) {
      throw new CoderException("cannot encode a null Map");
    }
    int size = map.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    for (Map.Entry<String, String> e : map.entrySet()) {
      pos = packString(e.getKey(), buf, pos, out);
      pos = packString(e.getValue(), buf, pos, out);
    }
    return pos;
  }

  public static Map<String, String> readStringStringMap(InputStream in, byte[] buf)
      throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size == 0) {
      return Collections.emptyMap();
    }
    if (size < 0) {
      throw new CoderException("Invalid map size: " + size);
    }
    Map<String, String> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      map.put(readString(in, buf), readString(in, buf));
    }
    return map;
  }

  public static int packStringIntMap(
      @Nullable Map<String, Integer> map, byte[] buf, int pos, OutputStream out)
      throws IOException {
    if (map == null) {
      throw new CoderException("cannot encode a null Map");
    }
    int size = map.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    for (Map.Entry<String, Integer> e : map.entrySet()) {
      pos = packString(e.getKey(), buf, pos, out);
      Integer v = e.getValue();
      if (v == null) {
        throw new CoderException("cannot encode a null Integer");
      }
      pos = packVarInt(v, buf, pos, out);
    }
    return pos;
  }

  public static Map<String, Integer> readStringIntMap(InputStream in, byte[] buf)
      throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size == 0) {
      return Collections.emptyMap();
    }
    if (size < 0) {
      throw new CoderException("Invalid map size: " + size);
    }
    Map<String, Integer> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      String k = readString(in, buf);
      int v = readVarInt(in);
      map.put(k, v);
    }
    return map;
  }

  public static int packStringDoubleMap(
      @Nullable Map<String, Double> map, byte[] buf, int pos, OutputStream out) throws IOException {
    if (map == null) {
      throw new CoderException("cannot encode a null Map");
    }
    int size = map.size();
    if (pos >= FLUSH_LIMIT) {
      out.write(buf, 0, pos);
      pos = 0;
    }
    pos = putBigEndianInt(buf, pos, size);
    if (size == 0) {
      return pos;
    }
    for (Map.Entry<String, Double> e : map.entrySet()) {
      pos = packString(e.getKey(), buf, pos, out);
      Double v = e.getValue();
      if (v == null) {
        throw new CoderException("cannot encode a null Double");
      }
      pos = packDouble(v, buf, pos, out);
    }
    return pos;
  }

  public static Map<String, Double> readStringDoubleMap(InputStream in, byte[] buf)
      throws IOException {
    int size = readBigEndianInt(in, buf);
    if (size == 0) {
      return Collections.emptyMap();
    }
    if (size < 0) {
      throw new CoderException("Invalid map size: " + size);
    }
    Map<String, Double> map = Maps.newHashMapWithExpectedSize(size);
    for (int i = 0; i < size; i++) {
      String k = readString(in, buf);
      double v = readDouble(in, buf);
      map.put(k, v);
    }
    return map;
  }

  // ---- Zero-allocation byte size helpers ----

  private static int utf8EncodedLength(String s) {
    int len = s.length();
    int i = 0;
    while (i < len && s.charAt(i) < 0x80) {
      i++;
    }
    if (i == len) {
      return len;
    }
    try {
      return Utf8.encodedLength(s);
    } catch (IllegalArgumentException e) {
      return s.getBytes(StandardCharsets.UTF_8).length;
    }
  }

  public static long sizeOfString(String s) {
    int len = utf8EncodedLength(s);
    return (long) VarInt.getLength(len) + len;
  }

  public static long sizeOfBytes(byte[] bytes) {
    return (long) VarInt.getLength(bytes.length) + bytes.length;
  }

  public static long sizeOfDecimal(BigDecimal bd) {
    int magBytes = (bd.unscaledValue().bitLength() >>> 3) + 1;
    return (long) VarInt.getLength(bd.scale()) + VarInt.getLength(magBytes) + magBytes;
  }

  public static long sizeOfLocalDate(LocalDate d) {
    return VarInt.getLength(d.toEpochDay());
  }

  public static long sizeOfLocalTime(LocalTime t) {
    return VarInt.getLength(t.toNanoOfDay());
  }

  public static long sizeOfLocalDateTime(LocalDateTime dt) {
    return 2L
        + VarInt.getLength(dt.toLocalDate().toEpochDay())
        + VarInt.getLength(dt.toLocalTime().toNanoOfDay());
  }

  public static long sizeOfUuid(UUID u) {
    return 2L
        + VarInt.getLength(u.getMostSignificantBits())
        + VarInt.getLength(u.getLeastSignificantBits());
  }

  public static long sizeOfNanosInstant(java.time.Instant i) {
    return 2L + VarInt.getLength(i.getEpochSecond()) + VarInt.getLength(i.getNano());
  }

  public static long sizeOfMicrosInstant(java.time.Instant i) {
    return 2L + VarInt.getLength(i.getEpochSecond()) + VarInt.getLength(i.getNano() / 1000);
  }

  public static long sizeOfNanosDuration(Duration d) {
    return 2L + VarInt.getLength(d.getSeconds()) + VarInt.getLength(d.getNano());
  }

  public static long sizeOfTimestamp(java.time.Instant i, int precision) {
    long secBytes = VarInt.getLength(i.getEpochSecond());
    if (precision < 5) {
      return 4L + secBytes;
    }
    int scalingFactor = TIMESTAMP_SCALING_FACTORS[precision];
    int subseconds = scalingFactor == 1 ? i.getNano() : i.getNano() / scalingFactor;
    return 2L + secBytes + VarInt.getLength(subseconds);
  }

  public static long sizeOfStringCollection(Collection<String> col) throws CoderException {
    long total = 4L;
    if (col instanceof List && col instanceof RandomAccess) {
      List<String> list = (List<String>) col;
      for (int i = 0, n = list.size(); i < n; i++) {
        String s = list.get(i);
        if (s == null) {
          throw new CoderException("cannot encode a null String");
        }
        total += sizeOfString(s);
      }
    } else {
      for (String s : col) {
        if (s == null) {
          throw new CoderException("cannot encode a null String");
        }
        total += sizeOfString(s);
      }
    }
    return total;
  }

  public static long sizeOfInt32Collection(Collection<Integer> col) throws CoderException {
    long total = 4L;
    if (col instanceof List && col instanceof RandomAccess) {
      List<Integer> list = (List<Integer>) col;
      for (int i = 0, n = list.size(); i < n; i++) {
        Integer v = list.get(i);
        if (v == null) {
          throw new CoderException("cannot encode a null Integer");
        }
        total += VarInt.getLength(v.intValue());
      }
    } else {
      for (Integer v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Integer");
        }
        total += VarInt.getLength(v.intValue());
      }
    }
    return total;
  }

  public static long sizeOfInt64Collection(Collection<Long> col) throws CoderException {
    long total = 4L;
    if (col instanceof List && col instanceof RandomAccess) {
      List<Long> list = (List<Long>) col;
      for (int i = 0, n = list.size(); i < n; i++) {
        Long v = list.get(i);
        if (v == null) {
          throw new CoderException("cannot encode a null Long");
        }
        total += VarInt.getLength(v.longValue());
      }
    } else {
      for (Long v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Long");
        }
        total += VarInt.getLength(v.longValue());
      }
    }
    return total;
  }

  public static long sizeOfDoubleCollection(Collection<Double> col) throws CoderException {
    int size = col.size();
    if (col instanceof List && col instanceof RandomAccess) {
      List<Double> list = (List<Double>) col;
      for (int i = 0; i < size; i++) {
        if (list.get(i) == null) {
          throw new CoderException("cannot encode a null Double");
        }
      }
    } else {
      for (Double v : col) {
        if (v == null) {
          throw new CoderException("cannot encode a null Double");
        }
      }
    }
    return 4L + ((long) size << 3);
  }

  public static long sizeOfStringLongMap(Map<String, Long> map) throws CoderException {
    long total = 4L;
    for (Map.Entry<String, Long> e : map.entrySet()) {
      String k = e.getKey();
      Long v = e.getValue();
      if (k == null) {
        throw new CoderException("cannot encode a null String");
      }
      if (v == null) {
        throw new CoderException("cannot encode a null Long");
      }
      total += sizeOfString(k) + VarInt.getLength(v.longValue());
    }
    return total;
  }

  public static long sizeOfStringStringMap(Map<String, String> map) throws CoderException {
    long total = 4L;
    for (Map.Entry<String, String> e : map.entrySet()) {
      String k = e.getKey();
      String v = e.getValue();
      if (k == null || v == null) {
        throw new CoderException("cannot encode a null String");
      }
      total += sizeOfString(k) + sizeOfString(v);
    }
    return total;
  }

  public static long sizeOfStringIntMap(Map<String, Integer> map) throws CoderException {
    long total = 4L;
    for (Map.Entry<String, Integer> e : map.entrySet()) {
      String k = e.getKey();
      Integer v = e.getValue();
      if (k == null) {
        throw new CoderException("cannot encode a null String");
      }
      if (v == null) {
        throw new CoderException("cannot encode a null Integer");
      }
      total += sizeOfString(k) + VarInt.getLength(v.intValue());
    }
    return total;
  }

  public static long sizeOfStringDoubleMap(Map<String, Double> map) throws CoderException {
    long total = 4L;
    for (Map.Entry<String, Double> e : map.entrySet()) {
      String k = e.getKey();
      Double v = e.getValue();
      if (k == null) {
        throw new CoderException("cannot encode a null String");
      }
      if (v == null) {
        throw new CoderException("cannot encode a null Double");
      }
      total += sizeOfString(k) + 8L;
    }
    return total;
  }

  public static long sizeOfNullMask(int highestNullPos) {
    if (highestNullPos < 0) {
      return 1L;
    }
    int numBytes = (highestNullPos >>> 3) + 1;
    return (long) VarInt.getLength(numBytes) + numBytes;
  }
}
