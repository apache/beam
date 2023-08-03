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
package org.apache.beam.sdk.io.redis;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedisCursor implements Comparable<RedisCursor>, Serializable {

  private final String cursor;
  private final long dbSize;
  private final boolean isStart;
  private static BigEndianLongCoder coder = BigEndianLongCoder.of();

  public static final ByteKey ZERO_KEY = ByteKey.of(0x00);
  // The zero cursor that represents the start position is represented as ByteKey.of(0x00)
  public static final RedisCursor ZERO_CURSOR = RedisCursor.of("0", 8, true);
  // The zero cursor that represents the start position is represented as ByteKey.EMPTY
  public static final RedisCursor END_CURSOR = RedisCursor.of("0", 8, false);

  public static RedisCursor of(String cursor, long dbSize, boolean isStart) {
    return new RedisCursor(cursor, dbSize, isStart);
  }

  private RedisCursor(String cursor, long dbSize, boolean isStart) {
    this.cursor = cursor;
    this.dbSize = dbSize;
    this.isStart = isStart;
  }

  /**
   * {@link RedisCursor} implements {@link Comparable Comparable&lt;RedisCursor&gt;} by transforming
   * the cursors to an index of the Redis table.
   */
  @Override
  public int compareTo(@Nonnull RedisCursor other) {
    checkNotNull(other, "other");
    if ("0".equals(cursor) && "0".equals(other.cursor)) {
      if (isStart && !other.isStart()) {
        return -1;
      } else if (!isStart && other.isStart()) {
        return 1;
      } else {
        return 0;
      }
    }
    return Long.compare(Long.parseLong(cursor), Long.parseLong(other.cursor));
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RedisCursor that = (RedisCursor) o;
    return dbSize == that.dbSize && isStart == that.isStart && Objects.equals(cursor, that.cursor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cursor, dbSize, isStart);
  }

  public String getCursor() {
    return cursor;
  }

  public long getDbSize() {
    return dbSize;
  }

  public boolean isStart() {
    return isStart;
  }

  /*
   In order to write the functions that converts to and from a Redis cursor and a ByteKey the following article and
   its sources were used as reference as they explain in detail how a Redis cursor is formed and how to transform it to
   a sequential value that represents the current position of the scan cursor in Redis tables.
   https://engineering.q42.nl/redis-scan-cursor/
  */
  @VisibleForTesting
  static ByteKey redisCursorToByteKey(RedisCursor cursor) {
    if ("0".equals(cursor.getCursor())) {
      if (cursor.isStart()) {
        return ByteKey.of(0x00);
      } else {
        return ByteKey.EMPTY;
      }
    }
    int nBits = getTablePow(cursor.getDbSize());
    long cursorLong = Long.parseLong(cursor.getCursor());
    long reversed = shiftBits(cursorLong, nBits);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      coder.encode(reversed, os);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid redis cursor " + cursor);
    }
    byte[] byteArray = os.toByteArray();
    return ByteKey.copyFrom(byteArray);
  }

  @VisibleForTesting
  static long shiftBits(long a, int nBits) {
    long b = 0;
    for (int i = 0; i < nBits; ++i) {
      b <<= 1;
      b |= (a & 1);
      a >>= 1;
    }
    return b;
  }

  @VisibleForTesting
  static int getTablePow(long nKeys) {
    return 64 - Long.numberOfLeadingZeros(nKeys - 1);
  }

  @VisibleForTesting
  static RedisCursor byteKeyToRedisCursor(ByteKey byteKeyStart, long nKeys, boolean isStart) {
    if (byteKeyStart.isEmpty()) {
      return RedisCursor.of("0", nKeys, false);
    } else if (byteKeyStart.equals(ByteKey.of(0x00))) {
      return RedisCursor.of("0", nKeys, true);
    }
    int nBits = getTablePow(nKeys);
    ByteBuffer bb = ByteBuffer.wrap(byteKeyStart.getBytes());
    long l = bb.getLong();
    l = shiftBits(l, nBits);
    return RedisCursor.of(Long.toString(l), nKeys, isStart);
  }
}
