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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.io.range.ByteKey;

public class RedisCursor implements Comparable<RedisCursor>, Serializable {

  public static final RedisCursor ZERO_CURSOR = RedisCursor.of("0", 8);

  private final String cursor;
  private final ByteKey byteCursor;
  private final long dbSize;
  private final int nBits;

  public static RedisCursor of(String cursor, long dbSize) {
    return new RedisCursor(cursor, dbSize);
  }

  public static RedisCursor of(ByteKey byteCursor, long dbSize) {
    return new RedisCursor(byteCursor, dbSize);
  }

  private RedisCursor(ByteKey byteCursor, long dbSize) {
    this.byteCursor = byteCursor;
    this.dbSize = dbSize;
    this.nBits = getTablePow(dbSize);
    this.cursor = byteKeyToString(byteCursor, nBits);
  }

  private RedisCursor(String cursor, long dbSize) {
    this.cursor = cursor;
    this.byteCursor = stringCursorToByteKey(cursor);
    this.dbSize = dbSize;
    this.nBits = getTablePow(dbSize);
  }

  /**
   * {@link RedisCursor} implements {@link Comparable Comparable&lt;RedisCursor&gt;} by transforming
   * the cursors to an index of the Redis table.
   */
  @Override
  public int compareTo(@Nonnull RedisCursor other) {
    checkNotNull(other, "other");
    return Long.compare(Long.parseLong(cursor), Long.parseLong(other.cursor));
  }

  public String getCursor() {
    return cursor;
  }

  public ByteKey getByteCursor() {
    return byteCursor;
  }

  public long getDbSize() {
    return dbSize;
  }

  private ByteKey stringCursorToByteKey(String cursor) {
    long cursorLong = Long.parseLong(cursor);
    long reversed = shiftBits(cursorLong);
    BigEndianLongCoder coder = BigEndianLongCoder.of();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try {
      coder.encode(reversed, os);
    } catch (IOException e) {
      throw new IllegalArgumentException("invalid redis cursor " + cursor);
    }
    byte[] byteArray = os.toByteArray();
    return ByteKey.copyFrom(byteArray);
  }

  private long shiftBits(long a) {
    long b = 0;
    for (int i = 0; i < nBits; ++i) {
      b <<= 1;
      b |= (a & 1);
      a >>= 1;
    }
    return b;
  }

  private int getTablePow(long nKeys) {
    return 64 - Long.numberOfLeadingZeros(nKeys - 1);
  }

  private String byteKeyToString(ByteKey byteKeyStart, int nBites) {
    ByteBuffer bb = ByteBuffer.wrap(byteKeyStart.getBytes());
    if (bb.capacity() < nBites) {
      int rem = nBites - bb.capacity();
      byte[] padding = new byte[rem];
      bb = ByteBuffer.allocate(nBites).put(padding).put(bb.array());
      bb.position(0);
    }
    long l = bb.getLong();
    return Long.toString(l);
  }
}
