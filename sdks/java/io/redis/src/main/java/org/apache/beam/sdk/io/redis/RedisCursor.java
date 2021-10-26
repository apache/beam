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

import java.io.Serializable;
import javax.annotation.Nonnull;

public class RedisCursor implements Comparable<RedisCursor>, Serializable {

  public static final String START_CURSOR = "0";
  public static final String END_CURSOR = "0";

  private final String cursor;
  private final long nKeys;
  private final long index;

  public static RedisCursor of(String cursor, long nKeys) throws IllegalStateException {
    if (nKeys == 0) {
      throw new IllegalStateException("zero keys");
    }
    return new RedisCursor(cursor, nKeys);
  }

  private RedisCursor(String cursor, long nKeys) {
    this.cursor = cursor;
    this.nKeys = nKeys;
    this.index = toIndex();
  }

  /**
   * {@link RedisCursor} implements {@link Comparable Comparable&lt;RedisCursor&gt;} by transformig
   * the cursors to an index of the Redis table.
   */
  @Override
  public int compareTo(@Nonnull RedisCursor other) {
    checkNotNull(other, "other");
    return Long.compare(index, other.index);
  }

  private int toIndex() {
    int cursorInt = Integer.parseInt(cursor);
    StringBuilder cursorBuilder = new StringBuilder(Integer.toBinaryString(cursorInt)).reverse();
    double pow = getTablePow();
    while (cursorBuilder.length() < pow) {
      cursorBuilder.append("0");
    }
    return Integer.parseInt(cursorBuilder.toString(), 2);
  }

  private double getTablePow() {
    return 64 - Long.numberOfLeadingZeros(nKeys - 1);
  }

  public String getCursor() {
    return cursor;
  }

  public long getIndex() {
    return index;
  }
}
