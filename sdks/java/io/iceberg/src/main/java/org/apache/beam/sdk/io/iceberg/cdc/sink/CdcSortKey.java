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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.beam.sdk.values.ValueKind;

/**
 * Builds the byte-comparable secondary sort key used by {@code SortValues} (extensions/sorter) to
 * order each (destination, shard, window) group: one primary key's records come out contiguous,
 * ordered by sequence number then {@link #kindRank(ValueKind)} within the key.
 *
 * <p>The key is {@code [pkLen:4][pkBytes][seq ^ Long.MIN_VALUE:8][kindRank:1]}, big-endian.
 */
final class CdcSortKey {

  private CdcSortKey() {}

  /** Ranks change kinds so before-images sort before after-images at an equal {@code seq}. */
  public static byte kindRank(ValueKind kind) {
    switch (kind) {
      case UPDATE_BEFORE:
        return 0;
      case DELETE:
        return 1;
      case UPDATE_AFTER:
        return 2;
      case INSERT:
        return 3;
      default:
        throw new IllegalArgumentException("Unknown ValueKind: " + kind);
    }
  }

  /**
   * Encodes the deterministic, byte-comparable sort key {@code [pkLen:4][pkBytes][seq ^
   * Long.MIN_VALUE:8][kindRank:1]} for one CDC record.
   *
   * <p>SortValues compares unsigned lexicographic byte order. The length prefix is needed to
   * accurately compare two primary keys of varying byte-lengths. Flipping the sequence number's
   * sign bit makes unsigned byte order match signed numeric order. kindRank breaks equal-seq ties.
   */
  public static byte[] encode(byte[] pkBytes, long seq, ValueKind kind) {
    return ByteBuffer.allocate(4 + pkBytes.length + 9)
        .putInt(pkBytes.length)
        .put(pkBytes)
        .putLong(seq ^ Long.MIN_VALUE)
        .put(kindRank(kind))
        .array();
  }

  /**
   * Whether two encoded sort keys carry the same primary key, compared on the raw {@code
   * [pkLen:4][pkBytes]} prefix.
   */
  public static boolean samePk(byte[] a, byte[] b) {
    int aPkEnd = 4 + ByteBuffer.wrap(a).getInt(0);
    int bPkEnd = 4 + ByteBuffer.wrap(b).getInt(0);
    return Arrays.equals(a, 0, aPkEnd, b, 0, bPkEnd);
  }
}
