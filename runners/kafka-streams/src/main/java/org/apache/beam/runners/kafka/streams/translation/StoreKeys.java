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
package org.apache.beam.runners.kafka.streams.translation;

/**
 * Byte helpers for the composite keys the runner's state and timer stores are addressed by.
 *
 * <p>Keys are built into a single pre-sized array rather than through a stream, because they are on
 * the hot path: one is built for every state cell read or written, several times per element.
 *
 * <p>Variable-length parts are written length-prefixed. A separator byte would be shorter, but both
 * an encoded Beam key (arbitrary user coder output) and a {@link
 * org.apache.beam.runners.core.StateNamespace#stringKey} can contain any byte value, so no
 * separator is safe from collisions — {@code key="a/b", ns="c"} and {@code key="a", ns="b/c"} would
 * produce the same bytes. Length prefixes also let the encoded key be read back out of a timer key,
 * which the timer scan needs.
 *
 * <p>Timestamps are written sign-flipped big-endian so that the unsigned lexicographic order Kafka
 * Streams compares keys by is the same as numeric order. That is what makes a range scan over a
 * timestamp-prefixed store return exactly the entries up to a point in time, which is how due
 * timers and the minimum watermark hold are found without scanning everything.
 */
final class StoreKeys {

  /** Bytes taken by a length prefix. */
  static final int LENGTH_BYTES = 4;

  /** Bytes taken by a sortable timestamp. */
  static final int TIMESTAMP_BYTES = 8;

  private StoreKeys() {}

  /** Bytes a length-prefixed segment occupies. */
  static int segmentLength(byte[] segment) {
    return LENGTH_BYTES + segment.length;
  }

  /** Writes {@code segment} length-prefixed at {@code offset}, returning the offset after it. */
  static int writeSegment(byte[] target, int offset, byte[] segment) {
    int next = writeLength(target, offset, segment.length);
    System.arraycopy(segment, 0, target, next, segment.length);
    return next + segment.length;
  }

  private static int writeLength(byte[] target, int offset, int length) {
    target[offset] = (byte) ((length >>> 24) & 0xff);
    target[offset + 1] = (byte) ((length >>> 16) & 0xff);
    target[offset + 2] = (byte) ((length >>> 8) & 0xff);
    target[offset + 3] = (byte) (length & 0xff);
    return offset + LENGTH_BYTES;
  }

  /** Reads the length prefix at {@code offset}. */
  static int readLength(byte[] source, int offset) {
    return ((source[offset] & 0xff) << 24)
        | ((source[offset + 1] & 0xff) << 16)
        | ((source[offset + 2] & 0xff) << 8)
        | (source[offset + 3] & 0xff);
  }

  /** Reads the length-prefixed segment starting at {@code offset}. */
  static byte[] readSegment(byte[] source, int offset) {
    int length = readLength(source, offset);
    byte[] segment = new byte[length];
    System.arraycopy(source, offset + LENGTH_BYTES, segment, 0, length);
    return segment;
  }

  /**
   * Writes a timestamp so that unsigned byte order matches numeric order: flipping the sign bit
   * maps {@link Long#MIN_VALUE}..{@link Long#MAX_VALUE} onto 0x00..&nbsp;0xff.. big-endian, so
   * negative timestamps (valid in Beam) sort before positive ones.
   */
  static int writeTimestamp(byte[] target, int offset, long millis) {
    long sortable = millis ^ Long.MIN_VALUE;
    for (int i = 0; i < TIMESTAMP_BYTES; i++) {
      target[offset + i] = (byte) ((sortable >>> (8 * (TIMESTAMP_BYTES - 1 - i))) & 0xff);
    }
    return offset + TIMESTAMP_BYTES;
  }

  /** Reads a timestamp written by {@link #writeTimestamp}. */
  static long readTimestamp(byte[] source, int offset) {
    long sortable = 0;
    for (int i = 0; i < TIMESTAMP_BYTES; i++) {
      sortable = (sortable << 8) | (source[offset + i] & 0xffL);
    }
    return sortable ^ Long.MIN_VALUE;
  }
}
