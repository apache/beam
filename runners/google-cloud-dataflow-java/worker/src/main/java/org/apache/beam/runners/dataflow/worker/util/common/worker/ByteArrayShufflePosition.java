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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static com.google.api.client.util.Base64.decodeBase64;
import static com.google.api.client.util.Base64.encodeBase64URLSafeString;

import java.util.Arrays;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents a position of a {@code GroupingShuffleReader} as an opaque array of bytes, encoded in
 * a way such that lexicographic ordering of the bytes is consistent with the inherent ordering of
 * {@code GroupingShuffleReader} positions.
 */
public class ByteArrayShufflePosition implements Comparable<ShufflePosition>, ShufflePosition {
  private final byte[] position;

  public ByteArrayShufflePosition(byte[] position) {
    this.position = position;
  }

  public static ByteArrayShufflePosition fromBase64(String position) {
    return ByteArrayShufflePosition.of(decodeBase64(position));
  }

  public static ByteArrayShufflePosition of(byte[] position) {
    if (position == null) {
      return null;
    }
    return new ByteArrayShufflePosition(position);
  }

  public static byte[] getPosition(@Nullable ShufflePosition shufflePosition) {
    if (shufflePosition == null) {
      return null;
    }
    Preconditions.checkArgument(shufflePosition instanceof ByteArrayShufflePosition);
    ByteArrayShufflePosition adapter = (ByteArrayShufflePosition) shufflePosition;
    return adapter.getPosition();
  }

  public byte[] getPosition() {
    return position;
  }

  public String encodeBase64() {
    return encodeBase64URLSafeString(position);
  }

  /**
   * Returns the {@link ByteArrayShufflePosition} that immediately follows this one, i.e. there are
   * no possible {@link ByteArrayShufflePosition ByteArrayShufflePositions} between this and its
   * successor.
   */
  public ByteArrayShufflePosition immediateSuccessor() {
    return new ByteArrayShufflePosition(Bytes.concat(position, new byte[] {0}));
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof ByteArrayShufflePosition) {
      ByteArrayShufflePosition that = (ByteArrayShufflePosition) o;
      return Arrays.equals(this.position, that.position);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(position);
  }

  @Override
  public String toString() {
    return "ShufflePosition(base64:" + encodeBase64() + ")";
  }

  /** May only compare homogenous ByteArrayShufflePosition types. */
  @Override
  public int compareTo(ShufflePosition o) {
    if (this == o) {
      return 0;
    }
    ByteArrayShufflePosition other = (ByteArrayShufflePosition) o;
    return UnsignedBytes.lexicographicalComparator().compare(position, other.position);
  }
}
