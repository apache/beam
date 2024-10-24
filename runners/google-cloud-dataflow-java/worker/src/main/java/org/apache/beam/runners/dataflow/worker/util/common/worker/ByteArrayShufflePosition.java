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

import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.UnsafeByteOperations;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents a position of a {@code GroupingShuffleReader} as an opaque array of bytes, encoded in
 * a way such that lexicographic ordering of the bytes is consistent with the inherent ordering of
 * {@code GroupingShuffleReader} positions.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ByteArrayShufflePosition implements Comparable<ShufflePosition>, ShufflePosition {
  private static final ByteString ZERO = ByteString.copyFrom(new byte[] {0});
  private final ByteString position;

  public ByteArrayShufflePosition(ByteString position) {
    this.position = position;
  }

  public static ByteArrayShufflePosition fromBase64(String position) {
    return ByteArrayShufflePosition.of(decodeBase64(position));
  }

  public static ByteArrayShufflePosition of(byte[] position) {
    if (position == null) {
      return null;
    }
    return new ByteArrayShufflePosition(UnsafeByteOperations.unsafeWrap(position));
  }

  public static ByteArrayShufflePosition of(ByteString position) {
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
    return adapter.getPosition().toByteArray();
  }

  public ByteString getPosition() {
    return position;
  }

  public String encodeBase64() {
    return encodeBase64URLSafeString(position.toByteArray());
  }

  /**
   * Returns the {@link ByteArrayShufflePosition} that immediately follows this one, i.e. there are
   * no possible {@link ByteArrayShufflePosition ByteArrayShufflePositions} between this and its
   * successor.
   */
  public ByteArrayShufflePosition immediateSuccessor() {
    return new ByteArrayShufflePosition(position.concat(ZERO));
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof ByteArrayShufflePosition) {
      ByteArrayShufflePosition that = (ByteArrayShufflePosition) o;
      return this.position.equals(that.position);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return position.hashCode();
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
    return ByteString.unsignedLexicographicalComparator().compare(position, other.position);
  }
}
