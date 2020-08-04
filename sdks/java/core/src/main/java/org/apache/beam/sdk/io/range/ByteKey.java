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
package org.apache.beam.sdk.io.range;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString.ByteIterator;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A class representing a key consisting of an array of bytes. Arbitrary-length {@code byte[]} keys
 * are typical in key-value stores such as Google Cloud Bigtable.
 *
 * <p>Instances of {@link ByteKey} are immutable.
 *
 * <p>{@link ByteKey} implements {@link Comparable Comparable&lt;ByteKey&gt;} by comparing the
 * arrays in lexicographic order. The smallest {@link ByteKey} is a zero-length array; the successor
 * to a key is the same key with an additional 0 byte appended; and keys have unbounded size.
 *
 * <p>Note that the empty {@link ByteKey} compares smaller than all other keys, but some systems
 * have the semantic that when an empty {@link ByteKey} is used as an upper bound, it represents the
 * largest possible key. In these cases, implementors should use {@link #isEmpty} to test whether an
 * upper bound key is empty.
 */
public final class ByteKey implements Comparable<ByteKey>, Serializable {
  /** An empty key. */
  public static final ByteKey EMPTY = ByteKey.of();

  /**
   * Creates a new {@link ByteKey} backed by a copy of the data remaining in the specified {@link
   * ByteBuffer}.
   */
  public static ByteKey copyFrom(ByteBuffer value) {
    return new ByteKey(ByteString.copyFrom(value));
  }

  /**
   * Creates a new {@link ByteKey} backed by a copy of the specified {@code byte[]}.
   *
   * <p>Makes a copy of the underlying array.
   */
  public static ByteKey copyFrom(byte[] bytes) {
    return new ByteKey(ByteString.copyFrom(bytes));
  }

  /**
   * Creates a new {@link ByteKey} backed by a copy of the specified {@code int[]}. This method is
   * primarily used as a convenience to create a {@link ByteKey} in code without casting down to
   * signed Java {@link Byte bytes}:
   *
   * <pre>{@code
   * ByteKey key = ByteKey.of(0xde, 0xad, 0xbe, 0xef);
   * }</pre>
   *
   * <p>Makes a copy of the input.
   */
  public static ByteKey of(int... bytes) {
    byte[] ret = new byte[bytes.length];
    for (int i = 0; i < bytes.length; ++i) {
      ret[i] = (byte) (bytes[i] & 0xff);
    }
    return ByteKey.copyFrom(ret);
  }

  /** Returns a read-only {@link ByteBuffer} representing this {@link ByteKey}. */
  public ByteBuffer getValue() {
    return value.asReadOnlyByteBuffer();
  }

  /**
   * Returns a newly-allocated {@code byte[]} representing this {@link ByteKey}.
   *
   * <p>Copies the underlying {@code byte[]}.
   */
  public byte[] getBytes() {
    return value.toByteArray();
  }

  /** Returns {@code true} if the {@code byte[]} backing this {@link ByteKey} is of length 0. */
  public boolean isEmpty() {
    return value.isEmpty();
  }

  /**
   * {@link ByteKey} implements {@link Comparable Comparable&lt;ByteKey&gt;} by comparing the arrays
   * in lexicographic order. The smallest {@link ByteKey} is a zero-length array; the successor to a
   * key is the same key with an additional 0 byte appended; and keys have unbounded size.
   */
  @Override
  public int compareTo(@Nonnull ByteKey other) {
    checkNotNull(other, "other");
    ByteIterator thisIt = value.iterator();
    ByteIterator otherIt = other.value.iterator();
    while (thisIt.hasNext() && otherIt.hasNext()) {
      // (byte & 0xff) converts [-128,127] bytes to [0,255] ints.
      int cmp = (thisIt.nextByte() & 0xff) - (otherIt.nextByte() & 0xff);
      if (cmp != 0) {
        return cmp;
      }
    }
    // If we get here, the prefix of both arrays is equal up to the shorter array. The array with
    // more bytes is larger.
    return value.size() - other.value.size();
  }

  ////////////////////////////////////////////////////////////////////////////////////
  private final ByteString value;

  private ByteKey(ByteString value) {
    this.value = value;
  }

  /** Array used as a helper in {@link #toString}. */
  private static final char[] HEX =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  // Prints the key as a string "[deadbeef]".
  @Override
  public String toString() {
    char[] encoded = new char[2 * value.size() + 2];
    encoded[0] = '[';
    int cnt = 1;
    ByteIterator iterator = value.iterator();
    while (iterator.hasNext()) {
      byte b = iterator.nextByte();
      encoded[cnt] = HEX[(b & 0xF0) >>> 4];
      ++cnt;
      encoded[cnt] = HEX[b & 0xF];
      ++cnt;
    }
    encoded[cnt] = ']';
    return new String(encoded);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ByteKey)) {
      return false;
    }
    ByteKey other = (ByteKey) o;
    return (other.value.size() == value.size()) && this.compareTo(other) == 0;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
