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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Verify.verify;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class representing a range of {@link ByteKey ByteKeys}.
 *
 * <p>Instances of {@link ByteKeyRange} are immutable.
 *
 * <p>A {@link ByteKeyRange} enforces the restriction that its start and end keys must form a valid,
 * non-empty range {@code [startKey, endKey)} that is inclusive of the start key and exclusive of
 * the end key.
 *
 * <p>When the end key is empty, it is treated as the largest possible key.
 *
 * <h3>Interpreting {@link ByteKey} in a {@link ByteKeyRange}</h3>
 *
 * <p>The primary role of {@link ByteKeyRange} is to provide functionality for {@link
 * #estimateFractionForKey(ByteKey)}, {@link #interpolateKey(double)}, and {@link #split(int)}.
 *
 * <p>{@link ByteKeyRange} implements these features by treating a {@link ByteKey}'s underlying
 * {@code byte[]} as the binary expansion of floating point numbers in the range {@code [0.0, 1.0]}.
 * For example, the keys {@code ByteKey.of(0x80)}, {@code ByteKey.of(0xc0)}, and {@code
 * ByteKey.of(0xe0)} are interpreted as {@code 0.5}, {@code 0.75}, and {@code 0.875} respectively.
 * The empty {@code ByteKey.EMPTY} is interpreted as {@code 0.0} when used as the start of a range
 * and {@code 1.0} when used as the end key.
 *
 * <p>Key interpolation, fraction estimation, and range splitting are all interpreted in these
 * floating-point semantics. See the respective implementations for further details. <b>Note:</b>
 * the underlying implementations of these functions use {@link BigInteger} and {@link BigDecimal},
 * so they can be slow and should not be called in hot loops. Dynamic work rebalancing will only
 * invoke these functions during periodic control operations, so they are not called on the critical
 * path.
 *
 * @see ByteKey
 */
public final class ByteKeyRange
    implements Serializable,
        HasDefaultTracker<
            ByteKeyRange, org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker> {

  private static final Logger LOG = LoggerFactory.getLogger(ByteKeyRange.class);

  /** The range of all keys, with empty start and end keys. */
  public static final ByteKeyRange ALL_KEYS = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.EMPTY);

  /**
   * Creates a new {@link ByteKeyRange} with the given start and end keys.
   *
   * <p>Note that if {@code endKey} is empty, it is treated as the largest possible key.
   *
   * @see ByteKeyRange
   * @throws IllegalArgumentException if {@code endKey} is less than or equal to {@code startKey},
   *     unless {@code endKey} is empty indicating the maximum possible {@link ByteKey}.
   */
  public static ByteKeyRange of(ByteKey startKey, ByteKey endKey) {
    return new ByteKeyRange(startKey, endKey);
  }

  /** Returns the {@link ByteKey} representing the lower bound of this {@link ByteKeyRange}. */
  public ByteKey getStartKey() {
    return startKey;
  }

  /**
   * Returns the {@link ByteKey} representing the upper bound of this {@link ByteKeyRange}.
   *
   * <p>Note that if {@code endKey} is empty, it is treated as the largest possible key.
   */
  public ByteKey getEndKey() {
    return endKey;
  }

  /** Returns {@code true} if the specified {@link ByteKey} is contained within this range. */
  public Boolean containsKey(ByteKey key) {
    return key.compareTo(startKey) >= 0 && endsAfterKey(key);
  }

  /** Returns {@code true} if the specified {@link ByteKeyRange} overlaps this range. */
  public Boolean overlaps(ByteKeyRange other) {
    // If each range starts before the other range ends, then they must overlap.
    //     { [] } -- one range inside the other   OR   { [ } ] -- partial overlap.
    return endsAfterKey(other.startKey) && other.endsAfterKey(startKey);
  }

  /**
   * Returns a list of up to {@code numSplits + 1} {@link ByteKey ByteKeys} in ascending order,
   * where the keys have been interpolated to form roughly equal sub-ranges of this {@link
   * ByteKeyRange}, assuming a uniform distribution of keys within this range.
   *
   * <p>The first {@link ByteKey} in the result is guaranteed to be equal to {@link #getStartKey},
   * and the last {@link ByteKey} in the result is guaranteed to be equal to {@link #getEndKey}.
   * Thus the resulting list exactly spans the same key range as this {@link ByteKeyRange}.
   *
   * <p>Note that the number of keys returned is not always equal to {@code numSplits + 1}.
   * Specifically, if this range is unsplittable (e.g., because the start and end keys are equal up
   * to padding by zero bytes), the list returned will only contain the start and end key.
   *
   * @throws IllegalArgumentException if the specified number of splits is less than 1
   * @see ByteKeyRange the ByteKeyRange class Javadoc for more information about split semantics.
   */
  public List<ByteKey> split(int numSplits) {
    checkArgument(numSplits > 0, "numSplits %s must be a positive integer", numSplits);

    try {
      ImmutableList.Builder<ByteKey> ret = ImmutableList.builder();
      ret.add(startKey);
      for (int i = 1; i < numSplits; ++i) {
        ret.add(interpolateKey(i / (double) numSplits));
      }
      ret.add(endKey);
      return ret.build();
    } catch (IllegalStateException e) {
      // The range is not splittable -- just return
      return ImmutableList.of(startKey, endKey);
    }
  }

  /**
   * Returns the fraction of this range {@code [startKey, endKey)} that is in the interval {@code
   * [startKey, key)}.
   *
   * @throws IllegalArgumentException if {@code key} does not fall within this range
   * @see ByteKeyRange the ByteKeyRange class Javadoc for more information about fraction semantics.
   */
  public double estimateFractionForKey(ByteKey key) {
    checkNotNull(key, "key");
    checkArgument(!key.isEmpty(), "Cannot compute fraction for an empty key");
    checkArgument(
        key.compareTo(startKey) >= 0, "Expected key %s >= range start key %s", key, startKey);

    if (key.equals(endKey)) {
      return 1.0;
    }
    checkArgument(containsKey(key), "Cannot compute fraction for %s outside this %s", key, this);

    byte[] startBytes = startKey.getBytes();
    byte[] endBytes = endKey.getBytes();
    byte[] keyBytes = key.getBytes();
    // If the endKey is unspecified, add a leading 1 byte to it and a leading 0 byte to all other
    // keys, to get a concrete least upper bound for the desired range.
    if (endKey.isEmpty()) {
      startBytes = addHeadByte(startBytes, (byte) 0);
      endBytes = addHeadByte(endBytes, (byte) 1);
      keyBytes = addHeadByte(keyBytes, (byte) 0);
    }

    // Pad to the longest of all 3 keys.
    int paddedKeyLength = Math.max(Math.max(startBytes.length, endBytes.length), keyBytes.length);
    BigInteger rangeStartInt = paddedPositiveInt(startBytes, paddedKeyLength);
    BigInteger rangeEndInt = paddedPositiveInt(endBytes, paddedKeyLength);
    BigInteger keyInt = paddedPositiveInt(keyBytes, paddedKeyLength);

    // Keys are equal subject to padding by 0.
    BigInteger range = rangeEndInt.subtract(rangeStartInt);
    if (range.equals(BigInteger.ZERO)) {
      LOG.warn(
          "Using 0.0 as the default fraction for this near-empty range {} where start and end keys"
              + " differ only by trailing zeros.",
          this);
      return 0.0;
    }

    // Compute the progress (key-start)/(end-start) scaling by 2^64, dividing (which rounds),
    // and then scaling down after the division. This gives ample precision when converted to
    // double.
    BigInteger progressScaled = keyInt.subtract(rangeStartInt).shiftLeft(64);
    return progressScaled.divide(range).doubleValue() / Math.pow(2, 64);
  }

  /**
   * Returns a {@link ByteKey} {@code key} such that {@code [startKey, key)} represents
   * approximately the specified fraction of the range {@code [startKey, endKey)}. The interpolation
   * is computed assuming a uniform distribution of keys.
   *
   * <p>For example, given the largest possible range (defined by empty start and end keys), the
   * fraction {@code 0.5} will return the {@code ByteKey.of(0x80)}, which will also be returned for
   * ranges {@code [0x40, 0xc0)} and {@code [0x6f, 0x91)}.
   *
   * <p>The key returned will never be empty.
   *
   * @throws IllegalArgumentException if {@code fraction} is outside the range [0, 1)
   * @throws IllegalStateException if this range cannot be interpolated
   * @see ByteKeyRange the ByteKeyRange class Javadoc for more information about fraction semantics.
   */
  public ByteKey interpolateKey(double fraction) {
    checkArgument(
        fraction >= 0.0 && fraction < 1.0, "Fraction %s must be in the range [0, 1)", fraction);
    byte[] startBytes = startKey.getBytes();
    byte[] endBytes = endKey.getBytes();
    // If the endKey is unspecified, add a leading 1 byte to it and a leading 0 byte to all other
    // keys, to get a concrete least upper bound for the desired range.
    if (endKey.isEmpty()) {
      startBytes = addHeadByte(startBytes, (byte) 0);
      endBytes = addHeadByte(endBytes, (byte) 1);
    }

    // Pad to the longest key.
    int paddedKeyLength = Math.max(startBytes.length, endBytes.length);
    BigInteger rangeStartInt = paddedPositiveInt(startBytes, paddedKeyLength);
    BigInteger rangeEndInt = paddedPositiveInt(endBytes, paddedKeyLength);

    // If the keys are equal subject to padding by 0, we can't interpolate.
    BigInteger range = rangeEndInt.subtract(rangeStartInt);
    checkState(
        !range.equals(BigInteger.ZERO),
        "Refusing to interpolate for near-empty %s where start and end keys differ only by trailing"
            + " zero bytes.",
        this);

    // Add precision so that range is at least 53 (double mantissa length) bits long. This way, we
    // can interpolate small ranges finely, e.g., split the range key 3 to key 4 into 1024 parts.
    // We add precision to range by adding zero bytes to the end of the keys, aka shifting the
    // underlying BigInteger left by a multiple of 8 bits.
    int bytesNeeded = ((53 - range.bitLength()) + 7) / 8;
    if (bytesNeeded > 0) {
      range = range.shiftLeft(bytesNeeded * 8);
      rangeStartInt = rangeStartInt.shiftLeft(bytesNeeded * 8);
      paddedKeyLength += bytesNeeded;
    }

    BigInteger interpolatedOffset =
        new BigDecimal(range).multiply(BigDecimal.valueOf(fraction)).toBigInteger();

    int outputKeyLength = endKey.isEmpty() ? (paddedKeyLength - 1) : paddedKeyLength;
    return ByteKey.copyFrom(
        fixupHeadZeros(rangeStartInt.add(interpolatedOffset).toByteArray(), outputKeyLength));
  }

  /** Returns new {@link ByteKeyRange} like this one, but with the specified start key. */
  public ByteKeyRange withStartKey(ByteKey startKey) {
    return new ByteKeyRange(startKey, endKey);
  }

  /** Returns new {@link ByteKeyRange} like this one, but with the specified end key. */
  public ByteKeyRange withEndKey(ByteKey endKey) {
    return new ByteKeyRange(startKey, endKey);
  }

  ////////////////////////////////////////////////////////////////////////////////////
  private final ByteKey startKey;
  private final ByteKey endKey;

  private ByteKeyRange(ByteKey startKey, ByteKey endKey) {
    this.startKey = checkNotNull(startKey, "startKey");
    this.endKey = checkNotNull(endKey, "endKey");
    checkArgument(
        endKey.isEmpty() || startKey.compareTo(endKey) <= 0,
        "Start %s must be less than or equal to end %s",
        startKey,
        endKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ByteKeyRange.class)
        .add("startKey", startKey)
        .add("endKey", endKey)
        .toString();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ByteKeyRange)) {
      return false;
    }
    ByteKeyRange other = (ByteKeyRange) o;
    return Objects.equals(startKey, other.startKey) && Objects.equals(endKey, other.endKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startKey, endKey);
  }

  /** Returns a copy of the specified array with the specified byte added at the front. */
  private static byte[] addHeadByte(byte[] array, byte b) {
    byte[] ret = new byte[array.length + 1];
    ret[0] = b;
    System.arraycopy(array, 0, ret, 1, array.length);
    return ret;
  }

  /**
   * Ensures the array is exactly {@code size} bytes long. Returns the input array if the condition
   * is met, otherwise either adds or removes zero bytes from the beginning of {@code array}.
   */
  private static byte[] fixupHeadZeros(byte[] array, int size) {
    int padding = size - array.length;
    if (padding == 0) {
      return array;
    }

    if (padding < 0) {
      // There is one zero byte at the beginning, added by BigInteger to make there be a sign
      // bit when converting to bytes.
      verify(
          padding == -1,
          "key %s: expected length %s with exactly one byte of padding, found %s",
          ByteKey.copyFrom(array),
          size,
          -padding);
      verify(
          (array[0] == 0) && ((array[1] & 0x80) == 0x80),
          "key %s: is 1 byte longer than expected, indicating BigInteger padding. Expect first byte"
              + " to be zero with set MSB in second byte.",
          ByteKey.copyFrom(array));
      return Arrays.copyOfRange(array, 1, array.length);
    }

    byte[] ret = new byte[size];
    System.arraycopy(array, 0, ret, padding, array.length);
    return ret;
  }

  /**
   * Returns {@code true} when the specified {@code key} is smaller this range's end key. The only
   * semantic change from {@code (key.compareTo(getEndKey()) < 0)} is that the empty end key is
   * treated as larger than all possible {@link ByteKey keys}.
   */
  boolean endsAfterKey(ByteKey key) {
    return endKey.isEmpty() || key.compareTo(endKey) < 0;
  }

  /** Builds a BigInteger out of the specified array, padded to the desired byte length. */
  private static BigInteger paddedPositiveInt(byte[] bytes, int length) {
    int bytePaddingNeeded = length - bytes.length;
    checkArgument(
        bytePaddingNeeded >= 0, "Required bytes.length {} < length {}", bytes.length, length);
    BigInteger ret = new BigInteger(1, bytes);
    return (bytePaddingNeeded == 0) ? ret : ret.shiftLeft(8 * bytePaddingNeeded);
  }

  @Override
  public org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker newTracker() {
    return org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker.of(this);
  }
}
