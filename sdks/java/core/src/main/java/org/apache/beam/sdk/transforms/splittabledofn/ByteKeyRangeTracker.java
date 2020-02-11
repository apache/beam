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
package org.apache.beam.sdk.transforms.splittabledofn;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;

/**
 * A {@link RestrictionTracker} for claiming {@link ByteKey}s in a {@link ByteKeyRange} in a
 * monotonically increasing fashion. The range is a semi-open bounded interval [startKey, endKey)
 * where the limits are both represented by {@link ByteKey#EMPTY}.
 *
 * <p>Note, one can complete a range by claiming the {@link ByteKey#EMPTY} once one runs out of keys
 * to process.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class ByteKeyRangeTracker extends RestrictionTracker<ByteKeyRange, ByteKey>
    implements Sizes.HasSize {
  /* An empty range which contains no keys. */
  @VisibleForTesting
  static final ByteKeyRange NO_KEYS = ByteKeyRange.of(ByteKey.EMPTY, ByteKey.of(0x00));

  private ByteKeyRange range;
  @Nullable private ByteKey lastClaimedKey = null;
  @Nullable private ByteKey lastAttemptedKey = null;

  private ByteKeyRangeTracker(ByteKeyRange range) {
    this.range = checkNotNull(range);
  }

  public static ByteKeyRangeTracker of(ByteKeyRange range) {
    return new ByteKeyRangeTracker(ByteKeyRange.of(range.getStartKey(), range.getEndKey()));
  }

  @Override
  public ByteKeyRange currentRestriction() {
    return range;
  }

  @Override
  public SplitResult<ByteKeyRange> trySplit(double fractionOfRemainder) {
    // TODO(BEAM-8871): Add support for splitting off a fixed amount of work for this restriction
    // instead of only supporting checkpointing.

    // If we haven't done any work, we should return the original range we were processing
    // as the checkpoint.
    if (lastAttemptedKey == null) {
      ByteKeyRange rval = range;
      // We update our current range to an interval that contains no elements.
      range = NO_KEYS;
      return SplitResult.of(range, rval);
    }

    // Return an empty range if the current range is done.
    if (lastAttemptedKey.isEmpty()
        || !(range.getEndKey().isEmpty() || range.getEndKey().compareTo(lastAttemptedKey) > 0)) {
      return SplitResult.of(range, NO_KEYS);
    }

    // Otherwise we compute the "remainder" of the range from the last key.
    assert lastAttemptedKey.equals(lastClaimedKey)
        : "Expect both keys to be equal since the last key attempted was a valid key in the range.";
    ByteKey nextKey = next(lastAttemptedKey);
    ByteKeyRange res = ByteKeyRange.of(nextKey, range.getEndKey());
    this.range = ByteKeyRange.of(range.getStartKey(), nextKey);
    return SplitResult.of(range, res);
  }

  /**
   * Attempts to claim the given key.
   *
   * <p>Must be larger than the last attempted key. Since this restriction tracker represents a
   * range over a semi-open bounded interval {@code [start, end)}, the last key that was attempted
   * may have failed but still have consumed the interval {@code [lastAttemptedKey, end)} since this
   * range tracker processes keys in a monotonically increasing order. Note that passing in {@link
   * ByteKey#EMPTY} claims all keys to the end of range and can only be claimed once.
   *
   * @return {@code true} if the key was successfully claimed, {@code false} if it is outside the
   *     current {@link ByteKeyRange} of this tracker.
   */
  @Override
  public boolean tryClaim(ByteKey key) {
    // Handle claiming the end of range EMPTY key
    if (key.isEmpty()) {
      checkArgument(
          lastAttemptedKey == null || !lastAttemptedKey.isEmpty(),
          "Trying to claim key %s while last attempted key was %s",
          key,
          lastAttemptedKey);
      lastAttemptedKey = key;
      return false;
    }

    checkArgument(
        lastAttemptedKey == null || key.compareTo(lastAttemptedKey) > 0,
        "Trying to claim key %s while last attempted key was %s",
        key,
        lastAttemptedKey);
    checkArgument(
        key.compareTo(range.getStartKey()) > -1,
        "Trying to claim key %s before start of the range %s",
        key,
        range);

    lastAttemptedKey = key;
    // No respective checkArgument for i < range.to() - it's ok to try claiming keys beyond
    if (!range.getEndKey().isEmpty() && key.compareTo(range.getEndKey()) > -1) {
      return false;
    }
    lastClaimedKey = key;
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // Handle checking the empty range which is implicitly done.
    // This case can occur if the range tracker is checkpointed before any keys have been claimed
    // or if the range tracker is checkpointed once the range is done.
    if (NO_KEYS.equals(range)) {
      return;
    }

    checkState(
        lastAttemptedKey != null,
        "Key range is non-empty %s and no keys have been attempted.",
        range);

    // Return if the last attempted key was the empty key representing the end of range for
    // all ranges.
    if (lastAttemptedKey.isEmpty()) {
      return;
    }

    // If the last attempted key was not at or beyond the end of the range then throw.
    if (range.getEndKey().isEmpty() || range.getEndKey().compareTo(lastAttemptedKey) > 0) {
      ByteKey nextKey = next(lastAttemptedKey);
      throw new IllegalStateException(
          String.format(
              "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
              lastAttemptedKey, range, nextKey, range.getEndKey()));
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("range", range)
        .add("lastClaimedKey", lastClaimedKey)
        .add("lastAttemptedKey", lastAttemptedKey)
        .toString();
  }

  // Utility methods
  /**
   * Calculates the next {@link ByteKey} for a given key by incrementing by one using byte
   * arithmetic. If the input key is empty it assumes it is a lower bound and returns the 00 byte
   * array.
   */
  @VisibleForTesting
  static ByteKey next(ByteKey key) {
    return ByteKey.copyFrom(Bytes.concat(key.getBytes(), ZERO_BYTE_ARRAY));
  }

  private static final byte[] ZERO_BYTE_ARRAY = new byte[] {0};

  @Override
  public double getSize() {
    // Return 0 for the empty range which is implicitly done.
    // This case can occur if the range tracker is checkpointed before any keys have been claimed
    // or if the range tracker is checkpointed once the range is done.
    if (NO_KEYS.equals(range)) {
      return 0;
    }

    // If we are attempting to get the backlog without processing a single key, we return 1.0
    if (lastAttemptedKey == null) {
      return 1;
    }

    // Return 0 if the last attempted key was the empty key representing the end of range for
    // all ranges or the last attempted key is beyond the end of the range.
    if (lastAttemptedKey.isEmpty()
        || !(range.getEndKey().isEmpty() || range.getEndKey().compareTo(lastAttemptedKey) > 0)) {
      return 0;
    }

    return range.estimateFractionForKey(lastAttemptedKey);
  }
}
