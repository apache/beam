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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Bytes;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class RedisCursorRangeTracker extends RestrictionTracker<RedisCursorRange, RedisCursor>
    implements RestrictionTracker.HasProgress {

  private RedisCursorRange range;
  private @Nullable ByteKey lastClaimedKey = null;
  private @Nullable ByteKey lastAttemptedKey = null;

  /* An empty range which contains no keys. */
  @VisibleForTesting
  static final RedisCursorRange NO_KEYS =
      RedisCursorRange.of(RedisCursor.ZERO_CURSOR, RedisCursor.ZERO_CURSOR);

  private RedisCursorRangeTracker(RedisCursorRange range) {
    this.range = checkNotNull(range);
  }

  public static RedisCursorRangeTracker of(RedisCursorRange range) {
    return new RedisCursorRangeTracker(
        RedisCursorRange.of(range.getStartPosition(), range.getEndPosition()));
  }

  @Override
  public RedisCursorRange currentRestriction() {
    return range;
  }

  @Override
  public SplitResult<RedisCursorRange> trySplit(double fractionOfRemainder) {
    // No split on an empty range.
    if (NO_KEYS.equals(range)
        || (!range.getEndPosition().getByteCursor().isEmpty()
            && range
                .getStartPosition()
                .getByteCursor()
                .equals(range.getEndPosition().getByteCursor()))) {
      return null;
    }
    // There is no more remaining work after the entire range has been claimed.
    if (lastAttemptedKey != null && lastAttemptedKey.isEmpty()) {
      return null;
    }

    RedisCursor unprocessedRangeStartKey =
        (lastAttemptedKey == null)
            ? range.getStartPosition()
            : RedisCursor.of(next(lastAttemptedKey), range.getStartPosition().getDbSize());
    RedisCursor endKey = range.getEndPosition();
    // There is no more space for split.
    if (!endKey.getByteCursor().isEmpty() && unprocessedRangeStartKey.compareTo(endKey) >= 0) {
      return null;
    }

    // Treat checkpoint specially because {@link ByteKeyRange#interpolateKey} computes a key with
    // trailing zeros when fraction is 0.
    if (fractionOfRemainder == 0.0) {
      // If we haven't done any work, we should return the original range we were processing
      // as the checkpoint.
      if (lastAttemptedKey == null) {
        // We update our current range to an interval that contains no elements.
        RedisCursorRange rval = range;
        range =
            range.getStartPosition().getByteCursor().isEmpty()
                ? NO_KEYS
                : RedisCursorRange.of(range.getStartPosition(), range.getStartPosition());
        return SplitResult.of(range, rval);
      } else {
        range = RedisCursorRange.of(range.getStartPosition(), unprocessedRangeStartKey);
        return SplitResult.of(range, RedisCursorRange.of(unprocessedRangeStartKey, endKey));
      }
    }

    return null;
  }

  @Override
  public boolean tryClaim(RedisCursor key) {
    // Handle claiming the end of range EMPTY key
    if (key.getByteCursor().isEmpty()) {
      checkArgument(
          lastAttemptedKey == null || !lastAttemptedKey.isEmpty(),
          "Trying to claim key %s while last attempted key was %s",
          key,
          lastAttemptedKey);
      lastAttemptedKey = key.getByteCursor();
      return false;
    }

    if (range.getStartPosition().getCursor().equals(RedisCursor.ZERO_CURSOR.getCursor())
        && range.getEndPosition().getCursor().equals(RedisCursor.ZERO_CURSOR.getCursor())) {
      lastAttemptedKey = key.getByteCursor();
      range.fromEndPosition(key);
      return true;
    }

    checkArgument(
        lastAttemptedKey == null || key.getByteCursor().compareTo(lastAttemptedKey) > 0,
        "Trying to claim key %s while last attempted key was %s",
        key,
        lastAttemptedKey);
    checkArgument(
        key.compareTo(range.getStartPosition()) >= 0,
        "Trying to claim key %s before start of the range %s",
        key,
        range);

    lastAttemptedKey = key.getByteCursor();
    // No respective checkArgument for i < range.to() - it's ok to try claiming keys beyond
    if (!range.getEndPosition().getByteCursor().isEmpty()
        && key.compareTo(range.getEndPosition()) >= 0) {
      return false;
    }
    lastClaimedKey = key.getByteCursor();
    range = range.fromEndPosition(key);
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // Handle checking the empty range which is implicitly done.
    // This case can occur if the range tracker is checkpointed before any keys have been claimed
    // or if the range tracker is checkpointed once the range is done.
    if (NO_KEYS.equals(range)
        || (!range.getEndPosition().getByteCursor().isEmpty()
            && range.getStartPosition().equals(range.getEndPosition()))) {
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

    // The lastAttemptedKey is the last key of current restriction.
    if (!range.getEndPosition().getByteCursor().isEmpty()
        && next(lastAttemptedKey).compareTo(range.getEndPosition().getByteCursor()) >= 0) {
      return;
    }

    // If the last attempted key was not at or beyond the end of the range then throw.
    if (range.getEndPosition().getByteCursor().isEmpty()
        || range.getEndPosition().getByteCursor().compareTo(lastAttemptedKey) > 0) {
      ByteKey nextKey = next(lastAttemptedKey);
      throw new IllegalStateException(
          String.format(
              "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
              lastAttemptedKey, range, nextKey, range.getEndPosition().getByteCursor()));
    }
  }

  @VisibleForTesting
  static ByteKey next(ByteKey key) {
    return ByteKey.copyFrom(Bytes.concat(key.getBytes(), ZERO_BYTE_ARRAY));
  }

  private static final byte[] ZERO_BYTE_ARRAY = new byte[] {0};

  @Override
  public Progress getProgress() {
    // Return [0,0] for the empty range which is implicitly done.
    // This case can occur if the range tracker is checkpointed before any keys have been claimed
    // or if the range tracker is checkpointed once the range is done.
    if (NO_KEYS.equals(range)) {
      return Progress.from(0, 0);
    }

    // If we are attempting to get the backlog without processing a single key, we return [0,1]
    if (lastAttemptedKey == null) {
      return Progress.from(0, 1);
    }

    // Return [1,0] if the last attempted key was the empty key representing the end of range for
    // all ranges or the last attempted key is beyond the end of the range.
    if (lastAttemptedKey.isEmpty()
        || !(range.getEndPosition().getByteCursor().isEmpty()
            || range.getEndPosition().getByteCursor().compareTo(lastAttemptedKey) > 0)) {
      return Progress.from(1, 0);
    }

    ByteKeyRange byteKeyRange =
        ByteKeyRange.of(
            range.getStartPosition().getByteCursor(), range.getEndPosition().getByteCursor());
    double workCompleted = byteKeyRange.estimateFractionForKey(lastAttemptedKey);
    return Progress.from(workCompleted, 1 - workCompleted);
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("range", range)
        .add("lastClaimedKey", lastClaimedKey)
        .add("lastAttemptedKey", lastAttemptedKey)
        .toString();
  }
}
