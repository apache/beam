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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
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
    ByteKey startKey = redisCursorToByteKey(range.getStartPosition());
    ByteKey endKey = redisCursorToByteKey(range.getEndPosition());
    // No split on an empty range.
    if (NO_KEYS.equals(range) || (!endKey.isEmpty() && startKey.equals(endKey))) {
      return null;
    }
    // There is no more remaining work after the entire range has been claimed.
    if (lastAttemptedKey != null && lastAttemptedKey.isEmpty()) {
      return null;
    }

    RedisCursor unprocessedRangeStartKey =
        (lastAttemptedKey == null)
            ? range.getStartPosition()
            : byteKeyToRedisCursor(
                next(lastAttemptedKey), range.getStartPosition().getDbSize(), true);
    RedisCursor endCursor = range.getEndPosition();
    // There is no more space for split.
    if (!redisCursorToByteKey(endCursor).isEmpty()
        && unprocessedRangeStartKey.compareTo(endCursor) >= 0) {
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
            startKey.isEmpty()
                ? NO_KEYS
                : RedisCursorRange.of(range.getStartPosition(), range.getStartPosition());
        return SplitResult.of(range, rval);
      } /* else {
          range = RedisCursorRange.of(range.getStartPosition(), unprocessedRangeStartKey);
          return SplitResult.of(range, RedisCursorRange.of(unprocessedRangeStartKey, endCursor));
        }*/
    }

    return null;
  }

  @Override
  public boolean tryClaim(RedisCursor cursor) {
    ByteKey key = redisCursorToByteKey(cursor);
    ByteKey startKey = redisCursorToByteKey(range.getStartPosition());
    ByteKey endKey = redisCursorToByteKey(range.getEndPosition());
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
        key.compareTo(startKey) > -1,
        "Trying to claim key %s before start of the range %s",
        key,
        range);

    lastAttemptedKey = key;
    // No respective checkArgument for i < range.to() - it's ok to try claiming keys beyond
    if (!endKey.isEmpty() && key.compareTo(endKey) > -1) {
      return false;
    }
    lastClaimedKey = key;
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    ByteKey endKey = redisCursorToByteKey(range.getEndPosition());
    // Handle checking the empty range which is implicitly done.
    // This case can occur if the range tracker is checkpointed before any keys have been claimed
    // or if the range tracker is checkpointed once the range is done.
    if (NO_KEYS.equals(range)
        || (!endKey.isEmpty() && range.getStartPosition().equals(range.getEndPosition()))) {
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
    if (!endKey.isEmpty() && next(lastAttemptedKey).compareTo(endKey) >= 0) {
      return;
    }

    // If the last attempted key was not at or beyond the end of the range then throw.
    if (endKey.isEmpty() || endKey.compareTo(lastAttemptedKey) > 0) {
      ByteKey nextKey = next(lastAttemptedKey);
      throw new IllegalStateException(
          String.format(
              "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
              lastAttemptedKey, range, nextKey, endKey));
    }
  }

  @VisibleForTesting
  static ByteKey next(ByteKey key) {
    return ByteKey.copyFrom(Bytes.concat(key.getBytes(), ZERO_BYTE_ARRAY));
  }

  private static final byte[] ZERO_BYTE_ARRAY = new byte[] {0};

  @Override
  public Progress getProgress() {
    ByteKey startKey = redisCursorToByteKey(range.getStartPosition());
    ByteKey endKey = redisCursorToByteKey(range.getEndPosition());
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
        || !(endKey.isEmpty() || endKey.compareTo(lastAttemptedKey) > 0)) {
      return Progress.from(1, 0);
    }

    ByteKeyRange byteKeyRange = ByteKeyRange.of(startKey, endKey);
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

  @VisibleForTesting
  static ByteKey redisCursorToByteKey(RedisCursor cursor) {
    if ("0".equals(cursor.getCursor())) {
      if (cursor.isStart()) {
        return ByteKey.of(0x00);
      } else {
        return ByteKey.EMPTY;
      }
    }
    int nBits = getTablePow(cursor.getDbSize());
    long cursorLong = Long.parseLong(cursor.getCursor());
    long reversed = shiftBits(cursorLong, nBits);
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

  @VisibleForTesting
  static long shiftBits(long a, int nBits) {
    long b = 0;
    for (int i = 0; i < nBits; ++i) {
      b <<= 1;
      b |= (a & 1);
      a >>= 1;
    }
    return b;
  }

  @VisibleForTesting
  static int getTablePow(long nKeys) {
    return 64 - Long.numberOfLeadingZeros(nKeys - 1);
  }

  @VisibleForTesting
  static RedisCursor byteKeyToRedisCursor(ByteKey byteKeyStart, long nKeys, boolean isStart) {
    if (byteKeyStart.isEmpty() || byteKeyStart.equals(RedisCursor.ZERO_KEY)) {
      return RedisCursor.of("0", nKeys, isStart);
    }
    int nBits = getTablePow(nKeys);
    ByteBuffer bb = ByteBuffer.wrap(byteKeyStart.getBytes());
    if (bb.capacity() < nBits) {
      int rem = nBits - bb.capacity();
      byte[] padding = new byte[rem];
      bb = ByteBuffer.allocate(nBits).put(padding).put(bb.array());
      bb.position(0);
    }
    long l = bb.getLong();
    l = shiftBits(l, nBits);
    return RedisCursor.of(Long.toString(l), nKeys, isStart);
  }
}
