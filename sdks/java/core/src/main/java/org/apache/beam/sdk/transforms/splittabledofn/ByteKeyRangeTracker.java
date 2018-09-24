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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.primitives.Bytes;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A {@link RestrictionTracker} for claiming {@link ByteKey}s in a {@link ByteKeyRange} in a
 * monotonically increasing fashion. The range is a semi-open bounded interval [startKey, endKey)
 * where the limits are both represented by ByteKey.EMPTY.
 */
public class ByteKeyRangeTracker extends RestrictionTracker<ByteKeyRange, ByteKey> {
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
  public synchronized ByteKeyRange currentRestriction() {
    return range;
  }

  @Override
  public synchronized ByteKeyRange checkpoint() {
    checkState(lastClaimedKey != null, "Can't checkpoint before any key was successfully claimed");
    ByteKey nextKey = next(lastClaimedKey);
    ByteKeyRange res = ByteKeyRange.of(nextKey, range.getEndKey());
    this.range = ByteKeyRange.of(range.getStartKey(), nextKey);
    return res;
  }

  /**
   * Attempts to claim the given key.
   *
   * <p>Must be larger than the last successfully claimed key.
   *
   * @return {@code true} if the key was successfully claimed, {@code false} if it is outside the
   *     current {@link ByteKeyRange} of this tracker (in that case this operation is a no-op).
   */
  @Override
  protected synchronized boolean tryClaimImpl(ByteKey key) {
    checkArgument(
        lastAttemptedKey == null || key.compareTo(lastAttemptedKey) > 0,
        "Trying to claim key %s while last attempted was %s",
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

  /**
   * Marks that there are no more keys to be claimed in the range.
   *
   * <p>E.g., a {@link DoFn} reading a file and claiming the key of each record in the file might
   * call this if it hits EOF - even though the last attempted claim was before the end of the
   * range, there are no more keys to claim.
   */
  public synchronized void markDone() {
    lastAttemptedKey = range.getEndKey();
  }

  @Override
  public synchronized void checkDone() throws IllegalStateException {
    checkState(lastAttemptedKey != null, "Can't check if done before any key claim was attempted");
    ByteKey nextKey = next(lastAttemptedKey);
    checkState(
        nextKey.compareTo(range.getEndKey()) > -1,
        "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastAttemptedKey,
        range,
        nextKey,
        range.getEndKey());
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
}
