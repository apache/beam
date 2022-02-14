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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.transforms.splittabledofn.ByteKeyRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This restriction tracker provides a decorator on top of the {@link ByteKeyRangeTracker} to allow
 * for using a {@link TimestampRange} and claim a {@link Timestamp} position.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TimestampRangeTracker extends RestrictionTracker<TimestampRange, Timestamp>
    implements HasProgress {

  private final ByteKeyRangeTracker tracker;
  private TimestampRange range;
  protected Timestamp lastAttemptedTimestamp;

  /**
   * It creates an internal {@link ByteKeyRangeTracker} from the received {@link TimestampRange}.
   * The conversion is lossless, so the start and end timestamps are represented internally as
   * {@link ByteKey}s, with a second and nanosecond component.
   *
   * @param range closed / open range interval for the start / end times of the given partition
   */
  public TimestampRangeTracker(TimestampRange range) {
    this.range = range;
    this.tracker =
        ByteKeyRangeTracker.of(
            ByteKeyRange.of(toByteKey(range.getFrom()), toByteKey(range.getTo())));
  }

  /**
   * Attempts to claim the given position.
   *
   * <p>Must be larger than the last successfully claimed position.
   *
   * @return {@code true} if the position was successfully claimed, {@code false} if it is outside
   *     the current {@link TimestampRange} of this tracker (in that case this operation is a
   *     no-op).
   */
  @Override
  public boolean tryClaim(Timestamp position) {
    final boolean canClaim = tracker.tryClaim(toByteKey(position));
    lastAttemptedTimestamp = position;

    return canClaim;
  }

  @Override
  public @Nullable SplitResult<TimestampRange> trySplit(double fractionOfRemainder) {
    final SplitResult<ByteKeyRange> result = tracker.trySplit(fractionOfRemainder);
    if (result == null) {
      return null;
    }

    final TimestampRange primary = toTimestampRange(result.getPrimary());
    final TimestampRange residual = toTimestampRange(result.getResidual());
    this.range = primary;
    return SplitResult.of(primary, residual);
  }

  @Override
  public TimestampRange currentRestriction() {
    return range;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // If the range is empty, it is done
    if (range.getFrom().compareTo(range.getTo()) == 0) {
      return;
    }

    // If nothing was attempted, throws an exception
    checkState(
        lastAttemptedTimestamp != null,
        "Key range is non-empty %s and no keys have been attempted.",
        range);

    // If the end of the range was claimed, it is done
    if (next(lastAttemptedTimestamp).compareTo(range.getTo()) >= 0) {
      return;
    }

    // If the last attempt was not the end of the range, throws an exception
    if (lastAttemptedTimestamp.compareTo(range.getTo()) < 0) {
      final Timestamp nextTimestamp = next(lastAttemptedTimestamp);
      throw new IllegalStateException(
          String.format(
              "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
              lastAttemptedTimestamp, range, nextTimestamp, range.getTo()));
    }
  }

  @Override
  public IsBounded isBounded() {
    return tracker.isBounded();
  }

  @Override
  public Progress getProgress() {
    return tracker.getProgress();
  }

  private TimestampRange toTimestampRange(ByteKeyRange range) {
    if (range == null) {
      return null;
    }

    return TimestampRange.of(toTimestamp(range.getStartKey()), toTimestamp(range.getEndKey()));
  }

  /**
   * Converts the given timestamp to a byte array. The byte array has 12 positions, the 8 first
   * positions are allocated for the seconds part of the timestamp (which is a long) and the next 4
   * positions are allocated for the nanoseconds part of the timestamp (which is an integer).
   *
   * @param timestamp the timestamp to be converted
   * @return 12 bytes containing the seconds in the first 8 bytes and the nanoseconds in the last 4
   *     bytes
   */
  protected ByteKey toByteKey(Timestamp timestamp) {
    return ByteKey.copyFrom(
        ByteBuffer.allocate(Long.BYTES + Integer.BYTES)
            .putLong(timestamp.getSeconds())
            .putInt(timestamp.getNanos())
            .array());
  }

  /**
   * Reads an array in which the first 8 bytes contain the seconds part of the timestamp and the
   * last 4 bytes contain the nanoseconds part of the timestamp. In total the array must have 12
   * bytes (positions). It returns a timestamp constructed from the seconds and nanoseconds read.
   *
   * @param key a {@link ByteKey} containing seconds in the first 8 bytes and nanoseconds in the
   *     last 4 bytes
   * @return a timestamp with the seconds and nanoseconds read
   */
  protected Timestamp toTimestamp(ByteKey key) {
    final ByteBuffer buffer = key.getValue();
    final long seconds = buffer.getLong();
    final int nanos = buffer.getInt();

    return Timestamp.ofTimeSecondsAndNanos(seconds, nanos);
  }

  private Timestamp next(Timestamp timestamp) {
    if (timestamp.equals(Timestamp.MAX_VALUE)) {
      return timestamp;
    }
    return Timestamp.ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos() + 1);
  }
}
