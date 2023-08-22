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

import static java.math.MathContext.DECIMAL128;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.next;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.toNanos;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.toTimestamp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.util.function.Supplier;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RestrictionTracker} for claiming positions in a {@link TimestampRange} in a
 * monotonically increasing fashion.
 *
 * <p>The smallest position is {@link Timestamp#MIN_VALUE} and the largest position is {@code
 * Timestamp.MAX_VALUE - 1 nanosecond}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TimestampRangeTracker extends RestrictionTracker<TimestampRange, Timestamp>
    implements HasProgress {

  private static final Logger LOG = LoggerFactory.getLogger(TimestampRangeTracker.class);
  protected TimestampRange range;
  protected @Nullable Timestamp lastAttemptedPosition;
  protected @Nullable Timestamp lastClaimedPosition;
  protected Supplier<Timestamp> timeSupplier;

  public TimestampRangeTracker(TimestampRange range) {
    this.range = checkNotNull(range);
    this.timeSupplier = () -> Timestamp.now();
  }

  @VisibleForTesting
  public void setTimeSupplier(Supplier<Timestamp> timeSupplier) {
    this.timeSupplier = timeSupplier;
  }

  /**
   * Attempts to claim the given position. Depending on the position following outcomes are
   * possible:
   *
   * <ul>
   *   <li>If the position is less than or equal to a previous attempted one, an {@link
   *       IllegalArgumentException} will be thrown
   *   <li>If the position is less than the restriction range start, an {@link
   *       IllegalArgumentException} will be thrown
   *   <li>If the position is greater than or equal to the range end, the position will not be
   *       claimed
   *   <li>If the position is greater or equal to the range start and less than the range end, the
   *       position will be claimed
   * </ul>
   *
   * If an error is not thrown, this function will register the position as the {@code
   * lastAttemptedPosition}.
   *
   * @return {@code true} if the position was successfully claimed, {@code false} otherwise
   */
  @Override
  public boolean tryClaim(Timestamp position) {
    checkArgument(
        lastAttemptedPosition == null || position.compareTo(lastAttemptedPosition) > 0,
        "Trying to claim offset %s while last attempted was %s",
        position,
        lastAttemptedPosition);
    checkArgument(
        position.compareTo(range.getFrom()) >= 0,
        "Trying to claim offset %s before start of the range %s",
        position,
        range);
    lastAttemptedPosition = position;

    // It is ok to try to claim offsets beyond of the range, this will simply return false
    if (position.compareTo(range.getTo()) >= 0) {
      return false;
    }
    lastClaimedPosition = position;
    return true;
  }

  public boolean tryClaim(Timestamp position, PartitionMetadata partitionMetadata) {
    checkArgument(
        lastAttemptedPosition == null || position.compareTo(lastAttemptedPosition) > 0,
        "Trying to claim offset %s while last attempted was %s. The partition metadata: %s",
        position,
        lastAttemptedPosition,
        partitionMetadata.toString());
    checkArgument(
        position.compareTo(range.getFrom()) >= 0,
        "Trying to claim offset %s before start of the range %s. The partition metadata: %s",
        position,
        range,
        partitionMetadata.toString());
    lastAttemptedPosition = position;

    // It is ok to try to claim offsets beyond of the range, this will simply return false
    if (position.compareTo(range.getTo()) >= 0) {
      return false;
    }
    lastClaimedPosition = position;
    return true;
  }

  /**
   * Splits the restriction through the following algorithm:
   *
   * <pre>
   *    currentPosition = lastAttemptedPosition == null ? (from - 1ns) : lastAttemptedPosition
   *    splitPosition = currentPosition + max(1, (range.to - currentPosition) * fractionOfRemainder)
   *    primary = [range.from, splitPosition)
   *    residual = [splitPosition, range.to)
   *    this.range = primary
   * </pre>
   *
   * If the {@code splitPosition} is greater than the {@code range.to}, {@code null} will be
   * returned. For checkpoints the {@code fractionOfRemainder} will always be zero.
   *
   * @return a {@link SplitResult} if a split was possible or {@code null} if the {@code
   *     splitPosition} is beyond the end of the range.
   */
  @Override
  public @Nullable SplitResult<TimestampRange> trySplit(double fractionOfRemainder) {
    final BigDecimal fromInNanos = toNanos(range.getFrom());
    final BigDecimal toInNanos = toNanos(range.getTo());
    final BigDecimal currentInNanos =
        lastAttemptedPosition == null
            ? fromInNanos.subtract(BigDecimal.ONE, DECIMAL128)
            : toNanos(lastAttemptedPosition);
    final BigDecimal nanosOffset =
        toInNanos
            .subtract(currentInNanos, DECIMAL128)
            .multiply(BigDecimal.valueOf(fractionOfRemainder), DECIMAL128)
            .max(BigDecimal.ONE);

    // splitPosition = current + max(1, (range.getTo() - current) * fractionOfRemainder)
    final BigDecimal splitPositionInNanos = currentInNanos.add(nanosOffset, DECIMAL128);
    final Timestamp splitPosition = toTimestamp(splitPositionInNanos);

    if (splitPosition.compareTo(range.getTo()) >= 0) {
      return null;
    }

    final TimestampRange primary = new TimestampRange(range.getFrom(), splitPosition);
    final TimestampRange residual = new TimestampRange(splitPosition, range.getTo());
    this.range = primary;
    return SplitResult.of(primary, residual);
  }

  /**
   * Checks if the restriction has been processed successfully. If not, throws an {@link
   * IllegalStateException}.
   *
   * <p>The restriction is considered processed successfully if:
   *
   * <ul>
   *   <li>The range is empty ({@code range.from == range.to})
   *   <li>The {@code lastAttemptedPosition + 1ns >= range.to}
   * </ul>
   *
   * The restriction is considered not processed successfully if:
   *
   * <ul>
   *   <li>No position claim was attempted for a non-empty range
   *   <li>The {@code lastAttemptedPosition + 1ns < range.to}
   * </ul>
   */
  @Override
  public void checkDone() throws IllegalStateException {
    // If the range is empty, it is done
    if (range.getFrom().compareTo(range.getTo()) == 0) {
      return;
    }

    // If nothing was attempted, throws an exception
    checkState(
        lastAttemptedPosition != null,
        "Key range is non-empty %s and no keys have been attempted.",
        range);

    // If the end of the range was not attempted, throws an exception
    final Timestamp nextPosition = next(lastAttemptedPosition);
    if (nextPosition.compareTo(range.getTo()) < 0) {
      throw new IllegalStateException(
          String.format(
              "Last attempted key was %s in range %s, claiming work in [%s, %s) was not attempted",
              lastAttemptedPosition, range, nextPosition, range.getTo()));
    }
  }

  /**
   * Returns the progress made within the restriction so far. If lastAttemptedPosition is null, the
   * start of the range is used as the completed work; otherwise, lastAttemptedPosition will be
   * used. The time gap between lastAttemptedPosition and now is used as the remaining work. In this
   * way, when the time gap becomes large, we will have more backlog to process and we should add
   * more resources.
   *
   * @return work completed and work remaining in seconds.
   */
  @Override
  public Progress getProgress() {
    BigDecimal end;
    if (range.getTo().compareTo(Timestamp.MAX_VALUE) == 0) {
      // When the given end timestamp equals to Timestamp.MAX_VALUE, this means that
      // the end timestamp is not specified which should be a streaming job. So we
      // use now() as the end timestamp.
      end = BigDecimal.valueOf(timeSupplier.get().getSeconds());
    } else {
      end = BigDecimal.valueOf(range.getTo().getSeconds());
    }
    BigDecimal current;
    if (lastClaimedPosition == null) {
      current = BigDecimal.valueOf(range.getFrom().getSeconds());
    } else {
      current = BigDecimal.valueOf(lastClaimedPosition.getSeconds());
    }
    // The remaining work must be greater than 0. Otherwise, it will cause an issue
    // that the watermark does not advance.
    final BigDecimal workRemaining = end.subtract(current).max(BigDecimal.ONE);

    LOG.debug(
        "Reported progress current: {}, end: {}, workRemaining: {}",
        current.doubleValue(),
        end.doubleValue(),
        workRemaining.doubleValue());

    return Progress.from(current.doubleValue(), workRemaining.doubleValue());
  }

  @Override
  public TimestampRange currentRestriction() {
    return range;
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }
}
