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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TimestampRangeTracker extends RestrictionTracker<TimestampRange, Timestamp>
    implements HasProgress {

  protected TimestampRange range;
  protected @Nullable Timestamp lastAttemptedTimestamp;
  protected @Nullable Timestamp lastClaimedTimestamp;

  public TimestampRangeTracker(TimestampRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public boolean tryClaim(Timestamp position) {
    checkArgument(
        lastAttemptedTimestamp == null || position.compareTo(lastAttemptedTimestamp) > 0,
        "Trying to claim offset %s while last attempted was %s",
        position,
        lastAttemptedTimestamp);
    checkArgument(
        position.compareTo(range.getFrom()) >= 0,
        "Trying to claim offset %s before start of the range %s",
        position,
        range);
    lastAttemptedTimestamp = position;

    // It is ok to try to claim offsets beyond of the range, this will simply return false
    if (position.compareTo(range.getTo()) >= 0) {
      return false;
    }
    lastClaimedTimestamp = position;
    return true;
  }

  @Override
  public @Nullable SplitResult<TimestampRange> trySplit(double fractionOfRemainder) {
    final BigDecimal fromInNanos = toNanos(range.getFrom());
    final BigDecimal toInNanos = toNanos(range.getTo());
    final BigDecimal currentInNanos =
        lastAttemptedTimestamp == null
            ? fromInNanos.subtract(BigDecimal.ONE, DECIMAL128)
            : toNanos(lastAttemptedTimestamp);
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
  public Progress getProgress() {
    final BigDecimal fromInNanos = toNanos(range.getFrom());
    final BigDecimal toInNanos = toNanos(range.getTo());
    final BigDecimal totalWork = toInNanos.subtract(fromInNanos, DECIMAL128);

    if (lastAttemptedTimestamp == null) {
      final double workCompleted = 0D;
      final double workRemaining = 1D;

      return Progress.from(workCompleted, workRemaining);
    } else {
      final BigDecimal currentInNanos = toNanos(lastAttemptedTimestamp);
      final BigDecimal workRemainingInNanos =
          toInNanos.subtract(currentInNanos, DECIMAL128).max(BigDecimal.ZERO);

      final double workCompleted = totalWork
          .subtract(workRemainingInNanos, DECIMAL128)
          .divide(totalWork, DECIMAL128)
          .doubleValue();
      final double workRemaining = workRemainingInNanos
          .divide(totalWork, DECIMAL128)
          .doubleValue();

      return Progress.from(workCompleted, workRemaining);
    }
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
