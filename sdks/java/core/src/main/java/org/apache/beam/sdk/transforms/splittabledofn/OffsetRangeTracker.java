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

import java.math.BigDecimal;
import java.math.MathContext;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

/**
 * A {@link RestrictionTracker} for claiming offsets in an {@link OffsetRange} in a monotonically
 * increasing fashion.
 *
 * <p>The smallest offset is {@code Long.MIN_VALUE} and the largest offset is {@code Long.MAX_VALUE
 * - 1}.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class OffsetRangeTracker extends RestrictionTracker<OffsetRange, Long>
    implements HasProgress {
  protected OffsetRange range;
  @Nullable protected Long lastClaimedOffset = null;
  @Nullable protected Long lastAttemptedOffset = null;

  public OffsetRangeTracker(OffsetRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public OffsetRange currentRestriction() {
    return range;
  }

  @Override
  public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
    // Convert to BigDecimal in computation to prevent overflow, which may result in loss of
    // precision.
    BigDecimal cur =
        (lastAttemptedOffset == null)
            ? BigDecimal.valueOf(range.getFrom()).subtract(BigDecimal.ONE, MathContext.DECIMAL128)
            : BigDecimal.valueOf(lastAttemptedOffset);
    // split = cur + max(1, (range.getTo() - cur) * fractionOfRemainder)
    BigDecimal splitPos =
        cur.add(
            BigDecimal.valueOf(range.getTo())
                .subtract(cur, MathContext.DECIMAL128)
                .multiply(BigDecimal.valueOf(fractionOfRemainder), MathContext.DECIMAL128)
                .max(BigDecimal.ONE),
            MathContext.DECIMAL128);

    long split = splitPos.longValue();
    if (split >= range.getTo()) {
      return null;
    }
    OffsetRange res = new OffsetRange(split, range.getTo());
    this.range = new OffsetRange(range.getFrom(), split);
    return SplitResult.of(range, res);
  }

  /**
   * Attempts to claim the given offset.
   *
   * <p>Must be larger than the last successfully claimed offset.
   *
   * @return {@code true} if the offset was successfully claimed, {@code false} if it is outside the
   *     current {@link OffsetRange} of this tracker (in that case this operation is a no-op).
   */
  @Override
  public boolean tryClaim(Long i) {
    checkArgument(
        lastAttemptedOffset == null || i > lastAttemptedOffset,
        "Trying to claim offset %s while last attempted was %s",
        i,
        lastAttemptedOffset);
    checkArgument(
        i >= range.getFrom(), "Trying to claim offset %s before start of the range %s", i, range);
    lastAttemptedOffset = i;
    // No respective checkArgument for i < range.to() - it's ok to try claiming offsets beyond it.
    if (i >= range.getTo()) {
      return false;
    }
    lastClaimedOffset = i;
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    if (range.getFrom() == range.getTo()) {
      return;
    }
    checkState(
        lastAttemptedOffset != null,
        "Last attempted offset should not be null. No work was claimed in non-empty range %s.",
        range);
    checkState(
        lastAttemptedOffset >= range.getTo() - 1,
        "Last attempted offset was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastAttemptedOffset,
        range,
        lastAttemptedOffset + 1,
        range.getTo());
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("range", range)
        .add("lastClaimedOffset", lastClaimedOffset)
        .add("lastAttemptedOffset", lastAttemptedOffset)
        .toString();
  }

  @Override
  public Progress getProgress() {
    // If we have never attempted an offset, we return the length of the entire range as work
    // remaining.
    // Convert to BigDecimal in computation to prevent overflow, which may result in loss of
    // precision.
    if (lastAttemptedOffset == null) {
      return Progress.from(
          0,
          BigDecimal.valueOf(range.getTo())
              .subtract(BigDecimal.valueOf(range.getFrom()), MathContext.DECIMAL128)
              .doubleValue());
    }

    // Compute the amount of work remaining from where we are to where we are attempting to get to
    // with a minimum of zero in case we have claimed beyond the end of the range.
    BigDecimal workRemaining =
        BigDecimal.valueOf(range.getTo())
            .subtract(BigDecimal.valueOf(lastAttemptedOffset), MathContext.DECIMAL128)
            .max(BigDecimal.ZERO);
    BigDecimal totalWork =
        BigDecimal.valueOf(range.getTo())
            .subtract(BigDecimal.valueOf(range.getFrom()), MathContext.DECIMAL128);
    return Progress.from(
        totalWork.subtract(workRemaining, MathContext.DECIMAL128).doubleValue(),
        workRemaining.doubleValue());
  }
}
