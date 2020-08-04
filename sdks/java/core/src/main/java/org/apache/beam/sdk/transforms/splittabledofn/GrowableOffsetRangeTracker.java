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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;

/**
 * An {@link OffsetRangeTracker} for tracking a growable offset range. {@code Long.MAX_VALUE} is
 * used as the end of the range to indicate infinity.
 *
 * <p>An offset range is considered growable when the end offset could grow (or change) during
 * execution time (e.g., Kafka topic partition offset, appended file, ...).
 *
 * <p>The growable range is marked as done by claiming {@code Long.MAX_VALUE}.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class GrowableOffsetRangeTracker extends OffsetRangeTracker {
  /**
   * Provides the estimated end offset of the range.
   *
   * <p>{@link #estimate} is called to give the end offset when {@link #trySplit} or {@link
   * #getProgress} is invoked. The end offset is exclusive for the range. The estimated end is not
   * required to monotonically increase as it will only be taken into consideration when the
   * estimated end offset is larger than the current position. Returning {@code Long.MAX_VALUE} as
   * the estimate implies the largest possible position for the range is {@code Long.MAX_VALUE - 1}.
   * Return {@code Long.MIN_VALUE} if an estimate can not be provided.
   *
   * <p>Providing a good estimate is important for an accurate progress signal and will impact
   * splitting decisions by the runner.
   *
   * <p>If {@link #estimate} is expensive to compute, consider wrapping the implementation with
   * {@link Suppliers#memoizeWithExpiration} or equivalent as an optimization.
   *
   * <p>TODO(BEAM-10032): Also consider using {@link RangeEndEstimator} when the range is not ended
   * with {@code Long.MAX_VALUE}.
   */
  @FunctionalInterface
  public interface RangeEndEstimator {
    long estimate();
  }

  private final RangeEndEstimator rangeEndEstimator;

  public GrowableOffsetRangeTracker(long start, RangeEndEstimator rangeEndEstimator) {
    super(new OffsetRange(start, Long.MAX_VALUE));
    this.rangeEndEstimator = checkNotNull(rangeEndEstimator);
  }

  @Override
  public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
    // If current tracking range is no longer growable, split it as a normal range.
    if (range.getTo() != Long.MAX_VALUE || range.getTo() == range.getFrom()) {
      return super.trySplit(fractionOfRemainder);
    }
    // If current range has been done, there is no more space to split.
    if (lastAttemptedOffset != null && lastAttemptedOffset == Long.MAX_VALUE) {
      return null;
    }
    BigDecimal cur =
        (lastAttemptedOffset == null)
            ? BigDecimal.valueOf(range.getFrom()).subtract(BigDecimal.ONE, MathContext.DECIMAL128)
            : BigDecimal.valueOf(lastAttemptedOffset);

    // Fetch the estimated end offset. If the estimated end is smaller than the next offset, use
    // the next offset as end.
    BigDecimal estimateRangeEnd =
        BigDecimal.valueOf(rangeEndEstimator.estimate())
            .max(cur.add(BigDecimal.ONE, MathContext.DECIMAL128));

    // Convert to BigDecimal in computation to prevent overflow, which may result in loss of
    // precision.
    // split = cur + max(1, (estimateRangeEnd - cur) * fractionOfRemainder)
    BigDecimal splitPos =
        cur.add(
            estimateRangeEnd
                .subtract(cur, MathContext.DECIMAL128)
                .multiply(BigDecimal.valueOf(fractionOfRemainder), MathContext.DECIMAL128)
                .max(BigDecimal.ONE),
            MathContext.DECIMAL128);
    long split = splitPos.longValue();
    if (split > estimateRangeEnd.longValue()) {
      return null;
    }
    OffsetRange res = new OffsetRange(split, range.getTo());
    this.range = new OffsetRange(range.getFrom(), split);
    return SplitResult.of(range, res);
  }

  @Override
  public Progress getProgress() {
    // If current tracking range is no longer growable, get progress as a normal range.
    if (range.getTo() != Long.MAX_VALUE || range.getTo() == range.getFrom()) {
      return super.getProgress();
    }

    // Convert to BigDecimal in computation to prevent overflow, which may result in lost of
    // precision.
    BigDecimal estimateRangeEnd = BigDecimal.valueOf(rangeEndEstimator.estimate());

    if (lastAttemptedOffset == null) {
      return Progress.from(
          0,
          estimateRangeEnd
              .subtract(BigDecimal.valueOf(range.getFrom()), MathContext.DECIMAL128)
              .max(BigDecimal.ZERO)
              .doubleValue());
    }

    BigDecimal workRemaining =
        estimateRangeEnd
            .subtract(BigDecimal.valueOf(lastAttemptedOffset), MathContext.DECIMAL128)
            .max(BigDecimal.ZERO);
    BigDecimal totalWork =
        estimateRangeEnd
            .max(BigDecimal.valueOf(lastAttemptedOffset))
            .subtract(BigDecimal.valueOf(range.getFrom()), MathContext.DECIMAL128);
    return Progress.from(
        totalWork.subtract(workRemaining, MathContext.DECIMAL128).doubleValue(),
        workRemaining.doubleValue());
  }

  @Override
  public IsBounded isBounded() {
    // If current range has been done, the range should be bounded.
    if (lastAttemptedOffset != null && lastAttemptedOffset == Long.MAX_VALUE) {
      return IsBounded.BOUNDED;
    }
    return range.getTo() == Long.MAX_VALUE ? IsBounded.UNBOUNDED : IsBounded.BOUNDED;
  }
}
