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
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.joda.time.Instant;

/**
 * A {@link RestrictionTracker} for claiming offsets in an {@link OrderedTimeRange} in a
 * monotonically increasing fashion.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class OrderedTimeRangeTracker extends RestrictionTracker<OrderedTimeRange, Long>
    implements HasProgress {
  private OrderedTimeRange range;
  private OffsetRangeTracker offsetRangeTracker;
  @Nullable private Long lastClaimedOffset = null;
  @Nullable private Long lastAttemptedOffset = null;

  public OrderedTimeRangeTracker(OrderedTimeRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public OrderedTimeRange currentRestriction() {
    return range;
  }

  @Override
  public SplitResult<OrderedTimeRange> trySplit(double fractionOfRemainder) {
    long cur =
        (lastAttemptedOffset == null) ? range.getFrom().minus(1L).getMillis() : lastAttemptedOffset;
    long splitPos =
        cur
            + Math.max(
                1L,
                (Double.valueOf((range.getTo().getMillis() - cur) * fractionOfRemainder))
                    .longValue());
    if (splitPos >= range.getTo().getMillis()) {
      return null;
    }
    OrderedTimeRange res = new OrderedTimeRange(Instant.ofEpochMilli(splitPos), range.getTo());
    this.range = new OrderedTimeRange(range.getFrom(), Instant.ofEpochMilli(splitPos));
    return SplitResult.of(range, res);
  }

  /**
   * Attempts to claim the given offset.
   *
   * <p>Must be larger than the last successfully claimed offset.
   *
   * @return {@code true} if the offset was successfully claimed, {@code false} if it is outside the
   *     current {@link OrderedTimeRange} of this tracker (in that case this operation is a no-op).
   */
  @Override
  public boolean tryClaim(Long i) {
    checkArgument(
        lastAttemptedOffset == null || i > lastAttemptedOffset,
        "Trying to claim offset %s while last attempted was %s",
        Instant.ofEpochMilli(i),
        lastAttemptedOffset == null ? null : Instant.ofEpochMilli(lastAttemptedOffset));
    checkArgument(
        i >= range.getFrom().getMillis(),
        "Trying to claim offset %s before start of the range %s",
        Instant.ofEpochMilli(i),
        range);
    lastAttemptedOffset = i;
    // No respective checkArgument for i < range.to() - it's ok to try claiming offsets beyond it.
    if (i >= range.getTo().getMillis()) {
      return false;
    }
    lastClaimedOffset = i;
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    if (range.getFrom().getMillis() == range.getTo().getMillis()) {
      return;
    }
    if (lastAttemptedOffset == null) {
      throw new IllegalStateException("lastAttemptedOffset should not be null");
    }
    checkState(
        lastAttemptedOffset >= range.getTo().getMillis() - 1,
        "Last attempted offset was %s in range %s, claiming work in [%s, %s) was not attempted",
        Instant.ofEpochMilli(lastAttemptedOffset),
        range,
        Instant.ofEpochMilli(lastAttemptedOffset + 1),
        range.getTo());
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
    if (lastAttemptedOffset == null) {
      return Progress.from(0, range.getTo().getMillis() - range.getFrom().getMillis());
    }

    // Compute the amount of work remaining from where we are to where we are attempting to get to
    // with a minimum of zero in case we have claimed beyond the end of the range.
    long workRemaining = Math.max(range.getTo().getMillis() - lastAttemptedOffset, 0);
    return Progress.from(
        range.getTo().getMillis() - range.getFrom().getMillis() - workRemaining, workRemaining);
  }
}
