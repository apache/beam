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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Stopwatch;
import org.joda.time.Duration;

/**
 * OffsetByteRangeTracker is an unbounded restriction tracker for Pub/Sub lite partitions that
 * tracks offsets for checkpointing and bytes for progress.
 *
 * <p>Any valid instance of an OffsetByteRangeTracker tracks one of exactly two types of ranges: -
 * Unbounded ranges whose last offset is Long.MAX_VALUE - Completed ranges that are either empty
 * (From == To) or fully claimed (lastClaimed == To - 1)
 *
 * <p>Also prevents splitting until minTrackingTime has passed or minBytesReceived have been
 * received. IMPORTANT: minTrackingTime must be strictly smaller than the SDF read timeout when it
 * would return ProcessContinuation.resume().
 */
class OffsetByteRangeTracker extends TrackerWithProgress {
  private final TopicBacklogReader unownedBacklogReader;
  private final Duration minTrackingTime;
  private final long minBytesReceived;
  private final Stopwatch stopwatch;
  private OffsetByteRange range;
  private @Nullable Long lastClaimed;

  public OffsetByteRangeTracker(
      OffsetByteRange range,
      TopicBacklogReader unownedBacklogReader,
      Stopwatch stopwatch,
      Duration minTrackingTime,
      long minBytesReceived) {
    checkArgument(
        range.getRange().getTo() == Long.MAX_VALUE,
        "May only construct OffsetByteRangeTracker with an unbounded range with no progress.");
    checkArgument(
        range.getByteCount() == 0L,
        "May only construct OffsetByteRangeTracker with an unbounded range with no progress.");
    this.unownedBacklogReader = unownedBacklogReader;
    this.minTrackingTime = minTrackingTime;
    this.minBytesReceived = minBytesReceived;
    this.stopwatch = stopwatch.reset().start();
    this.range = range;
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public boolean tryClaim(OffsetByteProgress position) {
    long toClaim = position.lastOffset().value();
    checkArgument(
        lastClaimed == null || toClaim > lastClaimed,
        "Trying to claim offset %s while last attempted was %s",
        position.lastOffset().value(),
        lastClaimed);
    checkArgument(
        toClaim >= range.getRange().getFrom(),
        "Trying to claim offset %s before start of the range %s",
        toClaim,
        range);
    // split() has already been called, truncating this range. No more offsets may be claimed.
    if (range.getRange().getTo() != Long.MAX_VALUE) {
      boolean isRangeEmpty = range.getRange().getTo() == range.getRange().getFrom();
      boolean isValidClosedRange = nextOffset() == range.getRange().getTo();
      checkState(
          isRangeEmpty || isValidClosedRange,
          "Violated class precondition: offset range improperly split. Please report a beam bug.");
      return false;
    }
    lastClaimed = toClaim;
    range = OffsetByteRange.of(range.getRange(), range.getByteCount() + position.batchBytes());
    return true;
  }

  @Override
  public OffsetByteRange currentRestriction() {
    return range;
  }

  private long nextOffset() {
    checkState(lastClaimed == null || lastClaimed < Long.MAX_VALUE);
    return lastClaimed == null ? currentRestriction().getRange().getFrom() : lastClaimed + 1;
  }

  /**
   * Whether the tracker has received enough data/been running for enough time that it can
   * checkpoint and be confident it can get sufficient throughput.
   */
  private boolean receivedEnough() {
    Duration duration = Duration.millis(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    if (duration.isLongerThan(minTrackingTime)) {
      return true;
    }
    if (currentRestriction().getByteCount() >= minBytesReceived) {
      return true;
    }
    return false;
  }

  @Override
  public @Nullable SplitResult<OffsetByteRange> trySplit(double fractionOfRemainder) {
    // Cannot split a bounded range. This should already be completely claimed.
    if (range.getRange().getTo() != Long.MAX_VALUE) {
      return null;
    }
    if (!receivedEnough()) {
      return null;
    }
    range =
        OffsetByteRange.of(
            new OffsetRange(currentRestriction().getRange().getFrom(), nextOffset()),
            range.getByteCount());
    return SplitResult.of(
        this.range, OffsetByteRange.of(new OffsetRange(nextOffset(), Long.MAX_VALUE), 0));
  }

  @Override
  @SuppressWarnings("unboxing.of.nullable")
  public void checkDone() throws IllegalStateException {
    if (range.getRange().getFrom() == range.getRange().getTo()) {
      return;
    }
    checkState(
        lastClaimed != null,
        "Last attempted offset should not be null. No work was claimed in non-empty range %s.",
        range);
    long lastClaimedNotNull = checkNotNull(lastClaimed);
    checkState(
        lastClaimedNotNull >= range.getRange().getTo() - 1,
        "Last attempted offset was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastClaimedNotNull,
        range,
        lastClaimedNotNull + 1,
        range.getRange().getTo());
  }

  @Override
  public Progress getProgress() {
    ComputeMessageStatsResponse stats =
        this.unownedBacklogReader.computeMessageStats(Offset.of(nextOffset()));
    return Progress.from(range.getByteCount(), stats.getMessageBytes());
  }
}
