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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;

/**
 * OffsetByteRangeTracker is an unbounded restriction tracker for Pub/Sub lite partitions that
 * tracks offsets for checkpointing and bytes for progress.
 */
class OffsetByteRangeTracker extends TrackerWithProgress {
  private final TopicBacklogReader unownedBacklogReader;
  private final OffsetRangeTracker rangeTracker;
  private long bytes;
  private @Nullable Long lastClaimed;

  public OffsetByteRangeTracker(OffsetByteRange range, TopicBacklogReader unownedBacklogReader) {
    checkArgument(
        range.getRange().getTo() == Long.MAX_VALUE,
        "May only construct OffsetByteRangeTracker with an unbounded range with no progress.");
    checkArgument(
        range.getByteCount() == 0L,
        "May only construct OffsetByteRangeTracker with an unbounded range with no progress.");
    this.unownedBacklogReader = unownedBacklogReader;
    this.rangeTracker = new OffsetRangeTracker(range.getRange());
    this.bytes = range.getByteCount();
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public boolean tryClaim(OffsetByteProgress position) {
    if (!rangeTracker.tryClaim(position.lastOffset().value())) {
      return false;
    }
    lastClaimed = position.lastOffset().value();
    bytes += position.batchBytes();
    return true;
  }

  @Override
  public OffsetByteRange currentRestriction() {
    return OffsetByteRange.of(rangeTracker.currentRestriction(), bytes);
  }

  private long nextOffset() {
    checkState(lastClaimed == null || lastClaimed < Long.MAX_VALUE);
    return lastClaimed == null ? currentRestriction().getRange().getFrom() : lastClaimed + 1;
  }

  @Override
  public @Nullable SplitResult<OffsetByteRange> trySplit(double fractionOfRemainder) {
    // Cannot split a bounded range. This should already be completely claimed.
    if (rangeTracker.currentRestriction().getTo() != Long.MAX_VALUE) {
      return null;
    }
    @Nullable SplitResult<OffsetRange> ranges = rangeTracker.trySplit(fractionOfRemainder);
    if (ranges == null) {
      return null;
    }
    checkArgument(rangeTracker.currentRestriction().equals(ranges.getPrimary()));
    return SplitResult.of(
        currentRestriction(), OffsetByteRange.of(checkArgumentNotNull(ranges.getResidual())));
  }

  @Override
  public void checkDone() throws IllegalStateException {
    rangeTracker.checkDone();
  }

  @Override
  public Progress getProgress() {
    ComputeMessageStatsResponse stats =
        this.unownedBacklogReader.computeMessageStats(Offset.of(nextOffset()));
    return Progress.from(bytes, stats.getMessageBytes());
  }
}
