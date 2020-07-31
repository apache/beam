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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.sdk.io.range.RangeTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeTracker} for positions used by {@code GroupingShuffleReader} ({@code
 * ShufflePosition}).
 *
 * <p>These positions roughly correspond to hashes of keys. In case of hash collisions, multiple
 * groups can have the same position. In that case, the first group at a particular position is
 * considered a split point (because it is the first to be returned when reading a position range
 * starting at this position), others are not.
 */
// Likely real bugs - https://issues.apache.org/jira/browse/BEAM-6563
@SuppressFBWarnings("IS2_INCONSISTENT_SYNC")
public class GroupingShuffleRangeTracker implements RangeTracker<ShufflePosition> {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingShuffleRangeTracker.class);

  // null means "no limit": read from the beginning of the data.
  private final @Nullable ShufflePosition startPosition;

  // null means "no limit": read until the end of the data.
  private @Nullable ShufflePosition stopPosition;

  private ShufflePosition lastGroupStart = null;
  private boolean lastGroupWasAtSplitPoint = false;
  private long splitPointsSeen = 0L;

  public GroupingShuffleRangeTracker(
      @Nullable ShufflePosition startPosition, @Nullable ShufflePosition stopPosition) {
    this.startPosition = startPosition;
    this.stopPosition = stopPosition;
  }

  @Override
  public ShufflePosition getStartPosition() {
    return startPosition;
  }

  @Override
  public synchronized ShufflePosition getStopPosition() {
    return stopPosition;
  }

  public synchronized ShufflePosition getLastGroupStart() {
    return lastGroupStart;
  }

  public synchronized boolean isStarted() {
    return getLastGroupStart() != null;
  }

  public synchronized boolean isDone() {
    return getStopPosition() == getLastGroupStart();
  }

  public synchronized long getSplitPointsProcessed() {
    if (!isStarted()) {
      return 0;
    } else if (isDone()) {
      return splitPointsSeen;
    } else {
      // There is a current split point, and it has not finished processing.
      checkState(
          splitPointsSeen > 0,
          "A started rangeTracker should have seen > 0 split points (is %s)",
          splitPointsSeen);
      return splitPointsSeen - 1;
    }
  }

  @Override
  public synchronized boolean tryReturnRecordAt(
      boolean isAtSplitPoint, @Nullable ShufflePosition groupStart) {
    if (lastGroupStart == null && !isAtSplitPoint) {
      throw new IllegalStateException(
          String.format("The first group [at %s] must be at a split point", groupStart.toString()));
    }
    if (this.startPosition != null && groupStart.compareTo(this.startPosition) < 0) {
      throw new IllegalStateException(
          String.format(
              "Trying to return record at %s which is before the starting position at %s",
              groupStart, this.startPosition));
    }
    int comparedToLast = (lastGroupStart == null) ? 1 : groupStart.compareTo(this.lastGroupStart);
    if (comparedToLast < 0) {
      throw new IllegalStateException(
          String.format(
              "Trying to return group at %s which is before the last-returned group at %s",
              groupStart, this.lastGroupStart));
    }
    if (isAtSplitPoint) {
      splitPointsSeen++;

      if (comparedToLast == 0) {
        throw new IllegalStateException(
            String.format(
                "Trying to return a group at a split point with same position as the "
                    + "previous group: both at %s, last group was %s",
                groupStart,
                lastGroupWasAtSplitPoint ? "at a split point." : "not at a split point."));
      }
      if (stopPosition != null && groupStart.compareTo(stopPosition) >= 0) {
        return false;
      }
    } else {
      checkState(
          comparedToLast == 0,
          // This case is not a violation of general RangeTracker semantics, but it is
          // contrary to how GroupingShuffleReader in particular works. Hitting it would
          // mean it's behaving unexpectedly.
          "Trying to return a group not at a split point, but with a different position "
              + "than the previous group: last group was %s at %s, current at %s",
          lastGroupWasAtSplitPoint ? "a split point" : "a non-split point",
          lastGroupStart,
          groupStart);
    }
    this.lastGroupStart = groupStart;
    this.lastGroupWasAtSplitPoint = isAtSplitPoint;
    return true;
  }

  @Override
  public synchronized boolean trySplitAtPosition(ShufflePosition splitPosition) {
    if (lastGroupStart == null) {
      LOG.debug("Will not split {} at {}: no data read yet.", this, splitPosition);
      return false;
    }
    if (splitPosition.compareTo(lastGroupStart) <= 0) {
      LOG.debug(
          "Will not split {} at {}: already progressed past proposed split position",
          this,
          splitPosition);
      return false;
    }
    if ((stopPosition != null && splitPosition.compareTo(stopPosition) >= 0)
        || (startPosition != null && splitPosition.compareTo(startPosition) <= 0)) {
      LOG.info(
          "Will not split {} at {}: proposed split position is past current range",
          this,
          splitPosition);
      return false;
    }
    LOG.debug("Split {} at {}", this, splitPosition);
    this.stopPosition = splitPosition;
    return true;
  }

  @Override
  public synchronized double getFractionConsumed() {
    // GroupingShuffle sources have special support on the service and the service
    // will estimate progress from positions for us.
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized String toString() {
    if (lastGroupStart != null) {
      return String.format(
          "<at position %s of shuffle range [%s, %s)>",
          lastGroupStart, startPosition, stopPosition);
    } else {
      return String.format("<unstarted in shuffle range [%s, %s)>", startPosition, stopPosition);
    }
  }

  @VisibleForTesting
  GroupingShuffleRangeTracker copy() {
    GroupingShuffleRangeTracker res =
        new GroupingShuffleRangeTracker(this.startPosition, this.stopPosition);
    res.lastGroupStart = this.lastGroupStart;
    res.lastGroupWasAtSplitPoint = lastGroupWasAtSplitPoint;
    return res;
  }
}
