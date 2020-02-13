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
package org.apache.beam.sdk.io.range;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeTracker} for non-negative positions of type {@code long}.
 *
 * <p>Not to be confused with {@link
 * org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker}.
 */
public class OffsetRangeTracker implements RangeTracker<Long> {
  private static final Logger LOG = LoggerFactory.getLogger(OffsetRangeTracker.class);

  private long startOffset;
  private long stopOffset;
  private long lastRecordStart = -1L;
  private long offsetOfLastSplitPoint = -1L;
  private long splitPointsSeen = 0L;
  private boolean done = false;

  /**
   * Offset corresponding to infinity. This can only be used as the upper-bound of a range, and
   * indicates reading all of the records until the end without specifying exactly what the end is.
   *
   * <p>Infinite ranges cannot be split because it is impossible to estimate progress within them.
   */
  public static final long OFFSET_INFINITY = Long.MAX_VALUE;

  /** Creates an {@code OffsetRangeTracker} for the specified range. */
  public OffsetRangeTracker(long startOffset, long stopOffset) {
    this.startOffset = startOffset;
    this.stopOffset = stopOffset;
  }

  private OffsetRangeTracker() {}

  public synchronized boolean isStarted() {
    // done => started: handles the case when the reader was empty.
    return (offsetOfLastSplitPoint != -1) || done;
  }

  public synchronized boolean isDone() {
    return done;
  }

  @Override
  public synchronized Long getStartPosition() {
    return startOffset;
  }

  @Override
  public synchronized Long getStopPosition() {
    return stopOffset;
  }

  @Override
  public boolean tryReturnRecordAt(boolean isAtSplitPoint, Long recordStart) {
    return tryReturnRecordAt(isAtSplitPoint, recordStart.longValue());
  }

  public synchronized boolean tryReturnRecordAt(boolean isAtSplitPoint, long recordStart) {
    if (!isStarted() && !isAtSplitPoint) {
      throw new IllegalStateException(
          String.format("The first record [starting at %d] must be at a split point", recordStart));
    }
    if (recordStart < startOffset) {
      throw new IllegalStateException(
          String.format(
              "Trying to return record [starting at %d] which is before the start offset [%d]",
              recordStart, startOffset));
    }
    if (recordStart < lastRecordStart) {
      throw new IllegalStateException(
          String.format(
              "Trying to return record [starting at %d] "
                  + "which is before the last-returned record [starting at %d]",
              recordStart, lastRecordStart));
    }

    if (lastRecordStart == -1) {
      startOffset = recordStart;
    }
    lastRecordStart = recordStart;

    if (isAtSplitPoint) {
      if (recordStart == offsetOfLastSplitPoint) {
        throw new IllegalStateException(
            String.format(
                "Record at a split point has same offset as the previous split point: "
                    + "previous split point at %d, current record starts at %d",
                offsetOfLastSplitPoint, recordStart));
      }
      if (recordStart >= stopOffset) {
        done = true;
        return false;
      }
      offsetOfLastSplitPoint = recordStart;
      ++splitPointsSeen;
    }

    return true;
  }

  @Override
  public boolean trySplitAtPosition(Long splitOffset) {
    return trySplitAtPosition(splitOffset.longValue());
  }

  public synchronized boolean trySplitAtPosition(long splitOffset) {
    if (stopOffset == OFFSET_INFINITY) {
      LOG.debug("Refusing to split {} at {}: stop position unspecified", this, splitOffset);
      return false;
    }
    if (!isStarted()) {
      LOG.debug("Refusing to split {} at {}: unstarted", this, splitOffset);
      return false;
    }

    // Note: technically it is correct to split at any position after the last returned
    // split point, not just the last returned record.
    // TODO: Investigate whether in practice this is useful or, rather, confusing.
    if (splitOffset <= lastRecordStart) {
      LOG.debug(
          "Refusing to split {} at {}: already past proposed split position", this, splitOffset);
      return false;
    }
    if (splitOffset < startOffset || splitOffset >= stopOffset) {
      LOG.debug(
          "Refusing to split {} at {}: proposed split position out of range", this, splitOffset);
      return false;
    }
    LOG.debug("Agreeing to split {} at {}", this, splitOffset);
    this.stopOffset = splitOffset;
    return true;
  }

  /**
   * Returns a position {@code P} such that the range {@code [start, P)} represents approximately
   * the given fraction of the range {@code [start, end)}. Assumes that the density of records in
   * the range is approximately uniform.
   */
  public synchronized long getPositionForFractionConsumed(double fraction) {
    if (stopOffset == OFFSET_INFINITY) {
      throw new IllegalArgumentException(
          "getPositionForFractionConsumed is not applicable to an unbounded range: " + this);
    }
    return (long) Math.floor(startOffset + fraction * (stopOffset - startOffset));
  }

  @Override
  public synchronized double getFractionConsumed() {
    if (!isStarted()) {
      return 0.0;
    } else if (isDone()) {
      return 1.0;
    } else if (stopOffset == OFFSET_INFINITY) {
      return 0.0;
    } else if (lastRecordStart >= stopOffset) {
      return 1.0;
    } else {
      // E.g., when reading [3, 6) and lastRecordStart is 4, that means we consumed 3 of 3,4,5
      // which is (4 - 3) / (6 - 3) = 33%.
      // Also, clamp to at most 1.0 because the last consumed position can extend past the
      // stop position.
      return Math.min(1.0, 1.0 * (lastRecordStart - startOffset) / (stopOffset - startOffset));
    }
  }

  /**
   * Returns the total number of split points that have been processed.
   *
   * <p>A split point at a particular offset has been seen if there has been a corresponding call to
   * {@link #tryReturnRecordAt(boolean, long)} with {@code isAtSplitPoint} true. It has been
   * processed if there has been a <em>subsequent</em> call to {@link #tryReturnRecordAt(boolean,
   * long)} with {@code isAtSplitPoint} true and at a larger offset.
   *
   * <p>Note that for correctness when implementing {@link BoundedReader#getSplitPointsConsumed()},
   * if a reader finishes before {@link #tryReturnRecordAt(boolean, long)} returns false, the reader
   * should add an additional call to {@link #markDone()}. This will indicate that processing for
   * the last seen split point has been finished.
   *
   * @see org.apache.beam.sdk.io.OffsetBasedSource for a {@link BoundedReader} implemented using
   *     {@link OffsetRangeTracker}.
   */
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

  /**
   * Marks this range tracker as being done. Specifically, this will mark the current split point,
   * if one exists, as being finished.
   *
   * <p>Always returns false, so that it can be used in an implementation of {@link
   * BoundedReader#start()} or {@link BoundedReader#advance()} as follows:
   *
   * <pre>{@code
   * public boolean start() {
   *   return startImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint, position)
   *       || rangeTracker.markDone();
   * }
   * }</pre>
   */
  public synchronized boolean markDone() {
    done = true;
    return false;
  }

  @Override
  public synchronized String toString() {
    String stopString = (stopOffset == OFFSET_INFINITY) ? "infinity" : String.valueOf(stopOffset);
    if (lastRecordStart >= 0) {
      return String.format(
          "<at [starting at %d] of offset range [%d, %s)>",
          lastRecordStart, startOffset, stopString);
    } else {
      return String.format("<unstarted in offset range [%d, %s)>", startOffset, stopString);
    }
  }

  /**
   * Returns a copy of this tracker for testing purposes (to simplify testing methods with side
   * effects).
   */
  @VisibleForTesting
  OffsetRangeTracker copy() {
    synchronized (this) {
      OffsetRangeTracker res = new OffsetRangeTracker();
      // This synchronized is not really necessary, because there's no concurrent access to "res",
      // however it is necessary to prevent findbugs from complaining about unsynchronized access.
      synchronized (res) {
        res.startOffset = this.startOffset;
        res.stopOffset = this.stopOffset;
        res.offsetOfLastSplitPoint = this.offsetOfLastSplitPoint;
        res.lastRecordStart = this.lastRecordStart;
        res.done = this.done;
        res.splitPointsSeen = this.splitPointsSeen;
      }
      return res;
    }
  }
}
