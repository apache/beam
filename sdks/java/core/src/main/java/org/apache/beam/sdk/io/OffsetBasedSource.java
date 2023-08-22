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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.io.range.OffsetRangeTracker;
import org.apache.beam.sdk.io.range.RangeTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BoundedSource} that uses offsets to define starting and ending positions.
 *
 * <p>{@link OffsetBasedSource} is a common base class for all bounded sources where the input can
 * be represented as a single range, and an input can be efficiently processed in parallel by
 * splitting the range into a set of disjoint ranges whose union is the original range. This class
 * should be used for sources that can be cheaply read starting at any given offset. {@link
 * OffsetBasedSource} stores the range and implements splitting into bundles.
 *
 * <p>Extend {@link OffsetBasedSource} to implement your own offset-based custom source. {@link
 * FileBasedSource}, which is a subclass of this, adds additional functionality useful for custom
 * sources that are based on files. If possible implementors should start from {@link
 * FileBasedSource} instead of {@link OffsetBasedSource}.
 *
 * <p>Consult {@link RangeTracker} for important semantics common to all sources defined by a range
 * of positions of a certain type, including the semantics of split points ({@link
 * OffsetBasedReader#isAtSplitPoint}).
 *
 * @param <T> Type of records represented by the source.
 * @see BoundedSource
 * @see FileBasedSource
 * @see RangeTracker
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class OffsetBasedSource<T> extends BoundedSource<T> {
  private final long startOffset;
  private final long endOffset;
  private final long minBundleSize;

  /**
   * @param startOffset starting offset (inclusive) of the source. Must be non-negative.
   * @param endOffset ending offset (exclusive) of the source. Use {@link Long#MAX_VALUE} to
   *     indicate that the entire source after {@code startOffset} should be read. Must be {@code >
   *     startOffset}.
   * @param minBundleSize minimum bundle size in offset units that should be used when splitting the
   *     source into sub-sources. This value may not be respected if the total range of the source
   *     is smaller than the specified {@code minBundleSize}. Must be non-negative.
   */
  public OffsetBasedSource(long startOffset, long endOffset, long minBundleSize) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.minBundleSize = minBundleSize;
  }

  /** Returns the starting offset of the source. */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Returns the specified ending offset of the source. Any returned value greater than or equal to
   * {@link #getMaxEndOffset(PipelineOptions)} should be treated as {@link
   * #getMaxEndOffset(PipelineOptions)}.
   */
  public long getEndOffset() {
    return endOffset;
  }

  /**
   * Returns the minimum bundle size that should be used when splitting the source into sub-sources.
   * This value may not be respected if the total range of the source is smaller than the specified
   * {@code minBundleSize}.
   */
  public long getMinBundleSize() {
    return minBundleSize;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    long trueEndOffset = (endOffset == Long.MAX_VALUE) ? getMaxEndOffset(options) : endOffset;
    return getBytesPerOffset() * (trueEndOffset - getStartOffset());
  }

  @Override
  public List<? extends OffsetBasedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // Split the range into bundles based on the desiredBundleSizeBytes. If the desired bundle
    // size is smaller than the minBundleSize of the source then minBundleSize will be used instead.

    long desiredBundleSizeOffsetUnits =
        Math.max(Math.max(1, desiredBundleSizeBytes / getBytesPerOffset()), minBundleSize);

    List<OffsetBasedSource<T>> subSources = new ArrayList<>();
    for (OffsetRange range :
        new OffsetRange(startOffset, Math.min(endOffset, getMaxEndOffset(options)))
            .split(desiredBundleSizeOffsetUnits, minBundleSize)) {
      subSources.add(createSourceForSubrange(range.getFrom(), range.getTo()));
    }
    return subSources;
  }

  @Override
  public void validate() {
    checkArgument(
        this.startOffset >= 0, "Start offset has value %s, must be non-negative", this.startOffset);
    checkArgument(
        this.endOffset >= 0, "End offset has value %s, must be non-negative", this.endOffset);
    checkArgument(
        this.startOffset <= this.endOffset,
        "Start offset %s may not be larger than end offset %s",
        this.startOffset,
        this.endOffset);
    checkArgument(
        this.minBundleSize >= 0,
        "minBundleSize has value %s, must be non-negative",
        this.minBundleSize);
  }

  @Override
  public String toString() {
    return "[" + startOffset + ", " + endOffset + ")";
  }

  /**
   * Returns approximately how many bytes of data correspond to a single offset in this source. Used
   * for translation between this source's range and methods defined in terms of bytes, such as
   * {@link #getEstimatedSizeBytes} and {@link #split}.
   *
   * <p>Defaults to {@code 1} byte, which is the common case for, e.g., file sources.
   */
  public long getBytesPerOffset() {
    return 1L;
  }

  /**
   * Returns the actual ending offset of the current source. The value returned by this function
   * will be used to clip the end of the range {@code [startOffset, endOffset)} such that the range
   * used is {@code [startOffset, min(endOffset, maxEndOffset))}.
   *
   * <p>As an example in which {@link OffsetBasedSource} is used to implement a file source, suppose
   * that this source was constructed with an {@code endOffset} of {@link Long#MAX_VALUE} to
   * indicate that a file should be read to the end. Then this function should determine the actual,
   * exact size of the file in bytes and return it.
   */
  public abstract long getMaxEndOffset(PipelineOptions options) throws Exception;

  /**
   * Returns an {@link OffsetBasedSource} for a subrange of the current source. The subrange {@code
   * [start, end)} must be within the range {@code [startOffset, endOffset)} of the current source,
   * i.e. {@code startOffset <= start < end <= endOffset}.
   */
  public abstract OffsetBasedSource<T> createSourceForSubrange(long start, long end);

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .addIfNotDefault(
            DisplayData.item("minBundleSize", minBundleSize).withLabel("Minimum Bundle Size"), 1L)
        .addIfNotDefault(
            DisplayData.item("startOffset", startOffset).withLabel("Start Read Offset"), 0L)
        .addIfNotDefault(
            DisplayData.item("endOffset", endOffset).withLabel("End Read Offset"), Long.MAX_VALUE);
  }

  /**
   * A {@link Source.Reader} that implements code common to readers of all {@link
   * OffsetBasedSource}s.
   *
   * <p>Subclasses have to implement:
   *
   * <ul>
   *   <li>The methods {@link #startImpl} and {@link #advanceImpl} for reading the first or
   *       subsequent records.
   *   <li>The methods {@link #getCurrent}, {@link #getCurrentOffset}, and optionally {@link
   *       #isAtSplitPoint} and {@link #getCurrentTimestamp} to access properties of the last record
   *       successfully read by {@link #startImpl} or {@link #advanceImpl}.
   * </ul>
   */
  public abstract static class OffsetBasedReader<T> extends BoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetBasedReader.class);
    private OffsetBasedSource<T> source;

    /** Returns true if the last call to {@link #start} or {@link #advance} returned false. */
    public final boolean isDone() {
      return rangeTracker.isDone();
    }

    /** Returns true if there has been a call to {@link #start}. */
    public final boolean isStarted() {
      return rangeTracker.isStarted();
    }

    /** The {@link OffsetRangeTracker} managing the range and current position of the source. */
    private final OffsetRangeTracker rangeTracker;

    /** @param source the {@link OffsetBasedSource} to be read by the current reader. */
    public OffsetBasedReader(OffsetBasedSource<T> source) {
      this.source = source;
      this.rangeTracker = new OffsetRangeTracker(source.getStartOffset(), source.getEndOffset());
    }

    /**
     * Returns the <i>starting</i> offset of the {@link Source.Reader#getCurrent current record},
     * which has been read by the last successful {@link Source.Reader#start} or {@link
     * Source.Reader#advance} call.
     *
     * <p>If no such call has been made yet, the return value is unspecified.
     *
     * <p>See {@link RangeTracker} for description of offset semantics.
     */
    protected abstract long getCurrentOffset() throws NoSuchElementException;

    /**
     * Returns whether the current record is at a split point (i.e., whether the current record
     * would be the first record to be read by a source with a specified start offset of {@link
     * #getCurrentOffset}).
     *
     * <p>See detailed documentation about split points in {@link RangeTracker}.
     */
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return true;
    }

    @Override
    public final boolean start() throws IOException {
      return (startImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint(), getCurrentOffset()))
          || rangeTracker.markDone();
    }

    @Override
    public final boolean advance() throws IOException {
      return (advanceImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint(), getCurrentOffset()))
          || rangeTracker.markDone();
    }

    /**
     * Initializes the {@link OffsetBasedSource.OffsetBasedReader} and advances to the first record,
     * returning {@code true} if there is a record available to be read. This method will be invoked
     * exactly once and may perform expensive setup operations that are needed to initialize the
     * reader.
     *
     * <p>This function is the {@code OffsetBasedReader} implementation of {@link
     * BoundedReader#start}. The key difference is that the implementor can ignore the possibility
     * that it should no longer produce the first record, either because it has exceeded the
     * original {@code endOffset} assigned to the reader, or because a concurrent call to {@link
     * #splitAtFraction} has changed the source to shrink the offset range being read.
     *
     * @see BoundedReader#start
     */
    protected abstract boolean startImpl() throws IOException;

    /**
     * Advances to the next record and returns {@code true}, or returns false if there is no next
     * record.
     *
     * <p>This function is the {@code OffsetBasedReader} implementation of {@link
     * BoundedReader#advance}. The key difference is that the implementor can ignore the possibility
     * that it should no longer produce the next record, either because it has exceeded the original
     * {@code endOffset} assigned to the reader, or because a concurrent call to {@link
     * #splitAtFraction} has changed the source to shrink the offset range being read.
     *
     * @see BoundedReader#advance
     */
    protected abstract boolean advanceImpl() throws IOException;

    @Override
    public synchronized OffsetBasedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public long getSplitPointsConsumed() {
      return rangeTracker.getSplitPointsProcessed();
    }

    @Override
    public long getSplitPointsRemaining() {
      if (isDone()) {
        return 0;
      } else if (!isStarted()) {
        // Note that even if the current source does not allow splitting, we don't know that
        // it's non-empty so we return UNKNOWN instead of 1.
        return BoundedReader.SPLIT_POINTS_UNKNOWN;
      } else if (!allowsDynamicSplitting()) {
        // Started (so non-empty) and unsplittable, so only the current task.
        return 1;
      } else if (getCurrentOffset() >= rangeTracker.getStopPosition() - 1) {
        // If this is true, the next element is outside the range. Note that even getCurrentOffset()
        // might be larger than the stop position when the current record is not a split point.
        return 1;
      } else {
        // Use the default.
        return super.getSplitPointsRemaining();
      }
    }

    /**
     * Whether this reader should allow dynamic splitting of the offset ranges.
     *
     * <p>True by default. Override this to return false if the reader cannot support dynamic
     * splitting correctly. If this returns false, {@link OffsetBasedReader#splitAtFraction} will
     * refuse all split requests.
     */
    public boolean allowsDynamicSplitting() {
      return true;
    }

    @Override
    public final synchronized OffsetBasedSource<T> splitAtFraction(double fraction) {
      if (!allowsDynamicSplitting()) {
        return null;
      }
      if (rangeTracker.getStopPosition() == Long.MAX_VALUE) {
        LOG.debug(
            "Refusing to split unbounded OffsetBasedReader {} at fraction {}",
            rangeTracker,
            fraction);
        return null;
      }
      long splitOffset = rangeTracker.getPositionForFractionConsumed(fraction);
      LOG.debug(
          "Proposing to split OffsetBasedReader {} at fraction {} (offset {})",
          rangeTracker,
          fraction,
          splitOffset);
      long start = source.getStartOffset();
      long end = source.getEndOffset();
      OffsetBasedSource<T> primary = source.createSourceForSubrange(start, splitOffset);
      OffsetBasedSource<T> residual = source.createSourceForSubrange(splitOffset, end);
      if (!rangeTracker.trySplitAtPosition(splitOffset)) {
        return null;
      }
      this.source = primary;
      return residual;
    }
  }
}
