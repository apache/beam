/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.io.range.RangeTracker;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link Source} that uses offsets to define starting and ending positions.
 *
 * <p>Extend this class to implement your own offset based custom source.
 * {@link FileBasedSource}, which is a subclass of this, adds additional functionality useful for
 * custom sources that are based on files. If possible implementors should start from
 * {@code FileBasedSource} instead of {@code OffsetBasedSource}.
 *
 * <p>This is a common base class for all sources that use an offset range. It stores the range
 * and implements splitting into bundles. This should be used for sources that can be cheaply read
 * starting at any given offset.
 *
 * <p>Consult {@link RangeTracker} for important semantics common to all sources defined by a range
 * of positions of a certain type, including the semantics of split points
 * ({@link OffsetBasedReader#isAtSplitPoint}).
 *
 * @param <T> Type of records represented by the source.
 */
public abstract class OffsetBasedSource<T> extends BoundedSource<T> {
  private static final long serialVersionUID = 0;

  private final long startOffset;
  private final long endOffset;
  private final long minBundleSize;

  /**
   * @param startOffset starting offset (inclusive) of the source. Must be non-negative.
   *
   * @param endOffset ending offset (exclusive) of the source. Any
   *        {@code offset >= getMaxEndOffset()}, e.g., {@code Long.MAX_VALUE}, means the same as
   *        {@code getMaxEndOffset()}. Must be {@code >= startOffset}.
   *
   * @param minBundleSize minimum bundle size in offset units that should be used when splitting the
   *                      source into sub-sources. This will not be respected if the total range of
   *                      the source is smaller than the specified {@code minBundleSize}.
   *                      Must be non-negative.
   */
  public OffsetBasedSource(long startOffset, long endOffset, long minBundleSize) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.minBundleSize = minBundleSize;
  }

  /**
   * Returns the starting offset of the source.
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Returns the specified ending offset of the source. If this is {@code >= getMaxEndOffset()},
   * e.g. Long.MAX_VALUE, this implies {@code getMaxEndOffset()}.
   */
  public long getEndOffset() {
    return endOffset;
  }

  /**
   * Returns the minimum bundle size that should be used when splitting the source into sub-sources.
   * This will not be respected if the total range of the source is smaller than the specified
   * {@code minBundleSize}.
   */
  public long getMinBundleSize() {
    return minBundleSize;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    long trueEndOffset = (endOffset == Long.MAX_VALUE) ? getMaxEndOffset(options) : endOffset;
    return getBytesPerOffset() * (trueEndOffset - getStartOffset() + 1);
  }

  @Override
  public List<? extends OffsetBasedSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // Split the range into bundles based on the desiredBundleSizeBytes. Final bundle is adjusted to
    // make sure that we do not end up with a too small bundle at the end. If the desired bundle
    // size is smaller than the minBundleSize of the source then minBundleSize will be used instead.

    long desiredBundleSizeOffsetUnits = Math.max(
        Math.max(1, desiredBundleSizeBytes / getBytesPerOffset()),
        minBundleSize);

    List<OffsetBasedSource<T>> subSources = new ArrayList<>();
    long start = startOffset;
    long maxEnd = Math.min(endOffset, getMaxEndOffset(options));

    while (start < maxEnd) {
      long end = start + desiredBundleSizeOffsetUnits;
      end = Math.min(end, maxEnd);
      // Avoid having a too small bundle at the end and ensure that we respect minBundleSize.
      long remaining = maxEnd - end;
      if ((remaining < desiredBundleSizeOffsetUnits / 4) || (remaining < minBundleSize)) {
        end = maxEnd;
      }
      subSources.add(createSourceForSubrange(start, end));

      start = end;
    }
    return subSources;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(
        this.startOffset >= 0,
        "Start offset has value %s, must be non-negative", this.startOffset);
    Preconditions.checkArgument(
        this.endOffset >= 0,
        "End offset has value %s, must be non-negative", this.endOffset);
    Preconditions.checkArgument(
        this.startOffset < this.endOffset,
        "Start offset %s must be before end offset %s",
        this.startOffset, this.endOffset);
    Preconditions.checkArgument(
        this.minBundleSize >= 0,
        "minBundleSize has value %s, must be non-negative",
        this.minBundleSize);
  }

  @Override
  public String toString() {
    return "[" + startOffset + ", " + endOffset + ")";
  }

  /**
   * Returns approximately how many bytes of data correspond to a single offset in this source.
   * Used for translation between this source's range and methods defined in terms of bytes, such
   * as {@link #getEstimatedSizeBytes} and {@link #splitIntoBundles}.
   */
  public long getBytesPerOffset() {
    return 1L;
  }

  /**
   * Returns the exact ending offset of the current source. This will be used if the source was
   * constructed with an endOffset value {@code Long.MAX_VALUE}.
   */
  public abstract long getMaxEndOffset(PipelineOptions options) throws Exception;

  /**
   * Returns an {@code OffsetBasedSource} for a subrange of the current source. [start, end) will
   * be within the range [startOffset, endOffset] of the current source.
   */
  public abstract OffsetBasedSource<T> createSourceForSubrange(long start, long end);

  /**
   * A {@link Source.Reader} that implements code common to readers of all
   * {@link OffsetBasedSource}s.
   *
   * <p>Subclasses have to implement:
   * <ul>
   *   <li>The methods {@link #startImpl} and {@link #advanceImpl} for reading the
   *   first or subsequent records.
   *   <li>The methods {@link #getCurrent}, {@link #getCurrentOffset}, and optionally
   *   {@link #isAtSplitPoint} and {@link #getCurrentTimestamp} to access properties of
   *   the last record successfully read by {@link #startImpl} or {@link #advanceImpl}.
   * </ul>
   */
  public abstract static class OffsetBasedReader<T> extends BoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetBasedReader.class);

    private OffsetBasedSource<T> source;
    /**
     * The {@link OffsetRangeTracker} managing the range and current position of the source.
     * Subclasses MUST use it before returning records from {@link #start} or {@link #advance}:
     * see documentation of {@link RangeTracker}.
     */
    private final OffsetRangeTracker rangeTracker;

    /**
     * @param source the {@code OffsetBasedSource} to be read by the current reader.
     */
    public OffsetBasedReader(OffsetBasedSource<T> source) {
      this.source = source;
      this.rangeTracker = new OffsetRangeTracker(source.getStartOffset(), source.getEndOffset());
    }

    /**
     * Returns the <i>starting</i> offset of the {@link Source.Reader#getCurrent current record},
     * which has been read by the last successful {@link Source.Reader#start} or
     * {@link Source.Reader#advance} call.
     * <p>If no such call has been made yet, the return value is unspecified.
     * <p>See {@link RangeTracker} for description of offset semantics.
     */
    protected abstract long getCurrentOffset() throws NoSuchElementException;

    /**
     * Returns whether the current record is at a split point (i.e., whether the current record
     * would be the first record to be read by a source with a specified start offset of
     * {@link #getCurrentOffset}).
     *
     * <p>See detailed documentation about split points in {@link RangeTracker}.
     */
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return true;
    }

    @Override
    public final boolean start() throws IOException {
      return startImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint(), getCurrentOffset());
    }

    @Override
    public final boolean advance() throws IOException {
      return advanceImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint(), getCurrentOffset());
    }

    /**
     * Same as {@link BoundedReader#start}, except {@link OffsetBasedReader} base class
     * takes care of coordinating against concurrent calls to {@link #splitAtFraction}.
     */
    protected abstract boolean startImpl() throws IOException;

    /**
     * Same as {@link BoundedReader#advance}, except {@link OffsetBasedReader} base class
     * takes care of coordinating against concurrent calls to {@link #splitAtFraction}.
     */
    protected abstract boolean advanceImpl() throws IOException;

    @Override
    public OffsetBasedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public final OffsetBasedSource<T> splitAtFraction(double fraction) {
      if (rangeTracker.getStopPosition() == Long.MAX_VALUE) {
        LOG.debug(
            "Refusing to split unbounded OffsetBasedReader {} at fraction {}",
            rangeTracker, fraction);
        return null;
      }
      long splitOffset = rangeTracker.getPositionForFractionConsumed(fraction);
      LOG.debug(
          "Proposing to split OffsetBasedReader {} at fraction {} (offset {})",
          rangeTracker, fraction, splitOffset);
      if (!rangeTracker.trySplitAtPosition(splitOffset)) {
        return null;
      }
      long start = source.getStartOffset();
      long end = source.getEndOffset();
      OffsetBasedSource<T> primary = source.createSourceForSubrange(start, splitOffset);
      OffsetBasedSource<T> residual = source.createSourceForSubrange(splitOffset, end);
      this.source = primary;
      return residual;
    }
  }
}
