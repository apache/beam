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

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link Source} that uses byte offsets to define starting and ending positions.
 *
 * <p>Extend this class to implement your own byte offset based custom source.
 * {@link FileBasedSource}, which is a subclass of this, adds additional functionality useful for
 * custom sources that are based on files. If possible implementors should start from
 * {@code FileBasedSource} instead of {@code ByteOffsetBasedSource}.
 *
 * <p>This is a common base class for all sources that use a byte offset range. It stores the range
 * and implements splitting into bundles. This should be used for sources that can be cheaply read
 * starting at any given byte offset.
 *
 * <p>The byte offset range of the source is between {@code startOffset} (inclusive) and endOffset
 * (exclusive), i.e. [{@code startOffset}, {@code endOffset}). The source may include a record if
 * its offset is at the range [{@code startOffset}, {@code endOffset}) even if the record extend
 * past the range. The source does not include any record at offsets before this range even if it
 * extend into this range because the previous range will include this record. A source may choose
 * to include records at offsets after this range. For example, a source may choose to set offset
 * boundaries based on blocks of records, in which case certain records may start after
 * {@code endOffset}. But for any given source type the combined set of data read by two sources for
 * ranges [A, B) and [B, C) must be the same as the records read by a single source of the same type
 * for the range [A, C).
 *
 * @param <T> Type of records represented by the source.
 */
public abstract class ByteOffsetBasedSource<T> extends BoundedSource<T> {
  private static final long serialVersionUID = 0;

  private final long startOffset;
  private final long endOffset;
  private final long minBundleSize;

  /**
   * @param startOffset starting byte offset (inclusive) of the source. Must be non-negative.
   *
   * @param endOffset ending byte offset (exclusive) of the source. Any
   *        {@code offset >= getMaxEndOffset()}, e.g., {@code Long.MAX_VALUE}, means the same as
   *        {@code getMaxEndOffset()}. Must be {@code >= startOffset}.
   *
   * @param minBundleSize minimum bundle size in bytes that should be used when splitting the source
   *        into sub-sources. This will not be respected if the total range of the source is smaller
   *        than the specified {@code minBundleSize}. Must be non-negative.
   */
  public ByteOffsetBasedSource(long startOffset, long endOffset, long minBundleSize) {
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
  public List<? extends ByteOffsetBasedSource<T>> splitIntoBundles(long desiredBundleSizeBytes,
      PipelineOptions options) throws Exception {
    // Split the range into bundles based on the desiredBundleSizeBytes. Final bundle is adjusted to
    // make sure that we do not end up with a too small bundle at the end. If desiredBundleSizeBytes
    // is smaller than the minBundleSize of the source then minBundleSize will be used instead.

    desiredBundleSizeBytes = Math.max(desiredBundleSizeBytes, minBundleSize);

    List<ByteOffsetBasedSource<T>> subSources = new ArrayList<>();
    long start = startOffset;
    long maxEnd = Math.min(endOffset, getMaxEndOffset(options));

    while (start < maxEnd) {
      long end = start + desiredBundleSizeBytes;
      end = Math.min(end, maxEnd);
      // Avoid having a too small bundle at the end and ensure that we respect minBundleSize.
      long remainingBytes = maxEnd - end;
      if ((remainingBytes < desiredBundleSizeBytes / 4) || (remainingBytes < minBundleSize)) {
        end = maxEnd;
      }
      subSources.add(createSourceForSubrange(start, end));

      start = end;
    }
    return subSources;
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(this.startOffset >= 0,
        "Start offset has value " + this.startOffset + ", must be non-negative");
    Preconditions.checkArgument(this.endOffset >= 0,
        "End offset has value " + this.endOffset + ", must be non-negative");
    Preconditions.checkArgument(this.startOffset < this.endOffset,
        "Start offset " + this.startOffset + " must be before end offset " + this.endOffset);
    Preconditions.checkArgument(this.minBundleSize >= 0,
        "minBundleSize has value " + this.minBundleSize + ", must be non-negative");
  }

  @Override
  public String toString() {
    return "[" + startOffset + ", " + endOffset + ")";
  }

  /**
   * Returns the exact ending offset of the current source. This will be used if the source was
   * constructed with an endOffset value {@code Long.MAX_VALUE}.
   */
  public abstract long getMaxEndOffset(PipelineOptions options) throws Exception;

  /**
   * Returns a {@code ByteOffsetBasedSource} for a subrange of the current source. [start, end) will
   * be within the range [startOffset, endOffset] of the current source.
   */
  public abstract ByteOffsetBasedSource<T> createSourceForSubrange(long start, long end);

  /**
   * A {@link Source.Reader} that implements code common
   * to readers of all {@link ByteOffsetBasedSource}s.
   */
  public abstract static class ByteOffsetBasedReader<T> extends AbstractBoundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ByteOffsetBasedReader.class);

    private ByteOffsetBasedSource<T> source;

    /**
     * @param source the {@code ByteOffsetBasedSource} to be read by the current reader.
     */
    public ByteOffsetBasedReader(ByteOffsetBasedSource<T> source) {
      this.source = source;
    }

    /**
     * Returns the current offset of the reader. The value returned by this method is undefined
     * until the method {@link Source.Reader#start} is called. After {@link Source.Reader#start} is
     * called the value returned by this method should represent the offset of the value that will
     * be returned by the {@link Source.Reader#getCurrent} call. Values returned for two consecutive
     * records should be non-strictly increasing. If the reader has reached the end of the stream
     * this should return {@code Long.MAX_VALUE}. The value returned may be outside the range
     * defined by the {@code ByteOffsetBasedSource} corresponding to this reader, for reasons
     * described in the comment to {@code ByteOffsetBasedSource}.
     */
    protected abstract long getCurrentOffset();

    @Override
    public ByteOffsetBasedSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public Double getFractionConsumed() {
      if (source.getEndOffset() == Long.MAX_VALUE) {
        // True fraction consumed is unknown.
        return null;
      }
      // TODO: a more sophisticated implementation could account for the fact that
      // the first record's offset is not necessarily the same as getStartOffset(),
      // and same for the last record. For example, we could assume that
      // the position of the last record is as far after getEndOffset() as the
      // position of the first record was after getStartOffset(), and compute
      // fraction based on this adjusted range.
      long current = getCurrentOffset();
      double fraction =
          1.0 * (current - source.getStartOffset())
              / (source.getEndOffset() - source.getStartOffset());
      return Math.max(0, Math.min(1, fraction));
    }

    @Override
    public ByteOffsetBasedSource<T> splitAtFraction(double fraction) {
      if (source.getEndOffset() == Long.MAX_VALUE) {
        // Impossible to convert fraction to an offset.
        LOG.debug("Refusing to split at fraction {} because source does not have an end offset",
            fraction);
        return null;
      }
      long start = source.getStartOffset();
      long end = source.getEndOffset();
      long splitOffset = (long) (start + fraction * (end - start));
      long current = getCurrentOffset();
      if (splitOffset <= current) {
        LOG.debug(
            "Refusing to split at fraction {} (offset {}) because current offset is {} of [{}, {})",
            fraction, splitOffset, current, start, end);
        return null;
      }
      if (splitOffset <= start || splitOffset >= end) {
        LOG.debug(
            "Refusing to split at fraction {} (offset {}) outside current range [{}, {})",
            fraction, splitOffset, start, end);
        return null;
      }
      // Note: we intentionally ignore minBundleSize here.
      // It is useful to respect it during initial splitting so we don't produce work items
      // that are likely to turn out too small - but once dynamic work rebalancing kicks in,
      // its estimates are far more precise and should take priority. If it says split into
      // tiny single-record bundles, we should do that.
      ByteOffsetBasedSource<T> primary = source.createSourceForSubrange(start, splitOffset);
      ByteOffsetBasedSource<T> residual = source.createSourceForSubrange(splitOffset, end);
      this.source = primary;
      LOG.debug("Split at fraction {} (offset {}) of [{}, {}) (current offset {})",
          fraction, splitOffset, start, end, current);
      return residual;
    }
  }
}
