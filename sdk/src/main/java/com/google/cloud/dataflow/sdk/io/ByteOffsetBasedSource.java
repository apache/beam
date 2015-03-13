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

import java.util.ArrayList;
import java.util.List;

/**
 * A source that uses byte offsets to define starting and ending positions. Extend this class to
 * implement your own byte offset based custom source. {@link FileBasedSource} which is a subclass
 * of this adds additional functionality useful for custom sources that are based on files. If
 * possible implementors should start from {@code FileBasedSource} instead of
 * {@code ByteOffsetBasedSource}.
 *
 * <p>This is a common base class for all sources that use a byte offset range. It stores the range
 * and implements splitting into shards. This should be used for sources which can be cheaply read
 * starting at any given byte offset.
 *
 * <p>The byte offset range of the source is between {@code startOffset} (inclusive) and endOffset
 * (exclusive), i.e. [{@code startOffset}, {@code endOffset}). The source may include a record if
 * its offset is at the range [{@code startOffset}, {@code endOffset}) even if the record extend
 * past the range. The source does not include any record at offsets before this range even if it
 * extend into this range because the previous range will include this record. A source may choose
 * to include records at offsets after this range. For example, a source may choose to set offset
 * boundaries based on blocks of records in which case certain records may start after
 * {@code endOffset}. But for any given source type the combined set of data read by two sources for
 * ranges [A, B) and [B, C) must be the same as the records read by a single source of the same type
 * for the range [A, C).
 *
 * @param <T> Type of records represented by the source.
 */
public abstract class ByteOffsetBasedSource<T> extends Source<T> {
  private final long startOffset;
  private final long endOffset;
  private final long minShardSize;

  /**
   * @param startOffset starting byte offset (inclusive) of the source. Must be non-negative.
   *
   * @param endOffset ending byte offset (exclusive) of the source. Any
   *        {@code offset >= getMaxEndOffset()}, e.g., {@code Long.MAX_VALUE}, means the same as
   *        {@code getMaxEndOffset()}. Must be {@code >= startOffset}.
   *
   * @param minShardSize minimum shard size in bytes that should be used when splitting the source
   *        into sub-sources. This will not be respected if the total range of the source is smaller
   *        than the specified {@code minShardSize}. Must be non-negative.
   */
  public ByteOffsetBasedSource(long startOffset, long endOffset, long minShardSize) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.minShardSize = minShardSize;
    Preconditions.checkArgument(startOffset >= 0,
        "Start offset has value " + startOffset + ", must be non-negative");
    Preconditions.checkArgument(endOffset >= 0,
        "End offset has value " + endOffset + ", must be non-negative");
    Preconditions.checkArgument(minShardSize >= 0,
        "minShardSize has value " + minShardSize + ", must be non-negative");
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
   * Returns the minimum shard size that should be used when splitting the source into sub-sources.
   * This will not be respected if the total range of the source is smaller than the specified
   * {@code minShardSize}.
   */
  public long getMinShardSize() {
    return minShardSize;
  }

  @Override
  public List<? extends ByteOffsetBasedSource<T>> splitIntoShards(long desiredShardSizeBytes,
      PipelineOptions options) throws Exception {
    // Split the range into shards based on the desiredShardSizeBytes. Final shard is adjusted to
    // make sure that we do not end up with a too small shard at the end. If desiredShardSizeBytes
    // is smaller than the minShardSize of the source then minShardSize will be used instead.

    desiredShardSizeBytes = Math.max(desiredShardSizeBytes, minShardSize);

    List<ByteOffsetBasedSource<T>> subSources = new ArrayList<>();
    long start = startOffset;
    long maxEnd = Math.min(endOffset, getMaxEndOffset(options));

    while (start < maxEnd) {
      long end = start + desiredShardSizeBytes;
      end = Math.min(end, maxEnd);
      // Avoid having a too small shard at the end and ensure that we respect minShardSize.
      long remainingBytes = maxEnd - end;
      if ((remainingBytes < desiredShardSizeBytes / 4) || (remainingBytes < minShardSize)) {
        end = maxEnd;
      }
      subSources.add(createSourceForSubrange(start, end));

      start = end;
    }
    return subSources;
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
   * A reader that implements code common to readers of all {@link ByteOffsetBasedSource}s.
   */
  public abstract static class ByteOffsetBasedReader<T> implements Reader<T> {

    /**
     * @param source the {@code ByteOffsetBasedSource} to be read by the current reader.
     */
    public ByteOffsetBasedReader(ByteOffsetBasedSource<T> source) {}

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
  }
}
