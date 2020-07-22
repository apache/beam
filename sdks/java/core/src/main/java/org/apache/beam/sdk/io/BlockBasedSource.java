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

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code BlockBasedSource} is a {@link FileBasedSource} where a file consists of blocks of
 * records.
 *
 * <p>{@code BlockBasedSource} should be derived from when a file format does not support efficient
 * seeking to a record in the file, but can support efficient seeking to a block. Alternatively,
 * records in the file cannot be offset-addressed, but blocks can (it is not possible to say that
 * record {code i} starts at offset {@code m}, but it is possible to say that block {@code j} starts
 * at offset {@code n}).
 *
 * <p>The records that will be read from a {@code BlockBasedSource} that corresponds to a subrange
 * of a file {@code [startOffset, endOffset)} are those records such that the record is contained in
 * a block that starts at offset {@code i}, where {@code i >= startOffset} and {@code i <
 * endOffset}. In other words, a record will be read from the source if its first byte is contained
 * in a block that begins within the range described by the source.
 *
 * <p>This entails that it is possible to determine the start offsets of all blocks in a file.
 *
 * <p>Progress reporting for reading from a {@code BlockBasedSource} is inaccurate. A {@link
 * BlockBasedReader} reports its current offset as {@code (offset of current block) + (current block
 * size) * (fraction of block consumed)}. However, only the offset of the current block is required
 * to be accurately reported by subclass implementations. As such, in the worst case, the current
 * offset is only updated at block boundaries.
 *
 * <p>{@code BlockBasedSource} supports dynamic splitting. However, because records in a {@code
 * BlockBasedSource} are not required to have offsets and progress reporting is inaccurate, {@code
 * BlockBasedReader} only supports splitting at block boundaries. In other words, {@link
 * BlockBasedReader#atSplitPoint} returns true iff the current record is the first record in a
 * block. See {@link FileBasedSource.FileBasedReader} for discussion about split points.
 *
 * @param <T> The type of records to be read from the source.
 */
@Experimental(Kind.SOURCE_SINK)
public abstract class BlockBasedSource<T> extends FileBasedSource<T> {
  /**
   * Creates a {@code BlockBasedSource} based on a file name or pattern. Subclasses must call this
   * constructor when creating a {@code BlockBasedSource} for a file pattern. See {@link
   * FileBasedSource} for more information.
   */
  public BlockBasedSource(
      String fileOrPatternSpec, EmptyMatchTreatment emptyMatchTreatment, long minBundleSize) {
    this(StaticValueProvider.of(fileOrPatternSpec), emptyMatchTreatment, minBundleSize);
  }

  /**
   * Like {@link #BlockBasedSource(String, EmptyMatchTreatment, long)} but with a default {@link
   * EmptyMatchTreatment} of {@link EmptyMatchTreatment#DISALLOW}.
   */
  public BlockBasedSource(String fileOrPatternSpec, long minBundleSize) {
    this(StaticValueProvider.of(fileOrPatternSpec), minBundleSize);
  }

  /** Like {@link #BlockBasedSource(String, long)}. */
  public BlockBasedSource(ValueProvider<String> fileOrPatternSpec, long minBundleSize) {
    this(fileOrPatternSpec, EmptyMatchTreatment.DISALLOW, minBundleSize);
  }

  /** Like {@link #BlockBasedSource(String, EmptyMatchTreatment, long)}. */
  public BlockBasedSource(
      ValueProvider<String> fileOrPatternSpec,
      EmptyMatchTreatment emptyMatchTreatment,
      long minBundleSize) {
    super(fileOrPatternSpec, emptyMatchTreatment, minBundleSize);
  }

  /**
   * Creates a {@code BlockBasedSource} for a single file. Subclasses must call this constructor
   * when implementing {@link BlockBasedSource#createForSubrangeOfFile}. See documentation in {@link
   * FileBasedSource}.
   */
  public BlockBasedSource(Metadata metadata, long minBundleSize, long startOffset, long endOffset) {
    super(metadata, minBundleSize, startOffset, endOffset);
  }

  /** Creates a {@code BlockBasedSource} for the specified range in a single file. */
  @Override
  protected abstract BlockBasedSource<T> createForSubrangeOfFile(
      Metadata metadata, long start, long end);

  /** Creates a {@code BlockBasedReader}. */
  @Override
  protected abstract BlockBasedReader<T> createSingleFileReader(PipelineOptions options);

  /** A {@code Block} represents a block of records that can be read. */
  @Experimental(Kind.SOURCE_SINK)
  protected abstract static class Block<T> {
    /** Returns the current record. */
    public abstract T getCurrentRecord();

    /** Reads the next record from the block and returns true iff one exists. */
    public abstract boolean readNextRecord() throws IOException;

    /**
     * Returns the fraction of the block already consumed, if possible, as a value in {@code [0,
     * 1]}. It should not include the current record. Successive results from this method must be
     * monotonically increasing.
     *
     * <p>If it is not possible to compute the fraction of the block consumed this method may return
     * zero. For example, when the total number of records in the block is unknown.
     */
    public abstract double getFractionOfBlockConsumed();
  }

  /**
   * A {@code Reader} that reads records from a {@link BlockBasedSource}. If the source is a
   * subrange of a file, the blocks that will be read by this reader are those such that the first
   * byte of the block is within the range {@code [start, end)}.
   */
  @Experimental(Kind.SOURCE_SINK)
  protected abstract static class BlockBasedReader<T> extends FileBasedReader<T> {
    private boolean atSplitPoint;

    protected BlockBasedReader(BlockBasedSource<T> source) {
      super(source);
    }

    /** Read the next block from the input. */
    public abstract boolean readNextBlock() throws IOException;

    /**
     * Returns the current block (the block that was read by the last successful call to {@link
     * BlockBasedReader#readNextBlock}). May return null initially, or if no block has been
     * successfully read.
     */
    public abstract @Nullable Block<T> getCurrentBlock();

    /**
     * Returns the size of the current block in bytes as it is represented in the underlying file,
     * if possible. This method may return {@code 0} if the size of the current block is unknown.
     *
     * <p>The size returned by this method must be such that for two successive blocks A and B,
     * {@code offset(A) + size(A) <= offset(B)}. If this is not satisfied, the progress reported by
     * the {@code BlockBasedReader} will be non-monotonic and will interfere with the quality (but
     * not correctness) of dynamic work rebalancing.
     *
     * <p>This method and {@link Block#getFractionOfBlockConsumed} are used to provide an estimate
     * of progress within a block ({@code getCurrentBlock().getFractionOfBlockConsumed() *
     * getCurrentBlockSize()}). It is acceptable for the result of this computation to be {@code 0},
     * but progress estimation will be inaccurate.
     */
    public abstract long getCurrentBlockSize();

    /**
     * Returns the largest offset such that starting to read from that offset includes the current
     * block.
     */
    public abstract long getCurrentBlockOffset();

    @Override
    public final T getCurrent() throws NoSuchElementException {
      Block<T> currentBlock = getCurrentBlock();
      if (currentBlock == null) {
        throw new NoSuchElementException(
            "No block has been successfully read from " + getCurrentSource());
      }
      return currentBlock.getCurrentRecord();
    }

    /**
     * Returns true if the reader is at a split point. A {@code BlockBasedReader} is at a split
     * point if the current record is the first record in a block. In other words, split points are
     * block boundaries.
     */
    @Override
    protected boolean isAtSplitPoint() {
      return atSplitPoint;
    }

    /**
     * Reads the next record from the {@link #getCurrentBlock() current block} if possible. Will
     * call {@link #readNextBlock()} to advance to the next block if not.
     *
     * <p>The first record read from a block is treated as a split point.
     */
    @Override
    protected final boolean readNextRecord() throws IOException {
      atSplitPoint = false;

      while (getCurrentBlock() == null || !getCurrentBlock().readNextRecord()) {
        if (!readNextBlock()) {
          return false;
        }
        // The first record in a block is a split point.
        atSplitPoint = true;
      }
      return true;
    }

    @Override
    public @Nullable Double getFractionConsumed() {
      if (!isStarted()) {
        return 0.0;
      }
      if (isDone()) {
        return 1.0;
      }
      FileBasedSource<T> source = getCurrentSource();
      if (source.getEndOffset() == Long.MAX_VALUE) {
        // Unknown end offset, so we cannot tell.
        return null;
      }

      long currentBlockOffset = getCurrentBlockOffset();
      long startOffset = source.getStartOffset();
      long endOffset = source.getEndOffset();
      double fractionAtBlockStart =
          ((double) (currentBlockOffset - startOffset)) / (endOffset - startOffset);
      double fractionAtBlockEnd =
          (double) (currentBlockOffset + getCurrentBlockSize() - startOffset)
              / (endOffset - startOffset);
      double blockFraction = getCurrentBlock().getFractionOfBlockConsumed();
      return Math.min(
          1.0, fractionAtBlockStart + blockFraction * (fractionAtBlockEnd - fractionAtBlockStart));
    }

    @Override
    protected long getCurrentOffset() {
      return getCurrentBlockOffset();
    }
  }
}
