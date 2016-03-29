/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.io.CountingInput.UnboundedCountingInput;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A source that produces longs. When used as a {@link BoundedSource}, {@link CountingSource}
 * starts at {@code 0} and counts up to a specified maximum. When used as an
 * {@link UnboundedSource}, it counts up to {@link Long#MAX_VALUE} and then never produces more
 * output. (In practice, this limit should never be reached.)
 *
 * <p>The bounded {@link CountingSource} is implemented based on {@link OffsetBasedSource} and
 * {@link OffsetBasedSource.OffsetBasedReader}, so it performs efficient initial splitting and it
 * supports dynamic work rebalancing.
 *
 * <p>To produce a bounded {@code PCollection<Long>}, use {@link CountingSource#upTo(long)}:
 *
 * <pre>{@code
 * Pipeline p = ...
 * PTransform<PBegin, PCollection<Long>> producer = CountingInput.upTo(1000);
 * PCollection<Long> bounded = p.apply(producer);
 * }</pre>
 *
 * <p>To produce an unbounded {@code PCollection<Long>}, use {@link CountingInput#unbounded()},
 * calling {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} to provide values
 * with timestamps other than {@link Instant#now}.
 *
 * <pre>{@code
 * Pipeline p = ...
 *
 * // To create an unbounded PCollection that uses processing time as the element timestamp.
 * PCollection<Long> unbounded = p.apply(CountingInput.unbounded());
 * // Or, to create an unbounded source that uses a provided function to set the element timestamp.
 * PCollection<Long> unboundedWithTimestamps =
 *     p.apply(CountingInput.unbounded().withTimestampFn(someFn));
 *
 * }</pre>
 */
public class CountingSource {
  /**
   * Creates a {@link BoundedSource} that will produce the specified number of elements,
   * from {@code 0} to {@code numElements - 1}.
   *
   * @deprecated use {@link CountingInput#upTo(long)} instead
   */
  @Deprecated
  public static BoundedSource<Long> upTo(long numElements) {
    checkArgument(numElements > 0, "numElements (%s) must be greater than 0", numElements);
    return new BoundedCountingSource(0, numElements);
  }

  /**
   * Creates an {@link UnboundedSource} that will produce numbers starting from {@code 0} up to
   * {@link Long#MAX_VALUE}.
   *
   * <p>After {@link Long#MAX_VALUE}, the source never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Elements in the resulting {@link PCollection PCollection&lt;Long&gt;} will have timestamps
   * corresponding to processing time at element generation, provided by {@link Instant#now}.
   *
   * @deprecated use {@link CountingInput#unbounded()} instead
   */
  @Deprecated
  public static UnboundedSource<Long, CounterMark> unbounded() {
    return unboundedWithTimestampFn(new NowTimestampFn());
  }

  /**
   * Creates an {@link UnboundedSource} that will produce numbers starting from {@code 0} up to
   * {@link Long#MAX_VALUE}, with element timestamps supplied by the specified function.
   *
   * <p>After {@link Long#MAX_VALUE}, the source never produces more output. (In practice, this
   * limit should never be reached.)
   *
   * <p>Note that the timestamps produced by {@code timestampFn} may not decrease.
   *
   * @deprecated use {@link CountingInput#unbounded()} and call
   *             {@link UnboundedCountingInput#withTimestampFn(SerializableFunction)} instead
   */
  @Deprecated
  public static UnboundedSource<Long, CounterMark> unboundedWithTimestampFn(
      SerializableFunction<Long, Instant> timestampFn) {
    return new UnboundedCountingSource(0, 1, timestampFn);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  /** Prevent instantiation. */
  private CountingSource() {}

  /**
   * A function that returns {@link Instant#now} as the timestamp for each generated element.
   */
  static class NowTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return Instant.now();
    }
  }

  /**
   * An implementation of {@link CountingSource} that produces a bounded {@link PCollection}.
   * It is implemented on top of {@link OffsetBasedSource} (with associated reader
   * {@link BoundedCountingReader}) and performs efficient initial splitting and supports dynamic
   * work rebalancing.
   */
  private static class BoundedCountingSource extends OffsetBasedSource<Long> {
    /**
     * Creates a {@link BoundedCountingSource} that generates the numbers in the specified
     * {@code [start, end)} range.
     */
    public BoundedCountingSource(long start, long end) {
      super(start, end, 1 /* can be split every 1 offset */);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public long getBytesPerOffset() {
      return 8;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) throws Exception {
      return getEndOffset();
    }

    @Override
    public OffsetBasedSource<Long> createSourceForSubrange(long start, long end) {
      return new BoundedCountingSource(start, end);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return true;
    }

    @Override
    public com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader<Long> createReader(
        PipelineOptions options) throws IOException {
      return new BoundedCountingReader(this);
    }

    @Override
    public Coder<Long> getDefaultOutputCoder() {
      return VarLongCoder.of();
    }
  }

  /**
   * The reader associated with {@link BoundedCountingSource}.
   *
   * @see BoundedCountingSource
   */
  private static class BoundedCountingReader extends OffsetBasedSource.OffsetBasedReader<Long> {
    private long current;

    public BoundedCountingReader(OffsetBasedSource<Long> source) {
      super(source);
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      return current;
    }

    @Override
    public synchronized BoundedCountingSource getCurrentSource()  {
      return (BoundedCountingSource) super.getCurrentSource();
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    protected boolean startImpl() throws IOException {
      current = getCurrentSource().getStartOffset();
      return true;
    }

    @Override
    protected boolean advanceImpl() throws IOException {
      current++;
      return true;
    }

    @Override
    public void close() throws IOException {}
  }

  /**
   * An implementation of {@link CountingSource} that produces an unbounded {@link PCollection}.
   */
  private static class UnboundedCountingSource extends UnboundedSource<Long, CounterMark> {
    /** The first number (>= 0) generated by this {@link UnboundedCountingSource}. */
    private final long start;
    /** The interval between numbers generated by this {@link UnboundedCountingSource}. */
    private final long stride;
    /** The function used to produce timestamps for the generated elements. */
    private final SerializableFunction<Long, Instant> timestampFn;

    /**
     * Creates an {@link UnboundedSource} that will produce numbers starting from {@code 0} up to
     * {@link Long#MAX_VALUE}, with element timestamps supplied by the specified function.
     *
     * <p>After {@link Long#MAX_VALUE}, the source never produces more output. (In practice, this
     * limit should never be reached.)
     *
     * <p>Note that the timestamps produced by {@code timestampFn} may not decrease.
     */
    public UnboundedCountingSource(
        long start, long stride, SerializableFunction<Long, Instant> timestampFn) {
      this.start = start;
      this.stride = stride;
      this.timestampFn = timestampFn;
    }

    /**
     * Splits an unbounded source {@code desiredNumSplits} ways by giving each split every
     * {@code desiredNumSplits}th element that this {@link UnboundedCountingSource}
     * produces.
     *
     * <p>E.g., if a source produces all even numbers {@code [0, 2, 4, 6, 8, ...)} and we want to
     * split into 3 new sources, then the new sources will produce numbers that are 6 apart and
     * are offset at the start by the original stride: {@code [0, 6, 12, ...)},
     * {@code [2, 8, 14, ...)}, and {@code [4, 10, 16, ...)}.
     */
    @Override
    public List<? extends UnboundedSource<Long, CountingSource.CounterMark>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      // Using Javadoc example, stride 2 with 3 splits becomes stride 6.
      long newStride = stride * desiredNumSplits;

      ImmutableList.Builder<UnboundedCountingSource> splits = ImmutableList.builder();
      for (int i = 0; i < desiredNumSplits; ++i) {
        // Starts offset by the original stride. Using Javadoc example, this generates starts of
        // 0, 2, and 4.
        splits.add(new UnboundedCountingSource(start + i * stride, newStride, timestampFn));
      }
      return splits.build();
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, CounterMark checkpointMark) {
      return new UnboundedCountingReader(this, checkpointMark);
    }

    @Override
    public Coder<CountingSource.CounterMark> getCheckpointMarkCoder() {
      return AvroCoder.of(CountingSource.CounterMark.class);
    }

    @Override
    public void validate() {}

    @Override
    public Coder<Long> getDefaultOutputCoder() {
      return VarLongCoder.of();
    }
  }

  /**
   * The reader associated with {@link UnboundedCountingSource}.
   *
   * @see UnboundedCountingSource
   */
  private static class UnboundedCountingReader extends UnboundedReader<Long> {
    private UnboundedCountingSource source;
    private long current;
    private Instant currentTimestamp;

    public UnboundedCountingReader(UnboundedCountingSource source, CounterMark mark) {
      this.source = source;
      if (mark == null) {
        // Because we have not emitted an element yet, and start() calls advance, we need to
        // "un-advance" so that start() produces the correct output.
        this.current = source.start - source.stride;
      } else {
        this.current = mark.getLastEmitted();
      }
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      // Overflow-safe check that (current + source.stride) <= LONG.MAX_VALUE. Else, stop producing.
      if (Long.MAX_VALUE - source.stride < current) {
        return false;
      }
      current += source.stride;
      currentTimestamp = source.timestampFn.apply(current);
      return true;
    }

    @Override
    public Instant getWatermark() {
      return source.timestampFn.apply(current);
    }

    @Override
    public CounterMark getCheckpointMark() {
      return new CounterMark(current);
    }

    @Override
    public UnboundedSource<Long, CounterMark> getCurrentSource() {
      return source;
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return currentTimestamp;
    }

    @Override
    public void close() throws IOException {}
  }

  /**
   * The checkpoint for an unbounded {@link CountingSource} is simply the last value produced. The
   * associated source object encapsulates the information needed to produce the next value.
   */
  @DefaultCoder(AvroCoder.class)
  public static class CounterMark implements UnboundedSource.CheckpointMark {
    /** The last value emitted. */
    private final long lastEmitted;

    /**
     * Creates a checkpoint mark reflecting the last emitted value.
     */
    public CounterMark(long lastEmitted) {
      this.lastEmitted = lastEmitted;
    }

    /**
     * Returns the last value emitted by the reader.
     */
    public long getLastEmitted() {
      return lastEmitted;
    }

    /////////////////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("unused") // For AvroCoder
    private CounterMark() {
      this.lastEmitted = 0L;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {}
   }
}
