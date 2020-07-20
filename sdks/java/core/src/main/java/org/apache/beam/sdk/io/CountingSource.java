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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SourceMetrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Most users should use {@link GenerateSequence} instead.
 *
 * <p>A source that produces longs. When used as a {@link BoundedSource}, {@link CountingSource}
 * starts at {@code 0} and counts up to a specified maximum. When used as an {@link
 * UnboundedSource}, it counts up to {@link Long#MAX_VALUE} and then never produces more output. (In
 * practice, this limit should never be reached.)
 *
 * <p>The bounded {@link CountingSource} is implemented based on {@link OffsetBasedSource} and
 * {@link OffsetBasedSource.OffsetBasedReader}, so it performs efficient initial splitting and it
 * supports dynamic work rebalancing.
 *
 * <p>To produce a bounded source, use {@link #createSourceForSubrange(long, long)}. To produce an
 * unbounded source, use {@link #createUnboundedFrom(long)}.
 */
public class CountingSource {
  /**
   * Creates a {@link BoundedSource} that will produce the specified number of elements, from {@code
   * 0} to {@code numElements - 1}.
   *
   * @deprecated use {@link GenerateSequence} instead
   */
  @Deprecated
  public static BoundedSource<Long> upTo(long numElements) {
    checkArgument(
        numElements >= 0, "numElements (%s) must be greater than or equal to 0", numElements);
    return new BoundedCountingSource(0, numElements);
  }

  /**
   * Creates a {@link BoundedSource} that will produce elements starting from {@code startIndex}
   * (inclusive) to {@code endIndex} (exclusive). If {@code startIndex == endIndex}, then no
   * elements will be produced.
   */
  static BoundedSource<Long> createSourceForSubrange(long startIndex, long endIndex) {
    checkArgument(
        endIndex >= startIndex,
        "endIndex (%s) must be greater than or equal to startIndex (%s)",
        endIndex,
        startIndex);

    return new BoundedCountingSource(startIndex, endIndex);
  }

  /** Create a new {@link UnboundedCountingSource}. */
  // package-private to return a typed UnboundedCountingSource rather than the UnboundedSource type.
  static UnboundedCountingSource createUnboundedFrom(long start) {
    return new UnboundedCountingSource(start, 1, 1L, Duration.ZERO, new NowTimestampFn());
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
   * @deprecated use {@link GenerateSequence} instead
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
   * @deprecated use {@link GenerateSequence} and call {@link
   *     GenerateSequence#withTimestampFn(SerializableFunction)} instead
   */
  @Deprecated
  public static UnboundedSource<Long, CounterMark> unboundedWithTimestampFn(
      SerializableFunction<Long, Instant> timestampFn) {
    return new UnboundedCountingSource(0, 1, 1L, Duration.ZERO, timestampFn);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////

  /** Prevent instantiation. */
  private CountingSource() {}

  /** A function that returns {@link Instant#now} as the timestamp for each generated element. */
  static class NowTimestampFn implements SerializableFunction<Long, Instant> {
    @Override
    public Instant apply(Long input) {
      return Instant.now();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof NowTimestampFn;
    }

    @Override
    public int hashCode() {
      return getClass().hashCode();
    }
  }

  /**
   * An implementation of {@link CountingSource} that produces a bounded {@link PCollection}. It is
   * implemented on top of {@link OffsetBasedSource} (with associated reader {@link
   * BoundedCountingReader}) and performs efficient initial splitting and supports dynamic work
   * rebalancing.
   */
  private static class BoundedCountingSource extends OffsetBasedSource<Long> {
    /**
     * Creates a {@link BoundedCountingSource} that generates the numbers in the specified {@code
     * [start, end)} range.
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
    public org.apache.beam.sdk.io.BoundedSource.BoundedReader<Long> createReader(
        PipelineOptions options) throws IOException {
      return new BoundedCountingReader(this);
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof BoundedCountingSource)) {
        return false;
      }
      BoundedCountingSource that = (BoundedCountingSource) other;
      return this.getStartOffset() == that.getStartOffset()
          && this.getEndOffset() == that.getEndOffset();
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.getStartOffset(), (int) this.getEndOffset());
    }
  }

  /**
   * The reader associated with {@link BoundedCountingSource}.
   *
   * @see BoundedCountingSource
   */
  private static class BoundedCountingReader extends OffsetBasedSource.OffsetBasedReader<Long> {
    private long current;

    private final Counter elementsRead = SourceMetrics.elementsRead();

    public BoundedCountingReader(OffsetBasedSource<Long> source) {
      super(source);
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      return current;
    }

    @Override
    public synchronized long getSplitPointsRemaining() {
      return Math.max(0, getCurrentSource().getEndOffset() - current);
    }

    @Override
    public synchronized BoundedCountingSource getCurrentSource() {
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
      elementsRead.inc();
      current++;
      return true;
    }

    @Override
    public void close() throws IOException {}
  }

  /** An implementation of {@link CountingSource} that produces an unbounded {@link PCollection}. */
  static class UnboundedCountingSource extends UnboundedSource<Long, CounterMark> {
    /** The first number (>= 0) generated by this {@link UnboundedCountingSource}. */
    private final long start;
    /** The interval between numbers generated by this {@link UnboundedCountingSource}. */
    private final long stride;
    /** The number of elements to produce each period. */
    private final long elementsPerPeriod;
    /** The time between producing numbers from this {@link UnboundedCountingSource}. */
    private final Duration period;
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
    private UnboundedCountingSource(
        long start,
        long stride,
        long elementsPerPeriod,
        Duration period,
        SerializableFunction<Long, Instant> timestampFn) {
      this.start = start;
      this.stride = stride;
      checkArgument(
          elementsPerPeriod > 0L,
          "Must produce at least one element per period, got %s",
          elementsPerPeriod);
      this.elementsPerPeriod = elementsPerPeriod;
      checkArgument(
          period.getMillis() >= 0L, "Must have a non-negative period length, got %s", period);
      this.period = period;
      this.timestampFn = timestampFn;
    }

    /**
     * Returns an {@link UnboundedCountingSource} like this one with the specified period. Elements
     * will be produced with an interval between them equal to the period.
     */
    public UnboundedCountingSource withRate(long elementsPerPeriod, Duration period) {
      return new UnboundedCountingSource(start, stride, elementsPerPeriod, period, timestampFn);
    }

    /**
     * Returns an {@link UnboundedCountingSource} like this one where the timestamp of output
     * elements are supplied by the specified function.
     *
     * <p>Note that timestamps produced by {@code timestampFn} may not decrease.
     */
    public UnboundedCountingSource withTimestampFn(
        SerializableFunction<Long, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return new UnboundedCountingSource(start, stride, elementsPerPeriod, period, timestampFn);
    }

    /**
     * Splits an unbounded source {@code desiredNumSplits} ways by giving each split every {@code
     * desiredNumSplits}th element that this {@link UnboundedCountingSource} produces.
     *
     * <p>E.g., if a source produces all even numbers {@code [0, 2, 4, 6, 8, ...)} and we want to
     * split into 3 new sources, then the new sources will produce numbers that are 6 apart and are
     * offset at the start by the original stride: {@code [0, 6, 12, ...)}, {@code [2, 8, 14, ...)},
     * and {@code [4, 10, 16, ...)}.
     */
    @Override
    public List<? extends UnboundedSource<Long, CountingSource.CounterMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      // Using Javadoc example, stride 2 with 3 splits becomes stride 6.
      long newStride = stride * desiredNumSplits;

      ImmutableList.Builder<UnboundedCountingSource> splits = ImmutableList.builder();
      for (int i = 0; i < desiredNumSplits; ++i) {
        // Starts offset by the original stride. Using Javadoc example, this generates starts of
        // 0, 2, and 4.
        splits.add(
            new UnboundedCountingSource(
                start + i * stride, newStride, elementsPerPeriod, period, timestampFn));
      }
      return splits.build();
    }

    @Override
    public UnboundedReader<Long> createReader(PipelineOptions options, CounterMark checkpointMark) {
      return new UnboundedCountingReader(this, checkpointMark);
    }

    @Override
    public Coder<CountingSource.CounterMark> getCheckpointMarkCoder() {
      return AvroCoder.of(CountingSource.CounterMark.class);
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof UnboundedCountingSource)) {
        return false;
      }
      UnboundedCountingSource that = (UnboundedCountingSource) other;
      return this.start == that.start
          && this.stride == that.stride
          && this.elementsPerPeriod == that.elementsPerPeriod
          && Objects.equals(this.period, that.period)
          && Objects.equals(this.timestampFn, that.timestampFn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(start, stride, elementsPerPeriod, period, timestampFn);
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

    // Initialized on first advance()
    private @Nullable Instant currentTimestamp;

    // Initialized in start()
    private @Nullable Instant firstStarted;

    private final Counter elementsRead = SourceMetrics.elementsRead();

    public UnboundedCountingReader(UnboundedCountingSource source, CounterMark mark) {
      this.source = source;
      if (mark == null) {
        // Because we have not emitted an element yet, and start() calls advance, we need to
        // "un-advance" so that start() produces the correct output.
        this.current = source.start - source.stride;
      } else {
        this.current = mark.getLastEmitted();
        this.firstStarted = mark.getStartTime();
      }
    }

    @Override
    public boolean start() throws IOException {
      if (firstStarted == null) {
        this.firstStarted = Instant.now();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      // Overflow-safe check that (current + source.stride) <= LONG.MAX_VALUE. Else, stop producing.
      if (Long.MAX_VALUE - source.stride < current) {
        return false;
      }
      long nextValue = current + source.stride;
      if (expectedValue() < nextValue) {
        return false;
      }
      elementsRead.inc();
      current = nextValue;
      currentTimestamp = source.timestampFn.apply(current);
      return true;
    }

    private long expectedValue() {
      if (source.period.getMillis() == 0L) {
        return Long.MAX_VALUE;
      }
      double periodsElapsed =
          (Instant.now().getMillis() - firstStarted.getMillis())
              / (double) source.period.getMillis();
      return (long) (source.elementsPerPeriod * periodsElapsed);
    }

    @Override
    public Instant getWatermark() {
      return source.timestampFn.apply(current);
    }

    @Override
    public CounterMark getCheckpointMark() {
      return new CounterMark(current, firstStarted);
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

    @Override
    public long getSplitBacklogBytes() {
      long expected = expectedValue();
      long backlogElements = (expected - current) / source.stride;
      return Math.max(0L, 8 * backlogElements);
    }
  }

  /**
   * The checkpoint for an unbounded {@link CountingSource} is simply the last value produced. The
   * associated source object encapsulates the information needed to produce the next value.
   */
  @DefaultCoder(AvroCoder.class)
  public static class CounterMark implements UnboundedSource.CheckpointMark {
    /** The last value emitted. */
    private final long lastEmitted;

    private final Instant startTime;

    /** Creates a checkpoint mark reflecting the last emitted value. */
    public CounterMark(long lastEmitted, Instant startTime) {
      this.lastEmitted = lastEmitted;
      this.startTime = startTime;
    }

    /** Returns the last value emitted by the reader. */
    public long getLastEmitted() {
      return lastEmitted;
    }

    /** Returns the time the reader was started. */
    public Instant getStartTime() {
      return startTime;
    }

    /////////////////////////////////////////////////////////////////////////////////////

    @SuppressWarnings("unused") // For AvroCoder
    private CounterMark() {
      this.lastEmitted = 0L;
      this.startTime = Instant.now();
    }

    @Override
    public void finalizeCheckpoint() throws IOException {}
  }
}
