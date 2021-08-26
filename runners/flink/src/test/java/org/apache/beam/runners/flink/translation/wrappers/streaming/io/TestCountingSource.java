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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An unbounded source for testing the unbounded sources framework code.
 *
 * <p>Each split of this sources produces records of the form KV(split_id, i), where i counts up
 * from 0. Each record has a timestamp of i, and the watermark accurately tracks these timestamps.
 * The reader will occasionally return false from {@code advance}, in order to simulate a source
 * where not all the data is available immediately.
 */
public class TestCountingSource
    extends UnboundedSource<KV<Integer, Integer>, TestCountingSource.CounterMark> {
  private static final Logger LOG = LoggerFactory.getLogger(TestCountingSource.class);

  private static List<Integer> finalizeTracker;
  private final int numMessagesPerShard;
  private final int shardNumber;
  private final boolean dedup;
  private final boolean throwOnFirstSnapshot;
  private final int fixedNumSplits;

  /** Flag to stall processing readers' elements. */
  private transient volatile boolean haltEmission;

  /**
   * We only allow an exception to be thrown from getCheckpointMark at most once. This must be
   * static since the entire TestCountingSource instance may re-serialized when the pipeline
   * recovers and retries.
   */
  private static boolean thrown = false;

  public static void setFinalizeTracker(List<Integer> finalizeTracker) {
    TestCountingSource.finalizeTracker = finalizeTracker;
  }

  public TestCountingSource(int numMessagesPerShard) {
    this(numMessagesPerShard, 0, false, false, -1);
  }

  public TestCountingSource withDedup() {
    return new TestCountingSource(numMessagesPerShard, shardNumber, true, throwOnFirstSnapshot, -1);
  }

  private TestCountingSource withShardNumber(int shardNumber) {
    return new TestCountingSource(
        numMessagesPerShard, shardNumber, dedup, throwOnFirstSnapshot, -1);
  }

  public TestCountingSource withThrowOnFirstSnapshot(boolean throwOnFirstSnapshot) {
    return new TestCountingSource(
        numMessagesPerShard, shardNumber, dedup, throwOnFirstSnapshot, -1);
  }

  public TestCountingSource withoutSplitting() {
    return new TestCountingSource(numMessagesPerShard, shardNumber, dedup, throwOnFirstSnapshot, 1);
  }

  public TestCountingSource withFixedNumSplits(int maxNumSplits) {
    return new TestCountingSource(
        numMessagesPerShard, shardNumber, dedup, throwOnFirstSnapshot, maxNumSplits);
  }

  private TestCountingSource(
      int numMessagesPerShard,
      int shardNumber,
      boolean dedup,
      boolean throwOnFirstSnapshot,
      int fixedNumSplits) {
    this.numMessagesPerShard = numMessagesPerShard;
    this.shardNumber = shardNumber;
    this.dedup = dedup;
    this.throwOnFirstSnapshot = throwOnFirstSnapshot;
    this.fixedNumSplits = fixedNumSplits;
  }

  /** Halts emission of elements until {@code continueEmission} is invoked. */
  void haltEmission() {
    haltEmission = true;
  }

  /** Continues processing elements after {@code haltEmission} was invoked. */
  void continueEmission() {
    haltEmission = false;
  }

  @Override
  public List<TestCountingSource> split(int desiredNumSplits, PipelineOptions options) {
    List<TestCountingSource> splits = new ArrayList<>();
    int actualNumSplits = (fixedNumSplits == -1) ? desiredNumSplits : fixedNumSplits;
    for (int i = 0; i < actualNumSplits; i++) {
      splits.add(withShardNumber(i));
    }
    return splits;
  }

  static class CounterMark implements UnboundedSource.CheckpointMark {
    int current;

    public CounterMark(int current) {
      this.current = current;
    }

    @Override
    public void finalizeCheckpoint() {
      if (finalizeTracker != null) {
        finalizeTracker.add(current);
      }
    }
  }

  @Override
  public Coder<CounterMark> getCheckpointMarkCoder() {
    return DelegateCoder.of(VarIntCoder.of(), new FromCounterMark(), new ToCounterMark());
  }

  @Override
  public boolean requiresDeduping() {
    return dedup;
  }

  /**
   * Public only so that the checkpoint can be conveyed from {@link #getCheckpointMark()} to {@link
   * TestCountingSource#createReader(PipelineOptions, CounterMark)} without cast.
   */
  public class CountingSourceReader extends UnboundedReader<KV<Integer, Integer>> {
    private int current;

    public CountingSourceReader(int startingPoint) {
      this.current = startingPoint;
    }

    @Override
    public boolean start() {
      return advance();
    }

    @Override
    public boolean advance() {
      if (current >= numMessagesPerShard - 1 || haltEmission) {
        return false;
      }
      // If testing dedup, occasionally insert a duplicate value;
      if (current >= 0 && dedup && ThreadLocalRandom.current().nextInt(5) == 0) {
        return true;
      }
      current++;
      return true;
    }

    @Override
    public KV<Integer, Integer> getCurrent() {
      return KV.of(shardNumber, current);
    }

    @Override
    public Instant getCurrentTimestamp() {
      return new Instant(current);
    }

    @Override
    public byte[] getCurrentRecordId() {
      try {
        return encodeToByteArray(KvCoder.of(VarIntCoder.of(), VarIntCoder.of()), getCurrent());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {}

    @Override
    public TestCountingSource getCurrentSource() {
      return TestCountingSource.this;
    }

    @Override
    public Instant getWatermark() {
      if (current >= numMessagesPerShard - 1) {
        // we won't emit further data, signal this with the final watermark
        return new Instant(BoundedWindow.TIMESTAMP_MAX_VALUE);
      }

      // The watermark is a promise about future elements, and the timestamps of elements are
      // strictly increasing for this source.
      return new Instant(current + 1);
    }

    @Override
    public CounterMark getCheckpointMark() {
      if (throwOnFirstSnapshot && !thrown) {
        thrown = true;
        LOG.error("Throwing exception while checkpointing counter");
        throw new RuntimeException("failed during checkpoint");
      }
      // The checkpoint can assume all records read, including the current, have
      // been commited.
      return new CounterMark(current);
    }

    @Override
    public long getSplitBacklogBytes() {
      return 7L;
    }
  }

  @Override
  public CountingSourceReader createReader(
      PipelineOptions options, @Nullable CounterMark checkpointMark) {
    if (checkpointMark == null) {
      LOG.debug("creating reader");
    } else {
      LOG.debug("restoring reader from checkpoint with current = {}", checkpointMark.current);
    }
    return new CountingSourceReader(checkpointMark != null ? checkpointMark.current : -1);
  }

  @Override
  public Coder<KV<Integer, Integer>> getOutputCoder() {
    return KvCoder.of(VarIntCoder.of(), VarIntCoder.of());
  }

  private static class FromCounterMark
      implements DelegateCoder.CodingFunction<CounterMark, Integer> {
    @Override
    public Integer apply(CounterMark input) {
      return input.current;
    }

    @Override
    public int hashCode() {
      return FromCounterMark.class.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj instanceof FromCounterMark;
    }
  }

  private static class ToCounterMark implements DelegateCoder.CodingFunction<Integer, CounterMark> {
    @Override
    public CounterMark apply(Integer input) {
      return new CounterMark(input);
    }

    @Override
    public int hashCode() {
      return ToCounterMark.class.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      return obj instanceof ToCounterMark;
    }
  }
}
