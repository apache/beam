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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded;

import static org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSourceReader.PENDING_BYTES_METRIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceReaderTestBase;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.SourceTestCompat.TestMetricGroup;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.metrics.Gauge;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Unite tests for {@link FlinkUnboundedSourceReader}. */
public class FlinkUnboundedSourceReaderTest
    extends FlinkSourceReaderTestBase<WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>> {

  @Test
  public void testSnapshotStateAndRestore() throws Exception {
    final int numSplits = 2;
    final int numRecordsPerSplit = 10;

    List<FlinkSourceSplit<KV<Integer, Integer>>> splits =
        createSplits(numSplits, numRecordsPerSplit, 0);
    RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);
    List<FlinkSourceSplit<KV<Integer, Integer>>> snapshot;

    // Create a reader, take a snapshot.
    try (SourceReader<
            WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>,
            FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      pollAndValidate(reader, splits, validatingOutput, numSplits * numRecordsPerSplit / 2);
      snapshot = reader.snapshotState(0L);
    }

    // Create another reader, add the snapshot splits back.
    try (SourceReader<
            WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>,
            FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      pollAndValidate(reader, snapshot, validatingOutput, Integer.MAX_VALUE);
    }
  }

  /**
   * This is a concurrency correctness test. It verifies that the main thread is always waken up by
   * the alarm runner executed in the executor thread.
   */
  @Test(timeout = 30000L)
  public void testIsAvailableAlwaysWakenUp() throws Exception {
    final int numFuturesRequired = 1_000_000;
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    List<FlinkSourceSplit<KV<Integer, Integer>>> splits = new ArrayList<>();
    splits.add(new FlinkSourceSplit<>(0, new DummySource(Integer.MAX_VALUE)));
    RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();

    try (SourceReader<
            WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>,
            FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader(executor, Long.MAX_VALUE)) {
      reader.start();
      reader.addSplits(splits);

      Thread mainThread =
          new Thread(
              () -> {
                try {
                  while (futures.size() < numFuturesRequired) {
                    // This poll will return NOTHING_AVAILABLE after each record emission.
                    if (reader.pollNext(validatingOutput) == InputStatus.NOTHING_AVAILABLE) {
                      CompletableFuture<Void> future = reader.isAvailable();
                      future.get();
                      futures.add(future);
                    }
                  }
                } catch (Exception e) {
                  if (!exceptionRef.compareAndSet(null, e)) {
                    exceptionRef.get().addSuppressed(e);
                  }
                }
              },
              "MainThread");

      Thread executorThread =
          new Thread(
              () -> {
                while (futures.size() < numFuturesRequired) {
                  executor.triggerScheduledTasks();
                }
              },
              "ExecutorThread");

      mainThread.start();
      executorThread.start();
      executorThread.join();
    }
  }

  @Test
  public void testIsAvailableOnSplitChangeWhenNoDataAvailableForAliveReaders() throws Exception {
    List<FlinkSourceSplit<KV<Integer, Integer>>> splits1 = new ArrayList<>();
    List<FlinkSourceSplit<KV<Integer, Integer>>> splits2 = new ArrayList<>();
    splits1.add(new FlinkSourceSplit<>(0, new DummySource(0)));
    splits2.add(new FlinkSourceSplit<>(1, new DummySource(0)));
    RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits1);
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();

    try (SourceReader<
            WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>,
            FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader(executor, Long.MAX_VALUE)) {
      reader.start();
      reader.addSplits(splits1);

      assertEquals(
          "The reader should have nothing available",
          InputStatus.NOTHING_AVAILABLE,
          reader.pollNext(validatingOutput));

      CompletableFuture<Void> future1 = reader.isAvailable();
      assertFalse("Future1 should be uncompleted without live split.", future1.isDone());

      reader.addSplits(splits2);
      assertTrue("Future1 should be completed upon addition of new splits.", future1.isDone());

      CompletableFuture<Void> future2 = reader.isAvailable();
      assertFalse("Future2 should be uncompleted without live split.", future2.isDone());

      reader.notifyNoMoreSplits();
      assertTrue("Future2 should be completed upon NoMoreSplitsNotification.", future2.isDone());
    }
  }

  @Test
  public void testWatermark() throws Exception {
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();
    try (FlinkUnboundedSourceReader<KV<Integer, Integer>> reader =
        (FlinkUnboundedSourceReader<KV<Integer, Integer>>) createReader(executor, -1L)) {
      List<FlinkSourceSplit<KV<Integer, Integer>>> splits = createSplits(2, 10, 0);
      RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);

      reader.start();
      reader.addSplits(splits);

      // Poll 3 records from split 0 and 2 records from split 1.
      for (int i = 0; i < 5; i++) {
        reader.pollNext(validatingOutput);
      }

      Map<String, TestSourceOutput> sourceOutputs = validatingOutput.createdSourceOutputs();
      assertEquals("There should be 2 source outputs created.", 2, sourceOutputs.size());
      assertNull(sourceOutputs.get("0").watermark());
      assertNull(sourceOutputs.get("1").watermark());

      // Trigger the periodic task marking the watermark emission flag.
      executor.triggerScheduledTasks();
      // Poll one more time to actually emit the watermark. Getting record value 2 from split_1.
      reader.pollNext(validatingOutput);

      assertEquals(3, sourceOutputs.get("0").watermark().getTimestamp());
      assertEquals(2, sourceOutputs.get("1").watermark().getTimestamp());

      // Poll one more time to ensure no additional watermark is emitted. Getting record value 3
      // from split_0.
      reader.pollNext(validatingOutput);
      assertEquals(3, sourceOutputs.get("0").watermark().getTimestamp());
      assertEquals(2, sourceOutputs.get("1").watermark().getTimestamp());

      // Trigger the task to mark the watermark emission flag again.
      executor.triggerScheduledTasks();
      // Poll to actually emit the watermark. Getting (split_1 -> 3).
      reader.pollNext(validatingOutput);

      assertEquals(4, sourceOutputs.get("0").watermark().getTimestamp());
      assertEquals(3, sourceOutputs.get("1").watermark().getTimestamp());
    }
  }

  @Test
  public void testPendingBytesMetric() throws Exception {
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();
    TestMetricGroup testMetricGroup = new TestMetricGroup();
    try (SourceReader<
            WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>,
            FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader(executor, 0L, null, testMetricGroup)) {
      reader.start();

      List<FlinkSourceSplit<KV<Integer, Integer>>> splits = createSplits(2, 10, 0);
      reader.addSplits(splits);
      RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);

      // Need to poll once to create all the readers.
      reader.pollNext(validatingOutput);
      Gauge<Long> pendingBytesGauge =
          (Gauge<Long>) testMetricGroup.registeredGauge.get(PENDING_BYTES_METRIC_NAME);
      assertNotNull(pendingBytesGauge);
      // The TestCountingSource.CountingSourceReader always return 7L as backlog bytes. Because we
      // have 2 splits,
      // the expected value is the magic number 14 here.
      assertEquals(14L, pendingBytesGauge.getValue().longValue());
    }
  }

  // --------------- private helper classes -----------------
  /** A source whose advance() method only returns true occasionally. */
  private static class DummySource extends TestCountingSource {

    public DummySource(int numMessagesPerShard) {
      super(numMessagesPerShard);
    }

    @Override
    public CountingSourceReader createReader(
        PipelineOptions options, @Nullable CounterMark checkpointMark) {
      CountingSourceReader reader = new DummySourceReader();
      createdReaders().add(reader);
      return reader;
    }

    private class DummySourceReader extends TestCountingSource.CountingSourceReader {
      private final Random random = new Random();

      public DummySourceReader() {
        super(0);
      }

      @Override
      public boolean advance() {
        // Return true once every three times advance is invoked.
        if (random.nextInt(3) == 0) {
          return super.advance();
        } else {
          return false;
        }
      }
    }
  }

  // --------------- abstract methods impl ------------------
  @Override
  protected KV<Integer, Integer> getKVPairs(
      WindowedValue<ValueWithRecordId<KV<Integer, Integer>>> record) {
    return record.getValue().getValue();
  }

  @Override
  protected FlinkUnboundedSourceReader<KV<Integer, Integer>> createReader(
      ScheduledExecutorService executor,
      long idleTimeoutMs,
      Function<WindowedValue<ValueWithRecordId<KV<Integer, Integer>>>, Long> timestampExtractor,
      TestMetricGroup metricGroup) {
    FlinkPipelineOptions pipelineOptions = FlinkPipelineOptions.defaults();
    pipelineOptions.setShutdownSourcesAfterIdleMs(idleTimeoutMs);
    pipelineOptions.setAutoWatermarkInterval(10L);
    SourceReaderContext mockContext = createSourceReaderContext(metricGroup);
    if (executor != null) {
      return new FlinkUnboundedSourceReader<>(
          "FlinkUnboundedReader", mockContext, pipelineOptions, executor, timestampExtractor);
    } else {
      return new FlinkUnboundedSourceReader<>(
          "FlinkUnboundedReader", mockContext, pipelineOptions, timestampExtractor);
    }
  }

  @Override
  protected Source<KV<Integer, Integer>> createBeamSource(int splitIndex, int numRecordsPerSplit) {
    return new TestCountingSource(numRecordsPerSplit)
        .withShardNumber(splitIndex)
        .withFixedNumSplits(1);
  }
}
