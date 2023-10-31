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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceReaderTestBase;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.SourceTestCompat.TestMetricGroup;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.junit.Test;
import org.mockito.Mockito;

/** Unite tests for {@link FlinkBoundedSourceReader}. */
public class FlinkBoundedSourceReaderTest
    extends FlinkSourceReaderTestBase<WindowedValue<KV<Integer, Integer>>> {

  @Test
  public void testPollWithIdleTimeout() throws Exception {
    ManuallyTriggeredScheduledExecutorService executor =
        new ManuallyTriggeredScheduledExecutorService();
    ReaderOutput<WindowedValue<KV<Integer, Integer>>> mockReaderOutput =
        Mockito.mock(ReaderOutput.class);
    try (FlinkBoundedSourceReader<KV<Integer, Integer>> reader =
        (FlinkBoundedSourceReader<KV<Integer, Integer>>) createReader(executor, 1)) {
      reader.notifyNoMoreSplits();
      assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(mockReaderOutput));

      executor.triggerScheduledTasks();
      assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(mockReaderOutput));
    }
  }

  @Test
  public void testPollWithoutIdleTimeout() throws Exception {
    ReaderOutput<WindowedValue<KV<Integer, Integer>>> mockReaderOutput =
        Mockito.mock(ReaderOutput.class);
    try (SourceReader<WindowedValue<KV<Integer, Integer>>, FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      reader.notifyNoMoreSplits();
      assertEquals(InputStatus.END_OF_INPUT, reader.pollNext(mockReaderOutput));
    }
  }

  @Test
  public void testIsAvailableOnSplitsAssignment() throws Exception {
    try (SourceReader<WindowedValue<KV<Integer, Integer>>, FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      reader.start();

      CompletableFuture<Void> future1 = reader.isAvailable();
      assertFalse("No split assigned yet, should not be available.", future1.isDone());

      // Data available on split assigned.
      reader.addSplits(createSplits(1, 1, 0));
      assertTrue("Adding a split should complete future1", future1.isDone());
      assertTrue("Data should be available with a live split.", reader.isAvailable().isDone());
    }
  }

  @Test
  public void testSnapshotStateAndRestore() throws Exception {
    final int numSplits = 2;
    final int numRecordsPerSplit = 10;

    List<FlinkSourceSplit<KV<Integer, Integer>>> splits =
        createSplits(numSplits, numRecordsPerSplit, 0);
    RecordsValidatingOutput validatingOutput = new RecordsValidatingOutput(splits);
    List<FlinkSourceSplit<KV<Integer, Integer>>> snapshot;

    // Create a reader, take a snapshot.
    try (SourceReader<WindowedValue<KV<Integer, Integer>>, FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      // Only poll half of the records in the first split.
      pollAndValidate(reader, splits, validatingOutput, numRecordsPerSplit / 2);
      snapshot = reader.snapshotState(0L);
    }

    // Create a new validating output because the first split will be consumed from very beginning.
    validatingOutput = new RecordsValidatingOutput(splits);
    // Create another reader, add the snapshot splits back.
    try (SourceReader<WindowedValue<KV<Integer, Integer>>, FlinkSourceSplit<KV<Integer, Integer>>>
        reader = createReader()) {
      pollAndValidate(reader, snapshot, validatingOutput, Integer.MAX_VALUE);
    }
  }

  // --------------- abstract methods impl ----------------
  @Override
  protected KV<Integer, Integer> getKVPairs(WindowedValue<KV<Integer, Integer>> record) {
    return record.getValue();
  }

  @Override
  protected Source<KV<Integer, Integer>> createBeamSource(int splitIndex, int numRecordsPerSplit) {
    return new TestBoundedCountingSource(splitIndex, numRecordsPerSplit);
  }

  @Override
  protected FlinkBoundedSourceReader<KV<Integer, Integer>> createReader(
      ScheduledExecutorService executor,
      long idleTimeoutMs,
      @Nullable Function<WindowedValue<KV<Integer, Integer>>, Long> timestampExtractor,
      TestMetricGroup testMetricGroup) {
    FlinkPipelineOptions pipelineOptions = FlinkPipelineOptions.defaults();
    pipelineOptions.setShutdownSourcesAfterIdleMs(idleTimeoutMs);
    SourceReaderContext mockContext = createSourceReaderContext(testMetricGroup);
    if (executor != null) {
      return new FlinkBoundedSourceReader<>(
          "FlinkBoundedSource", mockContext, pipelineOptions, executor, timestampExtractor);
    } else {
      return new FlinkBoundedSourceReader<>(
          "FlinkBoundedSource", mockContext, pipelineOptions, timestampExtractor);
    }
  }
}
