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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceEnumeratorState.AssignmentMode;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.Test;

/** Unit tests for the Flink source split enumerators. */
public class FlinkSourceSplitEnumeratorTest {
  private static final long MEBIBYTE = 1024L * 1024L;
  private static final long STATIC_SPLIT_THRESHOLD_MB = 6144L;
  private static final int SOURCE_PARALLELISM = 2;
  private static final int REQUESTED_SPLITS = 4;

  @Test
  public void testSelectsAssignmentModeFromEstimatedSizeAndConfiguration() throws Exception {
    assertEquals(0L, (long) FlinkPipelineOptions.defaults().getSourceStaticSplitThresholdMb());

    long thresholdBytes = STATIC_SPLIT_THRESHOLD_MB * MEBIBYTE;
    long largeEstimate = 1024L * MEBIBYTE;
    long[] estimatedSizes = {
      SOURCE_PARALLELISM * thresholdBytes - 1L,
      SOURCE_PARALLELISM * thresholdBytes,
      -1L,
      0L,
      Long.MAX_VALUE,
      largeEstimate,
      largeEstimate,
      largeEstimate,
      largeEstimate
    };
    long[] configuredThresholds = {
      STATIC_SPLIT_THRESHOLD_MB,
      STATIC_SPLIT_THRESHOLD_MB,
      STATIC_SPLIT_THRESHOLD_MB,
      STATIC_SPLIT_THRESHOLD_MB,
      STATIC_SPLIT_THRESHOLD_MB,
      -1L,
      -100L,
      0L,
      Long.MAX_VALUE
    };
    AssignmentMode[] expectedModes = {
      AssignmentMode.STATIC,
      AssignmentMode.LAZY,
      AssignmentMode.LAZY,
      AssignmentMode.LAZY,
      AssignmentMode.LAZY,
      AssignmentMode.STATIC,
      AssignmentMode.STATIC,
      AssignmentMode.LAZY,
      AssignmentMode.STATIC
    };

    for (int i = 0; i < estimatedSizes.length; i++) {
      assertAssignmentMode(
          estimatedSizes[i], configuredThresholds[i], expectedModes[i], REQUESTED_SPLITS);
    }
  }

  @Test
  public void testSizeBasedSelectionReplaysLazyRequestAfterInitialization() throws Exception {
    FlinkPipelineOptions options = thresholdOptions();
    long estimatedSizeBytes = SOURCE_PARALLELISM * STATIC_SPLIT_THRESHOLD_MB * MEBIBYTE;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      enumerator.start();
      context.registerReader(0, "reader-0");
      enumerator.addReader(0);
      enumerator.handleSplitRequest(0, "reader-0");

      context.getExecutorService().triggerAll();

      assertEquals(1, context.getSplitAssignments().get(0).getAssignedSplits().size());
      assertEquals(AssignmentMode.LAZY, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  @Test
  public void testSizeBasedSelectionAssignsStaticSplitsToEarlyReaders() throws Exception {
    FlinkPipelineOptions options = thresholdOptions();
    long estimatedSizeBytes = SOURCE_PARALLELISM * STATIC_SPLIT_THRESHOLD_MB * MEBIBYTE - 1L;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      enumerator.start();
      for (int subtaskId = 0; subtaskId < SOURCE_PARALLELISM; subtaskId++) {
        context.registerReader(subtaskId, "reader-" + subtaskId);
        enumerator.addReader(subtaskId);
      }

      context.getExecutorService().triggerAll();

      assertEquals(REQUESTED_SPLITS, countAssignedSplits(context));
      context
          .getSplitAssignments()
          .values()
          .forEach(state -> assertTrue(state.hasReceivedNoMoreSplitsSignal()));
      assertEquals(AssignmentMode.STATIC, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  @Test
  public void testRestoreKeepsLazyAssignmentAcrossRescaleWithoutEstimating() throws Exception {
    final int initialParallelism = 4;
    final int restoredParallelism = 1;
    final int generatedSplits = 4;
    FlinkPipelineOptions options = thresholdOptions();
    long thresholdBytes = STATIC_SPLIT_THRESHOLD_MB * MEBIBYTE;
    long estimatedSizeBytes = initialParallelism * thresholdBytes;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, generatedSplits);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, generatedSplits);
    FlinkSourceEnumeratorState<String> checkpoint;

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> initialContext =
        new TestingSplitEnumeratorContext<>(initialParallelism);
    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(initialContext)) {
      enumerator.start();
      initialContext.getExecutorService().triggerAll();
      checkpoint = roundTripState(flinkSource, enumerator.snapshotState(1L));
      assertEquals(AssignmentMode.LAZY, checkpoint.getAssignmentMode());
      assertEquals(generatedSplits, checkpoint.getPendingSplits().size());
    }

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> restoredContext =
        new TestingSplitEnumeratorContext<>(restoredParallelism);
    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> restored =
        flinkSource.restoreEnumerator(restoredContext, checkpoint)) {
      restored.start();
      restoredContext.registerReader(0, "reader-0");
      restored.addReader(0);
      for (int i = 0; i < generatedSplits; i++) {
        restored.handleSplitRequest(0, "reader-0");
      }
      restored.handleSplitRequest(0, "reader-0");

      assertEquals(
          generatedSplits, restoredContext.getSplitAssignments().get(0).getAssignedSplits().size());
      assertTrue(restoredContext.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal());
      assertEquals(
          "restoring a decided strategy must not estimate the source again",
          1,
          testSource.getEstimationCalls());
      assertEquals(AssignmentMode.LAZY, restored.snapshotState(2L).getAssignmentMode());
    }
  }

  @Test
  public void testLegacyLazyCheckpointMapUpgradesToStrategyNeutralState() throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    TestEstimatedSizeBoundedSource testSource = TestEstimatedSizeBoundedSource.create(1L, 1);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, 1);
    FlinkSourceSplit<String> pendingSplit = new FlinkSourceSplit<>(0, testSource);
    Map<Integer, List<FlinkSourceSplit<String>>> legacyCheckpoint =
        Collections.singletonMap(1, Collections.singletonList(pendingSplit));
    byte[] serialized = SerdeUtils.serializeObject(legacyCheckpoint);

    FlinkSourceEnumeratorState<String> upgraded =
        flinkSource.getEnumeratorCheckpointSerializer().deserialize(0, serialized);

    assertEquals(AssignmentMode.LAZY, upgraded.getAssignmentMode());
    assertEquals(1, upgraded.getPendingSplits().size());
    assertEquals(0, upgraded.getPendingSplits().get(0).splitIndex());
  }

  @Test
  public void testLegacyStaticCheckpointMapKeepsSplitsOnTheirOriginalReaders() throws Exception {
    final int parallelism = 2;
    TestEstimatedSizeBoundedSource testSource = TestEstimatedSizeBoundedSource.create(1L, 1);
    Map<Integer, List<FlinkSourceSplit<String>>> legacyCheckpoint = new HashMap<>();
    legacyCheckpoint.put(
        0,
        Arrays.asList(
            new FlinkSourceSplit<>(0, testSource), new FlinkSourceSplit<>(2, testSource)));
    legacyCheckpoint.put(1, Collections.singletonList(new FlinkSourceSplit<>(1, testSource)));
    byte[] serialized = SerdeUtils.serializeObject(legacyCheckpoint);

    FlinkSourceEnumeratorState<String> upgraded =
        new FlinkSourceEnumeratorStateSerializer<String>(AssignmentMode.STATIC)
            .deserialize(0, serialized);
    assertEquals(AssignmentMode.STATIC, upgraded.getAssignmentMode());
    assertEquals(3, upgraded.getPendingSplits().size());

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> restoredContext =
        new TestingSplitEnumeratorContext<>(parallelism);
    try (FlinkSourceSplitEnumerator<String> restored =
        new FlinkSourceSplitEnumerator<>(
            restoredContext, testSource, staticOptions(), 3, upgraded)) {
      restored.start();
      for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
        restoredContext.registerReader(subtaskId, "reader-" + subtaskId);
        restored.addReader(subtaskId);
      }

      assertEquals(Arrays.asList(0, 2), assignedSplitIndexesForSubtask(restoredContext, 0));
      assertEquals(
          Collections.singletonList(1), assignedSplitIndexesForSubtask(restoredContext, 1));
    }
  }

  @Test
  public void testSerializerRejectsUnknownVersionsAndUnexpectedPayloads() throws Exception {
    FlinkSourceEnumeratorStateSerializer<String> serializer =
        new FlinkSourceEnumeratorStateSerializer<>(AssignmentMode.LAZY);
    byte[] legacyMapBytes =
        SerdeUtils.serializeObject(Collections.singletonMap(1, new ArrayList<>()));
    byte[] stateBytes =
        serializer.serialize(
            new FlinkSourceEnumeratorState<>(AssignmentMode.LAZY, new ArrayList<>()));

    // The payload type must match the version it was written with.
    assertThrows(IOException.class, () -> serializer.deserialize(1, legacyMapBytes));
    assertThrows(IOException.class, () -> serializer.deserialize(0, stateBytes));
    assertThrows(IOException.class, () -> serializer.deserialize(2, stateBytes));
  }

  @Test
  public void testRestoreRepartitionsStaticSplitsForNewParallelismWithoutEstimating()
      throws Exception {
    final int initialParallelism = 2;
    final int restoredParallelism = 3;
    final int generatedSplits = 5;
    FlinkPipelineOptions options = thresholdOptions();
    long thresholdBytes = STATIC_SPLIT_THRESHOLD_MB * MEBIBYTE;
    long estimatedSizeBytes = initialParallelism * thresholdBytes - 1L;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, generatedSplits);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, generatedSplits);
    FlinkSourceEnumeratorState<String> checkpoint;

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> initialContext =
        new TestingSplitEnumeratorContext<>(initialParallelism);
    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(initialContext)) {
      enumerator.start();
      initialContext.getExecutorService().triggerAll();
      checkpoint = roundTripState(flinkSource, enumerator.snapshotState(1L));
      assertEquals(AssignmentMode.STATIC, checkpoint.getAssignmentMode());
      assertEquals(generatedSplits, checkpoint.getPendingSplits().size());
    }

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> restoredContext =
        new TestingSplitEnumeratorContext<>(restoredParallelism);
    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> restored =
        flinkSource.restoreEnumerator(restoredContext, checkpoint)) {
      restored.start();
      for (int subtaskId = 0; subtaskId < restoredParallelism; subtaskId++) {
        restoredContext.registerReader(subtaskId, "reader-" + subtaskId);
        restored.addReader(subtaskId);
      }

      assertEquals(generatedSplits, countAssignedSplits(restoredContext));
      assertEquals(2, restoredContext.getSplitAssignments().get(0).getAssignedSplits().size());
      assertEquals(2, restoredContext.getSplitAssignments().get(1).getAssignedSplits().size());
      assertEquals(1, restoredContext.getSplitAssignments().get(2).getAssignedSplits().size());
      restoredContext
          .getSplitAssignments()
          .values()
          .forEach(state -> assertTrue(state.hasReceivedNoMoreSplitsSignal()));
      assertEquals(1, testSource.getEstimationCalls());
    }
  }

  @Test
  public void testAssignSplitsWithBoundedSource() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    final int totalNumRecords = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource =
        new TestBoundedCountingSource(numSplits, totalNumRecords);

    assignSplits(testContext, testSource, numSplits);
    assertEquals(numSubtasks, testContext.getSplitAssignments().size());

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              int expectedNumSplitsPerSubtask = numSplits / numSubtasks;
              assertEquals(expectedNumSplitsPerSubtask, state.getAssignedSplits().size());
              assertTrue(state.hasReceivedNoMoreSplitsSignal());
              state
                  .getAssignedSplits()
                  .forEach(
                      split -> {
                        TestBoundedCountingSource source =
                            (TestBoundedCountingSource) split.getBeamSplitSource();
                        try {
                          int expectedSplitSize = totalNumRecords / numSplits;
                          assertEquals(
                              expectedSplitSize,
                              source.getEstimatedSizeBytes(FlinkPipelineOptions.defaults()));
                        } catch (Exception error) {
                          fail("Received exception " + error);
                        }
                      });
            });
  }

  @Test
  public void testAssignSplitsWithUnboundedSource() throws IOException {
    final int numSplits = 10;
    final int numSubtasks = 5;
    final int numRecordsPerSplit = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestCountingSource testSource = new TestCountingSource(numRecordsPerSplit);

    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new FlinkSourceSplitEnumerator<>(
            testContext, testSource, FlinkPipelineOptions.defaults(), numSplits)) {
      enumerator.start();
      for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
        testContext.registerReader(subtaskId, String.valueOf(subtaskId));
        enumerator.addReader(subtaskId);
      }
      testContext.getExecutorService().triggerAll();
    }

    testContext
        .getSplitAssignments()
        .forEach(
            (subtaskId, state) -> {
              assertEquals(numSplits / numSubtasks, state.getAssignedSplits().size());
              assertTrue(state.hasReceivedNoMoreSplitsSignal());
            });
  }

  @Test
  public void testAddSplitsBackToStaticReader() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 10;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource = new TestBoundedCountingSource(numSplits, numSplits);

    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new FlinkSourceSplitEnumerator<>(testContext, testSource, staticOptions(), numSplits)) {
      enumerator.start();
      testContext.registerReader(0, "0");
      enumerator.addReader(0);
      testContext.getExecutorService().triggerAll();

      List<FlinkSourceSplit<KV<Integer, Integer>>> returnedSplits =
          new ArrayList<>(testContext.getSplitAssignments().get(0).getAssignedSplits());
      assertEquals(numSplits / numSubtasks, returnedSplits.size());

      enumerator.addSplitsBack(returnedSplits, 0);
      enumerator.addReader(0);
      assertEquals(
          2 * numSplits / numSubtasks,
          testContext.getSplitAssignments().get(0).getAssignedSplits().size());
    }
  }

  private void assertAssignmentMode(
      long estimatedSizeBytes,
      long configuredThresholdMb,
      AssignmentMode expectedMode,
      int generatedSplits)
      throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setSourceStaticSplitThresholdMb(configuredThresholdMb);
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, generatedSplits);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, generatedSplits);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      if (configuredThresholdMb < 0) {
        assertTrue(enumerator instanceof FlinkSourceSplitEnumerator);
      } else if (configuredThresholdMb == 0) {
        assertTrue(enumerator instanceof LazyFlinkSourceSplitEnumerator);
      } else {
        assertTrue(enumerator instanceof SizeBasedFlinkSourceSplitEnumerator);
      }
      enumerator.start();
      context.getExecutorService().triggerAll();
      assertEquals(expectedMode, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  private void assignSplits(
      TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> context,
      Source<KV<Integer, Integer>> source,
      int numSplits)
      throws IOException {
    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new FlinkSourceSplitEnumerator<>(context, source, staticOptions(), numSplits)) {
      enumerator.start();
      for (int subtaskId = 0; subtaskId < context.currentParallelism(); subtaskId++) {
        context.registerReader(subtaskId, String.valueOf(subtaskId));
        enumerator.addReader(subtaskId);
      }
      context.getExecutorService().triggerAll();
    }
  }

  private static FlinkPipelineOptions staticOptions() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setSourceStaticSplitThresholdMb(-1L);
    return options;
  }

  private static FlinkPipelineOptions thresholdOptions() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setSourceStaticSplitThresholdMb(STATIC_SPLIT_THRESHOLD_MB);
    return options;
  }

  private static FlinkSource<String, ?> createBoundedSource(
      BoundedSource<String> source, FlinkPipelineOptions options, int numSplits) {
    return FlinkSource.bounded(
        "test-bounded-source", source, new SerializablePipelineOptions(options), numSplits);
  }

  private static FlinkSourceEnumeratorState<String> roundTripState(
      FlinkSource<String, ?> source, FlinkSourceEnumeratorState<String> state) throws IOException {
    SimpleVersionedSerializer<FlinkSourceEnumeratorState<String>> serializer =
        source.getEnumeratorCheckpointSerializer();
    byte[] serialized = serializer.serialize(state);
    return serializer.deserialize(serializer.getVersion(), serialized);
  }

  private static <T> int countAssignedSplits(
      TestingSplitEnumeratorContext<FlinkSourceSplit<T>> context) {
    return context.getSplitAssignments().values().stream()
        .mapToInt(state -> state.getAssignedSplits().size())
        .sum();
  }

  private static <T> List<Integer> assignedSplitIndexesForSubtask(
      TestingSplitEnumeratorContext<FlinkSourceSplit<T>> context, int subtaskId) {
    List<Integer> splitIndexes = new ArrayList<>();
    for (FlinkSourceSplit<T> split :
        context.getSplitAssignments().get(subtaskId).getAssignedSplits()) {
      splitIndexes.add(split.splitIndex());
    }
    Collections.sort(splitIndexes);
    return splitIndexes;
  }

  private static final class TestEstimatedSizeBoundedSource extends BoundedSource<String> {
    private final long estimatedSizeBytes;
    private final int generatedSplits;
    private final AtomicInteger estimationCalls;

    private TestEstimatedSizeBoundedSource(
        long estimatedSizeBytes, int generatedSplits, AtomicInteger estimationCalls) {
      this.estimatedSizeBytes = estimatedSizeBytes;
      this.generatedSplits = generatedSplits;
      this.estimationCalls = estimationCalls;
    }

    private static TestEstimatedSizeBoundedSource create(
        long estimatedSizeBytes, int generatedSplits) {
      return new TestEstimatedSizeBoundedSource(
          estimatedSizeBytes, generatedSplits, new AtomicInteger());
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      List<TestEstimatedSizeBoundedSource> splits = new ArrayList<>(generatedSplits);
      for (int i = 0; i < generatedSplits; i++) {
        splits.add(new TestEstimatedSizeBoundedSource(1L, 1, estimationCalls));
      }
      return splits;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      estimationCalls.incrementAndGet();
      return estimatedSizeBytes;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) {
      throw new UnsupportedOperationException("This source is only used to test split assignment");
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    private int getEstimationCalls() {
      return estimationCalls.get();
    }
  }
}
