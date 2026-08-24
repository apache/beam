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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.utils.SerdeUtils;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestBoundedCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.junit.Test;

/** Unit tests for the Flink source split enumerators. */
public class FlinkSourceSplitEnumeratorTest {
  private static final long MEBIBYTE = 1024L * 1024L;
  private static final long AUTO_THRESHOLD_MB = 6144L;
  private static final int SOURCE_PARALLELISM = 2;
  private static final int REQUESTED_SPLITS = 4;

  @Test
  public void testSmallBoundedSourceUsesStaticAssignmentAsynchronously() throws Exception {
    FlinkPipelineOptions options = autoOptions();
    long thresholdBytes = AUTO_THRESHOLD_MB * MEBIBYTE;
    long estimatedSizeBytes = SOURCE_PARALLELISM * thresholdBytes - 1L;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      assertTrue(enumerator instanceof SizeBasedFlinkSourceSplitEnumerator);
      assertEquals(0, testSource.getEstimationCalls());
      enumerator.start();
      assertEquals(
          "start must schedule estimation instead of blocking the coordinator thread",
          0,
          testSource.getEstimationCalls());

      context.getExecutorService().triggerAll();

      FlinkSourceEnumeratorState<String> state = enumerator.snapshotState(1L);
      assertEquals(FlinkSourceSplitAssignmentMode.STATIC, state.getAssignmentMode());
      assertEquals(1, testSource.getEstimationCalls());
      assertEquals(estimatedSizeBytes / REQUESTED_SPLITS, testSource.getDesiredBundleSizeBytes());
    }
  }

  @Test
  public void testLargeBoundedSourceUsesLazyAssignment() throws Exception {
    FlinkPipelineOptions options = autoOptions();
    long thresholdBytes = AUTO_THRESHOLD_MB * MEBIBYTE;
    long estimatedSizeBytes = SOURCE_PARALLELISM * thresholdBytes;
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(estimatedSizeBytes, REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      assertTrue(enumerator instanceof SizeBasedFlinkSourceSplitEnumerator);
      enumerator.start();
      context.getExecutorService().triggerAll();

      FlinkSourceEnumeratorState<String> state = enumerator.snapshotState(1L);
      assertEquals(FlinkSourceSplitAssignmentMode.LAZY, state.getAssignmentMode());
      assertEquals(1, testSource.getEstimationCalls());
      assertEquals(estimatedSizeBytes / REQUESTED_SPLITS, testSource.getDesiredBundleSizeBytes());
    }
  }

  @Test
  public void testSizeBasedSelectionReplaysLazyRequestAfterInitialization() throws Exception {
    FlinkPipelineOptions options = autoOptions();
    long estimatedSizeBytes = SOURCE_PARALLELISM * AUTO_THRESHOLD_MB * MEBIBYTE;
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
      assertEquals(
          FlinkSourceSplitAssignmentMode.LAZY, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  @Test
  public void testSizeBasedSelectionAssignsStaticSplitsToEarlyReaders() throws Exception {
    FlinkPipelineOptions options = autoOptions();
    long estimatedSizeBytes = SOURCE_PARALLELISM * AUTO_THRESHOLD_MB * MEBIBYTE - 1L;
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
      assertEquals(
          FlinkSourceSplitAssignmentMode.STATIC, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  @Test
  public void testEstimationFailureFailsSplitInitialization() throws Exception {
    FlinkPipelineOptions options = autoOptions();
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.createFailing(REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      enumerator.start();
      assertThrows(RuntimeException.class, () -> context.getExecutorService().triggerAll());
      assertEquals(1, testSource.getEstimationCalls());
    }
  }

  @Test
  public void testUnknownEstimatesUseLazyAssignmentAndPreserveBundleSize() throws Exception {
    long[] unknownEstimates = {-1L, Long.MAX_VALUE};
    for (long unknownEstimate : unknownEstimates) {
      FlinkPipelineOptions options = autoOptions();
      TestEstimatedSizeBoundedSource testSource =
          TestEstimatedSizeBoundedSource.create(unknownEstimate, REQUESTED_SPLITS);
      TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
          new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
      FlinkSource<String, ?> flinkSource =
          createBoundedSource(testSource, options, REQUESTED_SPLITS);

      try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>>
          enumerator = flinkSource.createEnumerator(context)) {
        enumerator.start();
        context.getExecutorService().triggerAll();

        FlinkSourceEnumeratorState<String> state = enumerator.snapshotState(1L);
        assertEquals(FlinkSourceSplitAssignmentMode.LAZY, state.getAssignmentMode());
        assertEquals(1, testSource.getEstimationCalls());
        assertEquals(unknownEstimate / REQUESTED_SPLITS, testSource.getDesiredBundleSizeBytes());
      }
    }
  }

  @Test
  public void testConfigurationCanForceLazyOrStaticAssignment() throws Exception {
    long largeEstimate = 1024L * MEBIBYTE;
    assertAssignmentMode(
        largeEstimate, -1L, FlinkSourceSplitAssignmentMode.STATIC, REQUESTED_SPLITS);
    assertAssignmentMode(
        largeEstimate, -100L, FlinkSourceSplitAssignmentMode.STATIC, REQUESTED_SPLITS);
    assertAssignmentMode(0L, 0L, FlinkSourceSplitAssignmentMode.LAZY, REQUESTED_SPLITS);
    assertAssignmentMode(
        largeEstimate, Long.MAX_VALUE, FlinkSourceSplitAssignmentMode.STATIC, REQUESTED_SPLITS);
  }

  @Test
  public void testDefaultConfigurationKeepsLazyAssignmentWithoutThresholdComparison()
      throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    assertEquals(0L, (long) options.getLazySourceSplitAssignmentMinSizeMbPerReader());
    TestEstimatedSizeBoundedSource testSource =
        TestEstimatedSizeBoundedSource.create(MEBIBYTE, REQUESTED_SPLITS);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> context =
        new TestingSplitEnumeratorContext<>(SOURCE_PARALLELISM);
    FlinkSource<String, ?> flinkSource = createBoundedSource(testSource, options, REQUESTED_SPLITS);

    try (SplitEnumerator<FlinkSourceSplit<String>, FlinkSourceEnumeratorState<String>> enumerator =
        flinkSource.createEnumerator(context)) {
      assertTrue(enumerator instanceof LazyFlinkSourceSplitEnumerator);
      enumerator.start();
      context.getExecutorService().triggerAll();
      assertEquals(
          FlinkSourceSplitAssignmentMode.LAZY, enumerator.snapshotState(1L).getAssignmentMode());
    }
  }

  @Test
  public void testEmptyBoundedSourceUsesStaticAssignment() throws Exception {
    // An estimated size of exactly 0 is a valid answer (e.g. an empty file glob), not an
    // unknown estimate, and sits below any positive threshold.
    assertAssignmentMode(
        0L, AUTO_THRESHOLD_MB, FlinkSourceSplitAssignmentMode.STATIC, REQUESTED_SPLITS);
  }

  @Test
  public void testRestoreKeepsLazyAssignmentAcrossRescaleWithoutEstimating() throws Exception {
    final int initialParallelism = 4;
    final int restoredParallelism = 1;
    final int generatedSplits = 4;
    FlinkPipelineOptions options = autoOptions();
    long thresholdBytes = AUTO_THRESHOLD_MB * MEBIBYTE;
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
      assertEquals(FlinkSourceSplitAssignmentMode.LAZY, checkpoint.getAssignmentMode());
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
      assertEquals(
          FlinkSourceSplitAssignmentMode.LAZY, restored.snapshotState(2L).getAssignmentMode());
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

    assertEquals(FlinkSourceSplitAssignmentMode.LAZY, upgraded.getAssignmentMode());
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
        new FlinkSourceEnumeratorStateSerializer<String>(FlinkSourceSplitAssignmentMode.STATIC)
            .deserialize(0, serialized);
    assertEquals(FlinkSourceSplitAssignmentMode.STATIC, upgraded.getAssignmentMode());
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
  public void testStaticRestoreReturnsPendingSplitsToOriginalOwnersAtSameParallelism()
      throws Exception {
    final int parallelism = 4;
    TestEstimatedSizeBoundedSource testSource = TestEstimatedSizeBoundedSource.create(1L, 1);
    ArrayList<FlinkSourceSplit<String>> pendingSplits = new ArrayList<>();
    pendingSplits.add(new FlinkSourceSplit<>(3, testSource));
    FlinkSourceEnumeratorState<String> checkpoint =
        new FlinkSourceEnumeratorState<>(FlinkSourceSplitAssignmentMode.STATIC, pendingSplits);

    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> restoredContext =
        new TestingSplitEnumeratorContext<>(parallelism);
    try (FlinkSourceSplitEnumerator<String> restored =
        new FlinkSourceSplitEnumerator<>(
            restoredContext, testSource, staticOptions(), parallelism, checkpoint)) {
      restored.start();
      for (int subtaskId = 0; subtaskId < parallelism; subtaskId++) {
        restoredContext.registerReader(subtaskId, "reader-" + subtaskId);
        restored.addReader(subtaskId);
      }

      for (int subtaskId = 0; subtaskId < parallelism - 1; subtaskId++) {
        assertEquals(
            0, restoredContext.getSplitAssignments().get(subtaskId).getAssignedSplits().size());
      }
      assertEquals(
          Collections.singletonList(3), assignedSplitIndexesForSubtask(restoredContext, 3));
      restoredContext
          .getSplitAssignments()
          .values()
          .forEach(state -> assertTrue(state.hasReceivedNoMoreSplitsSignal()));
    }
  }

  @Test
  public void testSerializerRejectsUnknownVersionsAndUnexpectedPayloads() throws Exception {
    FlinkSourceEnumeratorStateSerializer<String> serializer =
        new FlinkSourceEnumeratorStateSerializer<>(FlinkSourceSplitAssignmentMode.LAZY);
    byte[] legacyMapBytes =
        SerdeUtils.serializeObject(Collections.singletonMap(1, new ArrayList<>()));
    byte[] stateBytes =
        serializer.serialize(
            new FlinkSourceEnumeratorState<>(
                FlinkSourceSplitAssignmentMode.LAZY, new ArrayList<>()));

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
    FlinkPipelineOptions options = autoOptions();
    long thresholdBytes = AUTO_THRESHOLD_MB * MEBIBYTE;
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
      assertEquals(FlinkSourceSplitAssignmentMode.STATIC, checkpoint.getAssignmentMode());
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
  public void testSignalsNoMoreSplitsToEarlyReaderWithoutAssignment() throws IOException {
    final int numSubtasks = 2;
    final int numSplits = 1;
    TestingSplitEnumeratorContext<FlinkSourceSplit<KV<Integer, Integer>>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestBoundedCountingSource testSource = new TestBoundedCountingSource(numSplits, numSplits);

    try (FlinkSourceSplitEnumerator<KV<Integer, Integer>> enumerator =
        new FlinkSourceSplitEnumerator<>(testContext, testSource, staticOptions(), numSplits)) {
      enumerator.start();
      for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
        testContext.registerReader(subtaskId, String.valueOf(subtaskId));
        enumerator.addReader(subtaskId);
      }
      testContext.getExecutorService().triggerAll();

      assertEquals(numSubtasks, testContext.getSplitAssignments().size());
      assertEquals(1, testContext.getSplitAssignments().get(0).getAssignedSplits().size());
      assertEquals(0, testContext.getSplitAssignments().get(1).getAssignedSplits().size());
      assertTrue(testContext.getSplitAssignments().get(0).hasReceivedNoMoreSplitsSignal());
      assertTrue(testContext.getSplitAssignments().get(1).hasReceivedNoMoreSplitsSignal());
    }
  }

  @Test
  public void testStaticAssignmentRespectsFileInputSplitMaxSize() throws IOException {
    final int numSubtasks = 2;
    final int requestedSplits = 2;
    final long fileSize = 100L * MEBIBYTE;
    final long maxSplitSizeMb = 10L;
    final int expectedSplits = 10;
    FlinkPipelineOptions options = staticOptions();
    options.setFileInputSplitMaxSizeMB(maxSplitSizeMb);
    TestingSplitEnumeratorContext<FlinkSourceSplit<String>> testContext =
        new TestingSplitEnumeratorContext<>(numSubtasks);
    TestFileBasedSource testSource = TestFileBasedSource.create(fileSize);

    try (FlinkSourceSplitEnumerator<String> enumerator =
        new FlinkSourceSplitEnumerator<>(testContext, testSource, options, requestedSplits)) {
      enumerator.start();
      for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
        testContext.registerReader(subtaskId, String.valueOf(subtaskId));
        enumerator.addReader(subtaskId);
      }
      testContext.getExecutorService().triggerAll();

      assertEquals(expectedSplits, countAssignedSplits(testContext));
      testContext
          .getSplitAssignments()
          .values()
          .forEach(
              state -> {
                assertEquals(expectedSplits / numSubtasks, state.getAssignedSplits().size());
                assertTrue(state.hasReceivedNoMoreSplitsSignal());
              });
    }
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
      FlinkSourceSplitAssignmentMode expectedMode,
      int generatedSplits)
      throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setLazySourceSplitAssignmentMinSizeMbPerReader(configuredThresholdMb);
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
    options.setLazySourceSplitAssignmentMinSizeMbPerReader(-1L);
    return options;
  }

  private static FlinkPipelineOptions autoOptions() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setLazySourceSplitAssignmentMinSizeMbPerReader(AUTO_THRESHOLD_MB);
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
    private final boolean failEstimation;
    private final EstimationTracker tracker;

    private TestEstimatedSizeBoundedSource(
        long estimatedSizeBytes,
        int generatedSplits,
        boolean failEstimation,
        EstimationTracker tracker) {
      this.estimatedSizeBytes = estimatedSizeBytes;
      this.generatedSplits = generatedSplits;
      this.failEstimation = failEstimation;
      this.tracker = tracker;
    }

    private static TestEstimatedSizeBoundedSource create(
        long estimatedSizeBytes, int generatedSplits) {
      return new TestEstimatedSizeBoundedSource(
          estimatedSizeBytes, generatedSplits, false, new EstimationTracker());
    }

    private static TestEstimatedSizeBoundedSource createFailing(int generatedSplits) {
      return new TestEstimatedSizeBoundedSource(0L, generatedSplits, true, new EstimationTracker());
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      tracker.desiredBundleSizeBytes.set(desiredBundleSizeBytes);
      List<TestEstimatedSizeBoundedSource> splits = new ArrayList<>(generatedSplits);
      for (int i = 0; i < generatedSplits; i++) {
        splits.add(new TestEstimatedSizeBoundedSource(1L, 1, false, tracker));
      }
      return splits;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      tracker.estimationCalls.incrementAndGet();
      if (failEstimation) {
        throw new IOException("Expected test estimation failure");
      }
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
      return tracker.estimationCalls.get();
    }

    private long getDesiredBundleSizeBytes() {
      return tracker.desiredBundleSizeBytes.get();
    }
  }

  private static final class EstimationTracker implements Serializable {
    private static final long serialVersionUID = 1L;

    private final AtomicInteger estimationCalls = new AtomicInteger();
    private final AtomicLong desiredBundleSizeBytes = new AtomicLong(-1L);
  }

  private static final class TestFileBasedSource extends FileBasedSource<String> {
    private TestFileBasedSource(Metadata metadata, long startOffset, long endOffset) {
      super(metadata, 1L, startOffset, endOffset);
    }

    private static TestFileBasedSource create(long sizeBytes) {
      Metadata metadata =
          Metadata.builder()
              .setResourceId(FileSystems.matchNewResource("static-split-size-test", false))
              .setSizeBytes(sizeBytes)
              .setIsReadSeekEfficient(true)
              .build();
      return new TestFileBasedSource(metadata, 0L, sizeBytes);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    protected FileBasedSource<String> createForSubrangeOfFile(
        Metadata metadata, long start, long end) {
      return new TestFileBasedSource(metadata, start, end);
    }

    @Override
    protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
      throw new UnsupportedOperationException("This source is only used to test split sizing");
    }
  }
}
