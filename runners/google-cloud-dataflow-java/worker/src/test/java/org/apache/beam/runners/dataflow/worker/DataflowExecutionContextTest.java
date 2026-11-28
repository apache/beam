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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.core.metrics.ExecutionStateTracker.PROCESS_STATE_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.auto.service.AutoService;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.BatchModeExecutionContext.BatchModeExecutionState;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ContextActivationObserverRegistry. */
@RunWith(JUnit4.class)
public class DataflowExecutionContextTest {

  /** This type is used for testing the automatic registration mechanism. */
  private static class AutoRegistrationClass implements ContextActivationObserver {
    private static boolean WAS_CALLED = false;

    @Override
    public void close() throws IOException {
      AutoRegistrationClass.WAS_CALLED = true;
    }

    @Override
    public Closeable activate(ExecutionStateTracker e) {
      return this;
    }
  }

  /** This type is used for testing the automatic registration mechanism. */
  private static class AutoRegistrationClassNotActive implements ContextActivationObserver {
    private static boolean WAS_CALLED = false;

    @Override
    public void close() throws IOException {
      AutoRegistrationClassNotActive.WAS_CALLED = true;
    }

    @Override
    public Closeable activate(ExecutionStateTracker e) {
      return this;
    }
  }

  /**
   * A {@link ContextActivationObserver.Registrar} to demonstrate default {@link
   * ContextActivationObserver} registration.
   */
  @AutoService(ContextActivationObserver.Registrar.class)
  public static class RegisteredTestContextActivationObserverRegistrar
      implements ContextActivationObserver.Registrar {
    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public ContextActivationObserver getContextActivationObserver() {
      return new AutoRegistrationClass();
    }
  }

  /**
   * A {@link ContextActivationObserver.Registrar} to demonstrate disabling {@link
   * ContextActivationObserver} registration.
   */
  @AutoService(ContextActivationObserver.Registrar.class)
  public static class DisabledContextActivationObserverRegistrar
      implements ContextActivationObserver.Registrar {
    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public ContextActivationObserver getContextActivationObserver() {
      return new AutoRegistrationClassNotActive();
    }
  }

  @Test
  public void testContextActivationObserverActivation() throws Exception {
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "testStage");
    Closeable c = executionContext.getExecutionStateTracker().activate();
    c.close();
    // AutoRegistrationClass's variable was modified to 'true'.
    assertTrue(AutoRegistrationClass.WAS_CALLED);

    // AutoRegistrationClassNotActive class is not registered as registrar for the same is disabled.
    assertFalse(AutoRegistrationClassNotActive.WAS_CALLED);
  }

  @Test
  public void testDataflowExecutionStateTrackerRecordsActiveMessageMetadata() throws IOException {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(), null, null, options, "");
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP);

    Closeable closure = tracker.enterState(state);

    // After entering a process state, we should have an active message tracked.
    ActiveMessageMetadata expectedMetadata =
        ActiveMessageMetadata.create(
            NameContextsForTests.nameContextForTest().userName(), Stopwatch.createStarted());
    assertTrue(tracker.getActiveMessageMetadata().isPresent());
    Assert.assertEquals(
        expectedMetadata.userStepName(), tracker.getActiveMessageMetadata().get().userStepName());

    closure.close();

    // Once the state closes, the active message should get cleared.
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test
  public void testDataflowExecutionStateTrackerRecordsCompletedProcessingTimes()
      throws IOException {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);

    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(), null, null, options, "");

    // Enter a processing state
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP);
    tracker.enterState(state);
    // Enter a new processing state
    StreamingModeExecutionState newState =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP);
    tracker.enterState(newState);

    // The first completed state should be recorded and the new state should be active.
    Map<String, IntSummaryStatistics> gotProcessingTimes = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(1, gotProcessingTimes.size());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList(NameContextsForTests.nameContextForTest().userName())),
        gotProcessingTimes.keySet());
    ActiveMessageMetadata expectedMetadata =
        ActiveMessageMetadata.create(
            NameContextsForTests.nameContextForTest().userName(), Stopwatch.createStarted());
    assertTrue(tracker.getActiveMessageMetadata().isPresent());
    Assert.assertEquals(
        expectedMetadata.userStepName(), tracker.getActiveMessageMetadata().get().userStepName());
  }

  @Test
  public void testDataflowExecutionStateTrackerDoesNotRecordCompletedProcessingTimesForBatch()
      throws IOException {
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.instance(), null, null, PipelineOptionsFactory.create(), "");

    // Enter a processing state
    BatchModeExecutionState state =
        new BatchModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            "testState",
            null /* requestingStepName */,
            null /* inputIndex */,
            null /* metricsContainer */,
            NoopProfileScope.NOOP);
    tracker.enterState(state);
    // Enter a new processing state
    BatchModeExecutionState newState =
        new BatchModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            "testState2",
            null /* requestingStepName */,
            null /* inputIndex */,
            null /* metricsContainer */,
            NoopProfileScope.NOOP);
    tracker.enterState(newState);

    // The first completed state should be recorded and the new state should be active.
    Map<String, IntSummaryStatistics> gotProcessingTimes = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(0, gotProcessingTimes.size());
    Assert.assertEquals(0, gotProcessingTimes.keySet().size());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test
  public void testStreamingOperationContextFastPathReusesScopeAndTracksStats() throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);

    BatchModeExecutionContext execContext =
        BatchModeExecutionContext.forTesting(options, "testStreamingStage");
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        (DataflowExecutionContext.DataflowExecutionStateTracker)
            execContext.getExecutionStateTracker();
    DataflowOperationContext op1 =
        execContext.createOperationContext(NameContext.create("S01", "s1", "s1", "UserStep1"));
    DataflowOperationContext op2 =
        execContext.createOperationContext(NameContext.create("S01", "s2", "s2", "UserStep2"));

    // Pre-registered steps should not appear until an element finishes.
    Assert.assertTrue(tracker.getProcessingTimesByStepCopy().isEmpty());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());

    Closeable firstScopeOp1;
    try (Closeable c1 = op1.enterProcess()) {
      firstScopeOp1 = c1;
      Optional<ActiveMessageMetadata> active1 = tracker.getActiveMessageMetadata();
      assertTrue(active1.isPresent());
      Assert.assertEquals("UserStep1", active1.get().userStepName());

      Thread.sleep(5);
      assertTrue(active1.get().stopwatch().elapsed(TimeUnit.MILLISECONDS) >= 1);

      try (Closeable c2 = op2.enterProcess()) {
        // Entering op2 completed op1 and switched active metadata to op2.
        Optional<ActiveMessageMetadata> active2 = tracker.getActiveMessageMetadata();
        assertTrue(active2.isPresent());
        Assert.assertEquals("UserStep2", active2.get().userStepName());
        Map<String, IntSummaryStatistics> midStats = tracker.getProcessingTimesByStepCopy();
        Assert.assertEquals(1, midStats.size());
        Assert.assertEquals(1L, midStats.get("UserStep1").getCount());
        assertTrue(midStats.get("UserStep1").getMax() >= 1);
      }
      assertFalse(tracker.getActiveMessageMetadata().isPresent());
    }

    // Second element reuses op1's Closeable scope and records 0 ms min alongside prior >=1 ms max.
    try (Closeable c1Again = op1.enterProcess()) {
      Assert.assertSame(firstScopeOp1, c1Again);
      // Verify re-entrant enterProcess falls back to a distinct Closeable safely.
      try (Closeable reentrant = op1.enterProcess()) {
        Assert.assertNotSame(c1Again, reentrant);
      }
    }

    Map<String, IntSummaryStatistics> finalStats = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(
        new HashSet<>(Arrays.asList("UserStep1", "UserStep2")), finalStats.keySet());
    IntSummaryStatistics step1Stats = finalStats.get("UserStep1");
    Assert.assertEquals(3L, step1Stats.getCount());
    assertTrue(step1Stats.getMin() >= 0);
    assertTrue(step1Stats.getMin() <= step1Stats.getMax());
    assertTrue(step1Stats.getMax() >= 1);
    assertTrue(step1Stats.getSum() >= step1Stats.getCount() * step1Stats.getMin());
    assertTrue(step1Stats.getSum() <= step1Stats.getCount() * step1Stats.getMax());
    Assert.assertEquals(1L, finalStats.get("UserStep2").getCount());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());

    // Reset clears stats and active state while keeping pre-registered operation contexts working.
    tracker.reset();
    Assert.assertTrue(tracker.getProcessingTimesByStepCopy().isEmpty());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());

    try (Closeable afterReset = op1.enterProcess()) {
      Assert.assertSame(firstScopeOp1, afterReset);
    }
    Map<String, IntSummaryStatistics> postResetStats = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(1, postResetStats.size());
    Assert.assertEquals(1L, postResetStats.get("UserStep1").getCount());
  }

  @Test
  public void testStepProcessingStatsExactValuesAndConcurrentSnapshots() throws Exception {
    DataflowExecutionContext.DataflowExecutionStateTracker.StepProcessingStats stats =
        new DataflowExecutionContext.DataflowExecutionStateTracker.StepProcessingStats("StepX");
    Assert.assertNull(stats.snapshot());

    stats.recordZeroMillis();
    IntSummaryStatistics snap = stats.snapshot();
    Assert.assertNotNull(snap);
    Assert.assertEquals(1L, snap.getCount());
    Assert.assertEquals(0, snap.getMin());
    Assert.assertEquals(0, snap.getMax());
    Assert.assertEquals(0L, snap.getSum());

    stats.record(5);
    stats.record(10);
    stats.recordZeroMillis();
    snap = stats.snapshot();
    Assert.assertNotNull(snap);
    Assert.assertEquals(4L, snap.getCount());
    Assert.assertEquals(0, snap.getMin());
    Assert.assertEquals(10, snap.getMax());
    Assert.assertEquals(15L, snap.getSum());

    stats.reset();
    Assert.assertNull(stats.snapshot());

    // Stress concurrent non-zero record(), recordZeroMillis(), and reset() against snapshot().
    AtomicBoolean done = new AtomicBoolean(false);
    AtomicReference<Throwable> readerError = new AtomicReference<>();
    Thread reader =
        new Thread(
            () -> {
              try {
                while (!done.get()) {
                  IntSummaryStatistics s = stats.snapshot();
                  if (s != null) {
                    long count = s.getCount();
                    int min = s.getMin();
                    int max = s.getMax();
                    long sum = s.getSum();
                    if (count <= 0L
                        || min < 0
                        || min > max
                        || sum < count * min
                        || sum > count * max) {
                      readerError.set(new IllegalStateException("Inconsistent snapshot: " + s));
                      return;
                    }
                  }
                }
              } catch (Throwable t) {
                readerError.set(t);
              }
            });
    reader.start();

    try {
      for (int i = 0; i < 50_000; i++) {
        stats.record(50);
        stats.record(10);
        stats.record(100);
        stats.recordZeroMillis();
        stats.recordZeroMillis();
        stats.record(1);
        if ((i & 31) == 0) {
          stats.reset();
        }
      }
    } finally {
      done.set(true);
      reader.join(10_000);
    }

    assertFalse("Reader thread did not terminate", reader.isAlive());
    Assert.assertNull("Reader thread failed: " + readerError.get(), readerError.get());
  }

  @Test
  public void testStreamingTrackerConcurrentReadsAreConsistent() throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);

    BatchModeExecutionContext execContext =
        BatchModeExecutionContext.forTesting(options, "testConcurrentStage");
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        (DataflowExecutionContext.DataflowExecutionStateTracker)
            execContext.getExecutionStateTracker();
    DataflowOperationContext op1 =
        execContext.createOperationContext(NameContext.create("S01", "s1", "s1", "StepA"));
    DataflowOperationContext op2 =
        execContext.createOperationContext(NameContext.create("S01", "s2", "s2", "StepB"));
    Set<String> validSteps = new HashSet<>(Arrays.asList("StepA", "StepB"));
    long maxReasonableElapsedNanos = TimeUnit.MINUTES.toNanos(5);

    AtomicBoolean done = new AtomicBoolean(false);
    AtomicReference<Throwable> readerError = new AtomicReference<>();

    Thread reader =
        new Thread(
            () -> {
              try {
                while (!done.get()) {
                  Optional<ActiveMessageMetadata> active = tracker.getActiveMessageMetadata();
                  if (active.isPresent()) {
                    if (!validSteps.contains(active.get().userStepName())) {
                      readerError.set(
                          new IllegalStateException(
                              "Unexpected active step: " + active.get().userStepName()));
                      return;
                    }
                    long elapsedNanos = active.get().stopwatch().elapsed(TimeUnit.NANOSECONDS);
                    if (elapsedNanos < 0L || elapsedNanos > maxReasonableElapsedNanos) {
                      readerError.set(
                          new IllegalStateException(
                              "Corrupt active elapsed nanos: " + elapsedNanos));
                      return;
                    }
                  }
                  Map<String, IntSummaryStatistics> stats = tracker.getProcessingTimesByStepCopy();
                  for (Map.Entry<String, IntSummaryStatistics> entry : stats.entrySet()) {
                    if (!validSteps.contains(entry.getKey())) {
                      readerError.set(
                          new IllegalStateException("Unexpected stats step: " + entry.getKey()));
                      return;
                    }
                    IntSummaryStatistics s = entry.getValue();
                    long count = s.getCount();
                    int min = s.getMin();
                    int max = s.getMax();
                    long sum = s.getSum();
                    if (count <= 0L
                        || min < 0
                        || min > max
                        || sum < count * min
                        || sum > count * max) {
                      readerError.set(new IllegalStateException("Inconsistent stats: " + s));
                      return;
                    }
                  }
                }
              } catch (Throwable t) {
                readerError.set(t);
              }
            });
    reader.start();

    int rounds = 5;
    int iterationsPerRound = 20_000;
    try {
      for (int round = 0; round < rounds; round++) {
        if (round > 0) {
          tracker.reset();
        }
        try (Closeable activation = tracker.activate()) {
          for (int i = 0; i < iterationsPerRound; i++) {
            try (Closeable c1 = op1.enterProcess()) {
              try (Closeable c2 = op2.enterProcess()) {
                // Periodically cross 1 ms so non-zero record() updates run concurrently.
                if (i == 0) {
                  Thread.sleep(2);
                }
              }
            }
          }
        }
      }
    } finally {
      done.set(true);
      reader.join(10_000);
    }

    assertFalse("Reader thread did not terminate", reader.isAlive());
    Assert.assertNull("Reader thread failed: " + readerError.get(), readerError.get());
    Map<String, IntSummaryStatistics> finalStats = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(iterationsPerRound, finalStats.get("StepA").getCount());
    Assert.assertEquals(iterationsPerRound, finalStats.get("StepB").getCount());
    assertTrue(finalStats.get("StepB").getMax() >= 1);
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test(timeout = 5_000)
  public void testGetActiveMessageMetadataBoundsRetriesAndResetHealsOddSeq() throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(), null, null, options, "work-odd");

    // Simulate a preempted writer holding activeSeq odd; reader must bound retries and return empty
    // instead of spinning indefinitely.
    tracker.setActiveSeqForTesting(1L);
    assertFalse(tracker.getActiveMessageMetadata().isPresent());

    // Backing off on an interrupted reader thread must not clear its interrupt status.
    Thread.currentThread().interrupt();
    try {
      assertFalse(tracker.getActiveMessageMetadata().isPresent());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }

    // reset() must heal odd parity and strictly advance activeSeq to an even value > 1.
    tracker.reset();
    long healedSeq = tracker.getActiveSeqForTesting();
    assertTrue("Expected healed seq > 1, got " + healedSeq, healedSeq > 1L);
    Assert.assertEquals(0L, healedSeq & 1L);

    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP);
    try (Closeable c = tracker.enterState(state)) {
      Optional<ActiveMessageMetadata> active = tracker.getActiveMessageMetadata();
      assertTrue(active.isPresent());
      Assert.assertEquals(
          NameContextsForTests.nameContextForTest().userName(), active.get().userStepName());
    }
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test
  public void testCrossThreadResetKeepsSequenceMonotonicAndEven() throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(), null, null, options, "work-cross-reset");

    DataflowExecutionContext.DataflowExecutionStateTracker.StepProcessingStats stats =
        tracker.getOrCreateStepStats(NameContextsForTests.USER_NAME);
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP);
    try (Closeable c = tracker.enterState(state)) {
      assertTrue(tracker.getActiveMessageMetadata().isPresent());
    }
    long activeSeqBefore = tracker.getActiveSeqForTesting();
    long statsSeqBefore = stats.getSeqForTesting();
    Assert.assertEquals(0L, activeSeqBefore & 1L);
    Assert.assertEquals(0L, statsSeqBefore & 1L);

    AtomicReference<Throwable> resetError = new AtomicReference<>();
    Thread resetThread =
        new Thread(
            () -> {
              try {
                // Intentionally call reset() from a different thread without deactivate().
                tracker.reset();
              } catch (Throwable t) {
                resetError.set(t);
              }
            });
    resetThread.start();
    resetThread.join(5_000);
    assertFalse("Reset thread did not terminate", resetThread.isAlive());
    Assert.assertNull(resetError.get());

    long activeSeqAfter = tracker.getActiveSeqForTesting();
    long statsSeqAfter = stats.getSeqForTesting();
    assertTrue(activeSeqAfter > activeSeqBefore);
    assertTrue(statsSeqAfter > statsSeqBefore);
    Assert.assertEquals(0L, activeSeqAfter & 1L);
    Assert.assertEquals(0L, statsSeqAfter & 1L);
    assertTrue(tracker.getProcessingTimesByStepCopy().isEmpty());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test
  public void testRecordElapsedNanosClampingAndSaturation() {
    DataflowExecutionContext.DataflowExecutionStateTracker.StepProcessingStats stats =
        new DataflowExecutionContext.DataflowExecutionStateTracker.StepProcessingStats("ClampStep");

    // Negative and sub-millisecond elapsed nanos clamp to 0 ms.
    DataflowExecutionContext.DataflowExecutionStateTracker.recordElapsedNanos(stats, -5_000_000L);
    DataflowExecutionContext.DataflowExecutionStateTracker.recordElapsedNanos(stats, 999_999L);
    IntSummaryStatistics snap = stats.snapshot();
    Assert.assertNotNull(snap);
    Assert.assertEquals(2L, snap.getCount());
    Assert.assertEquals(0, snap.getMin());
    Assert.assertEquals(0, snap.getMax());
    Assert.assertEquals(0L, snap.getSum());

    // Exact millisecond elapsed nanos.
    DataflowExecutionContext.DataflowExecutionStateTracker.recordElapsedNanos(stats, 2_500_000L);
    snap = stats.snapshot();
    Assert.assertEquals(3L, snap.getCount());
    Assert.assertEquals(0, snap.getMin());
    Assert.assertEquals(2, snap.getMax());
    Assert.assertEquals(2L, snap.getSum());

    // > Integer.MAX_VALUE milliseconds saturates to Integer.MAX_VALUE rather than overflowing.
    DataflowExecutionContext.DataflowExecutionStateTracker.recordElapsedNanos(
        stats, Long.MAX_VALUE);
    snap = stats.snapshot();
    Assert.assertEquals(4L, snap.getCount());
    Assert.assertEquals(0, snap.getMin());
    Assert.assertEquals(Integer.MAX_VALUE, snap.getMax());
    Assert.assertEquals(2L + Integer.MAX_VALUE, snap.getSum());
  }

  @Test
  public void testEnterStateRollsBackOnElementExecutionTrackerException() {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs(
                "--experiments=enable_streaming_engine,"
                    + DataflowElementExecutionTracker.TIME_PER_ELEMENT_EXPERIMENT)
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);

    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(), null, null, options, "work-rollback");
    StreamingModeExecutionState baseState =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(), "other", null, NoopProfileScope.NOOP) {
          @Override
          public void onActivate(boolean pushing) {
            if (!pushing) {
              throw new IllegalStateException("secondary activate failure");
            }
          }
        };
    tracker.enterStateDirect(baseState);

    // Return a real NameContext for the streaming active-step update, then return null for
    // elementExecutionTracker.enter(...) so Optional.of(step) throws NullPointerException.
    StreamingModeExecutionState failingState =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(),
            PROCESS_STATE_NAME,
            null,
            NoopProfileScope.NOOP) {
          private int calls = 0;

          @Override
          public NameContext getStepName() {
            return (calls++ == 0) ? super.getStepName() : null;
          }
        };

    try {
      tracker.enterState(failingState);
      Assert.fail("Expected NullPointerException from elementExecutionTracker.enter()");
    } catch (NullPointerException expected) {
      Throwable[] suppressed = expected.getSuppressed();
      Assert.assertEquals(1, suppressed.length);
      Assert.assertTrue(suppressed[0] instanceof IllegalStateException);
      Assert.assertEquals("secondary activate failure", suppressed[0].getMessage());
    }

    Assert.assertSame(baseState, tracker.getCurrentState());
    assertFalse(tracker.getActiveMessageMetadata().isPresent());
  }

  @Test
  public void testStepStatsBufferGeometricGrowth() throws Exception {
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.fromArgs("--experiments=enable_streaming_engine")
            .as(DataflowWorkerHarnessOptions.class);
    options.setStreaming(true);

    BatchModeExecutionContext execContext =
        BatchModeExecutionContext.forTesting(options, "testGrowthStage");
    DataflowExecutionContext.DataflowExecutionStateTracker tracker =
        (DataflowExecutionContext.DataflowExecutionStateTracker)
            execContext.getExecutionStateTracker();

    int numSteps = 10;
    for (int i = 0; i < numSteps; i++) {
      String stepName = "Step-" + i;
      DataflowOperationContext op =
          execContext.createOperationContext(NameContext.create("S01", "s" + i, "s" + i, stepName));
      try (Closeable c = op.enterProcess()) {
        assertTrue(tracker.getActiveMessageMetadata().isPresent());
        Assert.assertEquals(stepName, tracker.getActiveMessageMetadata().get().userStepName());
      }
    }

    Map<String, IntSummaryStatistics> stats = tracker.getProcessingTimesByStepCopy();
    Assert.assertEquals(numSteps, stats.size());
    for (int i = 0; i < numSteps; i++) {
      Assert.assertEquals(1L, stats.get("Step-" + i).getCount());
    }
  }
}
