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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.apache.beam.runners.core.metrics.MetricUpdateMatchers.metricUpdate;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.approximateProgressAtIndex;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.positionAtIndex;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.positionFromProgress;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.positionFromSplitResult;
import static org.apache.beam.runners.dataflow.worker.ReaderTestUtils.splitRequestAtIndex;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudPositionToReaderPosition;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.cloudProgressToReaderProgress;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.splitRequestToApproximateSplitRequest;
import static org.apache.beam.runners.dataflow.worker.counters.CounterName.named;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.DataflowElementExecutionTracker;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.TestOperationContext;
import org.apache.beam.runners.dataflow.worker.TestOperationContext.TestDataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterDistribution;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils.TestReader;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests for {@link MapTaskExecutor}. */
@RunWith(JUnit4.class)
public class MapTaskExecutorTest {

  private static final String COUNTER_PREFIX = "test-";

  @Rule public ExpectedException thrown = ExpectedException.none();

  private final CounterSet counterSet = new CounterSet();

  static class TestOperation extends Operation {
    boolean aborted = false;
    private final Counter<Long, ?> counter;

    TestOperation(String counterPrefix, long count, OperationContext context) {
      super(new OutputReceiver[] {}, context);
      counter = context.counterFactory().longSum(CounterName.named(counterPrefix + "ElementCount"));
      this.counter.addValue(count);
    }

    @Override
    public void abort() throws Exception {
      aborted = true;
      super.abort();
    }
  }

  // A mock ReadOperation fed to a MapTaskExecutor in test.
  static class TestReadOperation extends ReadOperation {
    private ApproximateReportedProgress progress = null;

    TestReadOperation(OutputReceiver outputReceiver, OperationContext context) {
      super(
          new TestReader(),
          new OutputReceiver[] {outputReceiver},
          context,
          ReadOperation.bytesCounterName(context));
    }

    @Override
    public NativeReader.Progress getProgress() {
      return cloudProgressToReaderProgress(progress);
    }

    @Override
    public NativeReader.DynamicSplitResult requestDynamicSplit(
        NativeReader.DynamicSplitRequest splitRequest) {
      // Fakes the return with the same position as proposed.
      return new NativeReader.DynamicSplitResultWithPosition(
          cloudPositionToReaderPosition(
              splitRequestToApproximateSplitRequest(splitRequest).getPosition()));
    }

    public void setProgress(ApproximateReportedProgress progress) {
      this.progress = progress;
    }
  }

  @Test
  public void testExecuteMapTaskExecutor() throws Exception {

    Operation o1 = Mockito.mock(Operation.class);
    Operation o2 = Mockito.mock(Operation.class);
    Operation o3 = Mockito.mock(Operation.class);
    List<Operation> operations = Arrays.asList(new Operation[] {o1, o2, o3});

    ExecutionStateTracker stateTracker = Mockito.spy(ExecutionStateTracker.newForTest());

    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      executor.execute();
    }

    InOrder inOrder = Mockito.inOrder(stateTracker, o1, o2, o3);
    inOrder.verify(stateTracker).activate();
    inOrder.verify(o3).start();
    inOrder.verify(o2).start();
    inOrder.verify(o1).start();
    inOrder.verify(o1).finish();
    inOrder.verify(o2).finish();
    inOrder.verify(o3).finish();
    inOrder.verify(stateTracker).deactivate();
  }

  private TestOperation createOperation(String stepName, long count) {
    OperationContext operationContext = createContext(stepName);
    return new TestOperation(COUNTER_PREFIX + stepName + "-", count, operationContext);
  }

  private TestOperationContext createContext(String stepName) {
    return TestOperationContext.create(
        counterSet, NameContext.create("test", stepName, stepName, stepName));
  }

  private TestOperationContext createContext(String stepName, ExecutionStateTracker tracker) {
    return TestOperationContext.create(
        counterSet,
        NameContext.create("test", stepName, stepName, stepName),
        new MetricsContainerImpl(stepName),
        tracker);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetOutputCounters() throws Exception {
    List<Operation> operations =
        Arrays.asList(
            new Operation[] {
              createOperation("o1", 1), createOperation("o2", 2), createOperation("o3", 3)
            });

    ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      CounterSet counterSet = executor.getOutputCounters();

      CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
      counterSet.extractUpdates(false, updateExtractor);
      verify(updateExtractor).longSum(eq(named("test-o1-ElementCount")), anyBoolean(), eq(1L));
      verify(updateExtractor).longSum(eq(named("test-o2-ElementCount")), anyBoolean(), eq(2L));
      verify(updateExtractor).longSum(eq(named("test-o3-ElementCount")), anyBoolean(), eq(3L));
      verifyNoMoreInteractions(updateExtractor);
    }
  }

  private static class NoopParDoFn implements ParDoFn {
    @Override
    public void startBundle(Receiver... receivers) {}

    @Override
    public void processElement(Object elem) {}

    @Override
    public void processTimers() {}

    @Override
    public void finishBundle() {}

    @Override
    public void abort() {}
  }

  /** Verify counts for the per-element-output-time counter are correct. */
  @Test
  public void testPerElementProcessingTimeCounters() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(
            Lists.newArrayList(DataflowElementExecutionTracker.TIME_PER_ELEMENT_EXPERIMENT));
    ExecutionStateSampler stateSampler = ExecutionStateSampler.newForTest();
    DataflowExecutionStateTracker stateTracker =
        new DataflowExecutionStateTracker(
            stateSampler,
            new TestDataflowExecutionState(
                NameContext.forStage("test-stage"),
                "other",
                null /* requestingStepName */,
                null /* sideInputIndex */,
                null /* metricsContainer */,
                NoopProfileScope.NOOP),
            counterSet,
            options,
            "test-work-item-id");
    NameContext parDoName = nameForStep("s1");

    // Wire a read operation with 3 elements to a ParDoOperation and assert that we count
    // the correct number of elements.
    ReadOperation read =
        ReadOperation.forTest(
            new TestReader("a", "b", "c"),
            new OutputReceiver(),
            TestOperationContext.create(counterSet, nameForStep("s0"), null, stateTracker));
    ParDoOperation parDo =
        new ParDoOperation(
            new NoopParDoFn(),
            new OutputReceiver[0],
            TestOperationContext.create(counterSet, parDoName, null, stateTracker));
    parDo.attachInput(read, 0);
    List<Operation> operations = Lists.newArrayList(read, parDo);

    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      executor.execute();
    }
    stateSampler.doSampling(100L);

    CounterName counterName =
        CounterName.named("per-element-processing-time").withOriginalName(parDoName);
    Counter<Long, CounterDistribution> counter =
        (Counter<Long, CounterDistribution>) counterSet.getExistingCounter(counterName);

    assertThat(counter.getAggregate().getCount(), equalTo(3L));
  }

  private NameContext nameForStep(String originalName) {
    return NameContext.create(
        "test-stage", originalName, "system-" + originalName, "step-" + originalName);
  }

  @Test
  @SuppressWarnings("unchecked")
  /**
   * This test makes sure that any metrics reported within an operation are part of the metric
   * containers returned by {@link getMetricContainers}.
   */
  public void testGetMetricContainers() throws Exception {
    ExecutionStateTracker stateTracker =
        new DataflowExecutionStateTracker(
            ExecutionStateSampler.newForTest(),
            new TestDataflowExecutionState(
                NameContext.forStage("testStage"),
                "other",
                null /* requestingStepName */,
                null /* sideInputIndex */,
                null /* metricsContainer */,
                NoopProfileScope.NOOP),
            new CounterSet(),
            PipelineOptionsFactory.create(),
            "test-work-item-id");
    final String o1 = "o1";
    TestOperationContext context1 = createContext(o1, stateTracker);
    final String o2 = "o2";
    TestOperationContext context2 = createContext(o2, stateTracker);
    final String o3 = "o3";
    TestOperationContext context3 = createContext(o3, stateTracker);

    List<Operation> operations =
        Arrays.asList(
            new Operation(new OutputReceiver[] {}, context1) {
              @Override
              public void start() throws Exception {
                super.start();
                try (Closeable scope = context.enterStart()) {
                  Metrics.counter("TestMetric", "MetricCounter").inc(1L);
                }
              }
            },
            new Operation(new OutputReceiver[] {}, context2) {
              @Override
              public void start() throws Exception {
                super.start();
                try (Closeable scope = context.enterStart()) {
                  Metrics.counter("TestMetric", "MetricCounter").inc(2L);
                }
              }
            },
            new Operation(new OutputReceiver[] {}, context3) {
              @Override
              public void start() throws Exception {
                super.start();
                try (Closeable scope = context.enterStart()) {
                  Metrics.counter("TestMetric", "MetricCounter").inc(3L);
                }
              }
            });

    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      // Call execute so that we run all the counters
      executor.execute();

      assertThat(
          context1.metricsContainer().getUpdates().counterUpdates(),
          contains(metricUpdate("TestMetric", "MetricCounter", o1, 1L)));
      assertThat(
          context2.metricsContainer().getUpdates().counterUpdates(),
          contains(metricUpdate("TestMetric", "MetricCounter", o2, 2L)));
      assertThat(
          context3.metricsContainer().getUpdates().counterUpdates(),
          contains(metricUpdate("TestMetric", "MetricCounter", o3, 3L)));
      assertEquals(0, stateTracker.getMillisSinceBundleStart());
      assertEquals(TimeUnit.MINUTES.toMillis(10), stateTracker.getNextBundleLullDurationReportMs());
    }
  }

  @Test
  public void testNoOperation() throws Exception {
    // Test MapTaskExecutor without a single operation.
    ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    try (MapTaskExecutor executor =
        new MapTaskExecutor(new ArrayList<Operation>(), counterSet, stateTracker)) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("has no operation");
      executor.getReadOperation();
    }
  }

  @Test
  public void testNoReadOperation() throws Exception {
    // Test MapTaskExecutor without ReadOperation.
    List<Operation> operations =
        Arrays.<Operation>asList(createOperation("o1", 1), createOperation("o2", 2));
    ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      thrown.expect(IllegalStateException.class);
      thrown.expectMessage("is not a ReadOperation");
      executor.getReadOperation();
    }
  }

  @Test
  public void testValidOperations() throws Exception {
    TestOutputReceiver receiver =
        new TestOutputReceiver(counterSet, NameContextsForTests.nameContextForTest());
    List<Operation> operations =
        Arrays.<Operation>asList(new TestReadOperation(receiver, createContext("ReadOperation")));
    ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    try (MapTaskExecutor executor = new MapTaskExecutor(operations, counterSet, stateTracker)) {
      Assert.assertEquals(operations.get(0), executor.getReadOperation());
    }
  }

  @Test
  public void testGetProgressAndRequestSplit() throws Exception {
    TestOutputReceiver receiver =
        new TestOutputReceiver(counterSet, NameContextsForTests.nameContextForTest());
    TestReadOperation operation = new TestReadOperation(receiver, createContext("ReadOperation"));
    ExecutionStateTracker stateTracker = ExecutionStateTracker.newForTest();
    try (MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.asList(new Operation[] {operation}), counterSet, stateTracker)) {
      operation.setProgress(approximateProgressAtIndex(1L));
      Assert.assertEquals(positionAtIndex(1L), positionFromProgress(executor.getWorkerProgress()));
      Assert.assertEquals(
          positionAtIndex(1L),
          positionFromSplitResult(executor.requestDynamicSplit(splitRequestAtIndex(1L))));
    }
  }

  @Test
  public void testExceptionInStartAbortsAllOperations() throws Exception {
    Operation o1 = Mockito.mock(Operation.class);
    Operation o2 = Mockito.mock(Operation.class);
    Operation o3 = Mockito.mock(Operation.class);
    Mockito.doThrow(new Exception("in start")).when(o2).start();

    ExecutionStateTracker stateTracker = Mockito.spy(ExecutionStateTracker.newForTest());
    try (MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.<Operation>asList(o1, o2, o3), counterSet, stateTracker)) {
      executor.execute();
      fail("Should have thrown");
    } catch (Exception e) {
      InOrder inOrder = Mockito.inOrder(o1, o2, o3, stateTracker);
      inOrder.verify(stateTracker).activate();
      inOrder.verify(o3).start();
      inOrder.verify(o2).start();

      // Order of abort doesn't matter
      Mockito.verify(o1).abort();
      Mockito.verify(o2).abort();
      Mockito.verify(o3).abort();
      Mockito.verify(stateTracker).deactivate();
      Mockito.verifyNoMoreInteractions(o1, o2, o3);
    }
  }

  @Test
  public void testExceptionInFinishAbortsAllOperations() throws Exception {
    Operation o1 = Mockito.mock(Operation.class);
    Operation o2 = Mockito.mock(Operation.class);
    Operation o3 = Mockito.mock(Operation.class);
    Mockito.doThrow(new Exception("in finish")).when(o2).finish();

    ExecutionStateTracker stateTracker = Mockito.spy(ExecutionStateTracker.newForTest());
    try (MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.<Operation>asList(o1, o2, o3), counterSet, stateTracker)) {
      executor.execute();
      fail("Should have thrown");
    } catch (Exception e) {
      Mockito.verify(stateTracker).activate();
      InOrder inOrder = Mockito.inOrder(o1, o2, o3);
      inOrder.verify(o3).start();
      inOrder.verify(o2).start();
      inOrder.verify(o1).start();
      inOrder.verify(o1).finish();
      inOrder.verify(o2).finish();

      // Order of abort doesn't matter
      Mockito.verify(o1).abort();
      Mockito.verify(o2).abort();
      Mockito.verify(o3).abort();
      Mockito.verify(stateTracker).deactivate();
      Mockito.verifyNoMoreInteractions(o1, o2, o3);
    }
  }

  @Test
  public void testExceptionInAbortSuppressed() throws Exception {
    Operation o1 = Mockito.mock(Operation.class);
    Operation o2 = Mockito.mock(Operation.class);
    Operation o3 = Mockito.mock(Operation.class);
    Operation o4 = Mockito.mock(Operation.class);
    Mockito.doThrow(new Exception("in finish")).when(o2).finish();
    Mockito.doThrow(new Exception("suppressed in abort")).when(o3).abort();

    ExecutionStateTracker stateTracker = Mockito.spy(ExecutionStateTracker.newForTest());
    try (MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.<Operation>asList(o1, o2, o3, o4), counterSet, stateTracker)) {
      executor.execute();
      fail("Should have thrown");
    } catch (Exception e) {
      Mockito.verify(stateTracker).activate();
      InOrder inOrder = Mockito.inOrder(o1, o2, o3, o4, stateTracker);
      inOrder.verify(o4).start();
      inOrder.verify(o3).start();
      inOrder.verify(o2).start();
      inOrder.verify(o1).start();
      inOrder.verify(o1).finish();
      inOrder.verify(o2).finish(); // this fails

      // Order of abort doesn't matter
      Mockito.verify(o1).abort();
      Mockito.verify(o2).abort();
      Mockito.verify(o3).abort(); // will throw an exception, but we shouldn't fail
      Mockito.verify(o4).abort();
      Mockito.verify(stateTracker).deactivate();
      Mockito.verifyNoMoreInteractions(o1, o2, o3, o4);

      // Make sure the failure while aborting shows up as a suppressed error
      assertThat(e.getMessage(), equalTo("in finish"));
      assertThat(e.getSuppressed(), arrayWithSize(1));
      assertThat(e.getSuppressed()[0].getMessage(), equalTo("suppressed in abort"));
    }
  }

  @Test
  public void testAbort() throws Exception {
    // Operation must be an instance of ReadOperation or ReceivingOperation per preconditions
    // in MapTaskExecutor.
    ReadOperation o1 = Mockito.mock(ReadOperation.class);
    ReadOperation o2 = Mockito.mock(ReadOperation.class);

    ExecutionStateTracker stateTracker = Mockito.spy(ExecutionStateTracker.newForTest());
    MapTaskExecutor executor =
        new MapTaskExecutor(Arrays.<Operation>asList(o1, o2), counterSet, stateTracker);
    Mockito.doAnswer(
            invocation -> {
              executor.abort();
              return null;
            })
        .when(o1)
        .finish();
    executor.execute();
    Mockito.verify(stateTracker).activate();
    Mockito.verify(o1, atLeastOnce()).abortReadLoop();
    Mockito.verify(o2, atLeastOnce()).abortReadLoop();
    Mockito.verify(stateTracker).deactivate();
  }
}
