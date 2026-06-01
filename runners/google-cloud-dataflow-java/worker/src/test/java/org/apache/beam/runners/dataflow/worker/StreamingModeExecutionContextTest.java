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

import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;
import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.splitIntToLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateNamespaceForTest;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker.ExecutionState;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.KeySwitchListener;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionState;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext.StreamingModeExecutionStateRegistry;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.NoopProfileScope;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler.ProfileScope;
import org.apache.beam.runners.dataflow.worker.streaming.BoundedQueueExecutorWorkHandle;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.streaming.config.FakeGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputStateFetcher;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WorkExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateReader;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillTagEncodingV1;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillTagEncodingV2;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link StreamingModeExecutionContext}. */
@RunWith(JUnit4.class)
public class StreamingModeExecutionContextTest {

  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Mock private SideInputStateFetcher sideInputStateFetcher;
  @Mock private WindmillStateReader stateReader;
  @Mock private WorkExecutor workExecutor;

  private static final String COMPUTATION_ID = "computationId";

  private final StreamingModeExecutionStateRegistry executionStateRegistry =
      new StreamingModeExecutionStateRegistry();
  private StreamingModeExecutionContext executionContext;
  DataflowWorkerHarnessOptions options;
  private FakeGlobalConfigHandle globalConfigHandle;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    CounterSet counterSet = new CounterSet();
    ConcurrentHashMap<String, String> stateNameMap = new ConcurrentHashMap<>();
    globalConfigHandle = new FakeGlobalConfigHandle(StreamingGlobalConfig.builder().build());
    stateNameMap.put(NameContextsForTests.nameContextForTest().userName(), "testStateFamily");
    executionContext =
        new StreamingModeExecutionContext(
            counterSet,
            COMPUTATION_ID,
            new ReaderCache(Duration.standardMinutes(1), Executors.newCachedThreadPool()),
            stateNameMap,
            WindmillStateCache.builder()
                .setSizeMb(options.getWorkerCacheMb())
                .build()
                .forComputation("comp"),
            StreamingStepMetricsContainer.createRegistry(),
            new DataflowExecutionStateTracker(
                ExecutionStateSampler.newForTest(),
                executionStateRegistry.getState(
                    NameContext.forStage("stage"), "other", null, NoopProfileScope.NOOP),
                counterSet,
                PipelineOptionsFactory.create(),
                "test-work-item-id"),
            executionStateRegistry,
            globalConfigHandle,
            Long.MAX_VALUE,
            /*throwExceptionOnLargeOutput=*/ false);
  }

  private static Work createMockWork(Windmill.WorkItem workItem, Watermarks watermarks) {
    return Work.create(
        workItem,
        workItem.getSerializedSize(),
        watermarks,
        Work.createProcessingContext(
            COMPUTATION_ID, new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class)),
        false,
        Instant::now);
  }

  @Test
  public void testTimerInternalsSetTimer() throws Exception {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    NameContext nameContext = NameContextsForTests.nameContextForTest();
    DataflowOperationContext operationContext =
        executionContext.createOperationContext(nameContext);
    StreamingModeExecutionContext.StepContext stepContext =
        executionContext.getStepContext(operationContext);

    executionContext.start(
        "key",
        createMockWork(
            Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(17L).build(),
            Watermarks.builder().setInputDataWatermark(new Instant(1000)).build()),
        stateReader,
        sideInputStateFetcher,
        outputBuilder,
        workExecutor);

    TimerInternals timerInternals = stepContext.timerInternals();

    timerInternals.setTimer(
        TimerData.of(
            new StateNamespaceForTest("key"),
            new Instant(5000),
            new Instant(5000),
            TimeDomain.EVENT_TIME,
            CausedByDrain.NORMAL));
    executionContext.finishKey();
    executionContext.flushState();

    Windmill.Timer timer = outputBuilder.buildPartial().getOutputTimers(0);
    assertThat(timer.getTag().toStringUtf8(), equalTo("/skey+0:5000"));
    assertThat(timer.getTimestamp(), equalTo(TimeUnit.MILLISECONDS.toMicros(5000)));
    assertThat(timer.getType(), equalTo(Windmill.Timer.Type.WATERMARK));
  }

  @Test
  public void testTimerInternalsProcessingTimeSkew() {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();

    NameContext nameContext = NameContextsForTests.nameContextForTest();
    DataflowOperationContext operationContext =
        executionContext.createOperationContext(nameContext);
    StreamingModeExecutionContext.StepContext stepContext =
        executionContext.getStepContext(operationContext);
    Windmill.WorkItem.Builder workItemBuilder =
        Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(17L);
    Windmill.Timer.Builder timerBuilder = workItemBuilder.getTimersBuilder().addTimersBuilder();

    // Trigger a realtime timer that with clock skew but ensure that it would
    // still fire.
    Instant now = Instant.now();
    long offsetMillis = 60 * 1000;
    Instant timerTimestamp = now.plus(Duration.millis(offsetMillis));
    timerBuilder
        .setTag(ByteString.copyFromUtf8("a"))
        .setTimestamp(timerTimestamp.getMillis() * 1000)
        .setType(Windmill.Timer.Type.REALTIME);

    executionContext.start(
        "key",
        createMockWork(
            workItemBuilder.build(),
            Watermarks.builder().setInputDataWatermark(new Instant(1000)).build()),
        stateReader,
        sideInputStateFetcher,
        outputBuilder,
        workExecutor);
    TimerInternals timerInternals = stepContext.timerInternals();
    assertTrue(timerTimestamp.isBefore(timerInternals.currentProcessingTime()));
  }

  /**
   * Tests that the {@link SideInputReader} returned by the {@link StreamingModeExecutionContext}
   * contains the expected views when they are deserialized, as occurs on the service.
   */
  @Test
  public void testSideInputReaderReconstituted() {
    Pipeline p = Pipeline.create();

    PCollectionView<String> preview1 = p.apply(Create.of("")).apply(View.asSingleton());
    PCollectionView<String> preview2 = p.apply(Create.of("")).apply(View.asSingleton());
    PCollectionView<String> preview3 = p.apply(Create.of("")).apply(View.asSingleton());

    SideInputReader sideInputReader =
        executionContext.getSideInputReaderForViews(Arrays.asList(preview1, preview2));

    assertTrue(sideInputReader.contains(preview1));
    assertTrue(sideInputReader.contains(preview2));
    assertFalse(sideInputReader.contains(preview3));

    PCollectionView<String> view1 = SerializableUtils.ensureSerializable(preview1);
    PCollectionView<String> view2 = SerializableUtils.ensureSerializable(preview2);
    PCollectionView<String> view3 = SerializableUtils.ensureSerializable(preview3);

    assertTrue(sideInputReader.contains(view1));
    assertTrue(sideInputReader.contains(view2));
    assertFalse(sideInputReader.contains(view3));
  }

  @Test
  public void extractMsecCounters() {
    MetricsContainer metricsContainer = mock(MetricsContainer.class);
    ProfileScope profileScope = mock(ProfileScope.class);
    ExecutionState start1 =
        executionContext.executionStateRegistry.getState(
            NameContext.create("stage", "original-1", "system-1", "user-1"),
            ExecutionStateTracker.START_STATE_NAME,
            metricsContainer,
            profileScope);
    ExecutionState process1 =
        executionContext.executionStateRegistry.getState(
            NameContext.create("stage", "original-1", "system-1", "user-1"),
            ExecutionStateTracker.PROCESS_STATE_NAME,
            metricsContainer,
            profileScope);
    ExecutionState start2 =
        executionContext.executionStateRegistry.getState(
            NameContext.create("stage", "original-2", "system-2", "user-2"),
            ExecutionStateTracker.START_STATE_NAME,
            metricsContainer,
            profileScope);

    ExecutionState other =
        executionContext.executionStateRegistry.getState(
            NameContext.forStage("stage"), "other", null, NoopProfileScope.NOOP);

    other.takeSample(120);

    start1.takeSample(100);
    process1.takeSample(500);

    assertThat(
        executionStateRegistry.extractUpdates(false),
        containsInAnyOrder(
            msecStage("other-msecs", "stage", 120),
            msec("start-msecs", "stage", "original-1", 100),
            msec("process-msecs", "stage", "original-1", 500)));

    process1.takeSample(200);
    start2.takeSample(200);

    assertThat(
        executionStateRegistry.extractUpdates(false),
        containsInAnyOrder(
            msec("process-msecs", "stage", "original-1", 200),
            msec("start-msecs", "stage", "original-2", 200)));

    process1.takeSample(300);
    assertThat(
        executionStateRegistry.extractUpdates(false),
        containsInAnyOrder(msec("process-msecs", "stage", "original-1", 300)));
  }

  private CounterUpdate msec(
      String counterName, String stageName, String originalStepName, long value) {
    return new CounterUpdate()
        .setStructuredNameAndMetadata(
            new CounterStructuredNameAndMetadata()
                .setName(
                    new CounterStructuredName()
                        .setOrigin("SYSTEM")
                        .setName(counterName)
                        .setOriginalStepName(originalStepName)
                        .setExecutionStepName(stageName))
                .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
        .setCumulative(false)
        .setInteger(longToSplitInt(value));
  }

  private CounterUpdate msecStage(String counterName, String stageName, long value) {
    return new CounterUpdate()
        .setStructuredNameAndMetadata(
            new CounterStructuredNameAndMetadata()
                .setName(
                    new CounterStructuredName()
                        .setOrigin("SYSTEM")
                        .setName(counterName)
                        .setExecutionStepName(stageName))
                .setMetadata(new CounterMetadata().setKind(Kind.SUM.toString())))
        .setCumulative(false)
        .setInteger(longToSplitInt(value));
  }

  /**
   * Ensure that incrementing and extracting counter updates are correct under concurrent reader and
   * writer threads.
   */
  @Test
  public void testAtomicExtractUpdate() throws InterruptedException, ExecutionException {
    long numUpdates = 1_000_000;

    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(), "testState", null, NoopProfileScope.NOOP);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    AtomicBoolean doneWriting = new AtomicBoolean(false);

    Callable<Long> reader =
        () -> {
          long count = 0;
          boolean isLastRead;
          do {
            isLastRead = doneWriting.get();
            CounterUpdate update = state.extractUpdate(false);
            if (update != null) {
              count += splitIntToLong(update.getInteger());
            }
          } while (!isLastRead);
          return count;
        };
    Runnable writer =
        () -> {
          for (int i = 0; i < numUpdates; i++) {
            state.takeSample(1L);
          }
          doneWriting.set(true);
        };

    // NB: Reader is invoked before writer to ensure they execute concurrently.
    List<Future<Long>> results =
        executor.invokeAll(
            Lists.newArrayList(reader, Executors.callable(writer, 0L)), 2, TimeUnit.SECONDS);
    long count = results.get(0).get();
    assertThat(count, equalTo(numUpdates));
  }

  @Test(timeout = 2000)
  public void stateSamplingInStreaming() {
    // Test that when writing on one thread and reading from another, updates always eventually
    // reach the reading thread.
    StreamingModeExecutionState state =
        new StreamingModeExecutionState(
            NameContextsForTests.nameContextForTest(), "testState", null, NoopProfileScope.NOOP);
    ExecutionStateSampler sampler = ExecutionStateSampler.newForTest();
    try {
      sampler.start();

      ExecutionStateTracker tracker = new ExecutionStateTracker(sampler);

      Thread executionThread = new Thread();
      executionThread.setName("looping-thread-for-test");

      tracker.activate(executionThread);
      tracker.enterState(state);

      // Wait for the state to be incremented 3 times
      for (int i = 0; i < 3; i++) {
        CounterUpdate update = null;
        while (update == null) {
          update = state.extractUpdate(false);
        }
        long newValue = splitIntToLong(update.getInteger());
        assertThat(newValue, Matchers.greaterThan(0L));
      }
    } finally {
      sampler.stop();
    }
  }

  @Test
  public void testStateTagEncodingBasedOnConfig() {
    for (Boolean isV2Encoding : Lists.newArrayList(Boolean.TRUE, Boolean.FALSE)) {
      Class<?> expectedEncoding =
          isV2Encoding ? WindmillTagEncodingV2.class : WindmillTagEncodingV1.class;
      Windmill.WorkItemCommitRequest.Builder outputBuilder =
          Windmill.WorkItemCommitRequest.newBuilder();
      globalConfigHandle.setConfig(
          StreamingGlobalConfig.builder().setEnableStateTagEncodingV2(isV2Encoding).build());
      executionContext.start(
          "key",
          createMockWork(
              Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(17L).build(),
              Watermarks.builder().setInputDataWatermark(new Instant(1000)).build()),
          stateReader,
          sideInputStateFetcher,
          outputBuilder,
          workExecutor);
      assertEquals(expectedEncoding, executionContext.getWindmillTagEncoding().getClass());
    }
  }

  @Test
  public void testSetBacklogBytes() {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    NameContext nameContext = NameContextsForTests.nameContextForTest();
    DataflowOperationContext operationContext =
        executionContext.createOperationContext(nameContext);
    StreamingModeExecutionContext.StepContext stepContext =
        executionContext.getStepContext(operationContext);

    executionContext.start(
        "key",
        createMockWork(
            Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(17L).build(),
            Watermarks.builder().setInputDataWatermark(new Instant(1000)).build()),
        stateReader,
        sideInputStateFetcher,
        outputBuilder,
        workExecutor);

    stepContext.setBacklogBytes(1234.0);
    executionContext.finishKey();
    executionContext.flushState();

    assertEquals(1234, outputBuilder.getSourceBacklogBytes());
  }

  @Test
  public void testAdvanceKeySwitching() throws Exception {
    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .setCacheToken(1000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workA =
        createMockWork(
            workItemA, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .setCacheToken(2000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workB =
        createMockWork(
            workItemB, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
    BoundedQueueExecutorWorkHandle mockBudget = mock(BoundedQueueExecutorWorkHandle.class);
    ExecutableWork executableWorkB = ExecutableWork.create(workB, (w, h) -> {});

    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.of(executableWorkB));

    Windmill.WorkItemCommitRequest.Builder outputBuilderA =
        Windmill.WorkItemCommitRequest.newBuilder();

    java.util.concurrent.atomic.AtomicReference<Work> oldWorkRef =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<Work> newWorkRef =
        new java.util.concurrent.atomic.AtomicReference<>();
    KeySwitchListener listener =
        (oldW, newW) -> {
          oldWorkRef.set(oldW);
          newWorkRef.set(newW);
        };

    executionContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderA,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        5, // maxKeyGroupBatchSize
        1000000000L, // maxKeyGroupBatchTimeNanos (1s)
        10000000L, // maxKeyGroupBatchBytes (10MB)
        listener);

    // 1. Verify primary key info
    assertEquals("keyA", executionContext.getKey());
    assertEquals(workA, executionContext.getWork());
    assertEquals(1, executionContext.getExecutedWorks().size());
    assertEquals(1, executionContext.getOutputBuilders().size());

    // 2. Advance context (trigger key switch)
    boolean advanced = executionContext.advance();
    assertTrue(advanced);

    // 3. Verify that the context has transitioned to keyB
    assertEquals("keyB", executionContext.getKey());
    assertEquals(workB, executionContext.getWork());
    assertEquals(2, executionContext.getExecutedWorks().size());
    assertEquals(2, executionContext.getOutputBuilders().size());

    // Verify listener was called
    assertEquals(workA, oldWorkRef.get());
    assertEquals(workB, newWorkRef.get());

    // 4. Advance again (should return false since pollWork returns empty)
    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.empty());

    boolean advancedAgain = executionContext.advance();
    assertFalse(advancedAgain);
  }

  @Test
  public void testAdvanceLimitThresholds() throws Exception {
    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .setCacheToken(1000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workA =
        createMockWork(
            workItemA, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .setCacheToken(2000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workB =
        createMockWork(
            workItemB, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
    BoundedQueueExecutorWorkHandle mockBudget = mock(BoundedQueueExecutorWorkHandle.class);
    ExecutableWork executableWorkB = ExecutableWork.create(workB, (w, h) -> {});

    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.of(executableWorkB));

    Windmill.WorkItemCommitRequest.Builder outputBuilderA =
        Windmill.WorkItemCommitRequest.newBuilder();

    // Case 1: maxKeyGroupBatchSize is 0
    executionContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderA,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        0, // maxKeyGroupBatchSize = 0 (batch size threshold hit immediately!)
        1000000000L,
        10000000L,
        (k, c) -> {});

    assertFalse(executionContext.advance());

    // Case 2: maxKeyGroupBatchBytes limit is exceeded
    Windmill.WorkItemCommitRequest.Builder outputBuilderAForBytes =
        Windmill.WorkItemCommitRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8("some_non_empty_key_to_increase_size"));

    executionContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderAForBytes,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        5,
        1000000000L,
        1L, // maxKeyGroupBatchBytes = 1 byte (exceeded!)
        (k, c) -> {});

    assertFalse(executionContext.advance());
  }

  @Test
  public void testFinishKeyReentrantSafety() {
    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder();
    executionContext.start(
        "key",
        createMockWork(
            Windmill.WorkItem.newBuilder().setKey(ByteString.EMPTY).setWorkToken(17L).build(),
            Watermarks.builder().setInputDataWatermark(new Instant(1000)).build()),
        stateReader,
        sideInputStateFetcher,
        outputBuilder,
        workExecutor);

    // First call
    executionContext.finishKey();
    // Second call - should not throw any Exception
    executionContext.finishKey();
  }

  @Test
  public void testWorkIsFailed_heartbeatFailureOnPolledKey() throws Exception {
    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .setCacheToken(1000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workA =
        createMockWork(
            workItemA, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .setCacheToken(2000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workB =
        createMockWork(
            workItemB, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
    BoundedQueueExecutorWorkHandle mockBudget = mock(BoundedQueueExecutorWorkHandle.class);
    ExecutableWork executableWorkB = ExecutableWork.create(workB, (w, h) -> {});

    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.of(executableWorkB));

    Windmill.WorkItemCommitRequest.Builder outputBuilderA =
        Windmill.WorkItemCommitRequest.newBuilder();

    executionContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderA,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        5,
        1000000000L,
        10000000L,
        (k, c) -> {});

    // Heartbeat fails on primary work A
    workA.setFailed();

    assertTrue(executionContext.workIsFailed());
    assertEquals(workA, executionContext.getFailedWork());
  }

  @Test
  public void testAdvance_abortsOnFailedExecutedKey() throws Exception {
    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .setCacheToken(1000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workA =
        createMockWork(
            workItemA, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .setCacheToken(2000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workB =
        createMockWork(
            workItemB, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
    BoundedQueueExecutorWorkHandle mockBudget = mock(BoundedQueueExecutorWorkHandle.class);
    ExecutableWork executableWorkB = ExecutableWork.create(workB, (w, h) -> {});

    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.of(executableWorkB));

    Windmill.WorkItemCommitRequest.Builder outputBuilderA =
        Windmill.WorkItemCommitRequest.newBuilder();

    executionContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderA,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        5,
        1000000000L,
        10000000L,
        (k, c) -> {});

    // Simulate heartbeat failure on key A before switching to key B
    workA.setFailed();

    // Advance should fail immediately and throw WorkItemCancelledException
    boolean thrown = false;
    try {
      executionContext.advance();
      org.junit.Assert.fail("Expected WorkItemCancelledException");
    } catch (WorkItemCancelledException e) {
      thrown = true;
    }
    assertTrue(thrown);
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testInvalidateCache_clearsAllExecutedKeys() throws Exception {
    CounterSet counterSet = new CounterSet();
    ConcurrentHashMap<String, String> stateNameMap = new ConcurrentHashMap<>();
    stateNameMap.put(NameContextsForTests.nameContextForTest().userName(), "testStateFamily");

    // Mock stateCache
    WindmillStateCache.ForComputation mockStateCache =
        mock(WindmillStateCache.ForComputation.class);

    // Use a real ReaderCache so we can verify interactions directly
    ReaderCache testReaderCache =
        new ReaderCache(Duration.standardMinutes(1), Executors.newSingleThreadExecutor());

    StreamingModeExecutionContext testContext =
        new StreamingModeExecutionContext(
            counterSet,
            COMPUTATION_ID,
            testReaderCache,
            stateNameMap,
            mockStateCache,
            StreamingStepMetricsContainer.createRegistry(),
            new DataflowExecutionStateTracker(
                ExecutionStateSampler.newForTest(),
                executionStateRegistry.getState(
                    NameContext.forStage("stage"), "other", null, NoopProfileScope.NOOP),
                counterSet,
                PipelineOptionsFactory.create(),
                "test-work-item-id"),
            executionStateRegistry,
            globalConfigHandle,
            Long.MAX_VALUE,
            /*throwExceptionOnLargeOutput=*/ false);

    Windmill.WorkItem workItemA =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyA"))
            .setWorkToken(100L)
            .setCacheToken(1000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workA =
        createMockWork(
            workItemA, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    Windmill.WorkItem workItemB =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("keyB"))
            .setWorkToken(200L)
            .setCacheToken(2000L)
            .setKeyGroup(Windmill.Uint128Proto.newBuilder().setHigh(1L).setLow(2L).build())
            .build();
    Work workB =
        createMockWork(
            workItemB, Watermarks.builder().setInputDataWatermark(new Instant(1000)).build());

    BoundedQueueExecutor mockExecutor = mock(BoundedQueueExecutor.class);
    BoundedQueueExecutorWorkHandle mockBudget = mock(BoundedQueueExecutorWorkHandle.class);
    ExecutableWork executableWorkB = ExecutableWork.create(workB, (w, h) -> {});

    org.mockito.Mockito.when(
            mockExecutor.pollWork(
                org.mockito.Mockito.eq(COMPUTATION_ID),
                org.mockito.Mockito.eq(
                    org.apache.beam.runners.dataflow.worker.streaming.Work.KeyGroup.create(1L, 2L)),
                org.mockito.Mockito.eq(mockBudget)))
        .thenReturn(java.util.Optional.of(executableWorkB));

    Windmill.WorkItemCommitRequest.Builder outputBuilderA =
        Windmill.WorkItemCommitRequest.newBuilder();

    testContext.start(
        "keyA",
        workA,
        stateReader,
        sideInputStateFetcher,
        outputBuilderA,
        workExecutor,
        mockExecutor,
        mockBudget,
        new HotKeyLogger(),
        false,
        "step1",
        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
        5,
        1000000000L,
        10000000L,
        (k, c) -> {});

    // Cache reader for keyA
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader readerA =
        mock(org.apache.beam.sdk.io.UnboundedSource.UnboundedReader.class);
    org.apache.beam.sdk.io.UnboundedSource mockSourceA =
        mock(org.apache.beam.sdk.io.UnboundedSource.class);
    org.mockito.Mockito.when(readerA.getCurrentSource()).thenReturn(mockSourceA);
    org.mockito.Mockito.when(readerA.getWatermark()).thenReturn(new Instant(1000));
    testContext.setActiveReader(readerA);

    // Advance to keyB (which calls finishKey() for keyA, caching readerA)
    boolean advanced = testContext.advance();
    assertTrue(advanced);

    // Cache reader for keyB
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader readerB =
        mock(org.apache.beam.sdk.io.UnboundedSource.UnboundedReader.class);
    org.apache.beam.sdk.io.UnboundedSource mockSourceB =
        mock(org.apache.beam.sdk.io.UnboundedSource.class);
    org.mockito.Mockito.when(readerB.getCurrentSource()).thenReturn(mockSourceB);
    org.mockito.Mockito.when(readerB.getWatermark()).thenReturn(new Instant(1000));
    testContext.setActiveReader(readerB);

    // Call finishKey() for keyB, caching readerB
    testContext.finishKey();

    // Verify both readers are now cached in testReaderCache
    org.apache.beam.runners.dataflow.worker.WindmillComputationKey compKeyA =
        org.apache.beam.runners.dataflow.worker.WindmillComputationKey.create(
            COMPUTATION_ID, workA.getShardedKey());
    org.apache.beam.runners.dataflow.worker.WindmillComputationKey compKeyB =
        org.apache.beam.runners.dataflow.worker.WindmillComputationKey.create(
            COMPUTATION_ID, workB.getShardedKey());

    // We acquire readerA and readerB to ensure they were cached
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader<?> cachedA =
        testReaderCache.acquireReader(compKeyA, 1000L, 100L + 1);
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader<?> cachedB =
        testReaderCache.acquireReader(compKeyB, 2000L, 200L + 1);
    assertEquals(readerA, cachedA);
    assertEquals(readerB, cachedB);

    // Put them back into cache
    testReaderCache.cacheReader(compKeyA, 1000L, 100L, readerA);
    testReaderCache.cacheReader(compKeyB, 2000L, 200L, readerB);

    // Call invalidateCache()
    testContext.invalidateCache();

    // Verify both readers are invalidated from the cache (acquireReader returns null)
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader<?> clearedA =
        testReaderCache.acquireReader(compKeyA, 1000L, 100L + 1);
    org.apache.beam.sdk.io.UnboundedSource.UnboundedReader<?> clearedB =
        testReaderCache.acquireReader(compKeyB, 2000L, 200L + 1);
    org.junit.Assert.assertNull(clearedA);
    org.junit.Assert.assertNull(clearedB);

    // Verify that stateCache.invalidate was called for both keys
    org.mockito.Mockito.verify(mockStateCache)
        .invalidate(
            org.mockito.Mockito.eq(workItemA.getKey()),
            org.mockito.Mockito.eq(workItemA.getShardingKey()));
    org.mockito.Mockito.verify(mockStateCache)
        .invalidate(
            org.mockito.Mockito.eq(workItemB.getKey()),
            org.mockito.Mockito.eq(workItemB.getShardingKey()));
  }
}
