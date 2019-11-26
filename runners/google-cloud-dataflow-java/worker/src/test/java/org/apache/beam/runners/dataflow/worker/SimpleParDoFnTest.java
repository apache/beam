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

import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterStructuredNameMatcher.hasStructuredName;
import static org.apache.beam.runners.dataflow.worker.util.CounterHamcrestMatchers.CounterUpdateDistributionMatcher.hasDistribution;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterDistribution;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SimpleParDoFn}. */
@RunWith(JUnit4.class)
public class SimpleParDoFnTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private PipelineOptions options;
  private TestOperationContext operationContext;
  private BatchModeExecutionContext.StepContext stepContext;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create();
    // TODO: Remove once Distributions has shipped.
    options
        .as(DataflowPipelineDebugOptions.class)
        .setExperiments(Lists.newArrayList(SimpleParDoFn.OUTPUTS_PER_ELEMENT_EXPERIMENT));

    operationContext = TestOperationContext.create();
    stepContext =
        BatchModeExecutionContext.forTesting(
                options, operationContext.counterFactory(), "testStage")
            .getStepContext(operationContext);
  }

  // TODO: Replace TestDoFn usages with a mock DoFn to reduce boilerplate.
  static class TestDoFn extends DoFn<Integer, String> {
    enum State {
      UNSTARTED,
      SET_UP,
      STARTED,
      PROCESSING,
      FINISHED,
      TORN_DOWN
    }

    State state = State.UNSTARTED;

    final List<TupleTag<String>> outputTags;

    public TestDoFn(List<TupleTag<String>> outputTags) {
      this.outputTags = outputTags;
    }

    @Setup
    public void setup() {
      state = State.SET_UP;
    }

    @StartBundle
    public void startBundle() {
      assertThat(state, anyOf(equalTo(State.SET_UP), equalTo(State.FINISHED)));
      state = State.STARTED;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.PROCESSING;
      String value = "processing: " + c.element();
      c.output(value);
      for (TupleTag<String> additionalOutputTupleTag : outputTags) {
        c.output(additionalOutputTupleTag, additionalOutputTupleTag.getId() + ": " + value);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      assertThat(state, anyOf(equalTo(State.STARTED), equalTo(State.PROCESSING)));
      state = State.FINISHED;
      c.output("finished", BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow.INSTANCE);
      for (TupleTag<String> additionalOutputTupleTag : outputTags) {
        c.output(
            additionalOutputTupleTag,
            additionalOutputTupleTag.getId() + ": " + "finished",
            BoundedWindow.TIMESTAMP_MIN_VALUE,
            GlobalWindow.INSTANCE);
      }
    }

    @Teardown
    public void teardown() {
      assertThat(state, not(equalTo(State.TORN_DOWN)));
      state = State.TORN_DOWN;
    }
  }

  static class TestErrorDoFn extends DoFn<Integer, String> {
    // Used to test nested stack traces.
    private void nestedFunctionBeta(String s) {
      throw new RuntimeException(s);
    }

    private void nestedFunctionAlpha(String s) {
      nestedFunctionBeta(s);
    }

    @StartBundle
    public void startBundle() {
      nestedFunctionAlpha("test error in initialize");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      nestedFunctionBeta("test error in process");
    }

    @FinishBundle
    public void finishBundle() {
      throw new RuntimeException("test error in finalize");
    }
  }

  static class TestReceiver implements Receiver {
    List<Object> receivedElems = new ArrayList<>();

    @Override
    public void process(Object outputElem) {
      receivedElems.add(outputElem);
    }
  }

  private static final TupleTag<String> MAIN_OUTPUT = new TupleTag<>("1");

  @Test
  public void testOutputReceivers() throws Exception {
    TestDoFn fn =
        new TestDoFn(
            ImmutableList.of(
                new TupleTag<>("tag1"), new TupleTag<>("tag2"), new TupleTag<>("tag3")));
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    TestReceiver receiver = new TestReceiver();
    TestReceiver receiver1 = new TestReceiver();
    TestReceiver receiver2 = new TestReceiver();
    TestReceiver receiver3 = new TestReceiver();

    ParDoFn userParDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.cloningPool(fnInfo),
            new EmptySideInputReader(),
            MAIN_OUTPUT,
            ImmutableMap.of(
                MAIN_OUTPUT,
                0,
                new TupleTag<String>("tag1"),
                1,
                new TupleTag<String>("tag2"),
                2,
                new TupleTag<String>("tag3"),
                3),
            BatchModeExecutionContext.forTesting(options, "testStage")
                .getStepContext(operationContext),
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    userParDoFn.startBundle(receiver, receiver1, receiver2, receiver3);

    userParDoFn.processElement(WindowedValue.valueInGlobalWindow(3));
    userParDoFn.processElement(WindowedValue.valueInGlobalWindow(42));
    userParDoFn.processElement(WindowedValue.valueInGlobalWindow(666));

    userParDoFn.finishBundle();

    Object[] expectedReceivedElems = {
      WindowedValue.valueInGlobalWindow("processing: 3"),
      WindowedValue.valueInGlobalWindow("processing: 42"),
      WindowedValue.valueInGlobalWindow("processing: 666"),
      WindowedValue.valueInGlobalWindow("finished"),
    };
    assertArrayEquals(expectedReceivedElems, receiver.receivedElems.toArray());

    Object[] expectedReceivedElems1 = {
      WindowedValue.valueInGlobalWindow("tag1: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag1: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag1: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag1: finished"),
    };
    assertArrayEquals(expectedReceivedElems1, receiver1.receivedElems.toArray());

    Object[] expectedReceivedElems2 = {
      WindowedValue.valueInGlobalWindow("tag2: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag2: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag2: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag2: finished"),
    };
    assertArrayEquals(expectedReceivedElems2, receiver2.receivedElems.toArray());

    Object[] expectedReceivedElems3 = {
      WindowedValue.valueInGlobalWindow("tag3: processing: 3"),
      WindowedValue.valueInGlobalWindow("tag3: processing: 42"),
      WindowedValue.valueInGlobalWindow("tag3: processing: 666"),
      WindowedValue.valueInGlobalWindow("tag3: finished"),
    };
    assertArrayEquals(expectedReceivedElems3, receiver3.receivedElems.toArray());
  }

  @Test
  @SuppressWarnings("AssertionFailureIgnored")
  public void testUnexpectedNumberOfReceivers() throws Exception {
    TestDoFn fn = new TestDoFn(Collections.emptyList());
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    TestReceiver receiver = new TestReceiver();

    ParDoFn userParDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.singleInstance(fnInfo),
            new EmptySideInputReader(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage")
                .getStepContext(operationContext),
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    try {
      userParDoFn.startBundle();
      fail("should have failed");
    } catch (Throwable exn) {
      assertThat(exn.toString(), containsString("unexpected number of receivers"));
    }
    try {
      userParDoFn.startBundle(receiver, receiver);
      fail("should have failed");
    } catch (Throwable exn) {
      assertThat(exn.toString(), containsString("unexpected number of receivers"));
    }
  }

  private List<String> stackTraceFrameStrings(Throwable t) {
    List<String> stack = new ArrayList<>();
    for (StackTraceElement frame : t.getStackTrace()) {
      // Make sure that the frame has the expected name.
      stack.add(frame.toString());
    }
    return stack;
  }

  @Test
  public void testErrorPropagation() throws Exception {
    TestErrorDoFn fn = new TestErrorDoFn();
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    TestReceiver receiver = new TestReceiver();

    ParDoFn userParDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.singleInstance(fnInfo),
            new EmptySideInputReader(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage")
                .getStepContext(operationContext),
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    try {
      userParDoFn.startBundle(receiver);
      userParDoFn.processElement(null);
      fail("should have failed");
    } catch (Exception exn) {
      // Because we're calling this from inside the SDK and not from a
      // user's program (e.g. through Pipeline.run), the error should
      // be thrown as a UserCodeException. The cause of the
      // UserCodeError shouldn't contain any of the stack from within
      // the SDK, since we don't want to overwhelm users with stack
      // frames outside of their control.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace of the cause should contain three frames:
      // TestErrorDoFn.nestedFunctionBeta
      // TestErrorDoFn.nestedFunctionAlpha
      // TestErrorDoFn.startBundle
      assertThat(
          stackTraceFrameStrings(exn.getCause()),
          contains(
              containsString("TestErrorDoFn.nestedFunctionBeta"),
              containsString("TestErrorDoFn.nestedFunctionAlpha"),
              containsString("TestErrorDoFn.startBundle")));
      assertThat(exn.toString(), containsString("test error in initialize"));
    }

    try {
      userParDoFn.processElement(WindowedValue.valueInGlobalWindow(3));
      fail("should have failed");
    } catch (Exception exn) {
      // Exception should be a UserCodeException since we're calling
      // from inside the SDK.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace of the cause should contain two frames:
      // TestErrorDoFn.nestedFunctionBeta
      // TestErrorDoFn.processElement
      assertThat(
          stackTraceFrameStrings(exn.getCause()),
          contains(
              containsString("TestErrorDoFn.nestedFunctionBeta"),
              containsString("TestErrorDoFn.processElement")));
      assertThat(exn.toString(), containsString("test error in process"));
    }

    try {
      userParDoFn.finishBundle();
      fail("should have failed");
    } catch (Exception exn) {
      // Exception should be a UserCodeException since we're calling
      // from inside the SDK.
      assertThat(exn, instanceOf(UserCodeException.class));
      // Stack trace should only contain a single frame:
      // TestErrorDoFn.finishBundle
      assertThat(
          stackTraceFrameStrings(exn.getCause()),
          contains(containsString("TestErrorDoFn.finishBundle")));
      assertThat(exn.toString(), containsString("test error in finalize"));
    }
  }

  @Test
  public void testUndeclaredSideOutputs() throws Exception {
    TestDoFn fn =
        new TestDoFn(
            ImmutableList.of(
                new TupleTag<>("declared"),
                new TupleTag<>("undecl1"),
                new TupleTag<>("undecl2"),
                new TupleTag<>("undecl3")));
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    CounterSet counters = new CounterSet();
    TestOperationContext operationContext = TestOperationContext.create(counters);
    ParDoFn userParDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.cloningPool(fnInfo),
            NullSideInputReader.empty(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0, new TupleTag<String>("declared"), 1),
            BatchModeExecutionContext.forTesting(options, "testStage")
                .getStepContext(operationContext),
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    userParDoFn.startBundle(new TestReceiver(), new TestReceiver());

    thrown.expect(UserCodeException.class);
    thrown.expectCause(instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("Unknown output tag");
    userParDoFn.processElement(WindowedValue.valueInGlobalWindow(5));
  }

  @Test
  public void testStateTracking() throws Exception {
    ExecutionStateTracker tracker = ExecutionStateTracker.newForTest();

    TestOperationContext operationContext =
        TestOperationContext.create(
            new CounterSet(),
            NameContextsForTests.nameContextForTest(),
            new MetricsContainerImpl(NameContextsForTests.ORIGINAL_NAME),
            tracker);

    class StateTestingDoFn extends DoFn<Integer, String> {

      private boolean startCalled = false;

      @StartBundle
      public void startBundle() throws Exception {
        startCalled = true;
        assertThat(tracker.getCurrentState(), equalTo(operationContext.getStartState()));
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        assertThat(startCalled, equalTo(true));
        assertThat(tracker.getCurrentState(), equalTo(operationContext.getProcessState()));
      }
    }

    StateTestingDoFn fn = new StateTestingDoFn();
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    ParDoFn userParDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.singleInstance(fnInfo),
            NullSideInputReader.empty(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0, new TupleTag<>("declared"), 1),
            BatchModeExecutionContext.forTesting(
                    options, operationContext.counterFactory(), "testStage")
                .getStepContext(operationContext),
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    // This test ensures proper behavior of the state sampling even with lazy initialization.
    try (Closeable trackerCloser = tracker.activate()) {
      try (Closeable processCloser = operationContext.enterProcess()) {
        userParDoFn.processElement(WindowedValue.valueInGlobalWindow(5));
      }
    }
  }

  @Test
  public void testOutputsPerElementCounter() throws Exception {
    int[] inputData = new int[] {1, 2, 3, 4, 5};
    CounterDistribution expectedDistribution =
        CounterDistribution.builder()
            .minMax(1, 5)
            .count(5)
            .sum(1 + 2 + 3 + 4 + 5)
            .sumOfSquares(1 + 4 + 9 + 16 + 25)
            .buckets(1, Lists.newArrayList(1L, 3L, 1L))
            .build();

    List<CounterUpdate> counterUpdates = executeParDoFnCounterTest(inputData);
    CounterName expectedName =
        CounterName.named("per-element-output-count")
            .withOriginalName(stepContext.getNameContext());
    assertThat(
        counterUpdates,
        contains(
            allOf(
                hasStructuredName(expectedName, "DISTRIBUTION"),
                hasDistribution(expectedDistribution))));
  }

  // TODO: Remove once Distributions has shipped.
  @Test
  public void testOutputsPerElementCounterDisabledViaExperiment() throws Exception {
    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    List<String> experiments = debugOptions.getExperiments();
    experiments.remove(SimpleParDoFn.OUTPUTS_PER_ELEMENT_EXPERIMENT);
    debugOptions.setExperiments(experiments);

    List<CounterUpdate> counterUpdates = executeParDoFnCounterTest(0);
    CounterName expectedName =
        CounterName.named("per-element-output-count")
            .withOriginalName(stepContext.getNameContext());
    assertThat(counterUpdates, not(contains(hasStructuredName(expectedName, "DISTRIBUTION"))));
  }

  /**
   * Set up and execute a basic {@link ParDoFn} to validate reported counter values.
   *
   * @param inputData Input elements to process. For each element X, the DoFn will output a string
   *     repeated X times.
   * @return Delta counter updates extracted after execution.
   * @throws Exception
   */
  private List<CounterUpdate> executeParDoFnCounterTest(int... inputData) throws Exception {
    class RepeaterDoFn extends DoFn<Integer, String> {
      /** Takes as input the number of times to output a message. */
      @ProcessElement
      public void processElement(ProcessContext c) {
        int numTimes = c.element();
        for (int i = 0; i < numTimes; i++) {
          c.output(String.format("I will repeat this message %d times", numTimes));
        }
      }
    }

    DoFn<Integer, String> fn = new RepeaterDoFn();
    DoFnInfo<?, ?> fnInfo =
        DoFnInfo.forFn(
            fn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            MAIN_OUTPUT,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    ParDoFn parDoFn =
        new SimpleParDoFn<>(
            options,
            DoFnInstanceManagers.singleInstance(fnInfo),
            new EmptySideInputReader(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            Collections.emptyMap(),
            SimpleDoFnRunnerFactory.INSTANCE);

    parDoFn.startBundle(new TestReceiver());
    for (int input : inputData) {
      parDoFn.processElement(WindowedValue.valueInGlobalWindow(input));
    }

    return operationContext
        .counterSet()
        .extractUpdates(true, DataflowCounterUpdateExtractor.INSTANCE);
  }

  /**
   * Basic side input reader wrapping a tagged {@link Map} of side input iterables. Encapsulates
   * conversion according to the {@link PCollectionView} and projection to a particular window.
   */
  private static class EmptySideInputReader implements SideInputReader {
    private EmptySideInputReader() {}

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public <T> T get(PCollectionView<T> view, final BoundedWindow window) {
      throw new IllegalArgumentException("calling getSideInput() with unknown view");
    }
  }
}
