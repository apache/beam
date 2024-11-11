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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.theInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UserParDoFnFactory}. */
@RunWith(JUnit4.class)
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
public class UserParDoFnFactoryTest {
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

  private static class TestStatefulDoFn extends DoFn<KV<String, Integer>, Void> {

    public static final String STATE_ID = "state-id";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    public void processElement(ProcessContext c) {}
  }

  private static class TestStatefulDoFnWithWindowExpiration
      extends DoFn<KV<String, Integer>, Void> {

    public static final String STATE_ID = "state-id";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> spec = StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    public void processElement(ProcessContext c) {}

    @OnWindowExpiration
    public void onWindowExpiration() {}
  }

  private static final TupleTag<String> MAIN_OUTPUT = new TupleTag<>("1");

  private UserParDoFnFactory factory = UserParDoFnFactory.createDefault();

  @Test
  public void testFactoryReuseInStep() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    TestDoFn initialFn = new TestDoFn(Collections.<TupleTag<String>>emptyList());
    CloudObject cloudObject = getCloudObject(initialFn);
    TestOperationContext operationContext = TestOperationContext.create(counters);
    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            operationContext);

    Receiver rcvr = new OutputReceiver();

    parDoFn.startBundle(rcvr);
    parDoFn.processElement(WindowedValue.valueInGlobalWindow("foo"));

    TestDoFn fn = (TestDoFn) ((SimpleParDoFn) parDoFn).getDoFnInfo().getDoFn();
    assertThat(fn, not(theInstance(initialFn)));

    parDoFn.finishBundle();
    assertThat(fn.state, equalTo(TestDoFn.State.FINISHED));

    // The fn should be reused for the second call to create
    ParDoFn secondParDoFn =
        factory.create(
            options,
            cloudObject,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            operationContext);
    // The fn should still be finished from the last call; it should not be set up again
    assertThat(fn.state, equalTo(TestDoFn.State.FINISHED));

    secondParDoFn.startBundle(rcvr);
    secondParDoFn.processElement(WindowedValue.valueInGlobalWindow("spam"));
    TestDoFn reobtainedFn = (TestDoFn) ((SimpleParDoFn) secondParDoFn).getDoFnInfo().getDoFn();
    secondParDoFn.finishBundle();
    assertThat(reobtainedFn.state, equalTo(TestDoFn.State.FINISHED));

    assertThat(fn, theInstance(reobtainedFn));
  }

  @Test
  public void testFactorySimultaneousUse() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    TestDoFn initialFn = new TestDoFn(Collections.<TupleTag<String>>emptyList());
    CloudObject cloudObject = getCloudObject(initialFn);
    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create(counters));

    // The fn should not be reused while the first ParDoFn is not finished
    ParDoFn secondParDoFn =
        factory.create(
            options,
            cloudObject,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create(counters));

    Receiver rcvr = new OutputReceiver();
    parDoFn.startBundle(rcvr);
    parDoFn.processElement(WindowedValue.valueInGlobalWindow("foo"));

    // Must be after the first call to process element for reallyStartBundle to have been called
    TestDoFn firstDoFn = (TestDoFn) ((SimpleParDoFn) parDoFn).getDoFnInfo().getDoFn();

    secondParDoFn.startBundle(rcvr);
    secondParDoFn.processElement(WindowedValue.valueInGlobalWindow("spam"));

    // Must be after the first call to process element for reallyStartBundle to have been called
    TestDoFn secondDoFn = (TestDoFn) ((SimpleParDoFn) secondParDoFn).getDoFnInfo().getDoFn();

    parDoFn.finishBundle();
    secondParDoFn.finishBundle();

    assertThat(firstDoFn, not(theInstance(secondDoFn)));
    assertThat(firstDoFn.state, equalTo(TestDoFn.State.FINISHED));
    assertThat(secondDoFn.state, equalTo(TestDoFn.State.FINISHED));
  }

  @Test
  public void testFactoryDoesNotReuseAfterAborted() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    TestDoFn initialFn = new TestDoFn(Collections.<TupleTag<String>>emptyList());
    CloudObject cloudObject = getCloudObject(initialFn);
    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create(counters));

    Receiver rcvr = new OutputReceiver();

    parDoFn.startBundle(rcvr);
    parDoFn.processElement(WindowedValue.valueInGlobalWindow("foo"));
    TestDoFn fn = (TestDoFn) ((SimpleParDoFn) parDoFn).getDoFnInfo().getDoFn();

    parDoFn.abort();
    assertThat(fn.state, equalTo(TestDoFn.State.TORN_DOWN));

    // The fn should not be torn down here
    ParDoFn secondParDoFn =
        factory.create(
            options,
            cloudObject.clone(),
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            BatchModeExecutionContext.forTesting(options, "testStage"),
            TestOperationContext.create(counters));

    secondParDoFn.startBundle(rcvr);
    secondParDoFn.processElement(WindowedValue.valueInGlobalWindow("foo"));
    TestDoFn secondFn = (TestDoFn) ((SimpleParDoFn) secondParDoFn).getDoFnInfo().getDoFn();

    assertThat(secondFn, not(theInstance(fn)));
    assertThat(fn.state, equalTo(TestDoFn.State.TORN_DOWN));
    assertThat(secondFn.state, equalTo(TestDoFn.State.PROCESSING));
  }

  private CloudObject getCloudObject(DoFn<?, ?> fn) {
    return getCloudObject(fn, WindowingStrategy.globalDefault());
  }

  private CloudObject getCloudObject(DoFn<?, ?> fn, WindowingStrategy<?, ?> windowingStrategy) {
    CloudObject object = CloudObject.forClassName("DoFn");
    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "unchecked"
    })
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            fn,
            windowingStrategy,
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());
    object.set(
        PropertyNames.SERIALIZED_FN,
        StringUtils.byteArrayToJsonString(SerializableUtils.serializeToByteArray(info)));
    return object;
  }

  @Test
  public void testCleanupRegistered() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    DoFn<?, ?> initialFn = new TestStatefulDoFn();
    CloudObject cloudObject =
        getCloudObject(
            initialFn,
            WindowingStrategy.globalDefault().withWindowFn(FixedWindows.of(Duration.millis(10))));

    TimerInternals timerInternals = mock(TimerInternals.class);

    DataflowStepContext stepContext = mock(DataflowStepContext.class);
    when(stepContext.timerInternals()).thenReturn(timerInternals);

    DataflowExecutionContext<DataflowStepContext> executionContext =
        mock(DataflowExecutionContext.class);
    TestOperationContext operationContext = TestOperationContext.create(counters);
    when(executionContext.getStepContext(operationContext)).thenReturn(stepContext);
    when(executionContext.getSideInputReader(any(), any(), any()))
        .thenReturn(NullSideInputReader.empty());

    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            Collections.emptyList(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            executionContext,
            operationContext);

    Receiver rcvr = new OutputReceiver();
    parDoFn.startBundle(rcvr);

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(10));
    parDoFn.processElement(
        WindowedValue.of("foo", new Instant(1), firstWindow, PaneInfo.NO_FIRING));

    verify(stepContext)
        .setStateCleanupTimer(
            SimpleParDoFn.CLEANUP_TIMER_ID,
            firstWindow,
            IntervalWindow.getCoder(),
            firstWindow.maxTimestamp().plus(Duration.millis(1L)),
            firstWindow.maxTimestamp().plus(Duration.millis(1L)));
  }

  /**
   * Regression test for global window + OnWindowExpiration + allowed lateness > max allowed time
   */
  @Test
  public void testCleanupRegisteredForGlobalWindowWithAllowedLateness() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    DoFn<?, ?> initialFn = new TestStatefulDoFnWithWindowExpiration();
    Duration allowedLateness = Duration.standardDays(2);
    CloudObject cloudObject =
        getCloudObject(
            initialFn, WindowingStrategy.globalDefault().withAllowedLateness(allowedLateness));

    TimerInternals timerInternals = mock(TimerInternals.class);

    DataflowStepContext stepContext = mock(DataflowStepContext.class);
    when(stepContext.timerInternals()).thenReturn(timerInternals);

    DataflowExecutionContext<DataflowStepContext> executionContext =
        mock(DataflowExecutionContext.class);
    TestOperationContext operationContext = TestOperationContext.create(counters);
    when(executionContext.getStepContext(operationContext)).thenReturn(stepContext);
    when(executionContext.getSideInputReader(any(), any(), any()))
        .thenReturn(NullSideInputReader.empty());

    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            Collections.emptyList(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            executionContext,
            operationContext);

    Receiver rcvr = new OutputReceiver();
    parDoFn.startBundle(rcvr);

    GlobalWindow globalWindow = GlobalWindow.INSTANCE;
    parDoFn.processElement(
        WindowedValue.of("foo", new Instant(1), globalWindow, PaneInfo.NO_FIRING));

    assertThat(
        globalWindow.maxTimestamp().plus(allowedLateness),
        greaterThan(BoundedWindow.TIMESTAMP_MAX_VALUE));
    verify(stepContext)
        .setStateCleanupTimer(
            SimpleParDoFn.CLEANUP_TIMER_ID,
            globalWindow,
            GlobalWindow.Coder.INSTANCE,
            BoundedWindow.TIMESTAMP_MAX_VALUE,
            BoundedWindow.TIMESTAMP_MAX_VALUE.minus(Duration.millis(1)));
  }

  @Test
  public void testCleanupWorks() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CounterSet counters = new CounterSet();
    DoFn<?, ?> initialFn = new TestStatefulDoFn();
    CloudObject cloudObject =
        getCloudObject(initialFn, WindowingStrategy.of(FixedWindows.of(Duration.millis(10))));

    StateInternals stateInternals = InMemoryStateInternals.forKey("dummy");

    // The overarching step context that only ParDoFn gets
    DataflowStepContext stepContext = mock(DataflowStepContext.class);

    // The user step context that the DoFnRunner gets a handle on
    DataflowStepContext userStepContext = mock(DataflowStepContext.class);
    when(stepContext.namespacedToUser()).thenReturn(userStepContext);
    when(stepContext.stateInternals()).thenReturn(stateInternals);
    when(userStepContext.stateInternals()).thenReturn((StateInternals) stateInternals);

    DataflowExecutionContext<DataflowStepContext> executionContext =
        mock(DataflowExecutionContext.class);
    TestOperationContext operationContext = TestOperationContext.create(counters);
    when(executionContext.getStepContext(operationContext)).thenReturn(stepContext);
    when(executionContext.getSideInputReader(any(), any(), any()))
        .thenReturn(NullSideInputReader.empty());

    ParDoFn parDoFn =
        factory.create(
            options,
            cloudObject,
            Collections.emptyList(),
            MAIN_OUTPUT,
            ImmutableMap.of(MAIN_OUTPUT, 0),
            executionContext,
            operationContext);

    Receiver rcvr = new OutputReceiver();
    parDoFn.startBundle(rcvr);

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(9));
    IntervalWindow secondWindow = new IntervalWindow(new Instant(10), new Instant(19));

    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
    StateNamespace firstWindowNamespace = StateNamespaces.window(windowCoder, firstWindow);
    StateNamespace secondWindowNamespace = StateNamespaces.window(windowCoder, secondWindow);
    StateTag<ValueState<String>> tag =
        StateTags.tagForSpec(TestStatefulDoFn.STATE_ID, StateSpecs.value(StringUtf8Coder.of()));

    // Set up non-empty state. We don't mock + verify calls to clear() but instead
    // check that state is actually empty. We musn't care how it is accomplished.
    stateInternals.state(firstWindowNamespace, tag).write("first");
    stateInternals.state(secondWindowNamespace, tag).write("second");

    when(userStepContext.getNextFiredTimer(windowCoder)).thenReturn(null);

    when(stepContext.getNextFiredTimer(windowCoder))
        .thenReturn(
            TimerData.of(
                SimpleParDoFn.CLEANUP_TIMER_ID,
                firstWindowNamespace,
                firstWindow.maxTimestamp().plus(Duration.millis(1L)),
                firstWindow.maxTimestamp().plus(Duration.millis(1L)),
                TimeDomain.EVENT_TIME))
        .thenReturn(null);

    // This should fire the timer to clean up the first window
    parDoFn.processTimers();

    assertThat(stateInternals.state(firstWindowNamespace, tag).read(), nullValue());
    assertThat(stateInternals.state(secondWindowNamespace, tag).read(), equalTo("second"));

    when(stepContext.getNextFiredTimer((Coder) windowCoder))
        .thenReturn(
            TimerData.of(
                SimpleParDoFn.CLEANUP_TIMER_ID,
                secondWindowNamespace,
                secondWindow.maxTimestamp().plus(Duration.millis(1L)),
                secondWindow.maxTimestamp().plus(Duration.millis(1L)),
                TimeDomain.EVENT_TIME))
        .thenReturn(null);

    // And this should clean up the second window
    parDoFn.processTimers();

    assertThat(stateInternals.state(firstWindowNamespace, tag).read(), nullValue());
    assertThat(stateInternals.state(secondWindowNamespace, tag).read(), nullValue());
  }
}
