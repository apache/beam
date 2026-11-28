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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class SimpleParDoFnHelpersTest {
  private PipelineOptions options;
  @Mock DoFnInstanceManager doFnInstanceManager;
  @Mock SideInputReader sideInputReader;
  @Mock DataflowStepContext stepContext;
  @Mock DataflowStepContext userStepContext;
  @Mock DataflowOperationContext operationContext;
  @Mock DoFnRunnerFactory<String, String> runnerFactory;
  @Mock DoFnRunner<String, String> mockRunner;

  @Mock StreamingSideInputProcessor<String, GlobalWindow> sideInputProcessor;

  @Mock DoFnInfo<String, String> doFnInfo;
  @Mock CounterFactory counterFactory;

  private static class TestDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement() {}
  }

  private static class StatefulTestDoFn extends DoFn<String, String> {
    @StateId("count")
    private final StateSpec<ValueState<Integer>> count = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(@StateId("count") ValueState<Integer> state) {}
  }

  private TestDoFn doFn = new TestDoFn();

  private SimpleParDoFnHelpers<String, String, GlobalWindow> helpers;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    options = PipelineOptionsFactory.create();
    when(stepContext.namespacedToUser()).thenReturn(userStepContext);
    when(stepContext.getNameContext())
        .thenReturn(NameContext.create("stage", "original", "system", "step"));
    when(operationContext.counterFactory()).thenReturn(counterFactory);

    when(doFnInstanceManager.get()).thenReturn((DoFnInfo) doFnInfo);
    when(doFnInfo.getDoFn()).thenReturn(doFn);

    when(runnerFactory.createRunner(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
            any(), any()))
        .thenReturn(mockRunner);

    helpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            new TupleTag<>("main"),
            ImmutableMap.of(new TupleTag<>("main"), 0),
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            runnerFactory);
  }

  @Test
  public void testReallyStartBundle() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    verify(runnerFactory)
        .createRunner(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(),
            any(), any());
    verify(mockRunner).startBundle();
    assertFalse(helpers.hasState());
  }

  @Test
  public void testFinishBundle() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    helpers.finishBundle(sideInputProcessor);

    verify(mockRunner).finishBundle();
    verify(sideInputProcessor).handleFinishBundle();
    verify(doFnInstanceManager).complete(any());
    assertFalse(helpers.hasState());
  }

  @Test
  public void testAbort() throws Exception {
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();

    helpers.abort();

    verify(doFnInstanceManager).abort(any());
    assertFalse(helpers.hasState());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHasStateLifecycleAndAbortExceptionReset() throws Exception {
    when(doFnInfo.getDoFn()).thenReturn((DoFn) new StatefulTestDoFn());
    assertFalse(helpers.hasState());

    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();
    assertTrue(helpers.hasState());

    // Reject re-entrant startBundle before finish/abort.
    assertThrows(IllegalStateException.class, () -> helpers.startBundle(mock(Receiver.class)));

    helpers.finishBundle(sideInputProcessor);
    assertFalse(helpers.hasState());

    // Even if doFnInstanceManager.abort throws, internal state/hasState must be reset cleanly.
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();
    assertTrue(helpers.hasState());
    doThrow(new IllegalStateException("abort fail")).when(doFnInstanceManager).abort(any());
    assertThrows(IllegalStateException.class, () -> helpers.abort());
    assertFalse(helpers.hasState());

    // Can immediately start a subsequent bundle with a stateless DoFn.
    when(doFnInfo.getDoFn()).thenReturn(doFn);
    helpers.startBundle(mock(Receiver.class));
    helpers.reallyStartBundle();
    assertFalse(helpers.hasState());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOutputManagerRoutingAndMultiBundleReuse() throws Exception {
    CounterSet realCounters = new CounterSet();
    when(operationContext.counterFactory()).thenReturn(realCounters);

    TupleTag<String> mainTag = new TupleTag<>("main");
    TupleTag<String> side1 = new TupleTag<>("side1");
    TupleTag<String> side2 = new TupleTag<>("side2");
    TupleTag<String> nullSlotTag = new TupleTag<>("nullSlot");
    Map<TupleTag<?>, Integer> tagMap = new LinkedHashMap<>();
    tagMap.put(mainTag, 0);
    tagMap.put(side1, 1);
    tagMap.put(side2, 2);
    tagMap.put(nullSlotTag, 3);

    DoFnRunnerFactory<String, String> capturingFactory = mock(DoFnRunnerFactory.class);
    ArgumentCaptor<WindowedValueMultiReceiver> captor =
        ArgumentCaptor.forClass(WindowedValueMultiReceiver.class);
    when(capturingFactory.createRunner(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            captor.capture(),
            any(),
            any()))
        .thenReturn(mockRunner);

    SimpleParDoFnHelpers<String, String, GlobalWindow> multiHelpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            mainTag,
            tagMap,
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            capturingFactory);

    List<Object> main1 = new ArrayList<>();
    List<Object> s1 = new ArrayList<>();
    List<Object> s2 = new ArrayList<>();
    multiHelpers.startBundle(main1::add, s1::add, s2::add, null);
    multiHelpers.reallyStartBundle();

    WindowedValueMultiReceiver om1 = captor.getValue();
    // Exact mainTag, equal-but-deserialized mainTag, interleaved side tags (same & distinct refs).
    TupleTag<String> mainCopy = new TupleTag<>("main");
    TupleTag<String> side1Copy = new TupleTag<>("side1");
    TupleTag<String> side2Copy = new TupleTag<>("side2");
    om1.output(mainTag, WindowedValues.valueInGlobalWindow("m1"));
    om1.output(mainCopy, WindowedValues.valueInGlobalWindow("m2"));
    om1.output(mainCopy, WindowedValues.valueInGlobalWindow("m3"));
    om1.output(side1Copy, WindowedValues.valueInGlobalWindow("s1a"));
    om1.output(side2Copy, WindowedValues.valueInGlobalWindow("s2a"));
    om1.output(side1Copy, WindowedValues.valueInGlobalWindow("s1b"));
    om1.output(mainTag, WindowedValues.valueInGlobalWindow("m4"));
    om1.output(side2Copy, WindowedValues.valueInGlobalWindow("s2b"));

    // Undeclared output tag and declared null-receiver slot must both update implicit counters.
    TupleTag<String> undeclared = new TupleTag<>("extra");
    om1.output(undeclared, WindowedValues.valueInGlobalWindow("u1"));
    om1.output(new TupleTag<>("extra"), WindowedValues.valueInGlobalWindow("u2"));
    om1.output(nullSlotTag, WindowedValues.valueInGlobalWindow("n1"));
    om1.output(side1Copy, WindowedValues.valueInGlobalWindow("s1c"));
    om1.output(new TupleTag<>("nullSlot"), WindowedValues.valueInGlobalWindow("n2"));

    assertEquals(4, main1.size());
    assertEquals(3, s1.size());
    assertEquals(2, s2.size());
    assertEquals(
        2L,
        ((Long)
                realCounters
                    .getExistingCounter(CounterName.named("implicit-extra-ElementCount"))
                    .getAggregate())
            .longValue());
    assertEquals(
        2L,
        ((Long)
                realCounters
                    .getExistingCounter(CounterName.named("implicit-nullSlot-ElementCount"))
                    .getAggregate())
            .longValue());

    // Reuse helpers in a second bundle with a fresh Receiver array (must not route to stale main1).
    multiHelpers.finishBundle(null);
    List<Object> main2 = new ArrayList<>();
    List<Object> s1Second = new ArrayList<>();
    List<Object> s2Second = new ArrayList<>();
    multiHelpers.startBundle(main2::add, s1Second::add, s2Second::add, null);
    multiHelpers.reallyStartBundle();
    WindowedValueMultiReceiver om2 = captor.getValue();
    om2.output(mainTag, WindowedValues.valueInGlobalWindow("bundle2-m"));
    om2.output(side1Copy, WindowedValues.valueInGlobalWindow("bundle2-s1"));
    assertEquals(4, main1.size());
    assertEquals(3, s1.size());
    assertEquals(
        Collections.singletonList("bundle2-m"),
        Arrays.asList(((WindowedValue<?>) main2.get(0)).getValue()));
    assertEquals(
        Collections.singletonList("bundle2-s1"),
        Arrays.asList(((WindowedValue<?>) s1Second.get(0)).getValue()));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOutputManagerManyOutputsAndNullTagKey() throws Exception {
    // > 8 outputs (exceeds MAX_CACHED_OUTPUT_SLOTS) falls back to map lookup cleanly.
    Map<TupleTag<?>, Integer> bigMap = new HashMap<>();
    TupleTag<String> mainTag = new TupleTag<>("t0");
    List<TupleTag<String>> tags = new ArrayList<>();
    List<List<Object>> lists = new ArrayList<>();
    Receiver[] receivers = new Receiver[10];
    for (int i = 0; i < 10; i++) {
      TupleTag<String> t = i == 0 ? mainTag : new TupleTag<>("t" + i);
      tags.add(t);
      bigMap.put(t, i);
      List<Object> bucket = new ArrayList<>();
      lists.add(bucket);
      receivers[i] = bucket::add;
    }

    DoFnRunnerFactory<String, String> capturingFactory = mock(DoFnRunnerFactory.class);
    ArgumentCaptor<WindowedValueMultiReceiver> captor =
        ArgumentCaptor.forClass(WindowedValueMultiReceiver.class);
    when(capturingFactory.createRunner(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            captor.capture(),
            any(),
            any()))
        .thenReturn(mockRunner);

    SimpleParDoFnHelpers<String, String, GlobalWindow> bigHelpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            mainTag,
            bigMap,
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            capturingFactory);
    bigHelpers.startBundle(receivers);
    bigHelpers.reallyStartBundle();
    WindowedValueMultiReceiver om = captor.getValue();
    for (int i = 0; i < 10; i++) {
      om.output(tags.get(i), WindowedValues.valueInGlobalWindow("exact-" + i));
      om.output(new TupleTag<>("t" + i), WindowedValues.valueInGlobalWindow("copy-" + i));
    }
    for (int i = 0; i < 10; i++) {
      assertEquals(2, lists.get(i).size());
    }

    // Verify passing a null TupleTag neither matches altMainTag nor an uninitialized cachedTags
    // slot (which both must not route to a declared receiver); throws NPE in undeclared path.
    TupleTag<String> sideTag = new TupleTag<>("side");
    List<Object> mainBucket = new ArrayList<>();
    List<Object> sideBucket = new ArrayList<>();
    SimpleParDoFnHelpers<String, String, GlobalWindow> twoOutputHelpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            mainTag,
            ImmutableMap.of(mainTag, 0, sideTag, 1),
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            capturingFactory);
    twoOutputHelpers.startBundle(mainBucket::add, sideBucket::add);
    twoOutputHelpers.reallyStartBundle();
    WindowedValueMultiReceiver twoOm = captor.getValue();
    // Test before cachedTags is initialized.
    assertThrows(
        NullPointerException.class,
        () -> twoOm.output(null, WindowedValues.valueInGlobalWindow("null-before-cache")));
    // Populate cachedTags slot 1 (leaving slot 0 null) and test null again.
    twoOm.output(sideTag, WindowedValues.valueInGlobalWindow("side-elem"));
    assertThrows(
        NullPointerException.class,
        () -> twoOm.output(null, WindowedValues.valueInGlobalWindow("null-after-cache")));
    assertTrue(mainBucket.isEmpty());
    assertEquals(1, sideBucket.size());
  }

  private static class StatefulWithExpirationDoFn extends DoFn<String, String> {
    @StateId("count")
    private final StateSpec<ValueState<Integer>> count = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(@StateId("count") ValueState<Integer> state) {}

    @OnWindowExpiration
    public void onWindowExpiration() {}
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRegisterStateCleanupWindowedValueFastPath() throws Exception {
    // GlobalWindow stateful DoFn WITHOUT @OnWindowExpiration skips cleanup timer registration.
    when(doFnInfo.getDoFn()).thenReturn(new StatefulTestDoFn());
    helpers.startBundle(elem -> {});
    helpers.reallyStartBundle();
    WindowingStrategy<Object, GlobalWindow> globalStrategy =
        (WindowingStrategy<Object, GlobalWindow>)
            (WindowingStrategy<?, ?>) WindowingStrategy.globalDefault();
    helpers.registerStateCleanup(globalStrategy, WindowedValues.valueInGlobalWindow("a"));
    helpers.registerStateCleanup(
        globalStrategy, WindowedValues.timestampedValueInGlobalWindow("b", Instant.EPOCH));
    verify(stepContext, never()).setStateCleanupTimer(any(), any(), any(), any(), any());
    helpers.finishBundle(null);

    // GlobalWindow stateful DoFn WITH @OnWindowExpiration registers cleanup timer for GlobalWindow.
    when(doFnInfo.getDoFn()).thenReturn(new StatefulWithExpirationDoFn());
    helpers.startBundle(elem -> {});
    helpers.reallyStartBundle();
    helpers.registerStateCleanup(globalStrategy, WindowedValues.valueInGlobalWindow("c"));
    helpers.registerStateCleanup(
        globalStrategy, WindowedValues.timestampedValueInGlobalWindow("d", Instant.EPOCH));
    Instant expectedCleanup =
        helpers.earliestAllowableCleanupTime(GlobalWindow.INSTANCE, globalStrategy);
    Instant expectedOutputTs = expectedCleanup.minus(Duration.millis(1L));
    verify(stepContext, org.mockito.Mockito.times(2))
        .setStateCleanupTimer(
            eq(SimpleParDoFnHelpers.CLEANUP_TIMER_ID),
            eq(GlobalWindow.INSTANCE),
            any(),
            eq(expectedCleanup),
            eq(expectedOutputTs));
    helpers.finishBundle(null);

    // IntervalWindow falls through to elem.getWindows() and sets the cleanup timer for the window.
    SimpleParDoFnHelpers<String, String, IntervalWindow> intervalHelpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            new TupleTag<>("main"),
            ImmutableMap.of(new TupleTag<>("main"), 0),
            stepContext,
            operationContext,
            DoFnSchemaInformation.create(),
            ImmutableMap.of(),
            runnerFactory);
    when(doFnInfo.getDoFn()).thenReturn(new StatefulTestDoFn());
    intervalHelpers.startBundle(elem -> {});
    intervalHelpers.reallyStartBundle();
    WindowingStrategy<Object, IntervalWindow> fixedStrategy =
        (WindowingStrategy<Object, IntervalWindow>)
            (WindowingStrategy<?, ?>)
                WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10)));
    IntervalWindow window =
        new IntervalWindow(Instant.ofEpochMilli(0), Instant.ofEpochMilli(10_000));
    intervalHelpers.registerStateCleanup(
        fixedStrategy, WindowedValues.of("e", Instant.ofEpochMilli(5), window, PaneInfo.NO_FIRING));
    Instant expectedIntervalCleanup =
        intervalHelpers.earliestAllowableCleanupTime(window, fixedStrategy);
    verify(stepContext)
        .setStateCleanupTimer(
            eq(SimpleParDoFnHelpers.CLEANUP_TIMER_ID),
            eq(window),
            any(),
            eq(expectedIntervalCleanup),
            eq(expectedIntervalCleanup));
  }
}
