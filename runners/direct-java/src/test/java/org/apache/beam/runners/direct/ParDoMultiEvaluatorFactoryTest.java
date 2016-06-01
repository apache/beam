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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.direct.InMemoryWatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.OutputTimeFns;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.util.state.WatermarkHoldState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link ParDoMultiEvaluatorFactory}.
 */
@RunWith(JUnit4.class)
public class ParDoMultiEvaluatorFactoryTest implements Serializable {
  private transient BundleFactory bundleFactory = InProcessBundleFactory.create();

  @Test
  public void testParDoMultiInMemoryTransformEvaluator() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));

    TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<KV<String, Integer>>() {};
    final TupleTag<String> elementTag = new TupleTag<>();
    final TupleTag<Integer> lengthTag = new TupleTag<>();

    BoundMulti<String, KV<String, Integer>> pardo =
        ParDo.of(
                new DoFn<String, KV<String, Integer>>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(KV.<String, Integer>of(c.element(), c.element().length()));
                    c.sideOutput(elementTag, c.element());
                    c.sideOutput(lengthTag, c.element().length());
                  }
                })
            .withOutputTags(mainOutputTag, TupleTagList.of(elementTag).and(lengthTag));
    PCollectionTuple outputTuple = input.apply(pardo);

    CommittedBundle<String> inputBundle =
        bundleFactory.createRootBundle(input).commit(Instant.now());

    PCollection<KV<String, Integer>> mainOutput = outputTuple.get(mainOutputTag);
    PCollection<String> elementOutput = outputTuple.get(elementTag);
    PCollection<Integer> lengthOutput = outputTuple.get(lengthTag);

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<KV<String, Integer>> mainOutputBundle =
        bundleFactory.createRootBundle(mainOutput);
    UncommittedBundle<String> elementOutputBundle = bundleFactory.createRootBundle(elementOutput);
    UncommittedBundle<Integer> lengthOutputBundle = bundleFactory.createRootBundle(lengthOutput);

    when(evaluationContext.createBundle(inputBundle, mainOutput)).thenReturn(mainOutputBundle);
    when(evaluationContext.createBundle(inputBundle, elementOutput))
        .thenReturn(elementOutputBundle);
    when(evaluationContext.createBundle(inputBundle, lengthOutput)).thenReturn(lengthOutputBundle);

    InProcessExecutionContext executionContext =
        new InProcessExecutionContext(null, null, null, null);
    when(evaluationContext.getExecutionContext(mainOutput.getProducingTransformInternal(),
        inputBundle.getKey())).thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    TransformEvaluator<String> evaluator =
        new ParDoMultiEvaluatorFactory()
            .forApplication(
                mainOutput.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getOutputBundles(),
        Matchers.<UncommittedBundle<?>>containsInAnyOrder(
            lengthOutputBundle, mainOutputBundle, elementOutputBundle));
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(result.getCounters(), equalTo(counters));

    assertThat(
        mainOutputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<KV<String, Integer>>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("foo", 3)),
            WindowedValue.timestampedValueInGlobalWindow(KV.of("bara", 4), new Instant(1000)),
            WindowedValue.valueInGlobalWindow(
                KV.of("bazam", 5), PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(
        elementOutputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<String>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("foo"),
            WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)),
            WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(
        lengthOutputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<Integer>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(3),
            WindowedValue.timestampedValueInGlobalWindow(4, new Instant(1000)),
            WindowedValue.valueInGlobalWindow(5, PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void testParDoMultiUndeclaredSideOutput() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));

    TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<KV<String, Integer>>() {};
    final TupleTag<String> elementTag = new TupleTag<>();
    final TupleTag<Integer> lengthTag = new TupleTag<>();

    BoundMulti<String, KV<String, Integer>> pardo =
        ParDo.of(
                new DoFn<String, KV<String, Integer>>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.output(KV.<String, Integer>of(c.element(), c.element().length()));
                    c.sideOutput(elementTag, c.element());
                    c.sideOutput(lengthTag, c.element().length());
                  }
                })
            .withOutputTags(mainOutputTag, TupleTagList.of(elementTag));
    PCollectionTuple outputTuple = input.apply(pardo);

    CommittedBundle<String> inputBundle =
        bundleFactory.createRootBundle(input).commit(Instant.now());

    PCollection<KV<String, Integer>> mainOutput = outputTuple.get(mainOutputTag);
    PCollection<String> elementOutput = outputTuple.get(elementTag);

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<KV<String, Integer>> mainOutputBundle =
        bundleFactory.createRootBundle(mainOutput);
    UncommittedBundle<String> elementOutputBundle = bundleFactory.createRootBundle(elementOutput);

    when(evaluationContext.createBundle(inputBundle, mainOutput)).thenReturn(mainOutputBundle);
    when(evaluationContext.createBundle(inputBundle, elementOutput))
        .thenReturn(elementOutputBundle);

    InProcessExecutionContext executionContext =
        new InProcessExecutionContext(null, null, null, null);
    when(evaluationContext.getExecutionContext(mainOutput.getProducingTransformInternal(),
        inputBundle.getKey())).thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    TransformEvaluator<String> evaluator =
        new ParDoMultiEvaluatorFactory()
            .forApplication(
                mainOutput.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getOutputBundles(),
        Matchers.<UncommittedBundle<?>>containsInAnyOrder(mainOutputBundle, elementOutputBundle));
    assertThat(result.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
    assertThat(result.getCounters(), equalTo(counters));

    assertThat(
        mainOutputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<KV<String, Integer>>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(KV.of("foo", 3)),
            WindowedValue.timestampedValueInGlobalWindow(KV.of("bara", 4), new Instant(1000)),
            WindowedValue.valueInGlobalWindow(
                KV.of("bazam", 5), PaneInfo.ON_TIME_AND_ONLY_FIRING)));
    assertThat(
        elementOutputBundle.commit(Instant.now()).getElements(),
        Matchers.<WindowedValue<String>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("foo"),
            WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)),
            WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING)));
  }

  @Test
  public void finishBundleWithStatePutsStateInResult() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));

    TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<KV<String, Integer>>() {};
    final TupleTag<String> elementTag = new TupleTag<>();

    final StateTag<Object, WatermarkHoldState<BoundedWindow>> watermarkTag =
        StateTags.watermarkStateInternal("myId", OutputTimeFns.outputAtEndOfWindow());
    final StateTag<Object, BagState<String>> bagTag = StateTags.bag("myBag", StringUtf8Coder.of());
    final StateNamespace windowNs =
        StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);
    BoundMulti<String, KV<String, Integer>> pardo =
        ParDo.of(
                new DoFn<String, KV<String, Integer>>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.windowingInternals()
                        .stateInternals()
                        .state(StateNamespaces.global(), watermarkTag)
                        .add(new Instant(20202L + c.element().length()));
                    c.windowingInternals()
                        .stateInternals()
                        .state(
                            StateNamespaces.window(
                                GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE),
                            bagTag)
                        .add(c.element());
                  }
                })
            .withOutputTags(mainOutputTag, TupleTagList.of(elementTag));
    PCollectionTuple outputTuple = input.apply(pardo);

    CommittedBundle<String> inputBundle =
        bundleFactory.createRootBundle(input).commit(Instant.now());

    PCollection<KV<String, Integer>> mainOutput = outputTuple.get(mainOutputTag);
    PCollection<String> elementOutput = outputTuple.get(elementTag);

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<KV<String, Integer>> mainOutputBundle =
        bundleFactory.createRootBundle(mainOutput);
    UncommittedBundle<String> elementOutputBundle = bundleFactory.createRootBundle(elementOutput);

    when(evaluationContext.createBundle(inputBundle, mainOutput)).thenReturn(mainOutputBundle);
    when(evaluationContext.createBundle(inputBundle, elementOutput))
        .thenReturn(elementOutputBundle);

    InProcessExecutionContext executionContext = new InProcessExecutionContext(null,
        StructuralKey.of("myKey", StringUtf8Coder.of()),
        null,
        null);
    when(evaluationContext.getExecutionContext(mainOutput.getProducingTransformInternal(),
        inputBundle.getKey())).thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    TransformEvaluator<String> evaluator =
        new ParDoMultiEvaluatorFactory()
            .forApplication(
                mainOutput.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getOutputBundles(),
        Matchers.<UncommittedBundle<?>>containsInAnyOrder(mainOutputBundle, elementOutputBundle));
    assertThat(result.getWatermarkHold(), equalTo(new Instant(20205L)));
    assertThat(result.getState(), not(nullValue()));
    assertThat(
        result.getState().state(StateNamespaces.global(), watermarkTag).read(),
        equalTo(new Instant(20205L)));
    assertThat(
        result.getState().state(windowNs, bagTag).read(),
        containsInAnyOrder("foo", "bara", "bazam"));
  }

  @Test
  public void finishBundleWithStateAndTimersPutsTimersInResult() throws Exception {
    TestPipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of("foo", "bara", "bazam"));

    TupleTag<KV<String, Integer>> mainOutputTag = new TupleTag<KV<String, Integer>>() {};
    final TupleTag<String> elementTag = new TupleTag<>();

    final TimerData addedTimer =
        TimerData.of(
            StateNamespaces.window(
                IntervalWindow.getCoder(),
                new IntervalWindow(
                    new Instant(0).plus(Duration.standardMinutes(5)),
                    new Instant(1)
                        .plus(Duration.standardMinutes(5))
                        .plus(Duration.standardHours(1)))),
            new Instant(54541L),
            TimeDomain.EVENT_TIME);
    final TimerData deletedTimer =
        TimerData.of(
            StateNamespaces.window(
                IntervalWindow.getCoder(),
                new IntervalWindow(new Instant(0), new Instant(0).plus(Duration.standardHours(1)))),
            new Instant(3400000),
            TimeDomain.SYNCHRONIZED_PROCESSING_TIME);

    BoundMulti<String, KV<String, Integer>> pardo =
        ParDo.of(
                new DoFn<String, KV<String, Integer>>() {
                  @Override
                  public void processElement(ProcessContext c) {
                    c.windowingInternals().stateInternals();
                    c.windowingInternals()
                        .timerInternals()
                        .setTimer(
                            TimerData.of(
                                StateNamespaces.window(
                                    IntervalWindow.getCoder(),
                                    new IntervalWindow(
                                        new Instant(0).plus(Duration.standardMinutes(5)),
                                        new Instant(1)
                                            .plus(Duration.standardMinutes(5))
                                            .plus(Duration.standardHours(1)))),
                                new Instant(54541L),
                                TimeDomain.EVENT_TIME));
                    c.windowingInternals()
                        .timerInternals()
                        .deleteTimer(
                            TimerData.of(
                                StateNamespaces.window(
                                    IntervalWindow.getCoder(),
                                    new IntervalWindow(
                                        new Instant(0),
                                        new Instant(0).plus(Duration.standardHours(1)))),
                                new Instant(3400000),
                                TimeDomain.SYNCHRONIZED_PROCESSING_TIME));
                  }
                })
            .withOutputTags(mainOutputTag, TupleTagList.of(elementTag));
    PCollectionTuple outputTuple = input.apply(pardo);

    CommittedBundle<String> inputBundle =
        bundleFactory.createRootBundle(input).commit(Instant.now());

    PCollection<KV<String, Integer>> mainOutput = outputTuple.get(mainOutputTag);
    PCollection<String> elementOutput = outputTuple.get(elementTag);

    InProcessEvaluationContext evaluationContext = mock(InProcessEvaluationContext.class);
    UncommittedBundle<KV<String, Integer>> mainOutputBundle =
        bundleFactory.createRootBundle(mainOutput);
    UncommittedBundle<String> elementOutputBundle = bundleFactory.createRootBundle(elementOutput);

    when(evaluationContext.createBundle(inputBundle, mainOutput)).thenReturn(mainOutputBundle);
    when(evaluationContext.createBundle(inputBundle, elementOutput))
        .thenReturn(elementOutputBundle);

    InProcessExecutionContext executionContext = new InProcessExecutionContext(null,
        StructuralKey.of("myKey", StringUtf8Coder.of()),
        null, null);
    when(evaluationContext.getExecutionContext(mainOutput.getProducingTransformInternal(),
        inputBundle.getKey())).thenReturn(executionContext);
    CounterSet counters = new CounterSet();
    when(evaluationContext.createCounterSet()).thenReturn(counters);

    TransformEvaluator<String> evaluator =
        new ParDoMultiEvaluatorFactory()
            .forApplication(
                mainOutput.getProducingTransformInternal(), inputBundle, evaluationContext);

    evaluator.processElement(WindowedValue.valueInGlobalWindow("foo"));
    evaluator.processElement(
        WindowedValue.timestampedValueInGlobalWindow("bara", new Instant(1000)));
    evaluator.processElement(
        WindowedValue.valueInGlobalWindow("bazam", PaneInfo.ON_TIME_AND_ONLY_FIRING));

    InProcessTransformResult result = evaluator.finishBundle();
    assertThat(
        result.getTimerUpdate(),
        equalTo(
            TimerUpdate.builder(StructuralKey.of("myKey", StringUtf8Coder.of()))
                .setTimer(addedTimer)
                .setTimer(addedTimer)
                .setTimer(addedTimer)
                .deletedTimer(deletedTimer)
                .deletedTimer(deletedTimer)
                .deletedTimer(deletedTimer)
                .build()));
  }
}
