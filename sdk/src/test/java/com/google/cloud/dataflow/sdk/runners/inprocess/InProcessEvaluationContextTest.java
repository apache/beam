/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.io.CountingInput;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.FiredTimers;
import com.google.cloud.dataflow.sdk.runners.inprocess.InMemoryWatermarkManager.TimerUpdate;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessExecutionContext.InProcessStepContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.PCollectionViewWriter;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.WithKeys;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo.Timing;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link InProcessEvaluationContext}.
 */
@RunWith(JUnit4.class)
public class InProcessEvaluationContextTest {
  private TestPipeline p;
  private InProcessEvaluationContext context;

  private PCollection<Integer> created;
  private PCollection<KV<String, Integer>> downstream;
  private PCollectionView<Iterable<Integer>> view;
  private PCollection<Long> unbounded;
  private Collection<AppliedPTransform<?, ?, ?>> rootTransforms;
  private Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers;

  private BundleFactory bundleFactory;

  @Before
  public void setup() {
    InProcessPipelineRunner runner =
        InProcessPipelineRunner.fromOptions(PipelineOptionsFactory.create());

    p = TestPipeline.create();

    created = p.apply(Create.of(1, 2, 3));
    downstream = created.apply(WithKeys.<String, Integer>of("foo"));
    view = created.apply(View.<Integer>asIterable());
    unbounded = p.apply(CountingInput.unbounded());

    ConsumerTrackingPipelineVisitor cVis = new ConsumerTrackingPipelineVisitor();
    p.traverseTopologically(cVis);
    rootTransforms = cVis.getRootTransforms();
    valueToConsumers = cVis.getValueToConsumers();

    bundleFactory = InProcessBundleFactory.create();

    context =
        InProcessEvaluationContext.create(
            runner.getPipelineOptions(),
            InProcessBundleFactory.create(),
            rootTransforms,
            valueToConsumers,
            cVis.getStepNames(),
            cVis.getViews());
  }

  @Test
  public void writeToViewWriterThenReadReads() {
    PCollectionViewWriter<Integer, Iterable<Integer>> viewWriter =
        context.createPCollectionViewWriter(
            PCollection.<Iterable<Integer>>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED),
            view);
    BoundedWindow window = new TestBoundedWindow(new Instant(1024L));
    BoundedWindow second = new TestBoundedWindow(new Instant(899999L));
    WindowedValue<Integer> firstValue =
        WindowedValue.of(1, new Instant(1222), window, PaneInfo.ON_TIME_AND_ONLY_FIRING);
    WindowedValue<Integer> secondValue =
        WindowedValue.of(
            2, new Instant(8766L), second, PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0));
    Iterable<WindowedValue<Integer>> values = ImmutableList.of(firstValue, secondValue);
    viewWriter.add(values);

    SideInputReader reader =
        context.createSideInputReader(ImmutableList.<PCollectionView<?>>of(view));
    assertThat(reader.get(view, window), containsInAnyOrder(1));
    assertThat(reader.get(view, second), containsInAnyOrder(2));

    WindowedValue<Integer> overrittenSecondValue =
        WindowedValue.of(
            4444, new Instant(8677L), second, PaneInfo.createPane(false, true, Timing.LATE, 1, 1));
    viewWriter.add(Collections.singleton(overrittenSecondValue));
    assertThat(reader.get(view, second), containsInAnyOrder(4444));
  }

  @Test
  public void getExecutionContextSameStepSameKeyState() {
    InProcessExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), "foo");

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    InProcessStepContext stepContext = fooContext.getOrCreateStepContext("s1", "s1", null);
    stepContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    context.handleResult(
        InProcessBundleFactory.create()
            .createKeyedBundle(null, "foo", created)
            .commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(created.getProducingTransformInternal())
            .withState(stepContext.commitState())
            .build());

    InProcessExecutionContext secondFooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), "foo");
    assertThat(
        secondFooContext
            .getOrCreateStepContext("s1", "s1", null)
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        contains(1));
  }


  @Test
  public void getExecutionContextDifferentKeysIndependentState() {
    InProcessExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), "foo");

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext
        .getOrCreateStepContext("s1", "s1", null)
        .stateInternals()
        .state(StateNamespaces.global(), intBag)
        .add(1);

    InProcessExecutionContext barContext =
        context.getExecutionContext(created.getProducingTransformInternal(), "bar");
    assertThat(barContext, not(equalTo(fooContext)));
    assertThat(
        barContext
            .getOrCreateStepContext("s1", "s1", null)
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void getExecutionContextDifferentStepsIndependentState() {
    String myKey = "foo";
    InProcessExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), myKey);

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext
        .getOrCreateStepContext("s1", "s1", null)
        .stateInternals()
        .state(StateNamespaces.global(), intBag)
        .add(1);

    InProcessExecutionContext barContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);
    assertThat(
        barContext
            .getOrCreateStepContext("s1", "s1", null)
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void handleResultMergesCounters() {
    CounterSet counters = context.createCounterSet();
    Counter<Long> myCounter = Counter.longs("foo", AggregationKind.SUM);
    counters.addCounter(myCounter);

    myCounter.addValue(4L);
    InProcessTransformResult result =
        StepTransformResult.withoutHold(created.getProducingTransformInternal())
            .withCounters(counters)
            .build();
    context.handleResult(null, ImmutableList.<TimerData>of(), result);
    assertThat((Long) context.getCounters().getExistingCounter("foo").getAggregate(), equalTo(4L));

    CounterSet againCounters = context.createCounterSet();
    Counter<Long> myLongCounterAgain = Counter.longs("foo", AggregationKind.SUM);
    againCounters.add(myLongCounterAgain);
    myLongCounterAgain.addValue(8L);

    InProcessTransformResult secondResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withCounters(againCounters)
            .build();
    context.handleResult(
        context.createRootBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        secondResult);
    assertThat((Long) context.getCounters().getExistingCounter("foo").getAggregate(), equalTo(12L));
  }

  @Test
  public void handleResultStoresState() {
    String myKey = "foo";
    InProcessExecutionContext fooContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    CopyOnAccessInMemoryStateInternals<Object> state =
        fooContext.getOrCreateStepContext("s1", "s1", null).stateInternals();
    BagState<Integer> bag = state.state(StateNamespaces.global(), intBag);
    bag.add(1);
    bag.add(2);
    bag.add(4);

    InProcessTransformResult stateResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withState(state)
            .build();

    context.handleResult(
        context.createKeyedBundle(null, myKey, created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        stateResult);

    InProcessExecutionContext afterResultContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);

    CopyOnAccessInMemoryStateInternals<Object> afterResultState =
        afterResultContext.getOrCreateStepContext("s1", "s1", null).stateInternals();
    assertThat(afterResultState.state(StateNamespaces.global(), intBag).read(), contains(1, 2, 4));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAfterEndOfWatermarkCallsback() throws Exception {
    final CountDownLatch callLatch = new CountDownLatch(1);
    Runnable callback =
        new Runnable() {
          @Override
          public void run() {
            callLatch.countDown();
          }
        };

    // Should call back after the end of the global window
    context.scheduleAfterOutputWouldBeProduced(
        downstream, GlobalWindow.INSTANCE, WindowingStrategy.globalDefault(), callback);

    InProcessTransformResult result =
        StepTransformResult.withHold(created.getProducingTransformInternal(), new Instant(0))
            .build();

    context.handleResult(null, ImmutableList.<TimerData>of(), result);

    // Difficult to demonstrate that we took no action in a multithreaded world; poll for a bit
    // will likely be flaky if this logic is broken
    assertThat(callLatch.await(500L, TimeUnit.MILLISECONDS), is(false));

    InProcessTransformResult finishedResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    context.handleResult(null, ImmutableList.<TimerData>of(), finishedResult);
    // Obtain the value via blocking call
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAlreadyAfterCallsImmediately() throws Exception {
    InProcessTransformResult finishedResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    context.handleResult(null, ImmutableList.<TimerData>of(), finishedResult);

    final CountDownLatch callLatch = new CountDownLatch(1);
    Runnable callback =
        new Runnable() {
          @Override
          public void run() {
            callLatch.countDown();
          }
        };
    context.scheduleAfterOutputWouldBeProduced(
        downstream, GlobalWindow.INSTANCE, WindowingStrategy.globalDefault(), callback);
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void extractFiredTimersExtractsTimers() {
    InProcessTransformResult holdResult =
        StepTransformResult.withHold(created.getProducingTransformInternal(), new Instant(0))
            .build();
    context.handleResult(null, ImmutableList.<TimerData>of(), holdResult);

    String key = "foo";
    TimerData toFire =
        TimerData.of(StateNamespaces.global(), new Instant(100L), TimeDomain.EVENT_TIME);
    InProcessTransformResult timerResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withState(CopyOnAccessInMemoryStateInternals.withUnderlying(key, null))
            .withTimerUpdate(TimerUpdate.builder(key).setTimer(toFire).build())
            .build();

    // haven't added any timers, must be empty
    assertThat(context.extractFiredTimers().entrySet(), emptyIterable());
    context.handleResult(
        context.createKeyedBundle(null, key, created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        timerResult);

    // timer hasn't fired
    assertThat(context.extractFiredTimers().entrySet(), emptyIterable());

    InProcessTransformResult advanceResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    // Should cause the downstream timer to fire
    context.handleResult(null, ImmutableList.<TimerData>of(), advanceResult);

    Map<AppliedPTransform<?, ?, ?>, Map<Object, FiredTimers>> fired = context.extractFiredTimers();
    assertThat(
        fired,
        Matchers.<AppliedPTransform<?, ?, ?>>hasKey(downstream.getProducingTransformInternal()));
    Map<Object, FiredTimers> downstreamFired =
        fired.get(downstream.getProducingTransformInternal());
    assertThat(downstreamFired, Matchers.<Object>hasKey(key));

    FiredTimers firedForKey = downstreamFired.get(key);
    assertThat(firedForKey.getTimers(TimeDomain.PROCESSING_TIME), emptyIterable());
    assertThat(firedForKey.getTimers(TimeDomain.SYNCHRONIZED_PROCESSING_TIME), emptyIterable());
    assertThat(firedForKey.getTimers(TimeDomain.EVENT_TIME), contains(toFire));

    // Don't reextract timers
    assertThat(context.extractFiredTimers().entrySet(), emptyIterable());
  }

  @Test
  public void createBundleKeyedResultPropagatesKey() {
    CommittedBundle<KV<String, Integer>> newBundle =
        context
            .createBundle(
                bundleFactory.createKeyedBundle(null, "foo", created).commit(Instant.now()),
                downstream)
            .commit(Instant.now());
    assertThat(newBundle.getKey(), Matchers.<Object>equalTo("foo"));
  }

  @Test
  public void createKeyedBundleKeyed() {
    CommittedBundle<KV<String, Integer>> keyedBundle =
        context
            .createKeyedBundle(
                bundleFactory.createRootBundle(created).commit(Instant.now()), "foo", downstream)
            .commit(Instant.now());
    assertThat(keyedBundle.getKey(), Matchers.<Object>equalTo("foo"));
  }

  @Test
  public void isDoneWithUnboundedPCollectionAndShutdown() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(true);
    assertThat(context.isDone(unbounded.getProducingTransformInternal()), is(false));

    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(unbounded.getProducingTransformInternal()).build());
    assertThat(context.isDone(unbounded.getProducingTransformInternal()), is(true));
  }

  @Test
  public void isDoneWithUnboundedPCollectionAndNotShutdown() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(false);
    assertThat(context.isDone(unbounded.getProducingTransformInternal()), is(false));

    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(unbounded.getProducingTransformInternal()).build());
    assertThat(context.isDone(unbounded.getProducingTransformInternal()), is(false));
  }

  @Test
  public void isDoneWithOnlyBoundedPCollections() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(false);
    assertThat(context.isDone(created.getProducingTransformInternal()), is(false));

    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build());
    assertThat(context.isDone(created.getProducingTransformInternal()), is(true));
  }

  @Test
  public void isDoneWithPartiallyDone() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(true);
    assertThat(context.isDone(), is(false));

    UncommittedBundle<Integer> rootBundle = context.createRootBundle(created);
    rootBundle.add(WindowedValue.valueInGlobalWindow(1));
    CommittedResult handleResult =
        context.handleResult(
            null,
            ImmutableList.<TimerData>of(),
            StepTransformResult.withoutHold(created.getProducingTransformInternal())
                .addOutput(rootBundle)
                .build());
    @SuppressWarnings("unchecked")
    CommittedBundle<Integer> committedBundle =
        (CommittedBundle<Integer>) Iterables.getOnlyElement(handleResult.getOutputs());
    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(unbounded.getProducingTransformInternal()).build());
    assertThat(context.isDone(), is(false));

    for (AppliedPTransform<?, ?, ?> consumers : valueToConsumers.get(created)) {
      context.handleResult(
          committedBundle,
          ImmutableList.<TimerData>of(),
          StepTransformResult.withoutHold(consumers).build());
    }
    assertThat(context.isDone(), is(true));
  }

  @Test
  public void isDoneWithUnboundedAndNotShutdown() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(false);
    assertThat(context.isDone(), is(false));

    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build());
    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(unbounded.getProducingTransformInternal()).build());
    context.handleResult(
        context.createRootBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build());
    assertThat(context.isDone(), is(false));

    context.handleResult(
        context.createRootBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(view.getProducingTransformInternal()).build());
    assertThat(context.isDone(), is(false));
  }

  private static class TestBoundedWindow extends BoundedWindow {
    private final Instant ts;

    public TestBoundedWindow(Instant ts) {
      this.ts = ts;
    }

    @Override
    public Instant maxTimestamp() {
      return ts;
    }
  }
}
