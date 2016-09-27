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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.PCollectionViewWriter;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum.SumLongFn;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.TimeDomain;
import org.apache.beam.sdk.util.TimerInternals.TimerData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.BagState;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link EvaluationContext}.
 */
@RunWith(JUnit4.class)
public class EvaluationContextTest {
  private TestPipeline p;
  private EvaluationContext context;

  private PCollection<Integer> created;
  private PCollection<KV<String, Integer>> downstream;
  private PCollectionView<Iterable<Integer>> view;
  private PCollection<Long> unbounded;
  private Collection<AppliedPTransform<?, ?, ?>> rootTransforms;
  private Map<PValue, Collection<AppliedPTransform<?, ?, ?>>> valueToConsumers;

  private BundleFactory bundleFactory;

  @Before
  public void setup() {
    DirectRunner runner =
        DirectRunner.fromOptions(PipelineOptionsFactory.create());

    p = TestPipeline.create();

    created = p.apply(Create.of(1, 2, 3));
    downstream = created.apply(WithKeys.<String, Integer>of("foo"));
    view = created.apply(View.<Integer>asIterable());
    unbounded = p.apply(CountingInput.unbounded());

    ConsumerTrackingPipelineVisitor cVis = new ConsumerTrackingPipelineVisitor();
    p.traverseTopologically(cVis);
    rootTransforms = cVis.getRootTransforms();
    valueToConsumers = cVis.getValueToConsumers();

    bundleFactory = ImmutableListBundleFactory.create();

    context =
        EvaluationContext.create(
            runner.getPipelineOptions(),
            NanosOffsetClock.create(),
            ImmutableListBundleFactory.create(),
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
    assertThat(reader.get(view, second), containsInAnyOrder(2));
    // The cached value is served in the earlier reader
    reader = context.createSideInputReader(ImmutableList.<PCollectionView<?>>of(view));
    assertThat(reader.get(view, second), containsInAnyOrder(4444));
  }

  @Test
  public void getExecutionContextSameStepSameKeyState() {
    DirectExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(),
            StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    DirectStepContext stepContext = fooContext.getOrCreateStepContext("s1", "s1");
    stepContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    context.handleResult(
        ImmutableListBundleFactory.create()
            .createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), created)
            .commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(created.getProducingTransformInternal())
            .withState(stepContext.commitState())
            .build());

    DirectExecutionContext secondFooContext =
        context.getExecutionContext(created.getProducingTransformInternal(),
            StructuralKey.of("foo", StringUtf8Coder.of()));
    assertThat(
        secondFooContext
            .getOrCreateStepContext("s1", "s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        contains(1));
  }


  @Test
  public void getExecutionContextDifferentKeysIndependentState() {
    DirectExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(),
            StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext
        .getOrCreateStepContext("s1", "s1")
        .stateInternals()
        .state(StateNamespaces.global(), intBag)
        .add(1);

    DirectExecutionContext barContext =
        context.getExecutionContext(created.getProducingTransformInternal(),
            StructuralKey.of("bar", StringUtf8Coder.of()));
    assertThat(barContext, not(equalTo(fooContext)));
    assertThat(
        barContext
            .getOrCreateStepContext("s1", "s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void getExecutionContextDifferentStepsIndependentState() {
    StructuralKey<?> myKey = StructuralKey.of("foo", StringUtf8Coder.of());
    DirectExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), myKey);

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext
        .getOrCreateStepContext("s1", "s1")
        .stateInternals()
        .state(StateNamespaces.global(), intBag)
        .add(1);

    DirectExecutionContext barContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);
    assertThat(
        barContext
            .getOrCreateStepContext("s1", "s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void handleResultCommitsAggregators() {
    Class<?> fn = getClass();
    DirectExecutionContext fooContext =
        context.getExecutionContext(created.getProducingTransformInternal(), null);
    DirectExecutionContext.StepContext stepContext = fooContext.createStepContext(
        "STEP", created.getProducingTransformInternal().getTransform().getName());
    AggregatorContainer container = context.getAggregatorContainer();
    AggregatorContainer.Mutator mutator = container.createMutator();
    mutator.createAggregatorForDoFn(fn, stepContext, "foo", new SumLongFn()).addValue(4L);

    TransformResult result =
        StepTransformResult.withoutHold(created.getProducingTransformInternal())
            .withAggregatorChanges(mutator)
            .build();
    context.handleResult(null, ImmutableList.<TimerData>of(), result);
    assertThat((Long) context.getAggregatorContainer().getAggregate("STEP", "foo"), equalTo(4L));

    AggregatorContainer.Mutator mutatorAgain = container.createMutator();
    mutatorAgain.createAggregatorForDoFn(fn, stepContext, "foo", new SumLongFn()).addValue(12L);

    TransformResult secondResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withAggregatorChanges(mutatorAgain)
            .build();
    context.handleResult(
        context.createBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        secondResult);
    assertThat((Long) context.getAggregatorContainer().getAggregate("STEP", "foo"), equalTo(16L));
  }

  @Test
  public void handleResultStoresState() {
    StructuralKey<?> myKey = StructuralKey.of("foo".getBytes(), ByteArrayCoder.of());
    DirectExecutionContext fooContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);

    StateTag<Object, BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    CopyOnAccessInMemoryStateInternals<Object> state =
        fooContext.getOrCreateStepContext("s1", "s1").stateInternals();
    BagState<Integer> bag = state.state(StateNamespaces.global(), intBag);
    bag.add(1);
    bag.add(2);
    bag.add(4);

    TransformResult stateResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withState(state)
            .build();

    context.handleResult(
        context.createKeyedBundle(myKey, created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        stateResult);

    DirectExecutionContext afterResultContext =
        context.getExecutionContext(downstream.getProducingTransformInternal(), myKey);

    CopyOnAccessInMemoryStateInternals<Object> afterResultState =
        afterResultContext.getOrCreateStepContext("s1", "s1").stateInternals();
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

    TransformResult result =
        StepTransformResult.withHold(created.getProducingTransformInternal(), new Instant(0))
            .build();

    context.handleResult(null, ImmutableList.<TimerData>of(), result);
    // Difficult to demonstrate that we took no action in a multithreaded world; poll for a bit
    // will likely be flaky if this logic is broken
    assertThat(callLatch.await(500L, TimeUnit.MILLISECONDS), is(false));

    TransformResult finishedResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    context.handleResult(null, ImmutableList.<TimerData>of(), finishedResult);
    context.forceRefresh();
    // Obtain the value via blocking call
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAlreadyAfterCallsImmediately() throws Exception {
    TransformResult finishedResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    context.handleResult(null, ImmutableList.<TimerData>of(), finishedResult);

    final CountDownLatch callLatch = new CountDownLatch(1);
    context.extractFiredTimers();
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
    TransformResult holdResult =
        StepTransformResult.withHold(created.getProducingTransformInternal(), new Instant(0))
            .build();
    context.handleResult(null, ImmutableList.<TimerData>of(), holdResult);

    StructuralKey<?> key = StructuralKey.of("foo".length(), VarIntCoder.of());
    TimerData toFire =
        TimerData.of(StateNamespaces.global(), new Instant(100L), TimeDomain.EVENT_TIME);
    TransformResult timerResult =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal())
            .withState(CopyOnAccessInMemoryStateInternals.withUnderlying(key, null))
            .withTimerUpdate(TimerUpdate.builder(key).setTimer(toFire).build())
            .build();

    // haven't added any timers, must be empty
    assertThat(context.extractFiredTimers().entrySet(), emptyIterable());
    context.handleResult(
        context.createKeyedBundle(key, created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        timerResult);

    // timer hasn't fired
    assertThat(context.extractFiredTimers().entrySet(), emptyIterable());

    TransformResult advanceResult =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    // Should cause the downstream timer to fire
    context.handleResult(null, ImmutableList.<TimerData>of(), advanceResult);

    Map<AppliedPTransform<?, ?, ?>, Map<StructuralKey<?>, FiredTimers>> fired =
        context.extractFiredTimers();
    assertThat(
        fired,
        Matchers.<AppliedPTransform<?, ?, ?>>hasKey(downstream.getProducingTransformInternal()));
    Map<StructuralKey<?>, FiredTimers> downstreamFired =
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
  public void createKeyedBundleKeyed() {
    StructuralKey<String> key = StructuralKey.of("foo", StringUtf8Coder.of());
    CommittedBundle<KV<String, Integer>> keyedBundle =
        context.createKeyedBundle(
            key,
            downstream).commit(Instant.now());
    assertThat(keyedBundle.getKey(),
        Matchers.<StructuralKey<?>>equalTo(key));
  }

  @Test
  public void isDoneWithUnboundedPCollectionAndShutdown() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(true);
    assertThat(context.isDone(unbounded.getProducingTransformInternal()), is(false));

    context.handleResult(
        null,
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(unbounded.getProducingTransformInternal()).build());
    context.extractFiredTimers();
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
    context.extractFiredTimers();
    assertThat(context.isDone(created.getProducingTransformInternal()), is(true));
  }

  @Test
  public void isDoneWithPartiallyDone() {
    context.getPipelineOptions().setShutdownUnboundedProducersWithMaxWatermark(true);
    assertThat(context.isDone(), is(false));

    UncommittedBundle<Integer> rootBundle = context.createBundle(created);
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
    context.extractFiredTimers();
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
        context.createBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build());
    context.extractFiredTimers();
    assertThat(context.isDone(), is(false));

    context.handleResult(
        context.createBundle(created).commit(Instant.now()),
        ImmutableList.<TimerData>of(),
        StepTransformResult.withoutHold(view.getProducingTransformInternal()).build());
    context.extractFiredTimers();
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
