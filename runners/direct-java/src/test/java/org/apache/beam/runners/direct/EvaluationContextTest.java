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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.testing.PCollectionViewTesting.materializeValuesFor;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EvaluationContext}. */
@RunWith(JUnit4.class)
public class EvaluationContextTest implements Serializable {
  private transient EvaluationContext context;

  private transient PCollection<byte[]> impulse;
  private transient PCollection<KV<String, byte[]>> downstream;
  private transient PCollectionView<Iterable<byte[]>> view;
  private transient PCollection<byte[]> unbounded;

  private transient DirectGraph graph;

  private transient AppliedPTransform<?, ?, ?> impulseProducer;
  private transient AppliedPTransform<?, ?, ?> downstreamProducer;
  private transient AppliedPTransform<?, ?, ?> unboundedProducer;

  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    DirectRunner runner = DirectRunner.fromOptions(PipelineOptionsFactory.create());

    impulse = p.apply(Impulse.create());
    downstream = impulse.apply(WithKeys.of("foo"));
    view = impulse.apply(View.asIterable());
    unbounded = p.apply(Impulse.create()).setIsBoundedInternal(IsBounded.UNBOUNDED);

    // Views are not materialized unless they are read
    impulse.apply(
        ParDo.of(
                new DoFn<byte[], Void>() {
                  @ProcessElement
                  public void process() {}
                })
            .withSideInputs(view));

    runner.performRewrites(p);

    KeyedPValueTrackingVisitor keyedPValueTrackingVisitor = KeyedPValueTrackingVisitor.create();
    p.traverseTopologically(keyedPValueTrackingVisitor);

    BundleFactory bundleFactory = ImmutableListBundleFactory.create();
    DirectGraphs.performDirectOverrides(p);
    graph = DirectGraphs.getGraph(p);
    context =
        EvaluationContext.create(
            NanosOffsetClock.create(),
            bundleFactory,
            graph,
            keyedPValueTrackingVisitor.getKeyedPValues(),
            Executors.newSingleThreadExecutor());

    impulseProducer = graph.getProducer(impulse);
    downstreamProducer = graph.getProducer(downstream);
    unboundedProducer = graph.getProducer(unbounded);
  }

  @Test
  public void writeToViewWriterThenReadReads() {
    PCollectionViewWriter<?, Iterable<byte[]>> viewWriter =
        context.createPCollectionViewWriter(
            PCollection.createPrimitiveOutputInternal(
                p,
                WindowingStrategy.globalDefault(),
                IsBounded.BOUNDED,
                IterableCoder.of(KvCoder.of(VoidCoder.of(), VarIntCoder.of()))),
            view);
    BoundedWindow window = new TestBoundedWindow(new Instant(1024L));
    BoundedWindow second = new TestBoundedWindow(new Instant(899999L));
    ImmutableList.Builder<WindowedValue<?>> valuesBuilder = ImmutableList.builder();
    for (Object materializedValue :
        materializeValuesFor(view.getPipeline().getOptions(), View.asIterable(), 1)) {
      valuesBuilder.add(
          WindowedValue.of(
              materializedValue, new Instant(1222), window, PaneInfo.ON_TIME_AND_ONLY_FIRING));
    }
    for (Object materializedValue :
        materializeValuesFor(view.getPipeline().getOptions(), View.asIterable(), 2)) {
      valuesBuilder.add(
          WindowedValue.of(
              materializedValue,
              new Instant(8766L),
              second,
              PaneInfo.createPane(true, false, Timing.ON_TIME, 0, 0)));
    }
    viewWriter.add((Iterable) valuesBuilder.build());

    SideInputReader reader = context.createSideInputReader(ImmutableList.of(view));
    assertThat(reader.get(view, window), containsInAnyOrder(1));
    assertThat(reader.get(view, second), containsInAnyOrder(2));

    ImmutableList.Builder<WindowedValue<?>> overwrittenValuesBuilder = ImmutableList.builder();
    for (Object materializedValue :
        materializeValuesFor(view.getPipeline().getOptions(), View.asIterable(), 4444)) {
      overwrittenValuesBuilder.add(
          WindowedValue.of(
              materializedValue,
              new Instant(8677L),
              second,
              PaneInfo.createPane(false, true, Timing.LATE, 1, 1)));
    }
    viewWriter.add((Iterable) overwrittenValuesBuilder.build());
    assertThat(reader.get(view, second), containsInAnyOrder(2));
    // The cached value is served in the earlier reader
    reader = context.createSideInputReader(ImmutableList.of(view));
    assertThat(reader.get(view, second), containsInAnyOrder(4444));
  }

  @Test
  public void getExecutionContextSameStepSameKeyState() {
    DirectExecutionContext fooContext =
        context.getExecutionContext(impulseProducer, StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    DirectStepContext stepContext = fooContext.getStepContext("s1");
    stepContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    context.handleResult(
        ImmutableListBundleFactory.create()
            .createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), impulse)
            .commit(Instant.now()),
        ImmutableList.of(),
        StepTransformResult.withoutHold(impulseProducer)
            .withState(stepContext.commitState())
            .build());

    DirectExecutionContext secondFooContext =
        context.getExecutionContext(impulseProducer, StructuralKey.of("foo", StringUtf8Coder.of()));
    assertThat(
        secondFooContext
            .getStepContext("s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        contains(1));
  }

  @Test
  public void getExecutionContextDifferentKeysIndependentState() {
    DirectExecutionContext fooContext =
        context.getExecutionContext(impulseProducer, StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext.getStepContext("s1").stateInternals().state(StateNamespaces.global(), intBag).add(1);

    DirectExecutionContext barContext =
        context.getExecutionContext(impulseProducer, StructuralKey.of("bar", StringUtf8Coder.of()));
    assertThat(barContext, not(equalTo(fooContext)));
    assertThat(
        barContext
            .getStepContext("s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void getExecutionContextDifferentStepsIndependentState() {
    StructuralKey<?> myKey = StructuralKey.of("foo", StringUtf8Coder.of());
    DirectExecutionContext fooContext = context.getExecutionContext(impulseProducer, myKey);

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext.getStepContext("s1").stateInternals().state(StateNamespaces.global(), intBag).add(1);

    DirectExecutionContext barContext = context.getExecutionContext(downstreamProducer, myKey);
    assertThat(
        barContext
            .getStepContext("s1")
            .stateInternals()
            .state(StateNamespaces.global(), intBag)
            .read(),
        emptyIterable());
  }

  @Test
  public void handleResultStoresState() {
    StructuralKey<?> myKey = StructuralKey.of("foo".getBytes(UTF_8), ByteArrayCoder.of());
    DirectExecutionContext fooContext = context.getExecutionContext(downstreamProducer, myKey);

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    CopyOnAccessInMemoryStateInternals<?> state = fooContext.getStepContext("s1").stateInternals();
    BagState<Integer> bag = state.state(StateNamespaces.global(), intBag);
    bag.add(1);
    bag.add(2);
    bag.add(4);

    TransformResult<?> stateResult =
        StepTransformResult.withoutHold(downstreamProducer).withState(state).build();

    context.handleResult(
        context.createKeyedBundle(myKey, impulse).commit(Instant.now()),
        ImmutableList.of(),
        stateResult);

    DirectExecutionContext afterResultContext =
        context.getExecutionContext(downstreamProducer, myKey);

    CopyOnAccessInMemoryStateInternals<?> afterResultState =
        afterResultContext.getStepContext("s1").stateInternals();
    assertThat(afterResultState.state(StateNamespaces.global(), intBag).read(), contains(1, 2, 4));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAfterEndOfWatermarkCallsback() throws Exception {
    final CountDownLatch callLatch = new CountDownLatch(1);
    Runnable callback = callLatch::countDown;

    // Should call back after the end of the global window
    context.scheduleAfterOutputWouldBeProduced(
        downstream, GlobalWindow.INSTANCE, WindowingStrategy.globalDefault(), callback);

    TransformResult<?> result =
        StepTransformResult.withHold(impulseProducer, new Instant(0)).build();

    context.handleResult(null, ImmutableList.of(), result);
    // Difficult to demonstrate that we took no action in a multithreaded world; poll for a bit
    // will likely be flaky if this logic is broken
    assertThat(callLatch.await(500L, TimeUnit.MILLISECONDS), is(false));

    TransformResult<?> finishedResult = StepTransformResult.withoutHold(impulseProducer).build();
    context.handleResult(null, ImmutableList.of(), finishedResult);
    context.forceRefresh();
    // Obtain the value via blocking call
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAlreadyAfterCallsImmediately() throws Exception {
    TransformResult<?> finishedResult = StepTransformResult.withoutHold(impulseProducer).build();
    context.handleResult(null, ImmutableList.of(), finishedResult);

    final CountDownLatch callLatch = new CountDownLatch(1);
    context.extractFiredTimers();
    Runnable callback = callLatch::countDown;
    context.scheduleAfterOutputWouldBeProduced(
        downstream, GlobalWindow.INSTANCE, WindowingStrategy.globalDefault(), callback);
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void extractFiredTimersExtractsTimers() {
    TransformResult<?> holdResult =
        StepTransformResult.withHold(impulseProducer, new Instant(0)).build();
    context.handleResult(null, ImmutableList.of(), holdResult);

    StructuralKey<?> key = StructuralKey.of("foo".length(), VarIntCoder.of());
    TimerData toFire =
        TimerData.of(
            StateNamespaces.global(), new Instant(100L), new Instant(100L), TimeDomain.EVENT_TIME);
    TransformResult<?> timerResult =
        StepTransformResult.withoutHold(downstreamProducer)
            .withState(CopyOnAccessInMemoryStateInternals.withUnderlying(key, null))
            .withTimerUpdate(TimerUpdate.builder(key).setTimer(toFire).build())
            .build();

    // haven't added any timers, must be empty
    assertThat(context.extractFiredTimers(), emptyIterable());
    context.handleResult(
        context.createKeyedBundle(key, impulse).commit(Instant.now()),
        ImmutableList.of(),
        timerResult);

    // timer hasn't fired
    assertThat(context.extractFiredTimers(), emptyIterable());

    TransformResult<?> advanceResult = StepTransformResult.withoutHold(impulseProducer).build();
    // Should cause the downstream timer to fire
    context.handleResult(null, ImmutableList.of(), advanceResult);

    Collection<FiredTimers<AppliedPTransform<?, ?, ?>>> fired = context.extractFiredTimers();
    assertThat(Iterables.getOnlyElement(fired).getKey(), equalTo(key));

    FiredTimers<AppliedPTransform<?, ?, ?>> firedForKey = Iterables.getOnlyElement(fired);
    // Contains exclusively the fired timer
    assertThat(firedForKey.getTimers(), contains(toFire));

    // Don't reextract timers
    assertThat(context.extractFiredTimers(), emptyIterable());
  }

  @Test
  public void createKeyedBundleKeyed() {
    StructuralKey<String> key = StructuralKey.of("foo", StringUtf8Coder.of());
    CommittedBundle<KV<String, byte[]>> keyedBundle =
        context.createKeyedBundle(key, downstream).commit(Instant.now());
    assertThat(keyedBundle.getKey(), equalTo(key));
  }

  @Test
  public void isDoneWithUnboundedPCollection() {
    assertThat(context.isDone(unboundedProducer), is(false));

    context.handleResult(
        null, ImmutableList.of(), StepTransformResult.withoutHold(unboundedProducer).build());
    context.extractFiredTimers();
    assertThat(context.isDone(unboundedProducer), is(true));
  }

  @Test
  public void isDoneWithPartiallyDone() {
    assertThat(context.isDone(), is(false));

    // Impulse produces one element
    UncommittedBundle<byte[]> rootBundle = context.createBundle(impulse);
    rootBundle.add(WindowedValue.valueInGlobalWindow(new byte[0]));
    CommittedResult handleResult =
        context.handleResult(
            null,
            ImmutableList.of(),
            StepTransformResult.<Integer>withoutHold(impulseProducer)
                .addOutput(rootBundle)
                .build());

    // Unbounded PCollection commits a zero element bundle
    CommittedBundle<Integer> committedBundle =
        (CommittedBundle<Integer>) Iterables.getOnlyElement(handleResult.getOutputs());
    context.handleResult(
        null, ImmutableList.of(), StepTransformResult.withoutHold(unboundedProducer).build());
    assertThat(context.isDone(), is(false));

    for (AppliedPTransform<?, ?, ?> consumers : graph.getPerElementConsumers(impulse)) {
      context.handleResult(
          committedBundle, ImmutableList.of(), StepTransformResult.withoutHold(consumers).build());
    }
    context.extractFiredTimers();
    assertThat(context.isDone(), is(true));
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
