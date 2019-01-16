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
package org.apache.beam.runners.direct.portable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.direct.WatermarkManager.FiredTimers;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EvaluationContext}. */
@RunWith(JUnit4.class)
public class EvaluationContextTest {
  private EvaluationContext context;

  private PCollectionNode created;
  private PCollectionNode downstream;

  private ExecutableGraph<PTransformNode, PCollectionNode> graph;

  private PTransformNode createdProducer;
  private PTransformNode downstreamProducer;
  private PTransformNode unboundedProducer;

  @Before
  public void setup() {
    ExecutableGraphBuilder graphBuilder =
        ExecutableGraphBuilder.create()
            .addTransform("create", null, "created")
            .addTransform("downstream", "created", "downstream.out")
            .addTransform("unbounded", null, "unbounded.out");

    graph = graphBuilder.toGraph();
    created = graphBuilder.collectionNode("created");
    downstream = graphBuilder.collectionNode("downstream.out");
    createdProducer = graphBuilder.transformNode("create");
    downstreamProducer = graphBuilder.transformNode("downstream");
    unboundedProducer = graphBuilder.transformNode("unbounded");

    BundleFactory bundleFactory = ImmutableListBundleFactory.create();
    context = EvaluationContext.create(Instant::new, bundleFactory, graph, ImmutableSet.of());
  }

  @Test
  public void getExecutionContextSameStepSameKeyState() {
    StepStateAndTimers<String> fooContext =
        context.getStateAndTimers(createdProducer, StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    context.handleResult(
        ImmutableListBundleFactory.create()
            .createKeyedBundle(StructuralKey.of("foo", StringUtf8Coder.of()), created)
            .commit(Instant.now()),
        ImmutableList.of(),
        StepTransformResult.withoutHold(createdProducer)
            .withState(fooContext.stateInternals().commit())
            .build());

    StepStateAndTimers secondFooContext =
        context.getStateAndTimers(createdProducer, StructuralKey.of("foo", StringUtf8Coder.of()));
    assertThat(
        secondFooContext.stateInternals().state(StateNamespaces.global(), intBag).read(),
        contains(1));
  }

  @Test
  public void getExecutionContextDifferentKeysIndependentState() {
    StepStateAndTimers fooContext =
        context.getStateAndTimers(createdProducer, StructuralKey.of("foo", StringUtf8Coder.of()));

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    StepStateAndTimers barContext =
        context.getStateAndTimers(createdProducer, StructuralKey.of("bar", StringUtf8Coder.of()));
    assertThat(barContext, not(equalTo(fooContext)));
    assertThat(
        barContext.stateInternals().state(StateNamespaces.global(), intBag).read(),
        emptyIterable());
  }

  @Test
  public void getExecutionContextDifferentStepsIndependentState() {
    StructuralKey<?> myKey = StructuralKey.of("foo", StringUtf8Coder.of());
    StepStateAndTimers fooContext = context.getStateAndTimers(createdProducer, myKey);

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    fooContext.stateInternals().state(StateNamespaces.global(), intBag).add(1);

    StepStateAndTimers barContext = context.getStateAndTimers(downstreamProducer, myKey);
    assertThat(
        barContext.stateInternals().state(StateNamespaces.global(), intBag).read(),
        emptyIterable());
  }

  @Test
  public void handleResultStoresState() {
    StructuralKey<?> myKey = StructuralKey.of("foo".getBytes(UTF_8), ByteArrayCoder.of());
    StepStateAndTimers fooContext = context.getStateAndTimers(downstreamProducer, myKey);

    StateTag<BagState<Integer>> intBag = StateTags.bag("myBag", VarIntCoder.of());

    CopyOnAccessInMemoryStateInternals<?> state = fooContext.stateInternals();
    BagState<Integer> bag = state.state(StateNamespaces.global(), intBag);
    bag.add(1);
    bag.add(2);
    bag.add(4);

    TransformResult<?> stateResult =
        StepTransformResult.withoutHold(downstreamProducer).withState(state).build();

    context.handleResult(
        context.createKeyedBundle(myKey, created).commit(Instant.now()),
        ImmutableList.of(),
        stateResult);

    StepStateAndTimers afterResultContext = context.getStateAndTimers(downstreamProducer, myKey);

    CopyOnAccessInMemoryStateInternals<?> afterResultState = afterResultContext.stateInternals();
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
        StepTransformResult.withHold(createdProducer, new Instant(0)).build();

    context.handleResult(null, ImmutableList.of(), result);
    // Difficult to demonstrate that we took no action in a multithreaded world; poll for a bit
    // will likely be flaky if this logic is broken
    assertThat(callLatch.await(500L, TimeUnit.MILLISECONDS), is(false));

    TransformResult<?> finishedResult = StepTransformResult.withoutHold(createdProducer).build();
    context.handleResult(null, ImmutableList.of(), finishedResult);
    context.forceRefresh();
    // Obtain the value via blocking call
    assertThat(callLatch.await(1, TimeUnit.SECONDS), is(true));
  }

  @Test
  public void callAfterOutputMustHaveBeenProducedAlreadyAfterCallsImmediately() throws Exception {
    TransformResult<?> finishedResult = StepTransformResult.withoutHold(createdProducer).build();
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
        StepTransformResult.withHold(createdProducer, new Instant(0)).build();
    context.handleResult(null, ImmutableList.of(), holdResult);

    StructuralKey<?> key = StructuralKey.of("foo".length(), VarIntCoder.of());
    TimerData toFire =
        TimerData.of(StateNamespaces.global(), new Instant(100L), TimeDomain.EVENT_TIME);
    TransformResult<?> timerResult =
        StepTransformResult.withoutHold(downstreamProducer)
            .withState(CopyOnAccessInMemoryStateInternals.withUnderlying(key, null))
            .withTimerUpdate(TimerUpdate.builder(key).setTimer(toFire).build())
            .build();

    // haven't added any timers, must be empty
    assertThat(context.extractFiredTimers(), emptyIterable());
    context.handleResult(
        context.createKeyedBundle(key, created).commit(Instant.now()),
        ImmutableList.of(),
        timerResult);

    // timer hasn't fired
    assertThat(context.extractFiredTimers(), emptyIterable());

    TransformResult<?> advanceResult = StepTransformResult.withoutHold(createdProducer).build();
    // Should cause the downstream timer to fire
    context.handleResult(null, ImmutableList.of(), advanceResult);

    Collection<FiredTimers<PTransformNode>> fired = context.extractFiredTimers();
    assertThat(Iterables.getOnlyElement(fired).getKey(), Matchers.equalTo(key));

    FiredTimers<PTransformNode> firedForKey = Iterables.getOnlyElement(fired);
    // Contains exclusively the fired timer
    assertThat(firedForKey.getTimers(), contains(toFire));

    // Don't reextract timers
    assertThat(context.extractFiredTimers(), emptyIterable());
  }

  @Test
  public void createKeyedBundleKeyed() {
    StructuralKey<String> key = StructuralKey.of("foo", StringUtf8Coder.of());
    CommittedBundle<KV<String, Integer>> keyedBundle =
        context
            .<String, KV<String, Integer>>createKeyedBundle(key, downstream)
            .commit(Instant.now());
    assertThat(keyedBundle.getKey(), Matchers.equalTo(key));
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

    UncommittedBundle<Integer> rootBundle = context.createBundle(created);
    rootBundle.add(WindowedValue.valueInGlobalWindow(1));
    CommittedResult handleResult =
        context.handleResult(
            null,
            ImmutableList.of(),
            StepTransformResult.<Integer>withoutHold(createdProducer)
                .addOutput(rootBundle)
                .build());
    @SuppressWarnings("unchecked")
    CommittedBundle<Integer> committedBundle =
        (CommittedBundle<Integer>) Iterables.getOnlyElement(handleResult.getOutputs());
    context.handleResult(
        null, ImmutableList.of(), StepTransformResult.withoutHold(unboundedProducer).build());
    assertThat(context.isDone(), is(false));

    for (PTransformNode consumers : graph.getPerElementConsumers(created)) {
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
