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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables.getOnlyElement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.GroupAlsoByWindowsAggregators;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.direct.portable.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@code
 * DirectGroupAlsoByWindow} {@link PTransform}.
 */
class GroupAlsoByWindowEvaluatorFactory implements TransformEvaluatorFactory {
  private final BundleFactory bundleFactory;
  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;
  private final Components components;
  private final StepStateAndTimers.Provider stp;

  GroupAlsoByWindowEvaluatorFactory(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory,
      StepStateAndTimers.Provider stp) {
    this.bundleFactory = bundleFactory;
    this.graph = graph;
    this.components = components;
    this.stp = stp;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator(application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <K, V> TransformEvaluator<KeyedWorkItem<K, V>> createEvaluator(
      PTransformNode application, CommittedBundle<KeyedWorkItem<K, V>> inputBundle) {
    @SuppressWarnings("unchecked")
    StructuralKey<K> key = (StructuralKey<K>) inputBundle.getKey();
    return new GroupAlsoByWindowEvaluator<>(
        bundleFactory, key, application, graph, components, stp.forStepAndKey(application, key));
  }

  /**
   * A transform evaluator for the pseudo-primitive {@code DirectGroupAlsoByWindow}. The window of
   * the input {@link KeyedWorkItem} is ignored; it should be in the global window, as element
   * windows are reified in the {@link KeyedWorkItem#elementsIterable()}.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class GroupAlsoByWindowEvaluator<K, V>
      implements TransformEvaluator<KeyedWorkItem<K, V>> {
    private final BundleFactory bundleFactory;

    private final PTransformNode application;
    private final PCollectionNode outputCollection;

    private final StructuralKey<?> key;

    private final CopyOnAccessInMemoryStateInternals<K> stateInternals;
    private final DirectTimerInternals timerInternals;
    private final WindowingStrategy<?, BoundedWindow> windowingStrategy;

    private final Collection<UncommittedBundle<?>> outputBundles;

    private final SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn;
    private final Counter droppedDueToLateness;

    private GroupAlsoByWindowEvaluator(
        BundleFactory bundleFactory,
        StructuralKey<K> key,
        PTransformNode application,
        ExecutableGraph<PTransformNode, PCollectionNode> graph,
        Components components,
        StepStateAndTimers<K> stp) {
      this.bundleFactory = bundleFactory;
      this.application = application;
      this.outputCollection = getOnlyElement(graph.getProduced(application));
      this.key = key;

      this.stateInternals = stp.stateInternals();
      this.timerInternals = stp.timerInternals();

      PCollectionNode inputCollection = getOnlyElement(graph.getPerElementInputs(application));
      try {
        windowingStrategy =
            (WindowingStrategy<?, BoundedWindow>)
                RehydratedComponents.forComponents(components)
                    .getWindowingStrategy(
                        inputCollection.getPCollection().getWindowingStrategyId());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      outputBundles = new ArrayList<>();

      Coder<V> valueCoder;
      try {
        Coder<WindowedValue<KV<K, Iterable<V>>>> windowedValueCoder =
            WireCoders.instantiateRunnerWireCoder(outputCollection, components);
        checkArgument(windowedValueCoder instanceof WindowedValue.WindowedValueCoder);
        Coder<KV<K, Iterable<V>>> outputKvCoder =
            ((WindowedValueCoder<KV<K, Iterable<V>>>) windowedValueCoder).getValueCoder();
        checkArgument(outputKvCoder instanceof KvCoder);
        Coder<Iterable<V>> iterVCoder = ((KvCoder<K, Iterable<V>>) outputKvCoder).getValueCoder();
        checkArgument(iterVCoder instanceof IterableLikeCoder);
        valueCoder = ((IterableLikeCoder<V, ?>) iterVCoder).getElemCoder();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      reduceFn = SystemReduceFn.buffering(valueCoder);
      droppedDueToLateness =
          Metrics.counter(
              GroupAlsoByWindowEvaluator.class,
              GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_LATENESS_COUNTER);
    }

    @Override
    public void processElement(WindowedValue<KeyedWorkItem<K, V>> element) throws Exception {
      KeyedWorkItem<K, V> workItem = element.getValue();

      UncommittedBundle<KV<K, Iterable<V>>> bundle =
          bundleFactory.createKeyedBundle(this.key, outputCollection);
      outputBundles.add(bundle);
      RunnerApi.Trigger runnerApiTrigger =
          TriggerTranslation.toProto(windowingStrategy.getTrigger());
      ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner =
          new ReduceFnRunner<>(
              workItem.key(),
              windowingStrategy,
              ExecutableTriggerStateMachine.create(
                  TriggerStateMachines.stateMachineForTrigger(runnerApiTrigger)),
              stateInternals,
              timerInternals,
              new OutputWindowedValueToBundle<>(bundle),
              null,
              reduceFn,
              null);

      // Drop any elements within expired windows
      reduceFnRunner.processElements(
          dropExpiredWindows(workItem.key(), workItem.elementsIterable(), timerInternals));
      reduceFnRunner.onTimers(workItem.timersIterable());
      reduceFnRunner.persist();
    }

    @Override
    public TransformResult<KeyedWorkItem<K, V>> finishBundle() throws Exception {
      // State is initialized within the constructor. It can never be null.
      CopyOnAccessInMemoryStateInternals<?> state = stateInternals.commit();
      return StepTransformResult.<KeyedWorkItem<K, V>>withHold(
              application, state.getEarliestWatermarkHold())
          .withState(state)
          .addOutput(outputBundles)
          .withTimerUpdate(timerInternals.getTimerUpdate())
          .build();
    }

    /**
     * Returns an {@code Iterable<WindowedValue<InputT>>} that only contains non-late input
     * elements.
     */
    Iterable<WindowedValue<V>> dropExpiredWindows(
        final K key, Iterable<WindowedValue<V>> elements, final TimerInternals timerInternals) {
      return StreamSupport.stream(elements.spliterator(), false)
          .flatMap(wv -> StreamSupport.stream(wv.explodeWindows().spliterator(), false))
          .filter(
              input -> {
                BoundedWindow window = getOnlyElement(input.getWindows());
                boolean expired =
                    window
                        .maxTimestamp()
                        .plus(windowingStrategy.getAllowedLateness())
                        .isBefore(timerInternals.currentInputWatermarkTime());
                if (expired) {
                  // The element is too late for this window.
                  droppedDueToLateness.inc();
                  WindowTracing.debug(
                      "{}: Dropping element at {} for key: {}; "
                          + "window: {} since it is too far behind inputWatermark: {}",
                      DirectGroupAlsoByWindow.class.getSimpleName(),
                      input.getTimestamp(),
                      key,
                      window,
                      timerInternals.currentInputWatermarkTime());
                }
                // Keep the element if the window is not expired.
                return !expired;
              })
          .collect(Collectors.toList());
    }
  }

  private static class OutputWindowedValueToBundle<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private final UncommittedBundle<KV<K, Iterable<V>>> bundle;

    private OutputWindowedValueToBundle(UncommittedBundle<KV<K, Iterable<V>>> bundle) {
      this.bundle = bundle;
    }

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      bundle.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException(
          String.format("%s should not use tagged outputs", "DirectGroupAlsoByWindow"));
    }
  }
}
