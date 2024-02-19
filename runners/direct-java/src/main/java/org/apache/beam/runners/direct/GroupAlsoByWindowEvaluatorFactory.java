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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.GroupAlsoByWindowsAggregators;
import org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.UnsupportedSideInputReader;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowTracing;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link
 * DirectGroupAlsoByWindow} {@link PTransform}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
  "nullness"
}) // TODO(https://github.com/apache/beam/issues/20497)
class GroupAlsoByWindowEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;
  private final PipelineOptions options;

  GroupAlsoByWindowEvaluatorFactory(EvaluationContext evaluationContext, PipelineOptions options) {
    this.evaluationContext = evaluationContext;
    this.options = options;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator =
        createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <K, V> TransformEvaluator<KeyedWorkItem<K, V>> createEvaluator(
      AppliedPTransform<
              PCollection<KeyedWorkItem<K, V>>,
              PCollection<KV<K, Iterable<V>>>,
              DirectGroupAlsoByWindow<K, V>>
          application,
      CommittedBundle<KeyedWorkItem<K, V>> inputBundle) {
    return new GroupAlsoByWindowEvaluator<>(evaluationContext, options, inputBundle, application);
  }

  /**
   * A transform evaluator for the pseudo-primitive {@link DirectGroupAlsoByWindow}. The window of
   * the input {@link KeyedWorkItem} is ignored; it should be in the global window, as element
   * windows are reified in the {@link KeyedWorkItem#elementsIterable()}.
   *
   * @see GroupByKeyViaGroupByKeyOnly
   */
  private static class GroupAlsoByWindowEvaluator<K, V>
      implements TransformEvaluator<KeyedWorkItem<K, V>> {
    private final EvaluationContext evaluationContext;
    private final PipelineOptions options;
    private final AppliedPTransform<
            PCollection<KeyedWorkItem<K, V>>,
            PCollection<KV<K, Iterable<V>>>,
            DirectGroupAlsoByWindow<K, V>>
        application;

    private final DirectStepContext stepContext;
    private @SuppressWarnings("unchecked") final WindowingStrategy<?, BoundedWindow>
        windowingStrategy;

    private final StructuralKey<?> structuralKey;
    private final Collection<UncommittedBundle<?>> outputBundles;
    private final ImmutableList.Builder<WindowedValue<KeyedWorkItem<K, V>>> unprocessedElements;

    private final SystemReduceFn<K, V, Iterable<V>, Iterable<V>, BoundedWindow> reduceFn;
    private final Counter droppedDueToLateness;

    public GroupAlsoByWindowEvaluator(
        final EvaluationContext evaluationContext,
        PipelineOptions options,
        CommittedBundle<KeyedWorkItem<K, V>> inputBundle,
        final AppliedPTransform<
                PCollection<KeyedWorkItem<K, V>>,
                PCollection<KV<K, Iterable<V>>>,
                DirectGroupAlsoByWindow<K, V>>
            application) {
      this.evaluationContext = evaluationContext;
      this.options = options;
      this.application = application;

      structuralKey = inputBundle.getKey();
      stepContext =
          evaluationContext
              .getExecutionContext(application, inputBundle.getKey())
              .getStepContext(evaluationContext.getStepName(application));
      windowingStrategy =
          (WindowingStrategy<?, BoundedWindow>)
              application.getTransform().getInputWindowingStrategy();

      outputBundles = new ArrayList<>();
      unprocessedElements = ImmutableList.builder();

      Coder<V> valueCoder =
          application.getTransform().getValueCoder(inputBundle.getPCollection().getCoder());
      reduceFn = SystemReduceFn.buffering(valueCoder);
      droppedDueToLateness =
          Metrics.counter(
              GroupAlsoByWindowEvaluator.class,
              GroupAlsoByWindowsAggregators.DROPPED_DUE_TO_LATENESS_COUNTER);
    }

    @Override
    public void processElement(WindowedValue<KeyedWorkItem<K, V>> element) throws Exception {
      KeyedWorkItem<K, V> workItem = element.getValue();
      K key = workItem.key();

      UncommittedBundle<KV<K, Iterable<V>>> bundle =
          evaluationContext.createKeyedBundle(
              structuralKey,
              (PCollection<KV<K, Iterable<V>>>)
                  Iterables.getOnlyElement(application.getOutputs().values()));
      outputBundles.add(bundle);
      CopyOnAccessInMemoryStateInternals stateInternals = stepContext.stateInternals();
      DirectTimerInternals timerInternals = stepContext.timerInternals();
      RunnerApi.Trigger runnerApiTrigger =
          TriggerTranslation.toProto(windowingStrategy.getTrigger());
      ReduceFnRunner<K, V, Iterable<V>, BoundedWindow> reduceFnRunner =
          new ReduceFnRunner<>(
              key,
              windowingStrategy,
              ExecutableTriggerStateMachine.create(
                  TriggerStateMachines.stateMachineForTrigger(runnerApiTrigger)),
              stateInternals,
              timerInternals,
              new OutputWindowedValueToBundle<>(bundle),
              new UnsupportedSideInputReader(DirectGroupAlsoByWindow.class.getSimpleName()),
              reduceFn,
              options);

      // Drop any elements within expired windows
      reduceFnRunner.processElements(
          dropExpiredWindows(key, workItem.elementsIterable(), timerInternals));
      reduceFnRunner.onTimers(workItem.timersIterable());
      reduceFnRunner.persist();
    }

    @Override
    public TransformResult<KeyedWorkItem<K, V>> finishBundle() throws Exception {
      // State is initialized within the constructor. It can never be null.
      CopyOnAccessInMemoryStateInternals state = stepContext.commitState();
      return StepTransformResult.<KeyedWorkItem<K, V>>withHold(
              application, state.getEarliestWatermarkHold())
          .withState(state)
          .addOutput(outputBundles)
          .withTimerUpdate(stepContext.getTimerUpdate())
          .addUnprocessedElements(unprocessedElements.build())
          .withBundleFinalizations(stepContext.getAndClearFinalizations())
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
                BoundedWindow window = Iterables.getOnlyElement(input.getWindows());
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
          String.format(
              "%s should not use tagged outputs", DirectGroupAlsoByWindow.class.getSimpleName()));
    }
  }
}
