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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory.StatefulParDo;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

/** A {@link TransformEvaluatorFactory} for stateful {@link ParDo}. */
final class StatefulParDoEvaluatorFactory<K, InputT, OutputT> implements TransformEvaluatorFactory {

  private final ParDoEvaluatorFactory<KV<K, InputT>, OutputT> delegateFactory;

  private final EvaluationContext evaluationContext;

  StatefulParDoEvaluatorFactory(EvaluationContext evaluationContext, PipelineOptions options) {
    this.delegateFactory =
        new ParDoEvaluatorFactory<>(
            evaluationContext,
            ParDoEvaluator.<KV<K, InputT>, OutputT>defaultRunnerFactory(),
            new CacheLoader<AppliedPTransform<?, ?, ?>, DoFnLifecycleManager>() {
              @Override
              public DoFnLifecycleManager load(AppliedPTransform<?, ?, ?> appliedStatefulParDo)
                  throws Exception {
                // StatefulParDo is overridden after the portable pipeline is received, so we
                // do not go through the portability translation layers
                StatefulParDo<?, ?, ?> statefulParDo =
                    (StatefulParDo<?, ?, ?>) appliedStatefulParDo.getTransform();
                return DoFnLifecycleManager.of(statefulParDo.getDoFn());
              }
            },
            options);
    this.evaluationContext = evaluationContext;
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        (TransformEvaluator<T>)
            createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    delegateFactory.cleanup();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TransformEvaluator<KeyedWorkItem<K, KV<K, InputT>>> createEvaluator(
      AppliedPTransform<
              PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>,
              PCollectionTuple,
              StatefulParDo<K, InputT, OutputT>>
          application,
      CommittedBundle<KeyedWorkItem<K, KV<K, InputT>>> inputBundle)
      throws Exception {

    DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator =
        delegateFactory.createEvaluator(
            (AppliedPTransform) application,
            (PCollection) inputBundle.getPCollection(),
            inputBundle.getKey(),
            application.getTransform().getSideInputs(),
            application.getTransform().getMainOutputTag(),
            application.getTransform().getAdditionalOutputTags().getAll(),
            application.getTransform().getSchemaInformation(),
            application.getTransform().getSideInputMapping());

    DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getStepContext(evaluationContext.getStepName(application));

    stepContext.stateInternals().commit();
    return new StatefulParDoEvaluator<>(delegateEvaluator, stepContext);
  }

  @AutoValue
  abstract static class AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT> {
    abstract AppliedPTransform<
            PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>,
            PCollectionTuple,
            StatefulParDo<K, InputT, OutputT>>
        getTransform();

    abstract StructuralKey<K> getKey();

    abstract BoundedWindow getWindow();

    static <K, InputT, OutputT> AppliedPTransformOutputKeyAndWindow<K, InputT, OutputT> create(
        AppliedPTransform<
                PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>,
                PCollectionTuple,
                StatefulParDo<K, InputT, OutputT>>
            transform,
        StructuralKey<K> key,
        BoundedWindow w) {
      return new AutoValue_StatefulParDoEvaluatorFactory_AppliedPTransformOutputKeyAndWindow<>(
          transform, key, w);
    }
  }

  private static class StatefulParDoEvaluator<K, InputT>
      implements TransformEvaluator<KeyedWorkItem<K, KV<K, InputT>>> {

    private final DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator;
    private final List<TimerData> pushedBackTimers = new ArrayList<>();
    private final DirectTimerInternals timerInternals;

    DirectStepContext stepContext;

    public StatefulParDoEvaluator(
        DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator,
        DirectStepContext stepContext) {
      this.delegateEvaluator = delegateEvaluator;
      this.timerInternals = delegateEvaluator.getParDoEvaluator().getStepContext().timerInternals();
      this.stepContext = stepContext;
    }

    @Override
    public void processElement(WindowedValue<KeyedWorkItem<K, KV<K, InputT>>> gbkResult)
        throws Exception {
      for (WindowedValue<KV<K, InputT>> windowedValue : gbkResult.getValue().elementsIterable()) {
        delegateEvaluator.processElement(windowedValue);
      }

      final Instant inputWatermarkTime = timerInternals.currentInputWatermarkTime();
      PriorityQueue<TimerData> toBeFiredTimers =
          new PriorityQueue<>(Comparator.comparing(TimerData::getTimestamp));
      gbkResult.getValue().timersIterable().forEach(toBeFiredTimers::add);

      while (!timerInternals.containsUpdateForTimeBefore(inputWatermarkTime)
          && !toBeFiredTimers.isEmpty()) {
        TimerData timer = toBeFiredTimers.poll();
        checkState(
            timer.getNamespace() instanceof WindowNamespace,
            "Expected Timer %s to be in a %s, but got %s",
            timer,
            WindowNamespace.class.getSimpleName(),
            timer.getNamespace().getClass().getName());
        WindowNamespace<?> windowNamespace = (WindowNamespace) timer.getNamespace();
        BoundedWindow timerWindow = windowNamespace.getWindow();

        delegateEvaluator.onTimer(timer, gbkResult.getValue().key(), timerWindow);

        StateTag<WatermarkHoldState> timerWatermarkHoldTag = setTimerTag(timer);

        stepContext.stateInternals().state(timer.getNamespace(), timerWatermarkHoldTag).clear();
        stepContext.stateInternals().commit();
      }
      pushedBackTimers.addAll(toBeFiredTimers);
    }

    @Override
    public TransformResult<KeyedWorkItem<K, KV<K, InputT>>> finishBundle() throws Exception {

      TransformResult<KV<K, InputT>> delegateResult = delegateEvaluator.finishBundle();
      boolean isTimerDeclared = false;
      for (TimerData timerData : delegateResult.getTimerUpdate().getSetTimers()) {
        StateTag<WatermarkHoldState> timerWatermarkHoldTag = setTimerTag(timerData);

        stepContext
            .stateInternals()
            .state(timerData.getNamespace(), timerWatermarkHoldTag)
            .add(timerData.getOutputTimestamp());
        isTimerDeclared = true;
      }

      CopyOnAccessInMemoryStateInternals state;
      Instant watermarkHold;

      if (isTimerDeclared && delegateResult.getState() != null) { // For both State and Timer Holds
        state = delegateResult.getState();
        watermarkHold = stepContext.commitState().getEarliestWatermarkHold();
      } else if (isTimerDeclared) { // For only Timer holds
        state = stepContext.commitState();
        watermarkHold = state.getEarliestWatermarkHold();
      } else { // For only State ( non Timer ) holds
        state = delegateResult.getState();
        watermarkHold = delegateResult.getWatermarkHold();
      }

      TimerUpdate timerUpdate =
          delegateResult.getTimerUpdate().withPushedBackTimers(pushedBackTimers);
      pushedBackTimers.clear();
      StepTransformResult.Builder<KeyedWorkItem<K, KV<K, InputT>>> regroupedResult =
          StepTransformResult.<KeyedWorkItem<K, KV<K, InputT>>>withHold(
                  delegateResult.getTransform(), watermarkHold)
              .withTimerUpdate(timerUpdate)
              .withState(state)
              .withMetricUpdates(delegateResult.getLogicalMetricUpdates())
              .addOutput(Lists.newArrayList(delegateResult.getOutputBundles()));

      // The delegate may have pushed back unprocessed elements across multiple keys and windows.
      // Since processing is single-threaded per key and window, we don't need to regroup the
      // outputs, but just make a bunch of singletons
      for (WindowedValue<?> untypedUnprocessed : delegateResult.getUnprocessedElements()) {
        WindowedValue<KV<K, InputT>> windowedKv = (WindowedValue<KV<K, InputT>>) untypedUnprocessed;
        WindowedValue<KeyedWorkItem<K, KV<K, InputT>>> pushedBack =
            windowedKv.withValue(
                KeyedWorkItems.elementsWorkItem(
                    windowedKv.getValue().getKey(), Collections.singleton(windowedKv)));

        regroupedResult.addUnprocessedElements(pushedBack);
      }

      return regroupedResult.build();
    }
  }

  private static StateTag<WatermarkHoldState> setTimerTag(TimerData timerData) {
    return StateTags.makeSystemTagInternal(
        StateTags.watermarkStateInternal(
            "timer-" + timerData.getTimerId() + "+" + timerData.getTimerFamilyId(),
            TimestampCombiner.EARLIEST));
  }
}
