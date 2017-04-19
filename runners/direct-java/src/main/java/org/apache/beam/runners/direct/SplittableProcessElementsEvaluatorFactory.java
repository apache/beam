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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.ElementAndRestriction;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.core.SplittableParDo.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SplittableProcessElementsEvaluatorFactory<
        InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    implements TransformEvaluatorFactory {
  private final ParDoEvaluatorFactory<
          KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
      delegateFactory;
  private final EvaluationContext evaluationContext;

  SplittableProcessElementsEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    this.delegateFactory =
        new ParDoEvaluatorFactory<>(
            evaluationContext,
            SplittableProcessElementsEvaluatorFactory
                .<InputT, OutputT, RestrictionT>processFnRunnerFactory());
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
  private TransformEvaluator<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
      createEvaluator(
          AppliedPTransform<
                  PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>,
                  PCollectionTuple,
                  SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT, TrackerT>>
              application,
          CommittedBundle<InputT> inputBundle)
          throws Exception {
    final SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT, TrackerT> transform =
        application.getTransform();

    ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn =
        transform.newProcessFn(transform.getFn());

    DoFnLifecycleManager fnManager = DoFnLifecycleManager.of(processFn);
    processFn =
        ((ProcessFn<InputT, OutputT, RestrictionT, TrackerT>)
            fnManager
                .<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
                    get());

    String stepName = evaluationContext.getStepName(application);
    final DirectExecutionContext.DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);

    final ParDoEvaluator<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
        parDoEvaluator =
            delegateFactory.createParDoEvaluator(
                application,
                inputBundle.getKey(),
                transform.getSideInputs(),
                transform.getMainOutputTag(),
                transform.getAdditionalOutputTags().getAll(),
                stepContext,
                processFn,
                fnManager);

    processFn.setStateInternalsFactory(
        new StateInternalsFactory<String>() {
          @SuppressWarnings({"unchecked", "rawtypes"})
          @Override
          public StateInternals<String> stateInternalsForKey(String key) {
            return (StateInternals) stepContext.stateInternals();
          }
        });

    processFn.setTimerInternalsFactory(
        new TimerInternalsFactory<String>() {
          @Override
          public TimerInternals timerInternalsForKey(String key) {
            return stepContext.timerInternals();
          }
        });

    OutputWindowedValue<OutputT> outputWindowedValue =
        new OutputWindowedValue<OutputT>() {
          private final OutputManager outputManager = parDoEvaluator.getOutputManager();

          @Override
          public void outputWindowedValue(
              OutputT output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            outputManager.output(
                transform.getMainOutputTag(), WindowedValue.of(output, timestamp, windows, pane));
          }

          @Override
          public <AdditionalOutputT> void outputWindowedValue(
              TupleTag<AdditionalOutputT> tag,
              AdditionalOutputT output,
              Instant timestamp,
              Collection<? extends BoundedWindow> windows,
              PaneInfo pane) {
            outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
          }
        };
    processFn.setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<
            InputT, OutputT, RestrictionT, TrackerT>(
            transform.getFn(),
            evaluationContext.getPipelineOptions(),
            outputWindowedValue,
            evaluationContext.createSideInputReader(transform.getSideInputs()),
            // TODO: For better performance, use a higher-level executor?
            Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
            10000,
            Duration.standardSeconds(10)));

    return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(parDoEvaluator, fnManager);
  }

  private static <InputT, OutputT, RestrictionT>
  ParDoEvaluator.DoFnRunnerFactory<
                KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
          processFnRunnerFactory() {
    return new ParDoEvaluator.DoFnRunnerFactory<
            KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>() {
      @Override
      public PushbackSideInputDoFnRunner<
          KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
      createRunner(
          PipelineOptions options,
          DoFn<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT> fn,
          List<PCollectionView<?>> sideInputs,
          ReadyCheckingSideInputReader sideInputReader,
          OutputManager outputManager,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          DirectExecutionContext.DirectStepContext stepContext,
          AggregatorContainer.Mutator aggregatorChanges,
          WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy) {
        ProcessFn<InputT, OutputT, RestrictionT, ?> processFn =
            (ProcessFn) fn;
        return DoFnRunners.newProcessFnRunner(
            processFn,
            options,
            sideInputs,
            sideInputReader,
            outputManager,
            mainOutputTag,
            additionalOutputTags,
            stepContext,
            aggregatorChanges,
            windowingStrategy);
      }
    };
  }
}
