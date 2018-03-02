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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

class SplittableProcessElementsEvaluatorFactory<
        InputT,
        OutputT,
        RestrictionT,
        PositionT,
        TrackerT extends RestrictionTracker<RestrictionT, PositionT>>
    implements TransformEvaluatorFactory {
  private final ParDoEvaluatorFactory<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>
      delegateFactory;
  private final EvaluationContext evaluationContext;

  SplittableProcessElementsEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    this.delegateFactory =
        new ParDoEvaluatorFactory<>(
            evaluationContext,
            SplittableProcessElementsEvaluatorFactory
                .<InputT, OutputT, RestrictionT>processFnRunnerFactory(),
            ParDoEvaluatorFactory.basicDoFnCacheLoader());
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
  private TransformEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>> createEvaluator(
      AppliedPTransform<
              PCollection<KeyedWorkItem<String, KV<InputT, RestrictionT>>>, PCollectionTuple,
              ProcessElements<InputT, OutputT, RestrictionT, TrackerT>>
          application,
      CommittedBundle<InputT> inputBundle)
      throws Exception {
    final ProcessElements<InputT, OutputT, RestrictionT, TrackerT> transform =
        application.getTransform();

    ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn =
        transform.newProcessFn(transform.getFn());

    DoFnLifecycleManager fnManager = DoFnLifecycleManager.of(processFn);
    processFn =
        ((ProcessFn<InputT, OutputT, RestrictionT, TrackerT>)
            fnManager.<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>get());

    String stepName = evaluationContext.getStepName(application);
    final DirectExecutionContext.DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getStepContext(stepName);

    final ParDoEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>>
        parDoEvaluator =
            delegateFactory.createParDoEvaluator(
                application,
                inputBundle.getKey(),
                (PCollection<KeyedWorkItem<String, KV<InputT, RestrictionT>>>)
                    inputBundle.getPCollection(),
                transform.getSideInputs(),
                transform.getMainOutputTag(),
                transform.getAdditionalOutputTags().getAll(),
                stepContext,
                processFn,
                fnManager);

    processFn.setStateInternalsFactory(key -> (StateInternals) stepContext.stateInternals());

    processFn.setTimerInternalsFactory(key -> stepContext.timerInternals());

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
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            transform.getFn(),
            evaluationContext.getPipelineOptions(),
            outputWindowedValue,
            evaluationContext.createSideInputReader(transform.getSideInputs()),
            // TODO: For better performance, use a higher-level executor?
            // TODO: (BEAM-723) Create a shared ExecutorService for maintenance tasks in the
            // DirectRunner.
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                    .setThreadFactory(MoreExecutors.platformThreadFactory())
                    .setDaemon(true)
                    .setNameFormat("direct-splittable-process-element-checkpoint-executor")
                    .build()),
            // Setting small values here to stimulate frequent checkpointing and better exercise
            // splittable DoFn's in that respect.
            100,
            Duration.standardSeconds(1)));

    return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(parDoEvaluator, fnManager);
  }

  private static <InputT, OutputT, RestrictionT>
      ParDoEvaluator.DoFnRunnerFactory<KeyedWorkItem<String, KV<InputT, RestrictionT>>, OutputT>
          processFnRunnerFactory() {
    return (options,
        fn,
        sideInputs,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy) -> {
      ProcessFn<InputT, OutputT, RestrictionT, ?> processFn = (ProcessFn) fn;
      return DoFnRunners.newProcessFnRunner(
          processFn,
          options,
          sideInputs,
          sideInputReader,
          outputManager,
          mainOutputTag,
          additionalOutputTags,
          stepContext,
          windowingStrategy);
    };
  }
}
