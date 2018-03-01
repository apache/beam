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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ProcessFnRunner;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessFn;
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
  private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                    .setThreadFactory(MoreExecutors.platformThreadFactory())
                    .setNameFormat("direct-splittable-process-element-checkpoint-executor")
                    .build());

  SplittableProcessElementsEvaluatorFactory(EvaluationContext evaluationContext) {
    this.delegateFactory =
      new ParDoEvaluatorFactory<>(
        evaluationContext,
        SplittableProcessElementsEvaluatorFactory.
          <InputT, OutputT, RestrictionT>processFnRunnerFactory(),
          new CacheLoader<AppliedPTransform<?, ?, ?>, DoFnLifecycleManager>() {
            @Override
            public DoFnLifecycleManager load(final AppliedPTransform<?, ?, ?> application) {
              checkArgument(
                ProcessElements.class.isInstance(application.getTransform()),
                "No know extraction of the fn from " + application);
              final ProcessElements<InputT, OutputT, RestrictionT, TrackerT> transform =
                (ProcessElements<InputT, OutputT, RestrictionT, TrackerT>)
                  application.getTransform();
              return DoFnLifecycleManager.of(transform.newProcessFn(transform.getFn()));
            }
          });
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        (TransformEvaluator<T>)
            createEvaluator((AppliedPTransform) application,
                    (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    ses.shutdownNow(); // stop before cleaning
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
    final DoFnLifecycleManagerRemovingTransformEvaluator
      <KeyedWorkItem<String, KV<InputT, RestrictionT>>> evaluator =
      delegateFactory.createEvaluator(
        (AppliedPTransform) application,
        (PCollection<KeyedWorkItem<String, KV<InputT, RestrictionT>>>) inputBundle.getPCollection(),
        inputBundle.getKey(),
        application.getTransform().getSideInputs(),
        application.getTransform().getMainOutputTag(),
        application.getTransform().getAdditionalOutputTags().getAll());

    final ParDoEvaluator<KeyedWorkItem<String, KV<InputT, RestrictionT>>> pde =
      evaluator.getParDoEvaluator();
    final ProcessFn<InputT, OutputT, RestrictionT, TrackerT> processFn =
      (ProcessFn<InputT, OutputT, RestrictionT, TrackerT>)
        ProcessFnRunner.class.cast(pde.getFnRunner()).getFn();
    final DirectExecutionContext.DirectStepContext stepContext = pde.getStepContext();
    processFn.setStateInternalsFactory(key -> stepContext.stateInternals());
    processFn.setTimerInternalsFactory(key -> stepContext.timerInternals());

    OutputWindowedValue<OutputT> outputWindowedValue =
        new OutputWindowedValue<OutputT>() {
          private final OutputManager outputManager = pde.getOutputManager();

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
            delegateFactory.evaluationContext.getPipelineOptions(),
            outputWindowedValue,
            delegateFactory.evaluationContext.createSideInputReader(transform.getSideInputs()),
            ses,
            // Setting small values here to stimulate frequent checkpointing and better exercise
            // splittable DoFn's in that respect.
            100,
            Duration.standardSeconds(1)));
    return evaluator;
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
