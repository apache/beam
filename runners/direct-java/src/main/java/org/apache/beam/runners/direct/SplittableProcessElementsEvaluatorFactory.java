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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collection;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.ElementAndRestriction;
import org.apache.beam.runners.core.ElementAndRestrictionCoder;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItemCoder;
import org.apache.beam.sdk.util.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.StateInternals;
import org.apache.beam.sdk.util.state.StateInternalsFactory;
import org.apache.beam.sdk.util.state.TimerInternalsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SplittableProcessElementsEvaluatorFactory<InputT, OutputT, RestrictionT>
    implements TransformEvaluatorFactory {
  private static final Logger LOG =
      LoggerFactory.getLogger(SplittableProcessElementsEvaluatorFactory.class);

  private final LoadingCache<DoFn<?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;

  SplittableProcessElementsEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    fnClones =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<DoFn<?, ?>, DoFnLifecycleManager>() {
                  @Override
                  public DoFnLifecycleManager load(DoFn<?, ?> key) throws Exception {
                    return DoFnLifecycleManager.of(key);
                  }
                });
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
    DoFnLifecycleManagers.removeAllFromManagers(fnClones.asMap().values());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TransformEvaluator<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>
      createEvaluator(
          AppliedPTransform<
                  PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>,
                  PCollectionTuple, SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT>>
              application,
          CommittedBundle<InputT> inputBundle)
          throws Exception {
    PCollection<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>> input =
        application.getInput();

    final SplittableParDo.ProcessElements<InputT, OutputT, RestrictionT> transform =
        application.getTransform();

    ElementAndRestrictionCoder<InputT, RestrictionT> elementAndRestrictionCoder =
        ((ElementAndRestrictionCoder<InputT, RestrictionT>)
            ((KeyedWorkItemCoder<String, ElementAndRestriction<InputT, RestrictionT>>)
                    input.getCoder())
                .getElementCoder());

    // Cache the underlying DoFn rather than the ProcessFn itself.
    DoFnLifecycleManager fnManager = fnClones.getUnchecked(transform.getFn());

    try {
      SplittableParDo.ProcessFn<InputT, OutputT, RestrictionT, ?> processFn =
          new SplittableParDo.ProcessFn<>(
              ((DoFn<InputT, OutputT>) fnManager.get()),
              elementAndRestrictionCoder.getElementCoder(),
              elementAndRestrictionCoder.getRestrictionCoder(),
              input.getWindowingStrategy().getWindowFn().windowCoder());

      processFn.setup();

      String stepName = evaluationContext.getStepName(application);
      final DirectExecutionContext.DirectStepContext stepContext =
          evaluationContext
              .getExecutionContext(application, inputBundle.getKey())
              .getOrCreateStepContext(stepName, stepName);

      ParDoEvaluator<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, OutputT>
          parDoEvaluator =
              ParDoEvaluator.create(
                  evaluationContext,
                  stepContext,
                  application,
                  application.getInput().getWindowingStrategy(),
                  processFn,
                  transform.getSideInputs(),
                  transform.getMainOutputTag(),
                  transform.getSideOutputTags().getAll(),
                  application.getOutput().getAll());

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

      final OutputManager outputManager = parDoEvaluator.getOutputManager();
      processFn.setOutputWindowedValue(
          new OutputWindowedValue<OutputT>() {
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
            public <SideOutputT> void sideOutputWindowedValue(
                TupleTag<SideOutputT> tag,
                SideOutputT output,
                Instant timestamp,
                Collection<? extends BoundedWindow> windows,
                PaneInfo pane) {
              outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
            }
          });

      return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(parDoEvaluator, fnManager);
    } catch (Exception e) {
      try {
        fnManager.remove();
      } catch (Exception removalException) {
        LOG.error(
            "Exception encountered while cleaning up in ParDo evaluator construction",
            removalException);
        e.addSuppressed(removalException);
      }
      throw e;
    }
  }
}
