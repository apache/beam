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
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} for {@link ParDo}-like primitive {@link PTransform
 * PTransforms}, parameterized by some {@link TransformHooks transform-specific handling}.
 */
final class ParDoEvaluatorFactory<
        InputT,
        OutputT,
        TransformOutputT extends POutput,
        TransformT extends PTransform<PCollection<? extends InputT>, TransformOutputT>>
    implements TransformEvaluatorFactory {
  interface TransformHooks<
      InputT,
      OutputT,
      TransformOutputT extends POutput,
      TransformT extends PTransform<PCollection<? extends InputT>, TransformOutputT>> {
    /** Returns the {@link DoFn} contained in the given {@link ParDo} transform. */
    DoFn<InputT, OutputT> getDoFn(TransformT transform);

    /** Configures and creates a {@link ParDoEvaluator} for the given {@link DoFn}. */
    ParDoEvaluator<InputT, OutputT> createParDoEvaluator(
        EvaluationContext evaluationContext,
        AppliedPTransform<PCollection<InputT>, TransformOutputT, TransformT> application,
        DirectStepContext stepContext,
        DoFn<InputT, OutputT> fnLocal);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParDoEvaluatorFactory.class);
  private final LoadingCache<DoFn<?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;
  private final TransformHooks<InputT, OutputT, TransformOutputT, TransformT> hooks;

  ParDoEvaluatorFactory(
      EvaluationContext evaluationContext,
      TransformHooks<InputT, OutputT, TransformOutputT, TransformT> hooks) {
    this.evaluationContext = evaluationContext;
    this.hooks = hooks;
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
  private TransformEvaluator<InputT> createEvaluator(
      AppliedPTransform<PCollection<InputT>, TransformOutputT, TransformT> application,
      CommittedBundle<InputT> inputBundle)
      throws Exception {
    String stepName = evaluationContext.getStepName(application);
    DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);

    DoFnLifecycleManager fnManager =
        fnClones.getUnchecked(hooks.getDoFn(application.getTransform()));
    try {
      return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(
          hooks.createParDoEvaluator(
              evaluationContext, application, stepContext, (DoFn<InputT, OutputT>) fnManager.get()),
          fnManager);
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
