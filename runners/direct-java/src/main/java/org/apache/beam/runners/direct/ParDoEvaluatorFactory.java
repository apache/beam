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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link TransformEvaluatorFactory} for {@link ParDo.BoundMulti}. */
final class ParDoEvaluatorFactory<InputT, OutputT> implements TransformEvaluatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ParDoEvaluatorFactory.class);
  private final LoadingCache<DoFn<?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;

  ParDoEvaluatorFactory(EvaluationContext evaluationContext) {
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
  private TransformEvaluator<InputT> createEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollectionTuple, BoundMulti<InputT, OutputT>>
          application,
      CommittedBundle<InputT> inputBundle)
      throws Exception {
    String stepName = evaluationContext.getStepName(application);
    DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);

    DoFnLifecycleManager fnManager = fnClones.getUnchecked(application.getTransform().getNewFn());
    try {
      ParDo.BoundMulti<InputT, OutputT> transform = application.getTransform();
      return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(
          ParDoEvaluator.create(
              evaluationContext,
              stepContext,
              application,
              application.getInput().getWindowingStrategy(),
              fnManager.get(),
              transform.getSideInputs(),
              transform.getMainOutputTag(),
              transform.getSideOutputTags().getAll(),
              application.getOutput().getAll()),
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
