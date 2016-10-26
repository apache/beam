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
import java.util.Map;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link BoundMulti} primitive {@link PTransform}.
 */
class ParDoMultiEvaluatorFactory implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ParDoMultiEvaluatorFactory.class);
  private final LoadingCache<AppliedPTransform<?, ?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;

  public ParDoMultiEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    fnClones = CacheBuilder.newBuilder()
        .build(new CacheLoader<AppliedPTransform<?, ?, ?>, DoFnLifecycleManager>() {
          @Override
          public DoFnLifecycleManager load(AppliedPTransform<?, ?, ?> key)
              throws Exception {
            BoundMulti<?, ?> bound = (BoundMulti<?, ?>) key.getTransform();
            return DoFnLifecycleManager.of(bound.getNewFn());
          }
        });
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        createMultiEvaluator((AppliedPTransform) application, inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    DoFnLifecycleManagers.removeAllFromManagers(fnClones.asMap().values());
  }

  private <InT, OuT> TransformEvaluator<InT> createMultiEvaluator(
      AppliedPTransform<PCollection<InT>, PCollectionTuple, BoundMulti<InT, OuT>> application,
      CommittedBundle<InT> inputBundle) throws Exception {
    Map<TupleTag<?>, PCollection<?>> outputs = application.getOutput().getAll();

    DoFnLifecycleManager fnLocal = fnClones.getUnchecked((AppliedPTransform) application);
    String stepName = evaluationContext.getStepName(application);
    DirectStepContext stepContext =
        evaluationContext.getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);
    try {
      @SuppressWarnings({"unchecked", "rawtypes"})
      TransformEvaluator<InT> parDoEvaluator =
          ParDoEvaluator.create(
              evaluationContext,
              stepContext,
              inputBundle,
              application,
              (DoFn) fnLocal.get(),
              application.getTransform().getSideInputs(),
              application.getTransform().getMainOutputTag(),
              application.getTransform().getSideOutputTags().getAll(),
              outputs);
      return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(parDoEvaluator, fnLocal);
    } catch (Exception e) {
      try {
        fnLocal.remove();
      } catch (Exception removalException) {
        LOG.error("Exception encountered while cleaning up in ParDo evaluator construction",
            removalException);
        e.addSuppressed(removalException);
      }
      throw e;
    }
  }
}
