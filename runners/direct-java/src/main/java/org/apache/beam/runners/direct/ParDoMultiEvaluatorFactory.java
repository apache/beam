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

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;

import java.util.Map;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link BoundMulti} primitive {@link PTransform}.
 */
class ParDoMultiEvaluatorFactory implements TransformEvaluatorFactory {
  private final LoadingCache<DoFn<?, ?>, ThreadLocal<DoFn<?, ?>>> fnClones;

  public ParDoMultiEvaluatorFactory() {
    fnClones = CacheBuilder.newBuilder()
        .build(SerializableCloningThreadLocalCacheLoader.<DoFn<?, ?>>create());
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        createMultiEvaluator((AppliedPTransform) application, inputBundle, evaluationContext);
    return evaluator;
  }

  private <InT, OuT> TransformEvaluator<InT> createMultiEvaluator(
      AppliedPTransform<PCollection<InT>, PCollectionTuple, BoundMulti<InT, OuT>> application,
      CommittedBundle<InT> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    Map<TupleTag<?>, PCollection<?>> outputs = application.getOutput().getAll();
    DoFn<InT, OuT> fn = application.getTransform().getFn();

    @SuppressWarnings({"unchecked", "rawtypes"}) ThreadLocal<DoFn<InT, OuT>> fnLocal =
        (ThreadLocal) fnClones.getUnchecked(application.getTransform().getFn());
    try {
      TransformEvaluator<InT> parDoEvaluator = ParDoInProcessEvaluator.create(evaluationContext,
          inputBundle,
          application,
          fnLocal.get(),
          application.getTransform().getSideInputs(),
          application.getTransform().getMainOutputTag(),
          application.getTransform().getSideOutputTags().getAll(),
          outputs);
      return ThreadLocalInvalidatingTransformEvaluator.wrapping(parDoEvaluator, fnLocal);
    } catch (Exception e) {
      fnLocal.remove();
      throw e;
    }
  }
}
