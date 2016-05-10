/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo.Bound;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link Bound ParDo.Bound} primitive {@link PTransform}.
 */
class ParDoSingleEvaluatorFactory implements TransformEvaluatorFactory {
  private final LoadingCache<DoFn<?, ?>, ThreadLocal<DoFn<?, ?>>> fnClones;

  public ParDoSingleEvaluatorFactory() {
    fnClones = CacheBuilder.newBuilder()
        .build(SerializableCloningThreadLocalCacheLoader.<DoFn<?, ?>>create());
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      final AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        createSingleEvaluator((AppliedPTransform) application, inputBundle, evaluationContext);
    return evaluator;
  }

  private <InputT, OutputT> TransformEvaluator<InputT> createSingleEvaluator(
      @SuppressWarnings("rawtypes") AppliedPTransform<PCollection<InputT>, PCollection<OutputT>,
          Bound<InputT, OutputT>> application,
      CommittedBundle<InputT> inputBundle, InProcessEvaluationContext evaluationContext) {
    TupleTag<OutputT> mainOutputTag = new TupleTag<>("out");

    @SuppressWarnings({"unchecked", "rawtypes"}) ThreadLocal<DoFn<InputT, OutputT>> fnLocal =
        (ThreadLocal) fnClones.getUnchecked(application.getTransform().getFn());
    try {
      ParDoInProcessEvaluator<InputT> parDoEvaluator = ParDoInProcessEvaluator.create(
          evaluationContext,
          inputBundle,
          application,
          fnLocal.get(),
          application.getTransform().getSideInputs(),
          mainOutputTag,
          Collections.<TupleTag<?>>emptyList(),
          ImmutableMap.<TupleTag<?>, PCollection<?>>of(mainOutputTag, application.getOutput()));
      return ThreadLocalInvalidatingTransformEvaluator.wrapping(parDoEvaluator, fnLocal);
    } catch (Exception e) {
      fnLocal.remove();
      throw e;
    }
  }
}
