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

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.Bound;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link Bound ParDo.Bound} primitive {@link PTransform}.
 */
class ParDoSingleEvaluatorFactory implements TransformEvaluatorFactory {
  private final LoadingCache<AppliedPTransform<?, ?, Bound<?, ?>>, ThreadLocal<DoFn<?, ?>>>
      fnClones;

  public ParDoSingleEvaluatorFactory() {
    fnClones =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<AppliedPTransform<?, ?, Bound<?, ?>>, ThreadLocal<DoFn<?, ?>>>() {
                  @Override
                  public ThreadLocal<DoFn<?, ?>> load(AppliedPTransform<?, ?, Bound<?, ?>> key)
                      throws Exception {
                    @SuppressWarnings({"unchecked", "rawtypes"})
                    ThreadLocal threadLocal =
                        (ThreadLocal) CloningThreadLocal.of(key.getTransform().getFn());
                    return threadLocal;
                  }
                });
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      final AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext) {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        createSingleEvaluator((AppliedPTransform) application, inputBundle, evaluationContext);
    return evaluator;
  }

  private <InputT, OutputT> TransformEvaluator<InputT> createSingleEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, Bound<InputT, OutputT>>
          application,
      CommittedBundle<InputT> inputBundle,
      EvaluationContext evaluationContext) {
    TupleTag<OutputT> mainOutputTag = new TupleTag<>("out");

    @SuppressWarnings({"unchecked", "rawtypes"})
    ThreadLocal<DoFn<InputT, OutputT>> fnLocal =
        (ThreadLocal) fnClones.getUnchecked((AppliedPTransform) application);
    try {
      ParDoEvaluator<InputT> parDoEvaluator =
          ParDoEvaluator.create(
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
