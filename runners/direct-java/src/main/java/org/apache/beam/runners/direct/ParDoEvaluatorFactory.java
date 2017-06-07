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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link TransformEvaluatorFactory} for {@link ParDo.MultiOutput}. */
final class ParDoEvaluatorFactory<InputT, OutputT> implements TransformEvaluatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ParDoEvaluatorFactory.class);
  private final LoadingCache<DoFn<?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;
  private final ParDoEvaluator.DoFnRunnerFactory<InputT, OutputT> runnerFactory;

  ParDoEvaluatorFactory(
      EvaluationContext evaluationContext,
      ParDoEvaluator.DoFnRunnerFactory<InputT, OutputT> runnerFactory) {
    this.evaluationContext = evaluationContext;
    this.runnerFactory = runnerFactory;
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

    @SuppressWarnings("unchecked")
    AppliedPTransform<PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>>
        parDoApplication =
            (AppliedPTransform<
                    PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>>)
                application;

    ParDo.MultiOutput<InputT, OutputT> transform = parDoApplication.getTransform();
    final DoFn<InputT, OutputT> doFn = transform.getFn();

    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        (TransformEvaluator<T>)
            createEvaluator(
                (AppliedPTransform) application,
                (PCollection<InputT>) inputBundle.getPCollection(),
                inputBundle.getKey(),
                doFn,
                transform.getSideInputs(),
                transform.getMainOutputTag(),
                transform.getAdditionalOutputTags().getAll());
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    DoFnLifecycleManagers.removeAllFromManagers(fnClones.asMap().values());
  }

  /**
   * Creates an evaluator for an arbitrary {@link AppliedPTransform} node, with the pieces of the
   * {@link ParDo} unpacked.
   *
   * <p>This can thus be invoked regardless of whether the types in the {@link AppliedPTransform}
   * correspond with the type in the unpacked {@link DoFn}, side inputs, and output tags.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  DoFnLifecycleManagerRemovingTransformEvaluator<InputT> createEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollectionTuple, ?> application,
      PCollection<InputT> mainInput,
      StructuralKey<?> inputBundleKey,
      DoFn<InputT, OutputT> doFn,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags)
      throws Exception {
    String stepName = evaluationContext.getStepName(application);
    DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundleKey)
            .getStepContext(stepName);

    DoFnLifecycleManager fnManager = fnClones.getUnchecked(doFn);

    return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(
        createParDoEvaluator(
            application,
            inputBundleKey,
            mainInput,
            sideInputs,
            mainOutputTag,
            additionalOutputTags,
            stepContext,
            fnManager.<InputT, OutputT>get(),
            fnManager),
        fnManager);
  }

  ParDoEvaluator<InputT> createParDoEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollectionTuple, ?> application,
      StructuralKey<?> key,
      PCollection<InputT> mainInput,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      DirectStepContext stepContext,
      DoFn<InputT, OutputT> fn,
      DoFnLifecycleManager fnManager)
      throws Exception {
    try {
      return ParDoEvaluator.create(
          evaluationContext,
          stepContext,
          application,
          mainInput.getWindowingStrategy(),
          fn,
          key,
          sideInputs,
          mainOutputTag,
          additionalOutputTags,
          pcollections(application.getOutputs()),
          runnerFactory);
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

  static Map<TupleTag<?>, PCollection<?>> pcollections(Map<TupleTag<?>, PValue> outputs) {
    Map<TupleTag<?>, PCollection<?>> pcs = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      pcs.put(output.getKey(), (PCollection<?>) output.getValue());
    }
    return pcs;
  }
}
