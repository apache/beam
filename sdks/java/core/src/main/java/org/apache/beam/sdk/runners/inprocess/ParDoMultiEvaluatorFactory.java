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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.runners.inprocess.InProcessExecutionContext.InProcessStepContext;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.runners.inprocess.ParDoInProcessEvaluator.BundleOutputManager;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo.BoundMulti;
import org.apache.beam.sdk.util.DoFnRunner;
import org.apache.beam.sdk.util.DoFnRunners;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link BoundMulti} primitive {@link PTransform}.
 */
class ParDoMultiEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator = (TransformEvaluator<T>) createMultiEvaluator(
            (AppliedPTransform) application, inputBundle, evaluationContext);
    return evaluator;
  }

  private static <InT, OuT> ParDoInProcessEvaluator<InT> createMultiEvaluator(
      AppliedPTransform<PCollection<InT>, PCollectionTuple, BoundMulti<InT, OuT>> application,
      CommittedBundle<InT> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    PCollectionTuple output = application.getOutput();
    Map<TupleTag<?>, PCollection<?>> outputs = output.getAll();
    Map<TupleTag<?>, UncommittedBundle<?>> outputBundles = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      outputBundles.put(
          outputEntry.getKey(),
          evaluationContext.createBundle(inputBundle, outputEntry.getValue()));
    }
    InProcessExecutionContext executionContext =
        evaluationContext.getExecutionContext(application, inputBundle.getKey());
    String stepName = evaluationContext.getStepName(application);
    InProcessStepContext stepContext =
        executionContext.getOrCreateStepContext(stepName, stepName, null);

    CounterSet counters = evaluationContext.createCounterSet();

    DoFn<InT, OuT> fn = application.getTransform().getFn();
    DoFnRunner<InT, OuT> runner =
        DoFnRunners.createDefault(
            evaluationContext.getPipelineOptions(),
            fn,
            evaluationContext.createSideInputReader(application.getTransform().getSideInputs()),
            BundleOutputManager.create(outputBundles),
            application.getTransform().getMainOutputTag(),
            application.getTransform().getSideOutputTags().getAll(),
            stepContext,
            counters.getAddCounterMutator(),
            application.getInput().getWindowingStrategy());

    runner.startBundle();

    return new ParDoInProcessEvaluator<>(
        runner, application, counters, outputBundles.values(), stepContext);
  }
}
