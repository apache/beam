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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessExecutionContext.InProcessStepContext;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.ParDoInProcessEvaluator.BundleOutputManager;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo.Bound;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunners;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.Collections;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link Bound ParDo.Bound} primitive {@link PTransform}.
 */
class ParDoSingleEvaluatorFactory implements TransformEvaluatorFactory {
  @Override
  public <T> TransformEvaluator<T> forApplication(
      final AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator = (TransformEvaluator<T>) createSingleEvaluator(
            (AppliedPTransform) application, inputBundle, evaluationContext);
    return evaluator;
  }

  private static <InputT, OutputT> ParDoInProcessEvaluator<InputT> createSingleEvaluator(
      @SuppressWarnings("rawtypes") AppliedPTransform<PCollection<InputT>, PCollection<OutputT>,
          Bound<InputT, OutputT>> application,
      CommittedBundle<InputT> inputBundle, InProcessEvaluationContext evaluationContext) {
    TupleTag<OutputT> mainOutputTag = new TupleTag<>("out");
    UncommittedBundle<OutputT> outputBundle =
        evaluationContext.createBundle(inputBundle, application.getOutput());

    InProcessExecutionContext executionContext =
        evaluationContext.getExecutionContext(application, inputBundle.getKey());
    String stepName = evaluationContext.getStepName(application);
    InProcessStepContext stepContext =
        executionContext.getOrCreateStepContext(stepName, stepName, null);

    CounterSet counters = evaluationContext.createCounterSet();

    DoFnRunner<InputT, OutputT> runner =
        DoFnRunners.createDefault(
            evaluationContext.getPipelineOptions(),
            application.getTransform().getFn(),
            evaluationContext.createSideInputReader(application.getTransform().getSideInputs()),
            BundleOutputManager.create(
                Collections.<TupleTag<?>, UncommittedBundle<?>>singletonMap(
                    mainOutputTag, outputBundle)),
            mainOutputTag,
            Collections.<TupleTag<?>>emptyList(),
            stepContext,
            counters.getAddCounterMutator(),
            application.getInput().getWindowingStrategy());

    runner.startBundle();
    return new ParDoInProcessEvaluator<InputT>(
        runner,
        application,
        counters,
        Collections.<UncommittedBundle<?>>singleton(outputBundle),
        stepContext);
  }
}
