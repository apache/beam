/*
 * Copyright (C) 2016 Google Inc.
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
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.Flatten.FlattenPCollectionList;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the {@link Flatten}
 * {@link PTransform}.
 */
class FlattenEvaluatorFactory implements TransformEvaluatorFactory {
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    return createInMemoryEvaluator((AppliedPTransform) application, inputBundle, evaluationContext);
  }

  private <InputT> TransformEvaluator<InputT> createInMemoryEvaluator(
      final AppliedPTransform<
              PCollectionList<InputT>, PCollection<InputT>, FlattenPCollectionList<InputT>>
          application,
      final CommittedBundle<InputT> inputBundle,
      final InProcessEvaluationContext evaluationContext) {
    if (inputBundle == null) {
      // it is impossible to call processElement on a flatten with no input bundle. A Flatten with
      // no input bundle occurs as an output of Flatten.pcollections(PCollectionList.empty())
      return new FlattenEvaluator<>(
          null, StepTransformResult.withoutHold(application).build());
    }
    final UncommittedBundle<InputT> outputBundle =
        evaluationContext.createBundle(inputBundle, application.getOutput());
    final InProcessTransformResult result =
        StepTransformResult.withoutHold(application).addOutput(outputBundle).build();
    return new FlattenEvaluator<>(outputBundle, result);
  }

  private static class FlattenEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final UncommittedBundle<InputT> outputBundle;
    private final InProcessTransformResult result;

    public FlattenEvaluator(
        UncommittedBundle<InputT> outputBundle, InProcessTransformResult result) {
      this.outputBundle = outputBundle;
      this.result = result;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      outputBundle.add(element);
    }

    @Override
    public InProcessTransformResult finishBundle() {
      return result;
    }
  }
}
