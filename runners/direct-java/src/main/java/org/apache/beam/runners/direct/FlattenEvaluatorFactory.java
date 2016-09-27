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
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.FlattenPCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link Flatten}
 * {@link PTransform}.
 */
class FlattenEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;

  FlattenEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      CommittedBundle<?> inputBundle
      ) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = (TransformEvaluator<InputT>) createInMemoryEvaluator(
            (AppliedPTransform) application, inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {}

  private <InputT> TransformEvaluator<InputT> createInMemoryEvaluator(
      final AppliedPTransform<
              PCollectionList<InputT>, PCollection<InputT>, FlattenPCollectionList<InputT>>
          application,
      final CommittedBundle<InputT> inputBundle) {
    if (inputBundle == null) {
      // it is impossible to call processElement on a flatten with no input bundle. A Flatten with
      // no input bundle occurs as an output of Flatten.pcollections(PCollectionList.empty())
      return new FlattenEvaluator<>(
          null, StepTransformResult.withoutHold(application).build());
    }
    final UncommittedBundle<InputT> outputBundle =
        evaluationContext.createBundle(application.getOutput());
    final TransformResult result =
        StepTransformResult.withoutHold(application).addOutput(outputBundle).build();
    return new FlattenEvaluator<>(outputBundle, result);
  }

  private static class FlattenEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final UncommittedBundle<InputT> outputBundle;
    private final TransformResult result;

    public FlattenEvaluator(
        UncommittedBundle<InputT> outputBundle, TransformResult result) {
      this.outputBundle = outputBundle;
      this.result = result;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      outputBundle.add(element);
    }

    @Override
    public TransformResult finishBundle() {
      return result;
    }
  }
}
