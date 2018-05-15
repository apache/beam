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
package org.apache.beam.runners.direct.portable;

import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;

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
      PTransformNode application,  CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = createInMemoryEvaluator(application);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {}

  private <InputT> TransformEvaluator<InputT> createInMemoryEvaluator(
      final PTransformNode application) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private static class FlattenEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final UncommittedBundle<InputT> outputBundle;
    private final TransformResult<InputT> result;

    public FlattenEvaluator(
        UncommittedBundle<InputT> outputBundle, TransformResult<InputT> result) {
      this.outputBundle = outputBundle;
      this.result = result;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      outputBundle.add(element);
    }

    @Override
    public TransformResult<InputT> finishBundle() {
      return result;
    }
  }
}
