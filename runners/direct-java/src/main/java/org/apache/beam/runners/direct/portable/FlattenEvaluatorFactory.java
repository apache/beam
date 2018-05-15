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

import static com.google.common.collect.Iterables.getOnlyElement;

import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link Flatten}
 * {@link PTransform}.
 */
class FlattenEvaluatorFactory implements TransformEvaluatorFactory {
  private final BundleFactory bundleFactory;
  private final ExecutableGraph<PTransformNode, PCollectionNode> graph;

  FlattenEvaluatorFactory(
      BundleFactory bundleFactory, ExecutableGraph<PTransformNode, PCollectionNode> graph) {
    this.bundleFactory = bundleFactory;
    this.graph = graph;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application,  CommittedBundle<?> inputBundle) {
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    TransformEvaluator<InputT> evaluator = createInMemoryEvaluator(application);
    return evaluator;
  }

  @Override
  public void cleanup() {}

  private <InputT> TransformEvaluator<InputT> createInMemoryEvaluator(
      final PTransformNode transform) {
    return new FlattenEvaluator<>(transform);
  }

  private class FlattenEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final PTransformNode transform;
    private final UncommittedBundle<InputT> bundle;

    FlattenEvaluator(PTransformNode transform) {
      this.transform = transform;
      PCollectionNode output = getOnlyElement(graph.getProduced(transform));
      bundle = bundleFactory.createBundle(output);
    }

    @Override
    public void processElement(WindowedValue<InputT> element) {
      bundle.add(element);
    }

    @Override
    public TransformResult<InputT> finishBundle() {
      return StepTransformResult.<InputT>withoutHold(transform).addOutput(bundle).build();
    }
  }
}
