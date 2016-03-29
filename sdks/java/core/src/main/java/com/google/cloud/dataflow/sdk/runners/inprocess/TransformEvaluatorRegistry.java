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

import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Flatten.FlattenPCollectionList;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  public static TransformEvaluatorRegistry defaultRegistry() {
    @SuppressWarnings("rawtypes")
    ImmutableMap<Class<? extends PTransform>, TransformEvaluatorFactory> primitives =
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .put(Read.Bounded.class, new BoundedReadEvaluatorFactory())
            .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory())
            .put(ParDo.Bound.class, new ParDoSingleEvaluatorFactory())
            .put(ParDo.BoundMulti.class, new ParDoMultiEvaluatorFactory())
            .put(
                GroupByKeyEvaluatorFactory.InProcessGroupByKeyOnly.class,
                new GroupByKeyEvaluatorFactory())
            .put(FlattenPCollectionList.class, new FlattenEvaluatorFactory())
            .put(ViewEvaluatorFactory.WriteView.class, new ViewEvaluatorFactory())
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories;

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws Exception {
    TransformEvaluatorFactory factory = factories.get(application.getTransform().getClass());
    return factory.forApplication(application, inputBundle, evaluationContext);
  }
}
