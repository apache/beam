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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.ExecutableGraph;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEvaluatorRegistry.class);

  static TransformEvaluatorRegistry portableRegistry(
      ExecutableGraph<PTransformNode, PCollectionNode> graph,
      Components components,
      BundleFactory bundleFactory,
      JobBundleFactory jobBundleFactory,
      StepStateAndTimers.Provider stepStateAndTimers) {
    return new TransformEvaluatorRegistry(
        ImmutableMap.<String, TransformEvaluatorFactory>builder()
            .put(
                PTransformTranslation.IMPULSE_TRANSFORM_URN,
                new ImpulseEvaluatorFactory(graph, bundleFactory))
            .put(
                PTransformTranslation.FLATTEN_TRANSFORM_URN,
                new FlattenEvaluatorFactory(graph, bundleFactory))
            .put(
                DirectGroupByKey.DIRECT_GBKO_URN,
                new GroupByKeyOnlyEvaluatorFactory(graph, components, bundleFactory))
            .put(
                DirectGroupByKey.DIRECT_GABW_URN,
                new GroupAlsoByWindowEvaluatorFactory(
                    graph, components, bundleFactory, stepStateAndTimers))
            .put(
                ExecutableStage.URN,
                new RemoteStageEvaluatorFactory(bundleFactory, jobBundleFactory))
            .put(
                SplittableRemoteStageEvaluatorFactory.URN,
                new SplittableRemoteStageEvaluatorFactory(
                    bundleFactory, jobBundleFactory, stepStateAndTimers))
            .build());
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  private final Map<String, TransformEvaluatorFactory> factories;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes") Map<String, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, CommittedBundle<?> inputBundle) throws Exception {
    checkState(
        !finished.get(), "Tried to get an evaluator for a finished TransformEvaluatorRegistry");

    String urn = PTransformTranslation.urnForTransformOrNull(application.getTransform());

    TransformEvaluatorFactory factory =
        checkNotNull(factories.get(urn), "No evaluator for PTransform \"%s\"", urn);
    return factory.forApplication(application, inputBundle);
  }

  public void cleanup() throws Exception {
    Collection<Exception> thrownInCleanup = new ArrayList<>();
    for (TransformEvaluatorFactory factory : factories.values()) {
      try {
        factory.cleanup();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        thrownInCleanup.add(e);
      }
    }
    finished.set(true);
    if (!thrownInCleanup.isEmpty()) {
      LOG.error("Exceptions {} thrown while cleaning up evaluators", thrownInCleanup);
      Exception toThrow = null;
      for (Exception e : thrownInCleanup) {
        if (toThrow == null) {
          toThrow = e;
        } else {
          toThrow.addSuppressed(e);
        }
      }
      throw toThrow;
    }
  }
}
