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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Callable} responsible for constructing a {@link TransformEvaluator} from a {@link
 * TransformEvaluatorFactory} and evaluating it on some bundle of input, and registering the result
 * using a registered {@link CompletionCallback}.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
class DirectTransformExecutor<T> implements TransformExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DirectTransformExecutor.class);

  static class Factory implements TransformExecutorFactory {
    private final EvaluationContext context;
    private final TransformEvaluatorRegistry registry;
    private final Map<String, Collection<ModelEnforcementFactory>> transformEnforcements;

    Factory(
        EvaluationContext context,
        TransformEvaluatorRegistry registry,
        Map<String, Collection<ModelEnforcementFactory>> transformEnforcements) {
      this.context = context;
      this.registry = registry;
      this.transformEnforcements = transformEnforcements;
    }

    @Override
    public TransformExecutor create(
        CommittedBundle<?> bundle,
        AppliedPTransform<?, ?, ?> transform,
        CompletionCallback onComplete,
        TransformExecutorService executorService) {
      Collection<ModelEnforcementFactory> enforcements =
          MoreObjects.firstNonNull(
              transformEnforcements.get(
                  PTransformTranslation.urnForTransform(transform.getTransform())),
              Collections.<ModelEnforcementFactory>emptyList());
      return new DirectTransformExecutor<>(
          context, registry, enforcements, bundle, transform, onComplete, executorService);
    }
  }

  private final TransformEvaluatorRegistry evaluatorRegistry;
  private final Iterable<? extends ModelEnforcementFactory> modelEnforcements;

  /** The transform that will be evaluated. */
  private final AppliedPTransform<?, ?, ?> transform;
  /** The inputs this {@link DirectTransformExecutor} will deliver to the transform. */
  private final CommittedBundle<T> inputBundle;

  private final CompletionCallback onComplete;
  private final TransformExecutorService transformEvaluationState;
  private final EvaluationContext context;

  @VisibleForTesting
  DirectTransformExecutor(
      EvaluationContext context,
      TransformEvaluatorRegistry factory,
      Iterable<? extends ModelEnforcementFactory> modelEnforcements,
      CommittedBundle<T> inputBundle,
      AppliedPTransform<?, ?, ?> transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    this.evaluatorRegistry = factory;
    this.modelEnforcements = modelEnforcements;

    this.inputBundle = inputBundle;
    this.transform = transform;

    this.onComplete = completionCallback;

    this.transformEvaluationState = transformEvaluationState;
    this.context = context;
  }

  @Override
  public void run() {
    MetricsContainerImpl metricsContainer = new MetricsContainerImpl(transform.getFullName());
    try (Closeable metricsScope = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      Collection<ModelEnforcement<T>> enforcements = new ArrayList<>();
      for (ModelEnforcementFactory enforcementFactory : modelEnforcements) {
        ModelEnforcement<T> enforcement = enforcementFactory.forBundle(inputBundle, transform);
        enforcements.add(enforcement);
      }
      @Nullable
      TransformEvaluator<T> evaluator = evaluatorRegistry.forApplication(transform, inputBundle);
      if (evaluator == null) {
        onComplete.handleEmpty(transform);
        // Nothing to do
        return;
      }

      processElements(evaluator, metricsContainer, enforcements);

      finishBundle(evaluator, metricsContainer, enforcements);
    } catch (Exception e) {
      onComplete.handleException(inputBundle, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    } catch (Error err) {
      LOG.error("Error occurred within {}", this, err);
      onComplete.handleError(err);
      throw err;
    } finally {
      // Report the physical metrics from the end of this step.
      context.getMetrics().commitPhysical(inputBundle, metricsContainer.getCumulative());

      transformEvaluationState.complete(this);
    }
  }

  /**
   * Processes all the elements in the input bundle using the transform evaluator, applying any
   * necessary {@link ModelEnforcement ModelEnforcements}.
   */
  private void processElements(
      TransformEvaluator<T> evaluator,
      MetricsContainerImpl metricsContainer,
      Collection<ModelEnforcement<T>> enforcements)
      throws Exception {
    if (inputBundle != null) {
      for (WindowedValue<T> value : inputBundle.getElements()) {
        for (ModelEnforcement<T> enforcement : enforcements) {
          enforcement.beforeElement(value);
        }

        evaluator.processElement(value);

        // Report the physical metrics after each element
        MetricUpdates deltas = metricsContainer.getUpdates();
        if (deltas != null) {
          context.getMetrics().updatePhysical(inputBundle, deltas);
          metricsContainer.commitUpdates();
        }

        for (ModelEnforcement<T> enforcement : enforcements) {
          enforcement.afterElement(value);
        }
      }
    }
  }

  /**
   * Finishes processing the input bundle and commit the result using the {@link
   * CompletionCallback}, applying any {@link ModelEnforcement} if necessary.
   *
   * @return the {@link TransformResult} produced by {@link TransformEvaluator#finishBundle()}
   */
  private TransformResult<T> finishBundle(
      TransformEvaluator<T> evaluator,
      MetricsContainerImpl metricsContainer,
      Collection<ModelEnforcement<T>> enforcements)
      throws Exception {
    TransformResult<T> result =
        evaluator.finishBundle().withLogicalMetricUpdates(metricsContainer.getCumulative());
    CommittedResult outputs = onComplete.handleResult(inputBundle, result);
    for (ModelEnforcement<T> enforcement : enforcements) {
      enforcement.afterFinish(inputBundle, result, outputs.getOutputs());
    }
    return result;
  }
}
