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

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.concurrent.Callable;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.core.metrics.MetricUpdates;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Callable} responsible for constructing a {@link TransformEvaluator} from a {@link
 * TransformEvaluatorFactory} and evaluating it on some bundle of input, and registering the result
 * using a registered {@link CompletionCallback}.
 */
class DirectTransformExecutor<T> implements TransformExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(DirectTransformExecutor.class);

  static class Factory implements TransformExecutorFactory {
    private final EvaluationContext context;
    private final TransformEvaluatorRegistry registry;

    Factory(EvaluationContext context, TransformEvaluatorRegistry registry) {
      this.context = context;
      this.registry = registry;
    }

    @Override
    public TransformExecutor create(
        CommittedBundle<?> bundle,
        PTransformNode transform,
        CompletionCallback onComplete,
        TransformExecutorService executorService) {
      return new DirectTransformExecutor<>(
          context, registry, bundle, transform, onComplete, executorService);
    }
  }

  private final TransformEvaluatorRegistry evaluatorRegistry;

  /** The transform that will be evaluated. */
  private final PTransformNode transform;
  /** The inputs this {@link DirectTransformExecutor} will deliver to the transform. */
  private final CommittedBundle<T> inputBundle;

  private final CompletionCallback onComplete;
  private final TransformExecutorService transformEvaluationState;
  private final EvaluationContext context;

  @VisibleForTesting
  DirectTransformExecutor(
      EvaluationContext context,
      TransformEvaluatorRegistry factory,
      CommittedBundle<T> inputBundle,
      PTransformNode transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    this.evaluatorRegistry = factory;

    this.inputBundle = inputBundle;
    this.transform = transform;

    this.onComplete = completionCallback;

    this.transformEvaluationState = transformEvaluationState;
    this.context = context;
  }

  @Override
  public void run() {
    MetricsContainerImpl metricsContainer = new MetricsContainerImpl(transform.getId());
    try (Closeable metricsScope = MetricsEnvironment.scopedMetricsContainer(metricsContainer)) {
      TransformEvaluator<T> evaluator =
          evaluatorRegistry.forApplication(transform, inputBundle);
      if (evaluator == null) {
        onComplete.handleEmpty(transform);
        // Nothing to do
        return;
      }

      processElements(evaluator, metricsContainer);

      finishBundle(evaluator, metricsContainer);
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
   * Processes all the elements in the input bundle using the transform evaluator.
   */
  private void processElements(
      TransformEvaluator<T> evaluator,
      MetricsContainerImpl metricsContainer)
      throws Exception {
    if (inputBundle != null) {
      for (WindowedValue<T> value : inputBundle.getElements()) {
        evaluator.processElement(value);

        // Report the physical metrics after each element
        MetricUpdates deltas = metricsContainer.getUpdates();
        if (deltas != null) {
          context.getMetrics().updatePhysical(inputBundle, deltas);
          metricsContainer.commitUpdates();
        }
      }
    }
  }

  /**
   * Finishes processing the input bundle and commit the result using the
   * {@link CompletionCallback}.
   *
   * @return the {@link TransformResult} produced by
   *         {@link TransformEvaluator#finishBundle()}
   */
  private TransformResult<T> finishBundle(
      TransformEvaluator<T> evaluator, MetricsContainerImpl metricsContainer)
      throws Exception {
    TransformResult<T> result =
        evaluator.finishBundle().withLogicalMetricUpdates(metricsContainer.getCumulative());
    onComplete.handleResult(inputBundle, result);
    return result;
  }
}
