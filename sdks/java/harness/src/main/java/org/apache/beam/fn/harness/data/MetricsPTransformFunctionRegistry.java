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
package org.apache.beam.fn.harness.data;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMapEnvironment;
import org.apache.beam.runners.core.metrics.SimpleExecutionState;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.runners.core.metrics.SimpleStateRegistry;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.MetricsEnvironment;

/**
 * A class to to register and retrieve functions for bundle processing (i.e. the start, or finish
 * function). The purpose of this class is to wrap these functions with instrumentation for metrics
 * and other telemetry collection.
 *
 * <p>Usage: // Instantiate and use the registry for each class of functions. i.e. start. finish.
 *
 * <pre>
 * PTransformFunctionRegistry startFunctionRegistry;
 * PTransformFunctionRegistry finishFunctionRegistry;
 * startFunctionRegistry.register(myStartThrowingRunnable);
 * finishFunctionRegistry.register(myFinishThrowingRunnable);
 *
 * // Then invoke the functions by iterating over them, in your desired order: i.e.
 * for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
 *   startFunction.run();
 * }
 *
 * for (ThrowingRunnable finishFunction : Lists.reverse(finishFunctionRegistry.getFunctions())) {
 *   finishFunction.run();
 * }
 * // Note: this is used in ProcessBundleHandler.
 * </pre>
 */
public class MetricsPTransformFunctionRegistry extends PTransformFunctionRegistry {

  private String executionTimeUrn;
  private SimpleStateRegistry executionStates = new SimpleStateRegistry();

  /**
   * Construct the registry to run for either start or finish bundle functions.
   *
   * @param executionTimeUrn - The URN to use for the execution time metrics.
   */
  public MetricsPTransformFunctionRegistry(String executionTimeUrn) {
    super();
    this.executionTimeUrn = executionTimeUrn;
  }

  /**
   * Register the runnable to process the specific pTransformId and track its execution time.
   *
   * @param pTransformId
   * @param runnable
   */
  @Override
  public void register(String pTransformId, ThrowingRunnable runnable) {
    HashMap<String, String> labelsMetadata = new HashMap<String, String>();
    labelsMetadata.put(SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL, pTransformId);
    SimpleExecutionState state = new SimpleExecutionState(this.executionTimeUrn, labelsMetadata);
    executionStates.register(state);

    ThrowingRunnable wrapped =
        () -> {
          MetricsContainerStepMap metricsContainerRegistry =
              MetricsContainerStepMapEnvironment.getCurrent();
          MetricsContainerImpl container = metricsContainerRegistry.getContainer(pTransformId);
          try (Closeable metricCloseable = MetricsEnvironment.scopedMetricsContainer(container)) {
            try (Closeable trackerCloseable =
                ExecutionStateTracker.enterStateForCurrentTracker(state)) {
              runnable.run();
            }
          }
        };
    super.register(pTransformId, wrapped);
  }

  /** @return Execution Time MonitoringInfos based on the tracked start or finish function. */
  public List<MonitoringInfo> getExecutionTimeMonitoringInfos() {
    return executionStates.getExecutionTimeMonitoringInfos();
  }
}
