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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.io.Closeable;

/**
 * A Utility class to support set a thread local MetricContainerStepMap. The thread local
 * MetricContainerStepMap can used to set the appropriate MetricContainer by downstream pCollection
 * consumers code, without changing the FnDataReceiver interface.
 *
 * <p>TODO(ajamato): Consider putting this logic in MetricsEnvironment, after moving files around to
 * prevent dependency cycles.
 */
public class MetricsContainerStepMapEnvironment {

  private static final ThreadLocal<MetricsContainerStepMap> METRICS_CONTAINER_STEP_MAP_FOR_THREAD =
      new ThreadLocal<>();

  /**
   * Sets a MetricsContainerStepMap on the current scope.
   *
   * @return A {@link Closeable} to deactivate state sampling.
   */
  private static Closeable activate() {
    checkState(
        METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get() == null,
        "MetricsContainerStepMapEnvironment is already active in current scope.");
    METRICS_CONTAINER_STEP_MAP_FOR_THREAD.set(new MetricsContainerStepMap());
    return MetricsContainerStepMapEnvironment::deactivate;
  }

  private static void deactivate() {
    METRICS_CONTAINER_STEP_MAP_FOR_THREAD.remove();
  }

  /**
   * @return The currently in scope thread local MetricContainerStepMap or a new
   *     MetricsContainerStepMap().
   */
  public static MetricsContainerStepMap getCurrent() {
    checkState(
        METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get() != null,
        "MetricsContainerStepMapEnvironment is not already active in current scope.");
    return METRICS_CONTAINER_STEP_MAP_FOR_THREAD.get();
  }

  /**
   * Activates a MetricsContainerStepMap and ExecutionStateTracker on the current scope.
   *
   * @return a single closeable which closes the metrics environment and tracker and the
   */
  public static Closeable setupMetricEnvironment() {
    Closeable closeMetricsMap = MetricsContainerStepMapEnvironment.activate();
    ExecutionStateTracker stateTracker =
        new ExecutionStateTracker(ExecutionStateSampler.instance());
    Closeable closeTracker = stateTracker.activate();
    return () -> {
      closeTracker.close();
      closeMetricsMap.close();
    };
  }
}
