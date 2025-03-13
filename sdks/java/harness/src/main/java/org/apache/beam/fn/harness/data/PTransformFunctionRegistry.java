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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionState;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.function.ThrowingRunnable;

/**
 * A class to register and retrieve functions for bundle processing (i.e. the start, or finish
 * function). The purpose of this class is to wrap these functions with instrumentation for metrics
 * and other telemetry collection.
 *
 * <p>Usage:
 *
 * <pre>
 * // Instantiate and use the registry for each class of functions. i.e. start. finish.
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
public class PTransformFunctionRegistry {

  private final ExecutionStateTracker stateTracker;
  private final String executionStateUrn;
  private final ShortIdMap shortIds;
  private final List<ThrowingRunnable> runnables = new ArrayList<>();
  private final String stateName;

  /**
   * Construct the registry to run for either start or finish bundle functions.
   *
   * @param shortIds - Provides short ids for {@link MonitoringInfo}.
   * @param stateTracker - The tracker to enter states in order to calculate execution time metrics.
   * @param executionStateUrn - The URN for the execution state .
   */
  public PTransformFunctionRegistry(
      ShortIdMap shortIds, ExecutionStateTracker stateTracker, String executionStateUrn) {
    switch (executionStateUrn) {
      case Urns.START_BUNDLE_MSECS:
        stateName = org.apache.beam.runners.core.metrics.ExecutionStateTracker.START_STATE_NAME;
        break;
      case Urns.FINISH_BUNDLE_MSECS:
        stateName = org.apache.beam.runners.core.metrics.ExecutionStateTracker.FINISH_STATE_NAME;
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown URN %s", executionStateUrn));
    }
    this.shortIds = shortIds;
    this.executionStateUrn = executionStateUrn;
    this.stateTracker = stateTracker;
  }

  /**
   * Register the runnable to process the specific pTransformId and track its execution time.
   *
   * @param pTransformId
   * @param pTransformUniqueName
   * @param runnable
   */
  public void register(
      String pTransformId, String pTransformUniqueName, ThrowingRunnable runnable) {
    SimpleMonitoringInfoBuilder miBuilder = new SimpleMonitoringInfoBuilder();
    miBuilder.setUrn(executionStateUrn);
    miBuilder.setType(MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE);
    miBuilder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, pTransformId);
    MonitoringInfo mi = miBuilder.build();
    if (mi == null) {
      throw new IllegalStateException(
          String.format(
              "Unable to construct %s counter for PTransform {id=%s, name=%s}",
              executionStateUrn, pTransformId, pTransformUniqueName));
    }
    String shortId = shortIds.getOrCreateShortId(mi);
    ExecutionState executionState =
        stateTracker.create(shortId, pTransformId, pTransformUniqueName, stateName);

    ThrowingRunnable wrapped =
        () -> {
          executionState.activate();
          try {
            runnable.run();
          } finally {
            executionState.deactivate();
          }
        };
    runnables.add(wrapped);
  }

  /**
   * @return A list of wrapper functions which will invoke the registered functions indirectly. The
   *     order of registry is maintained.
   */
  public List<ThrowingRunnable> getFunctions() {
    return runnables;
  }
}
