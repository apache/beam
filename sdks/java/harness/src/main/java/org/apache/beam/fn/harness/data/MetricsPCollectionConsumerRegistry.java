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
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMapEnvironment;
import org.apache.beam.runners.core.metrics.SimpleExecutionState;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.runners.core.metrics.SimpleStateRegistry;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * The {@code MetricsPCollectionConsumerRegistry} is used to maintain a collection of consuming
 * FnDataReceiver for each pCollectionId. Registering with this class allows inserting an element
 * count counter for every pCollection. The consumers are wrapped with extra logic to collect user
 * metrics, execution time metrics and element count metrics.
 */
public class MetricsPCollectionConsumerRegistry extends PCollectionConsumerRegistry {

  private Map<String, ElementCountFnDataReceiver> pCollectionIdsToWrappedConsumer;
  private SimpleStateRegistry executionStates = new SimpleStateRegistry();

  public MetricsPCollectionConsumerRegistry() {
    this.pCollectionIdsToWrappedConsumer = new HashMap<String, ElementCountFnDataReceiver>();
  }

  /**
   * Register the specified consumer to handle the elements in the pCollection associated with
   * pCollectionId. All consumers must be registered before extracting the combined consumer by
   * calling getMultiplexingConsumer(), or an exception will be thrown.
   *
   * <p>This will allow User, Element Count and Process Bundle Execution time metrics to be
   * collected.
   *
   * @param pCollectionId
   * @param pTransformId
   * @param consumer
   * @param <T> the element type of the PCollection
   * @throws RuntimeException if {@code register()} is called after {@code
   *     getMultiplexingConsumer()} is called.
   */
  @Override
  public <T> void register(
      String pCollectionId, String pTransformId, FnDataReceiver<WindowedValue<T>> consumer) {
    // Just save these consumers for now, but package them up later with an
    // ElementCountFnDataReceiver and possibly a MultiplexingFnDataReceiver
    // if there are multiple consumers.
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer != null) {
      throw new RuntimeException(
          "New consumers for a pCollectionId cannot be register()-d after calling "
              + "getMultiplexingConsumer.");
    }

    HashMap<String, String> labelsMetadata = new HashMap<String, String>();
    labelsMetadata.put(SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL, pTransformId);
    SimpleExecutionState state =
        new SimpleExecutionState(
            SimpleMonitoringInfoBuilder.PROCESS_BUNDLE_MSECS_URN, labelsMetadata);
    executionStates.register(state);
    // Wrap the consumer with extra logic to set the metric container with the appropriate
    // PTransform context. This ensures that user metrics obtain the pTransform ID when they are
    // created. Also use the ExecutionStateTracker and enter an appropriate state to track the
    // Process Bundle Execution time metric.
    // Note: That the consumer will be invoked concurrently by separate threads.
    FnDataReceiver<WindowedValue<T>> wrapAndEnableMetricContainer =
        (WindowedValue<T> input) -> {
          // This code is invoked CONCURRENTLY!
          final MetricsContainerImpl container =
              MetricsContainerStepMapEnvironment.getCurrent().getContainer(pTransformId);
          try (Closeable closeable = MetricsEnvironment.scopedMetricsContainer(container)) {
            try (Closeable trackerCloseable =
                ExecutionStateTracker.enterStateForCurrentTracker(state)) {
              consumer.accept(input);
            }
          }
        };
    super.register(pCollectionId, pTransformId, wrapAndEnableMetricContainer);
  }

  /**
   * New consumers should not be register()-ed after calling this method. This will cause a
   * RuntimeException, as this would fail to properly wrap the late-added consumer to the
   * ElementCountFnDataReceiver.
   *
   * @return A single ElementCountFnDataReceiver which directly wraps all the registered consumers.
   *     The returned consumer must be invoked in a scope with both an active ExecutionStateTracker
   *     and an active MetricsContainerStepMapEnvironment. Or else
   */
  @Override
  public FnDataReceiver<WindowedValue<?>> getMultiplexingConsumer(String pCollectionId) {
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer == null) {
      FnDataReceiver<WindowedValue<?>> multiplexingConsumer =
          super.getMultiplexingConsumer(pCollectionId);
      wrappedConsumer = new ElementCountFnDataReceiver(multiplexingConsumer, pCollectionId);
      pCollectionIdsToWrappedConsumer.put(pCollectionId, wrappedConsumer);
    }
    return wrappedConsumer;
  }

  /** @return Execution Time MonitoringInfos based on the tracked start or finish function. */
  public List<MonitoringInfo> getExecutionTimeMonitoringInfos() {
    return executionStates.getExecutionTimeMonitoringInfos();
  }
}
