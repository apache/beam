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
import java.util.Set;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ListMultimap;

/**
 * The {@code PCollectionConsumerRegistry} is used to maintain a collection of consuming
 * FnDataReceiver for each pCollectionId. Registering with this class allows inserting an element
 * count counter for every pCollection. A combined MultiplexingConsumer (Wrapped with an
 * ElementCountFnDataReceiver) is returned by calling getMultiplexingConsumer.
 */
public class PCollectionConsumerRegistry {

  private ListMultimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers;
  private Map<String, ElementCountFnDataReceiver> pCollectionIdsToWrappedConsumer;
  private MetricsContainerStepMap metricsContainerRegistry;

  public PCollectionConsumerRegistry(MetricsContainerStepMap metricsContainerRegistry) {
    this.metricsContainerRegistry = metricsContainerRegistry;
    this.pCollectionIdsToConsumers = ArrayListMultimap.create();
    this.pCollectionIdsToWrappedConsumer = new HashMap<String, ElementCountFnDataReceiver>();
  }

  /**
   * Register the specified consumer to handle the elements in the pCollection associated with
   * pCollectionId. All consumers must be registered before extracting the combined consumer by
   * calling getMultiplexingConsumer(), or an exception will be thrown.
   *
   * @param pCollectionId
   * @param pTransformId
   * @param consumer
   * @param <T> the element type of the PCollection
   * @throws RuntimeException if {@code register()} is called after {@code
   *     getMultiplexingConsumer()} is called.
   */
  public <T> void register(
      String pCollectionId, String pTransformId, FnDataReceiver<WindowedValue<T>> consumer) {
    // Just save these consumers for now, but package them up later with an
    // ElementCountFnDataReceiver and possibly a MultiplexingFnDataReceiver
    // if there are multiple consumers.
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer != null) {
      throw new RuntimeException(
          "New consumers for a pCollectionId cannot be register()-d after "
              + "calling getMultiplexingConsumer.");
    }
    // Wrap the consumer with extra logic to set the metric container with the appropriate
    // PTransform context. This ensures that user metrics obtain the pTransform ID when they are
    // created.
    FnDataReceiver<WindowedValue<T>> wrapAndEnableMetricContainer =
        (WindowedValue<T> input) -> {
          MetricsContainerImpl container = metricsContainerRegistry.getContainer(pTransformId);
          try (Closeable closeable = MetricsEnvironment.scopedMetricsContainer(container)) {
            // TODO(ajamato): Set the proper state sampler state for ExecutionTime Metrics to use.
            consumer.accept(input);
          }
        };
    pCollectionIdsToConsumers.put(pCollectionId, (FnDataReceiver) wrapAndEnableMetricContainer);
  }

  /** @return the list of pcollection ids. */
  public Set<String> keySet() {
    return pCollectionIdsToConsumers.keySet();
  }

  /**
   * New consumers should not be register()-ed after calling this method. This will cause a
   * RuntimeException, as this would fail to properly wrap the late-added consumer to the
   * ElementCountFnDataReceiver.
   *
   * @return A single ElementCountFnDataReceiver which directly wraps all the registered consumers.
   */
  public FnDataReceiver<WindowedValue<?>> getMultiplexingConsumer(String pCollectionId) {
    ElementCountFnDataReceiver wrappedConsumer =
        pCollectionIdsToWrappedConsumer.getOrDefault(pCollectionId, null);
    if (wrappedConsumer == null) {
      List<FnDataReceiver<WindowedValue<?>>> consumers =
          pCollectionIdsToConsumers.get(pCollectionId);
      FnDataReceiver<WindowedValue<?>> consumer =
          MultiplexingFnDataReceiver.forConsumers(consumers);
      wrappedConsumer =
          new ElementCountFnDataReceiver(consumer, pCollectionId, metricsContainerRegistry);
      pCollectionIdsToWrappedConsumer.put(pCollectionId, wrappedConsumer);
    }
    return wrappedConsumer;
  }

  /**
   * @return the number of underlying consumers for a pCollectionId, some tests may wish to check
   *     this.
   */
  public List<FnDataReceiver<WindowedValue<?>>> getUnderlyingConsumers(String pCollectionId) {
    return pCollectionIdsToConsumers.get(pCollectionId);
  }
}
