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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMapEnvironment;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricsBeamFnDataClient} introduces a so that they can be consumed and processed in the
 * thread which calls @{link #drainAndBlock}.
 */
public class MetricsBeamFnDataClient implements BeamFnDataClient {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsBeamFnDataClient.class);

  private final BeamFnDataClient mainClient;
  private final ConcurrentHashMap<InboundDataClient, Object> inboundDataClients;
  private final ConcurrentLinkedQueue<MetricsContainerStepMap> metricsContainerStepMaps;

  public MetricsBeamFnDataClient(BeamFnDataClient mainClient) {
    this.mainClient = mainClient;
    this.inboundDataClients = new ConcurrentHashMap<>();
    this.metricsContainerStepMaps = new ConcurrentLinkedQueue<>();
  }

  @Override
  public <T> InboundDataClient receive(
      ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> consumer) {
    LOG.debug(
        "Registering consumer for instruction {} and target {}",
        inputLocation.getInstructionId(),
        inputLocation.getTarget());

    MetricsFnDataReceiver<T> metricsConsumer = new MetricsFnDataReceiver<T>(consumer);
    InboundDataClient inboundDataClient =
        this.mainClient.receive(apiServiceDescriptor, inputLocation, coder, metricsConsumer);
    this.inboundDataClients.computeIfAbsent(
        inboundDataClient, (InboundDataClient idcToStore) -> idcToStore);
    return inboundDataClient;
  }

  // Returns true if all the InboundDataClients have finished or cancelled.
  private boolean allDone() {
    for (InboundDataClient inboundDataClient : inboundDataClients.keySet()) {
      if (!inboundDataClient.isDone()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Drains the internal queue of this class, by waiting for all WindowedValues to be passed to
   * their consumers. The thread which wishes to process() the elements should call this method, as
   * this will cause the consumers to invoke element processing. All receive() and send() calls must
   * be made prior to calling drainAndBlock, in order to properly terminate.
   *
   * <p>All {@link InboundDataClient}s will be failed if processing throws an exception.
   *
   * <p>This method is NOT thread safe. This should only be invoked by a single thread, and is
   * intended for use with a newly constructed MetricsBeamFnDataClient in {@link
   * ProcessBundleHandler#processBundle(InstructionRequest)}.
   */
  public void waitTillDone() throws Exception {
    while (true) {
      if (allDone()) {
        break;
      }
      Thread.sleep(100);
    }
  }

  /** @return The MonitoringInfos from all the merged MetricContainerStepMaps */
  public Iterable<MonitoringInfo> getMonitoringInfos() {
    MetricsContainerStepMap merged = new MetricsContainerStepMap();
    for (MetricsContainerStepMap metricMap : metricsContainerStepMaps) {
      merged.updateAll(metricMap);
    }
    return merged.getMonitoringInfos();
  }

  @Override
  public <T> CloseableFnDataReceiver<WindowedValue<T>> send(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      LogicalEndpoint outputLocation,
      Coder<WindowedValue<T>> coder) {
    LOG.debug(
        "Creating output consumer for instruction {} and target {}",
        outputLocation.getInstructionId(),
        outputLocation.getTarget());
    return this.mainClient.send(apiServiceDescriptor, outputLocation, coder);
  }

  /**
   * The MetricsFnDataReceiver is a a FnDataReceiver used by the MetricsBeamFnDataClient.
   *
   * <p>All {@link #accept accept()ed} values will be put onto a synchronous queue which will cause
   * the calling thread to block until {@link MetricsBeamFnDataClient#drainAndBlock} is called.
   * {@link MetricsBeamFnDataClient#drainAndBlock} is responsible for processing values from the
   * queue.
   */
  public class MetricsFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {
    private final FnDataReceiver<WindowedValue<T>> consumer;

    public MetricsFnDataReceiver(FnDataReceiver<WindowedValue<T>> consumer) {
      this.consumer = consumer;
    }

    /**
     * This method is thread safe, we expect multiple threads to call this, passing in data when new
     * data arrives via the MetricsBeamFnDataClient's mainClient.
     *
     * <p>This code is invoked CONCURRENTLY, by separate threads.
     */
    @Override
    public void accept(WindowedValue<T> value) throws Exception {
      try {
        try (Closeable close = MetricsContainerStepMapEnvironment.setupMetricEnvironment()) {
          metricsContainerStepMaps.add(MetricsContainerStepMapEnvironment.getCurrent());
          this.consumer.accept(value);
        }
      } catch (Exception e) {
        LOG.error("Client failed to dequeue and process WindowedValue", e);
        for (InboundDataClient inboundDataClient : inboundDataClients.keySet()) {
          inboundDataClient.fail(e);
        }
        throw e;
      }
    }
  }
}
