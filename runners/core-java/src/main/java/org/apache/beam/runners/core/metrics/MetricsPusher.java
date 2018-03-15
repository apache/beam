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

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;

/**
 * Component that regularly merges metrics and pushes them to a metrics sink.
 */
public class MetricsPusher implements Serializable {

  private MetricsSink metricsSink;
  private long period;
  @Nullable
  private transient ScheduledFuture<?> scheduledFuture;
  private transient PipelineResult pipelineResult;
  private MetricsContainerStepMap metricsContainerStepMap;

  public MetricsPusher(
      MetricsContainerStepMap metricsContainerStepMap,
      PipelineOptions pipelineOptions,
      PipelineResult pipelineResult) {
    this.metricsContainerStepMap = metricsContainerStepMap;
    this.pipelineResult = pipelineResult;
    period = pipelineOptions.getMetricsPushPeriod();
    // calls the constructor of MetricsSink implementation specified in
    // pipelineOptions.getMetricsSink() passing the pipelineOptions
    metricsSink =
        InstanceBuilder.ofType(MetricsSink.class)
            .fromClass(pipelineOptions.getMetricsSink())
            .withArg(PipelineOptions.class, pipelineOptions)
            .build();
  }

  public void start() {
    if (!(metricsSink instanceof NoOpMetricsSink)) {
      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(false).setNameFormat("MetricsPusher-thread").build());
      scheduledFuture =
          scheduler.scheduleAtFixedRate(() -> run(), 0, period, TimeUnit.SECONDS);
    }
  }
  private void tearDown(){
    pushMetrics();
    if (!scheduledFuture.isCancelled()) {
      scheduledFuture.cancel(true);
    }
  }

  private void run(){
    pushMetrics();
    if (pipelineResult != null) {
      PipelineResult.State pipelineState = pipelineResult.getState();
      if (pipelineState.isTerminal()) {
        tearDown();
      }
    }
  }

  private void pushMetrics(){
    if (!(metricsSink instanceof NoOpMetricsSink)) {
      try {
        // merge metrics
          MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainerStepMap);
          MetricQueryResults metricQueryResults = metricResults
              .queryMetrics(MetricsFilter.builder().build());
        if ((Iterables.size(metricQueryResults.distributions()) != 0)
            || (Iterables.size(metricQueryResults.gauges()) != 0)
            || (Iterables.size(metricQueryResults.counters()) != 0)) {
            metricsSink.writeMetrics(metricQueryResults);
          }

      } catch (Exception e) {
        MetricsPushException metricsPushException = new MetricsPushException(e);
        metricsPushException.printStackTrace();
      }
    }
  }

  /**
   * Exception related to MetricsPusher to wrap technical exceptions.
   */
  public static class MetricsPushException extends Exception{
    MetricsPushException(Throwable cause) {
      super(cause);
    }
  }
}
