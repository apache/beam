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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Component that regularly merges metrics and pushes them to a metrics sink. It needs to be a
 * singleton because spark runner might call initAccumulators several times and this method calls
 * {@link MetricsPusher#init} and needs to be idempotent
 */
public class MetricsPusher implements Serializable {

  public static final long DEFAULT_PERIOD = 5L;
  private static volatile MetricsPusher instance;
  private static MetricsSink metricsSink;
  private static long period;
  private static List<MetricsContainerStepMap> metricsContainerStepMaps;
  private static ScheduledFuture<?> scheduledFuture;
  private static PipelineResult pipelineResult;


  private static void addMetricsContainerStepMap(MetricsContainerStepMap metricsContainerStepMap){
    if (metricsContainerStepMaps.indexOf(metricsContainerStepMap) == -1) {
      metricsContainerStepMaps.add(metricsContainerStepMap);
    }
  }

  public static synchronized void init(
      MetricsContainerStepMap metricsContainerStepMap, PipelineOptions pipelineOptions) {
    if (instance == null) {
      instance = new MetricsPusher();
      configureFromPipelineOptions(pipelineOptions);
      metricsContainerStepMaps = new ArrayList<>();
      start();
    }
    /*
      MetricsPusher.init will be called several times in a pipeline
      (e.g Flink calls it with each UDF with a different MetricsContainerStepMap instance).
      Then the singleton needs to register a new metricsContainerStepMap to merge
      */
    addMetricsContainerStepMap(metricsContainerStepMap);
  }

  private static void configureFromPipelineOptions(PipelineOptions pipelineOptions) {
    switch (pipelineOptions.getMetricsSink()) {
      case "org.apache.beam.runners.core.metrics.MetricsHttpSink":
        metricsSink = new MetricsHttpSink(pipelineOptions.getMetricsHttpSinkUrl());
        break;
      default:
        metricsSink = new DummyMetricsSink();
    }
    period =
        pipelineOptions.getMetricsPushPeriod() != null
            ? pipelineOptions.getMetricsPushPeriod()
            : DEFAULT_PERIOD;
  }

  /**
   * Lazily set the pipelineResult.
   * @param pr
   */
  public static void setPipelineResult(PipelineResult pr){
    pipelineResult = pr;
  }
  private static void start() {
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("MetricsPusher-thread")
                .build());
    scheduledFuture = scheduler
        .scheduleAtFixedRate(new PushingThread(), 0, period,
            TimeUnit.SECONDS);
  }
  private static synchronized void tearDown(){
    if (!scheduledFuture.isCancelled()) {
      scheduledFuture.cancel(true);
    }
    /*
    it is the end of the pipeline, so set the instance to null so that it can be initialized
    for the next pipeline runnin gin that JVM
    */
    instance = null;
  }

  public static void pushMetrics(){
    try {
      // merge metrics
      for (MetricsContainerStepMap metricsContainerStepMap : metricsContainerStepMaps) {
        MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainerStepMap);
        MetricQueryResults metricQueryResults =
            metricResults.queryMetrics(MetricsFilter.builder().build());
        if ((Iterables.size(metricQueryResults.distributions()) != 0)
            || (Iterables.size(metricQueryResults.gauges()) != 0)
            || (Iterables.size(metricQueryResults.counters()) != 0)) {
          metricsSink.writeMetrics(metricQueryResults);
        }
      }
      if (pipelineResult != null) {
        PipelineResult.State pipelineState = pipelineResult.getState();
        if (pipelineState.isTerminal()) {
          tearDown();
        }
      }

    } catch (Exception e) {
      MetricsPushException metricsPushException = new MetricsPushException(e);
      metricsPushException.printStackTrace();
    }
  }

  private static class PushingThread implements Runnable {

    @Override
    public void run() {
      pushMetrics();
    }
  }

  /**
   * Exception related to MetricsPusher to wrap technical exceptions
   */
  public static class MetricsPushException extends Exception{
    MetricsPushException(Throwable cause) {
      super(cause);
    }
  }
}
