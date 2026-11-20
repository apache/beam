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
package org.apache.beam.runners.flink.metrics;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Util for invoking {@link Source.Reader} methods that might require a {@link MetricsContainerImpl}
 * to be active. Source.Reader decorator which registers {@link MetricsContainerImpl}. It updates
 * Flink metrics after start and advance.
 *
 * <p>Updating Flink metrics walks all the metrics of the task, which is expensive compared to
 * reading a single record. Flink metrics are therefore updated at most once every {@link
 * #FLINK_METRICS_UPDATE_INTERVAL_MILLIS}, on the first call, and whenever the reader has no more
 * data. The metrics reported in the pipeline result are collected separately and are not affected.
 */
public class ReaderInvocationUtil<OutputT, ReaderT extends Source.Reader<OutputT>> {

  static final long FLINK_METRICS_UPDATE_INTERVAL_MILLIS = 1000;

  private static final long FLINK_METRICS_UPDATE_INTERVAL_NANOS =
      TimeUnit.MILLISECONDS.toNanos(FLINK_METRICS_UPDATE_INTERVAL_MILLIS);

  private final String stepName;
  private final FlinkMetricContainerBase container;
  private final Boolean enableMetrics;
  private final LongSupplier nanoTime;
  private boolean metricsUpdated;
  private long lastMetricsUpdateNanos;

  public ReaderInvocationUtil(
      String stepName, PipelineOptions options, FlinkMetricContainerBase container) {
    this(stepName, options, container, System::nanoTime);
  }

  ReaderInvocationUtil(
      String stepName,
      PipelineOptions options,
      FlinkMetricContainerBase container,
      LongSupplier nanoTime) {
    FlinkPipelineOptions flinkPipelineOptions = options.as(FlinkPipelineOptions.class);
    this.stepName = stepName;
    this.enableMetrics = !flinkPipelineOptions.getDisableMetrics();
    this.container = container;
    this.nanoTime = nanoTime;
  }

  public boolean invokeStart(ReaderT reader) throws IOException {
    if (enableMetrics) {
      try (Closeable ignored =
          MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer(stepName))) {
        boolean result = reader.start();
        maybeUpdateMetrics(result);
        return result;
      }
    } else {
      return reader.start();
    }
  }

  public boolean invokeAdvance(ReaderT reader) throws IOException {
    if (enableMetrics) {
      try (Closeable ignored =
          MetricsEnvironment.scopedMetricsContainer(container.getMetricsContainer(stepName))) {
        boolean result = reader.advance();
        maybeUpdateMetrics(result);
        return result;
      }
    } else {
      return reader.advance();
    }
  }

  private void maybeUpdateMetrics(boolean dataAvailable) {
    long now = nanoTime.getAsLong();
    // Always update on the first call, so that the metrics are registered in Flink right away, and
    // when the reader has no data, so that the final values of a finished reader are reported.
    if (!metricsUpdated
        || !dataAvailable
        || now - lastMetricsUpdateNanos >= FLINK_METRICS_UPDATE_INTERVAL_NANOS) {
      container.updateMetrics(stepName);
      metricsUpdated = true;
      lastMetricsUpdateNanos = now;
    }
  }
}
