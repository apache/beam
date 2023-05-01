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
package org.apache.beam.runners.samza.metrics;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SlidingTimeWindowReservoir;
import org.apache.samza.metrics.Timer;

/**
 * Metrics like throughput, latency and watermark progress for each Beam transform for Samza Runner.
 */
@SuppressWarnings("return")
public class SamzaTransformMetrics implements Serializable {
  private static final String ENABLE_TASK_METRICS = "runner.samza.transform.enable.task.metrics";

  private static final int DEFAULT_LOOKBACK_TIMER_WINDOW_SIZE_MS = 180000;
  private static final String GROUP = "SamzaBeamTransformMetrics";
  private static final String METRIC_NAME_PATTERN = "%s-%s";
  private static final String TRANSFORM_LATENCY_METRIC = "handle-message-ns";
  private static final String TRANSFORM_WATERMARK_PROGRESS = "output-watermark-ms";
  private static final String TRANSFORM_IP_THROUGHPUT = "num-input-messages";
  private static final String TRANSFORM_OP_THROUGHPUT = "num-output-messages";

  // Transform name to metric maps
  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Timer> transformLatency;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Gauge<Long>> transformWatermarkProgress;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Counter> transformInputThroughput;

  @SuppressFBWarnings("SE_BAD_FIELD")
  private final Map<String, Counter> transformOutputThroughPut;

  public SamzaTransformMetrics() {
    this.transformLatency = new ConcurrentHashMap<>();
    this.transformOutputThroughPut = new ConcurrentHashMap<>();
    this.transformWatermarkProgress = new ConcurrentHashMap<>();
    this.transformInputThroughput = new ConcurrentHashMap<>();
  }

  public void register(String transformName, Context ctx) {
    // Output Watermark metric per transform will always be per transform, per task, since per
    // container output watermark is not useful for debugging
    transformWatermarkProgress.putIfAbsent(
        transformName,
        ctx.getTaskContext()
            .getTaskMetricsRegistry()
            .newGauge(
                GROUP, getMetricNameWithPrefix(TRANSFORM_WATERMARK_PROGRESS, transformName), 0L));

    // Latency, throughput metrics can be per container (default) or per task
    final boolean enablePerTaskMetrics =
        ctx.getJobContext().getConfig().getBoolean(ENABLE_TASK_METRICS, false);
    final MetricsRegistry metricsRegistry =
        enablePerTaskMetrics
            ? ctx.getTaskContext().getTaskMetricsRegistry()
            : ctx.getContainerContext().getContainerMetricsRegistry();
    transformLatency.putIfAbsent(
        transformName,
        metricsRegistry.newTimer(GROUP, getTimerWithCustomizedLookBackWindow(transformName)));
    transformOutputThroughPut.putIfAbsent(
        transformName,
        metricsRegistry.newCounter(
            GROUP, getMetricNameWithPrefix(TRANSFORM_OP_THROUGHPUT, transformName)));
    transformInputThroughput.putIfAbsent(
        transformName,
        metricsRegistry.newCounter(
            GROUP, getMetricNameWithPrefix(TRANSFORM_IP_THROUGHPUT, transformName)));
  }

  public Timer getTransformLatencyMetric(String transformName) {
    return transformLatency.get(transformName);
  }

  public Counter getTransformInputThroughput(String transformName) {
    return transformInputThroughput.get(transformName);
  }

  public Counter getTransformOutputThroughput(String transformName) {
    return transformOutputThroughPut.get(transformName);
  }

  public Gauge<Long> getTransformWatermarkProgress(String transformName) {
    return transformWatermarkProgress.get(transformName);
  }

  // Customize in-memory window size for timer, default from samza is 5 mins which causes memory
  // pressure if a lot of timers are registered
  private static Timer getTimerWithCustomizedLookBackWindow(String transformName) {
    return new Timer(
        getMetricNameWithPrefix(TRANSFORM_LATENCY_METRIC, transformName),
        new SlidingTimeWindowReservoir(DEFAULT_LOOKBACK_TIMER_WINDOW_SIZE_MS));
  }

  private static String getMetricNameWithPrefix(String metricName, String transformName) {
    // Replace all non-alphanumeric characters with underscore
    final String samzaSafeMetricName = transformName.replaceAll("[^A-Za-z0-9_]", "_");
    return String.format(METRIC_NAME_PATTERN, samzaSafeMetricName, metricName);
  }
}
