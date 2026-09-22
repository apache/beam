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
package org.apache.beam.it.common.storage;

import static org.apache.beam.it.common.utils.ByteSizeUtils.formatBytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.dataflow.DefaultPipelineLauncher;
import org.apache.beam.it.common.dataflow.IOLoadTestBase;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for GCS IO load tests.
 *
 * <p>In addition to the runner/pipeline metrics collected by {@link IOLoadTestBase}, this class
 * collects the GCS client performance metrics (all counters and distributions whose name starts
 * with {@value #GCS_METRIC_PREFIX}) that are emitted by {@code GcsUtil} when the pipeline is run
 * with {@code --gcsPerformanceMetrics=true}.
 *
 * <p>Results are currently only reported to standard output (see {@link #printMetrics}); nothing is
 * persisted to BigQuery or InfluxDB.
 */
public class GcsIOLoadTestBase extends IOLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIOLoadTestBase.class);

  /** Prefix shared by all GCS client performance metrics. */
  public static final String GCS_METRIC_PREFIX = "gcs_";

  /**
   * Pipeline option that has to be enabled for the GCS client to report the {@value
   * #GCS_METRIC_PREFIX} metrics.
   */
  public static final String GCS_PERFORMANCE_METRICS_OPTION = "gcsPerformanceMetrics";

  /** Horizontal separator for console metrics table. */
  private static final String SEPARATOR =
      "================================================================================";

  /** Width of the metric name column. */
  private static final int NAME_COLUMN_WIDTH = 48;

  @Override
  protected Map<String, Double> getMetrics(
      PipelineLauncher.LaunchInfo launchInfo, MetricsConfiguration config)
      throws IOException, InterruptedException, ParseException {
    Map<String, Double> metrics = super.getMetrics(launchInfo, config);
    boolean hasGcsMetrics =
        metrics.keySet().stream().anyMatch(k -> k.startsWith(GCS_METRIC_PREFIX));
    PipelineResult pipelineResult = DefaultPipelineLauncher.getPipelineResult(launchInfo.jobId());
    if (!hasGcsMetrics && pipelineResult != null) {
      try {
        MetricQueryResults queryResults = pipelineResult.metrics().allMetrics();
        for (MetricResult<Long> counter : queryResults.getCounters()) {
          String name = counter.getName().getName();
          if (name != null && name.startsWith(GCS_METRIC_PREFIX)) {
            Long val = counter.getAttempted();
            if (val != null) {
              metrics.merge(name, val.doubleValue(), Double::sum);
            }
          }
        }
        for (MetricResult<DistributionResult> dist : queryResults.getDistributions()) {
          String name = dist.getName().getName();
          if (name != null && name.startsWith(GCS_METRIC_PREFIX)) {
            DistributionResult val = dist.getAttempted();
            if (val != null) {
              metrics.merge(name + "_COUNT", (double) val.getCount(), Double::sum);
              metrics.merge(name + "_SUM", (double) val.getSum(), Double::sum);
              metrics.merge(name + "_MIN", (double) val.getMin(), Math::min);
              metrics.merge(name + "_MAX", (double) val.getMax(), Math::max);
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Unable to query in-memory SDK metrics for job {}", launchInfo.jobId(), e);
      }
    }
    return metrics;
  }

  /**
   * Collects all the metrics of the job and prints them to standard output with clean alignment and
   * smart formatting.
   *
   * <p>This is intentionally used instead of {@code exportMetricsToBigQuery} while these tests are
   * still being developed: results are only reported to the console, nothing is persisted.
   */
  protected void printMetrics(
      PipelineLauncher.LaunchInfo launchInfo, MetricsConfiguration metricsConfig) {
    Map<String, Double> metrics;
    try {
      metrics = getMetrics(launchInfo, metricsConfig);
    } catch (Exception e) {
      LOG.warn("Unable to get metrics due to error", e);
      return;
    }

    StringBuilder report = new StringBuilder();
    report.append("\n").append(SEPARATOR).append("\n");
    report.append(String.format(Locale.US, "  PIPELINE METRICS (job %s)%n", launchInfo.jobId()));
    report.append(SEPARATOR).append("\n");

    if (metrics.isEmpty()) {
      report.append("  No metrics found.\n");
    } else {
      for (Map.Entry<String, Double> entry : new TreeMap<>(metrics).entrySet()) {
        report.append(
            String.format(
                Locale.US,
                "  %-" + NAME_COLUMN_WIDTH + "s : %s%n",
                entry.getKey(),
                formatMetricValue(entry.getKey(), entry.getValue())));
      }
    }
    report.append(SEPARATOR);
    System.out.println(report);
  }

  /** Formats a metric value according to its semantics (bytes, latency, integer count, etc.). */
  private static String formatMetricValue(String name, Double value) {
    if (value == null) {
      return "null";
    }
    String lower = name.toLowerCase(Locale.ROOT);

    // Byte counts
    if (lower.contains("bytes") && !lower.contains("persec")) {
      return formatBytes(value.longValue());
    }

    // Latency / duration
    if (lower.endsWith("_ms") || lower.endsWith("_msec")) {
      return String.format(Locale.US, "%,.1f ms", value);
    }
    if (lower.endsWith("sec") || lower.endsWith("seconds") || lower.endsWith("time")) {
      return String.format(Locale.US, "%,.2f s", value);
    }

    // Integer / Counter counts
    if (value == Math.floor(value) && !Double.isInfinite(value)) {
      return String.format(Locale.US, "%,d", value.longValue());
    }

    // Default scalar representation
    return String.format(Locale.US, "%,.3f", value);
  }
}
