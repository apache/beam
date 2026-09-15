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
import org.checkerframework.checker.nullness.qual.Nullable;
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
 * <p>These metrics are regular Beam SDK metrics registered under the {@code GcsHttp} namespace, so
 * they are collected in a runner agnostic way through {@code
 * PipelineResult.metrics().allMetrics()}. Examples of collected metrics include:
 *
 * <ul>
 *   <li>{@code gcs_http_read_wire_bytes_received} / {@code gcs_http_write_wire_bytes_sent}
 *   <li>{@code gcs_http_read_request_count} / {@code gcs_http_write_request_count}
 *   <li>{@code gcs_http_read_request_count_ranged} / {@code gcs_http_read_request_count_unbounded}
 *   <li>{@code gcs_http_read_status_2xx} / {@code gcs_http_read_status_4xx} / {@code
 *       gcs_http_read_status_5xx} (and their write counterparts)
 * </ul>
 *
 * <p>Results are currently only reported to standard output (see {@link #printMetrics}); nothing is
 * persisted to BigQuery or InfluxDB.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/27438)
})
public class GcsIOLoadTestBase extends IOLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIOLoadTestBase.class);

  /** Prefix shared by all GCS client performance metrics. */
  public static final String GCS_METRIC_PREFIX = "gcs_";

  /**
   * Pipeline option that has to be enabled for the GCS client to report the {@value
   * #GCS_METRIC_PREFIX} metrics.
   */
  public static final String GCS_PERFORMANCE_METRICS_OPTION = "gcsPerformanceMetrics";

  /**
   * Returns all metrics of the job, including the GCS client performance metrics.
   *
   * <p>The GCS metrics are aggregated over all the steps of the pipeline so that they can be
   * reported as flat scalar values. The per step breakdown is printed by {@link #printGcsMetrics}.
   */
  @Override
  protected Map<String, Double> getMetrics(
      PipelineLauncher.LaunchInfo launchInfo, MetricsConfiguration config)
      throws IOException, InterruptedException, ParseException {
    Map<String, Double> metrics = super.getMetrics(launchInfo, config);
    metrics.putAll(getGcsMetrics(launchInfo.jobId()));
    return metrics;
  }

  /**
   * Collects all the metrics of the job and prints them to standard output.
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
    report.append("\n==========================================================\n");
    report.append(String.format(Locale.US, "  PIPELINE METRICS (job %s)%n", launchInfo.jobId()));
    report.append("==========================================================\n");
    if (metrics.isEmpty()) {
      report.append("  No metrics found.\n");
    } else {
      for (Map.Entry<String, Double> entry : new TreeMap<>(metrics).entrySet()) {
        report.append(
            String.format(Locale.US, "  %-46s %,.3f%n", entry.getKey() + ":", entry.getValue()));
      }
    }
    report.append("==========================================================");
    print(report.toString());

    // Also print the GCS specific report, which includes the per step breakdown.
    printGcsMetrics(launchInfo.jobId());
  }

  /**
   * Collects the GCS client performance metrics of the given job, aggregated over all steps.
   *
   * <p>Counters are summed up across steps. Distributions are reported as four separate scalar
   * metrics, suffixed with {@code _COUNT}, {@code _SUM}, {@code _MIN} and {@code _MAX}, which
   * matches how the Dataflow launcher reports distributions.
   *
   * @param jobId the id of the job to query
   * @return a map of GCS metric name to value, empty if no GCS metric was reported
   */
  protected Map<String, Double> getGcsMetrics(String jobId) {
    Map<String, Double> gcsMetrics = new TreeMap<>();
    Map<String, Map<String, Long>> counters = getGcsCountersByStep(jobId);
    for (Map.Entry<String, Map<String, Long>> entry : counters.entrySet()) {
      long total = entry.getValue().values().stream().mapToLong(Long::longValue).sum();
      gcsMetrics.put(entry.getKey(), (double) total);
    }

    Map<String, Map<String, DistributionResult>> distributions = getGcsDistributionsByStep(jobId);
    for (Map.Entry<String, Map<String, DistributionResult>> entry : distributions.entrySet()) {
      String name = entry.getKey();
      long count = 0;
      long sum = 0;
      Long min = null;
      Long max = null;
      for (DistributionResult distribution : entry.getValue().values()) {
        count += distribution.getCount();
        sum += distribution.getSum();
        min = (min == null) ? distribution.getMin() : Math.min(min, distribution.getMin());
        max = (max == null) ? distribution.getMax() : Math.max(max, distribution.getMax());
      }
      gcsMetrics.put(name + "_COUNT", (double) count);
      gcsMetrics.put(name + "_SUM", (double) sum);
      if (min != null) {
        gcsMetrics.put(name + "_MIN", (double) min);
      }
      if (max != null) {
        gcsMetrics.put(name + "_MAX", (double) max);
      }
    }

    if (gcsMetrics.isEmpty()) {
      LOG.warn(
          "No {}* metrics found for job {}. Make sure the pipeline was launched with --{}=true.",
          GCS_METRIC_PREFIX,
          jobId,
          GCS_PERFORMANCE_METRICS_OPTION);
    }
    return gcsMetrics;
  }

  /** Prints a human readable report of the GCS metrics, including the per step breakdown. */
  protected void printGcsMetrics(String jobId) {
    Map<String, Map<String, Long>> counters = getGcsCountersByStep(jobId);
    Map<String, Map<String, DistributionResult>> distributions = getGcsDistributionsByStep(jobId);

    StringBuilder report = new StringBuilder();
    report.append("\n==========================================================\n");
    report.append("                       GCS METRICS                        \n");
    report.append("==========================================================\n");
    if (counters.isEmpty() && distributions.isEmpty()) {
      report.append("  No ").append(GCS_METRIC_PREFIX).append("* metrics found.\n");
    } else {
      for (Map.Entry<String, Map<String, Long>> entry : counters.entrySet()) {
        String metricName = entry.getKey();
        Map<String, Long> stepMap = entry.getValue();
        long total = stepMap.values().stream().mapToLong(Long::longValue).sum();
        report.append(
            String.format(Locale.US, "  %-36s %s\n", metricName + ":", format(metricName, total)));
        if (stepMap.size() > 1 || (!stepMap.containsKey("global") && !stepMap.isEmpty())) {
          for (Map.Entry<String, Long> stepEntry : stepMap.entrySet()) {
            report.append(
                String.format(
                    Locale.US,
                    "    [%s]: %s\n",
                    stepEntry.getKey(),
                    format(metricName, stepEntry.getValue())));
          }
        }
      }
      for (Map.Entry<String, Map<String, DistributionResult>> entry : distributions.entrySet()) {
        String metricName = entry.getKey();
        for (Map.Entry<String, DistributionResult> stepEntry : entry.getValue().entrySet()) {
          DistributionResult d = stepEntry.getValue();
          report.append(
              String.format(
                  Locale.US,
                  "  %-36s count=%,d, sum=%,d, min=%,d, max=%,d, mean=%.2f [%s]\n",
                  metricName + ":",
                  d.getCount(),
                  d.getSum(),
                  d.getMin(),
                  d.getMax(),
                  d.getMean(),
                  stepEntry.getKey()));
        }
      }
    }
    report.append("==========================================================");
    print(report.toString());
  }

  /** Writes the report to standard output. */
  private static void print(String report) {
    System.out.println(report);
  }

  private static Map<String, Map<String, Long>> getGcsCountersByStep(String jobId) {
    Map<String, Map<String, Long>> countersByStep = new TreeMap<>();
    MetricQueryResults metricResults = queryAllMetrics(jobId);
    if (metricResults == null) {
      return countersByStep;
    }
    for (MetricResult<Long> counter : metricResults.getCounters()) {
      String name = counter.getName().getName();
      if (name == null || !name.startsWith(GCS_METRIC_PREFIX)) {
        continue;
      }
      Long value = getCommittedOrAttempted(counter);
      if (value != null) {
        countersByStep.computeIfAbsent(name, k -> new TreeMap<>()).put(stepOf(counter), value);
      }
    }
    return countersByStep;
  }

  private static Map<String, Map<String, DistributionResult>> getGcsDistributionsByStep(
      String jobId) {
    Map<String, Map<String, DistributionResult>> distributionsByStep = new TreeMap<>();
    MetricQueryResults metricResults = queryAllMetrics(jobId);
    if (metricResults == null) {
      return distributionsByStep;
    }
    for (MetricResult<DistributionResult> distribution : metricResults.getDistributions()) {
      String name = distribution.getName().getName();
      if (name == null || !name.startsWith(GCS_METRIC_PREFIX)) {
        continue;
      }
      DistributionResult value = getCommittedOrAttempted(distribution);
      if (value != null) {
        distributionsByStep
            .computeIfAbsent(name, k -> new TreeMap<>())
            .put(stepOf(distribution), value);
      }
    }
    return distributionsByStep;
  }

  private static @Nullable MetricQueryResults queryAllMetrics(String jobId) {
    PipelineResult result = DefaultPipelineLauncher.getPipelineResult(jobId);
    if (result == null) {
      LOG.warn("No PipelineResult available for job {}, skipping GCS metrics.", jobId);
      return null;
    }
    try {
      return result.metrics().allMetrics();
    } catch (Exception e) {
      LOG.warn("Unable to query pipeline metrics for job {}: ", jobId, e);
      return null;
    }
  }

  private static String stepOf(MetricResult<?> metricResult) {
    String step = metricResult.getKey().stepName();
    return (step == null || step.isEmpty()) ? "global" : step;
  }

  private static <T> @Nullable T getCommittedOrAttempted(MetricResult<T> metricResult) {
    try {
      T committed = metricResult.getCommitted();
      if (committed != null) {
        return committed;
      }
    } catch (UnsupportedOperationException e) {
      // Runner does not support committed metrics; fall back to attempted.
    }
    try {
      return metricResult.getAttempted();
    } catch (UnsupportedOperationException e) {
      return null;
    }
  }

  private static String format(String metricName, long value) {
    return metricName.contains("bytes")
        ? formatBytes(value)
        : String.format(Locale.US, "%,d", value);
  }

  private static String formatBytes(long bytes) {
    if (bytes >= 1024L * 1024L * 1024L) {
      return String.format(Locale.US, "%,d B (%.2f GB)", bytes, bytes / (1024.0 * 1024.0 * 1024.0));
    } else if (bytes >= 1024L * 1024L) {
      return String.format(Locale.US, "%,d B (%.2f MB)", bytes, bytes / (1024.0 * 1024.0));
    } else if (bytes >= 1024L) {
      return String.format(Locale.US, "%,d B (%.2f KB)", bytes, bytes / 1024.0);
    }
    return String.format(Locale.US, "%,d B", bytes);
  }
}
