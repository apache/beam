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
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.dataflow.IOLoadTestBase;
import org.apache.beam.it.common.utils.MetricsReport;
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
 * they are collected in a runner agnostic way by {@link MetricsReport}. Examples of collected
 * metrics include:
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
public class GcsIOLoadTestBase extends IOLoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(GcsIOLoadTestBase.class);

  /** Prefix shared by all GCS client performance metrics. */
  public static final String GCS_METRIC_PREFIX = "gcs_";

  /**
   * Pipeline option that has to be enabled for the GCS client to report the {@value
   * #GCS_METRIC_PREFIX} metrics.
   */
  public static final String GCS_PERFORMANCE_METRICS_OPTION = "gcsPerformanceMetrics";

  /** Selects the metrics this class reports, i.e. every {@value #GCS_METRIC_PREFIX} metric. */
  private static final Pattern GCS_METRICS =
      Pattern.compile(Pattern.quote(GCS_METRIC_PREFIX) + ".*");

  /** Title of the GCS section of the printed report. */
  private static final String GCS_SECTION_TITLE = "GCS METRICS";

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
    System.out.println(report);

    // Also print the GCS specific report, which includes the per step breakdown.
    printGcsMetrics(launchInfo.jobId());
  }

  /**
   * Collects the GCS client performance metrics of the given job, aggregated over all steps.
   *
   * @param jobId the id of the job to query
   * @return a map of GCS metric name to value, empty if no GCS metric was reported
   */
  protected Map<String, Double> getGcsMetrics(String jobId) {
    Map<String, Double> gcsMetrics = MetricsReport.collect(jobId, GCS_METRICS).toScalarMetrics();
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
    System.out.println(MetricsReport.collect(jobId, GCS_METRICS).format(GCS_SECTION_TITLE));
  }
}
