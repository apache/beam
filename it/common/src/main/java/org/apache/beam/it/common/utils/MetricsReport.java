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
package org.apache.beam.it.common.utils;

import static org.apache.beam.it.common.utils.ByteSizeUtils.formatBytes;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.beam.it.common.dataflow.DefaultPipelineLauncher;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The metrics of a job whose names match a pattern, ready to be consumed or reported as a section.
 *
 * <p>The metrics are collected in a runner agnostic way through {@code
 * PipelineResult.metrics().allMetrics()}, kept only when their name matches, and grouped by metric
 * name and then by step. A load test typically wants both views of that data:
 *
 * <pre>{@code
 * MetricsReport gcs = MetricsReport.collect(jobId, Pattern.compile("gcs_.*"));
 * Map<String, Double> scalars = gcs.toScalarMetrics(); // to export
 * System.out.println(gcs.format("GCS METRICS")); // to read
 * }</pre>
 *
 * <p>Several sections can be built out of a single query, which is worth doing because {@code
 * allMetrics()} is a remote call on Dataflow:
 *
 * <pre>{@code
 * MetricQueryResults results = MetricsReport.queryAllMetrics(jobId);
 * String report =
 *     MetricsReport.of(results, Pattern.compile("gcs_http_read_.*")).format("GCS READ")
 *         + MetricsReport.of(results, Pattern.compile("gcs_http_write_.*")).format("GCS WRITE");
 * }</pre>
 */
public final class MetricsReport {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsReport.class);

  /** Step name used for a metric that the runner did not attribute to any step. */
  private static final String GLOBAL_STEP = "global";

  /** Horizontal rule of a formatted section, its length is the width of the section. */
  private static final String SEPARATOR =
      "==========================================================";

  /** Width of the metric name column of a formatted section. */
  private static final int NAME_COLUMN_WIDTH = 36;

  private final Pattern namePattern;
  private final Map<String, Map<String, Long>> counters;
  private final Map<String, Map<String, DistributionResult>> distributions;

  private MetricsReport(
      Pattern namePattern,
      Map<String, Map<String, Long>> counters,
      Map<String, Map<String, DistributionResult>> distributions) {
    this.namePattern = namePattern;
    this.counters = counters;
    this.distributions = distributions;
  }

  /**
   * Collects the metrics of a job whose names match the given pattern.
   *
   * @param jobId the id of the job to query
   * @param namePattern pattern the metric name has to match entirely, e.g. {@code gcs_.*}
   * @return the matching metrics, empty when the job reported none or when the query failed
   */
  public static MetricsReport collect(String jobId, Pattern namePattern) {
    return of(queryAllMetrics(jobId), namePattern);
  }

  /**
   * Same as {@link #collect}, from an already executed query, so that several sections can share a
   * single query.
   *
   * @param metricResults the metrics of a job, {@code null} when the query failed
   * @param namePattern pattern the metric name has to match entirely, e.g. {@code gcs_.*}
   * @return the matching metrics, empty when none matched or when {@code metricResults} is null
   */
  public static MetricsReport of(@Nullable MetricQueryResults metricResults, Pattern namePattern) {
    if (metricResults == null) {
      return new MetricsReport(namePattern, new TreeMap<>(), new TreeMap<>());
    }
    return new MetricsReport(
        namePattern,
        byStep(metricResults.getCounters(), namePattern),
        byStep(metricResults.getDistributions(), namePattern));
  }

  /**
   * Queries every metric of a job.
   *
   * @param jobId the id of the job to query
   * @return all the metrics of the job, or {@code null} when they are not available
   */
  public static @Nullable MetricQueryResults queryAllMetrics(String jobId) {
    PipelineResult result = DefaultPipelineLauncher.getPipelineResult(jobId);
    if (result == null) {
      LOG.warn("No PipelineResult available for job {}, skipping its metrics.", jobId);
      return null;
    }
    try {
      return result.metrics().allMetrics();
    } catch (Exception e) {
      LOG.warn("Unable to query pipeline metrics for job {}: ", jobId, e);
      return null;
    }
  }

  /** Whether no metric matched the pattern. */
  public boolean isEmpty() {
    return counters.isEmpty() && distributions.isEmpty();
  }

  /**
   * Flat scalar view of the metrics.
   *
   * <p>Counters are summed over the steps. Distributions are expanded into four scalars, suffixed
   * with {@code _COUNT}, {@code _SUM}, {@code _MIN} and {@code _MAX}, which matches how the
   * Dataflow launcher reports distributions.
   *
   * @return a map of metric name to value, sorted by name
   */
  public Map<String, Double> toScalarMetrics() {
    Map<String, Double> scalars = new TreeMap<>();
    counters.forEach((name, byStep) -> scalars.put(name, (double) totalOf(byStep)));
    distributions.forEach(
        (name, byStep) -> {
          DistributionResult merged = mergeDistributions(byStep.values());
          scalars.put(name + "_COUNT", (double) merged.getCount());
          scalars.put(name + "_SUM", (double) merged.getSum());
          scalars.put(name + "_MIN", (double) merged.getMin());
          scalars.put(name + "_MAX", (double) merged.getMax());
        });
    return scalars;
  }

  /**
   * Renders the metrics as a titled section: the total of every counter, its per step breakdown
   * when it has one, and one line per step for every distribution.
   *
   * <p>A metric whose name contains {@code bytes} is printed as a byte count, see {@link
   * ByteSizeUtils#formatBytes}.
   *
   * @param title title of the section, centered in its banner
   * @return the section, without a trailing newline
   */
  public String format(String title) {
    StringBuilder report = new StringBuilder();
    report.append("\n").append(SEPARATOR).append("\n");
    report.append(center(title)).append("\n");
    report.append(SEPARATOR).append("\n");
    if (isEmpty()) {
      report.append("  No metrics matching ").append(namePattern).append(" found.\n");
    } else {
      counters.forEach((name, byStep) -> appendCounter(report, name, byStep));
      distributions.forEach((name, byStep) -> appendDistribution(report, name, byStep));
    }
    report.append(SEPARATOR);
    return report.toString();
  }

  /**
   * Keeps the metrics whose name matches the pattern and groups them by metric name and then by
   * step. Counters and distributions only differ by their value type, so both are collected here.
   */
  private static <T> Map<String, Map<String, T>> byStep(
      Iterable<MetricResult<T>> results, Pattern namePattern) {
    Map<String, Map<String, T>> byStep = new TreeMap<>();
    for (MetricResult<T> result : results) {
      String name = result.getName().getName();
      if (name == null || !namePattern.matcher(name).matches()) {
        continue;
      }
      T value = getCommittedOrAttempted(result);
      if (value != null) {
        byStep.computeIfAbsent(name, k -> new TreeMap<>()).put(stepOf(result), value);
      }
    }
    return byStep;
  }

  /** Appends the total of one counter, followed by its per step breakdown when there is one. */
  private static void appendCounter(
      StringBuilder report, String metricName, Map<String, Long> byStep) {
    report.append(
        String.format(
            Locale.US,
            "  %-" + NAME_COLUMN_WIDTH + "s %s%n",
            metricName + ":",
            format(metricName, totalOf(byStep))));
    // A lone global step is the total that was just printed, so it is not repeated.
    if (byStep.size() > 1 || !byStep.containsKey(GLOBAL_STEP)) {
      byStep.forEach(
          (step, value) ->
              report.append(
                  String.format(Locale.US, "    [%s]: %s%n", step, format(metricName, value))));
    }
  }

  /** Appends one line per step for one distribution. */
  private static void appendDistribution(
      StringBuilder report, String metricName, Map<String, DistributionResult> byStep) {
    byStep.forEach(
        (step, distribution) ->
            report.append(
                String.format(
                    Locale.US,
                    "  %-"
                        + NAME_COLUMN_WIDTH
                        + "s count=%,d, sum=%,d, min=%,d, max=%,d,"
                        + " mean=%.2f [%s]%n",
                    metricName + ":",
                    distribution.getCount(),
                    distribution.getSum(),
                    distribution.getMin(),
                    distribution.getMax(),
                    distribution.getMean(),
                    step)));
  }

  /** Sum of one counter over all the steps it was reported for. */
  private static long totalOf(Map<String, Long> byStep) {
    return byStep.values().stream().mapToLong(Long::longValue).sum();
  }

  /** Combines the per step distributions of one metric into a single distribution. */
  private static DistributionResult mergeDistributions(
      Collection<DistributionResult> distributions) {
    long count = 0;
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (DistributionResult distribution : distributions) {
      count += distribution.getCount();
      sum += distribution.getSum();
      min = Math.min(min, distribution.getMin());
      max = Math.max(max, distribution.getMax());
    }
    return DistributionResult.create(sum, count, min, max);
  }

  /** Step a metric was reported for, {@value #GLOBAL_STEP} when the runner did not name one. */
  private static String stepOf(MetricResult<?> metricResult) {
    String step = metricResult.getKey().stepName();
    return (step == null || step.isEmpty()) ? GLOBAL_STEP : step;
  }

  /** Committed value of a metric, falling back to the attempted one for the runners without it. */
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

  /** Formats a metric value, as a byte count when the metric name says so. */
  private static String format(String metricName, long value) {
    return metricName.contains("bytes")
        ? formatBytes(value)
        : String.format(Locale.US, "%,d", value);
  }

  /** Centers a title within the width of the section. */
  private static String center(String title) {
    if (title.length() >= SEPARATOR.length()) {
      return title;
    }
    int leading = (SEPARATOR.length() - title.length()) / 2;
    return Strings.padEnd(
        Strings.padStart(title, leading + title.length(), ' '), SEPARATOR.length(), ' ');
  }
}
