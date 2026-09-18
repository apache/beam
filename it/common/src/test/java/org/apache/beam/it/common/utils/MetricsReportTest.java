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

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.junit.Test;

/** Unit tests for {@link MetricsReport}. */
public class MetricsReportTest {

  private static final Pattern GCS = Pattern.compile("gcs_.*");

  @Test
  public void testKeepsOnlyMatchingMetrics() {
    MetricQueryResults results =
        queryResults(
            Arrays.asList(
                counter("gcs_http_read_request_count", "Read", 3L),
                counter("elements_written", "Write", 99L)),
            Collections.emptyList());

    assertThat(MetricsReport.of(results, GCS).toScalarMetrics())
        .containsExactly("gcs_http_read_request_count", 3.0);
  }

  @Test
  public void testSumsCountersAcrossSteps() {
    MetricQueryResults results =
        queryResults(
            Arrays.asList(
                counter("gcs_http_read_request_count", "Read", 3L),
                counter("gcs_http_read_request_count", "Reshuffle", 4L)),
            Collections.emptyList());

    assertThat(MetricsReport.of(results, GCS).toScalarMetrics())
        .containsExactly("gcs_http_read_request_count", 7.0);
  }

  @Test
  public void testMergesDistributionsAcrossSteps() {
    MetricQueryResults results =
        queryResults(
            Collections.emptyList(),
            Arrays.asList(
                distribution(
                    "gcs_http_read_latency_ms", "Read", DistributionResult.create(30, 2, 10, 20)),
                distribution(
                    "gcs_http_read_latency_ms",
                    "Reshuffle",
                    DistributionResult.create(5, 1, 5, 5))));

    assertThat(MetricsReport.of(results, GCS).toScalarMetrics())
        .containsExactly(
            "gcs_http_read_latency_ms_COUNT", 3.0,
            "gcs_http_read_latency_ms_SUM", 35.0,
            "gcs_http_read_latency_ms_MIN", 5.0,
            "gcs_http_read_latency_ms_MAX", 20.0);
  }

  @Test
  public void testEmptyWhenNothingMatches() {
    MetricQueryResults results =
        queryResults(
            Collections.singletonList(counter("elements_written", "Write", 1L)),
            Collections.emptyList());

    MetricsReport report = MetricsReport.of(results, GCS);
    assertThat(report.isEmpty()).isTrue();
    assertThat(report.toScalarMetrics()).isEmpty();
    assertThat(report.format("GCS METRICS")).contains("No metrics matching gcs_.* found.");
  }

  @Test
  public void testEmptyWhenQueryFailed() {
    assertThat(MetricsReport.of(null, GCS).isEmpty()).isTrue();
  }

  @Test
  public void testFormatShowsTotalAndPerStepBreakdown() {
    MetricQueryResults results =
        queryResults(
            Arrays.asList(
                counter("gcs_http_read_wire_bytes_received", "Read", 2048L),
                counter("gcs_http_read_wire_bytes_received", "Reshuffle", 1024L)),
            Collections.emptyList());

    String report = MetricsReport.of(results, GCS).format("GCS METRICS");

    assertThat(report).contains("GCS METRICS");
    // A "bytes" metric is rendered as a byte count. The column padding is not asserted, only that
    // the total is present and precedes the per step breakdown.
    assertThat(report).contains("gcs_http_read_wire_bytes_received:");
    assertThat(report).contains("3,072 B (3.00 KB)");
    assertThat(report).contains("[Read]: 2,048 B (2.00 KB)");
    assertThat(report).contains("[Reshuffle]: 1,024 B (1.00 KB)");
    assertThat(report.indexOf("3,072 B")).isLessThan(report.indexOf("[Read]"));
  }

  @Test
  public void testFormatOmitsBreakdownForASingleGlobalStep() {
    MetricQueryResults results =
        queryResults(
            Collections.singletonList(counter("gcs_http_read_request_count", "", 7L)),
            Collections.emptyList());

    String report = MetricsReport.of(results, GCS).format("GCS METRICS");

    assertThat(report).contains("gcs_http_read_request_count:");
    assertThat(report).doesNotContain("[global]");
  }

  private static MetricResult<Long> counter(String name, String step, long value) {
    return MetricResult.create(
        MetricKey.create(step, MetricName.named("GcsHttp", name)), value, value);
  }

  private static MetricResult<DistributionResult> distribution(
      String name, String step, DistributionResult value) {
    return MetricResult.create(
        MetricKey.create(step, MetricName.named("GcsHttp", name)), value, value);
  }

  private static MetricQueryResults queryResults(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions) {
    return MetricQueryResults.create(
        counters,
        distributions,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }
}
