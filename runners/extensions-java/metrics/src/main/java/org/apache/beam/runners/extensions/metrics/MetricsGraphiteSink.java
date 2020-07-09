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
package org.apache.beam.runners.extensions.metrics;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.regex.Pattern;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.metrics.MetricsSink;

/**
 * Sink to push metrics to Graphite. Graphite requires a timestamp. So metrics are reported with the
 * timestamp (seconds from epoch) when the push to the sink was done (except with gauges that
 * already have a timestamp value). The graphite metric name will be in the form of
 * beam.metricType.metricNamespace.metricName.[committed|attempted].metricValueType For example:
 * {@code beam.counter.throughput.nbRecords.attempted.value} Or {@code
 * beam.distribution.throughput.nbRecordsPerSec.attempted.mean}
 */
public class MetricsGraphiteSink implements MetricsSink {
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Pattern WHITESPACE = Pattern.compile("[\\s]+");
  private static final String SPACE_REPLACEMENT = "_";
  private final String address;
  private final int port;
  private final Charset charset;

  public MetricsGraphiteSink(MetricsOptions pipelineOptions) {
    this.address = pipelineOptions.getMetricsGraphiteHost();
    this.port = pipelineOptions.getMetricsGraphitePort();
    this.charset = UTF_8;
  }

  @Override
  public void writeMetrics(MetricQueryResults metricQueryResults) throws Exception {
    final long metricTimestamp = System.currentTimeMillis() / 1000L;
    Socket socket = new Socket(InetAddress.getByName(address), port);
    BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), charset));
    StringBuilder messagePayload = new StringBuilder();
    Iterable<MetricResult<Long>> counters = metricQueryResults.getCounters();
    Iterable<MetricResult<GaugeResult>> gauges = metricQueryResults.getGauges();
    Iterable<MetricResult<DistributionResult>> distributions =
        metricQueryResults.getDistributions();

    for (MetricResult<Long> counter : counters) {
      messagePayload.append(new CounterMetricMessage(counter, "value", metricTimestamp).toString());
    }

    for (MetricResult<GaugeResult> gauge : gauges) {
      messagePayload.append(new GaugeMetricMessage(gauge, "value").toString());
    }

    for (MetricResult<DistributionResult> distribution : distributions) {
      messagePayload.append(
          new DistributionMetricMessage(distribution, "min", metricTimestamp).toString());
      messagePayload.append(
          new DistributionMetricMessage(distribution, "max", metricTimestamp).toString());
      messagePayload.append(
          new DistributionMetricMessage(distribution, "count", metricTimestamp).toString());
      messagePayload.append(
          new DistributionMetricMessage(distribution, "sum", metricTimestamp).toString());
      messagePayload.append(
          new DistributionMetricMessage(distribution, "mean", metricTimestamp).toString());
    }
    writer.write(messagePayload.toString());
    writer.flush();
    writer.close();
    socket.close();
  }

  private abstract static class MetricMessage {
    @Override
    public String toString() {
      StringBuilder messagePayload = new StringBuilder();
      // if committed metrics are not supported, exception is thrown and we don't append the message
      try {
        messagePayload.append(createCommittedMessage());
      } catch (UnsupportedOperationException e) {
        if (!e.getMessage().contains("committed metrics")) {
          throw e;
        }
      }
      messagePayload.append(createAttemptedMessage());
      return messagePayload.toString();
    }

    protected abstract String createCommittedMessage();

    protected abstract String createAttemptedMessage();
  }

  private static class CounterMetricMessage extends MetricMessage {
    private String valueType;
    private MetricResult<Long> counter;
    private long metricTimestamp;

    private CounterMetricMessage(
        MetricResult<Long> counter, String valueType, long metricTimestamp) {
      this.valueType = valueType;
      this.counter = counter;
      this.metricTimestamp = metricTimestamp;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createCommittedMessage() {
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  counter, "counter", valueType, CommittedOrAttemped.COMMITTED),
              counter.getCommitted(),
              metricTimestamp);
      return metricMessage;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createAttemptedMessage() {
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  counter, "counter", valueType, CommittedOrAttemped.ATTEMPTED),
              counter.getAttempted(),
              metricTimestamp);
      return metricMessage;
    }
  }

  private static class GaugeMetricMessage extends MetricMessage {
    private String valueType;
    private MetricResult<GaugeResult> gauge;

    private GaugeMetricMessage(MetricResult<GaugeResult> gauge, String valueType) {
      this.valueType = valueType;
      this.gauge = gauge;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createCommittedMessage() {
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(gauge, "gauge", valueType, CommittedOrAttemped.COMMITTED),
              gauge.getCommitted().getValue(),
              gauge.getCommitted().getTimestamp().getMillis() / 1000L);
      return metricMessage;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createAttemptedMessage() {
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(gauge, "gauge", valueType, CommittedOrAttemped.ATTEMPTED),
              gauge.getAttempted().getValue(),
              gauge.getAttempted().getTimestamp().getMillis() / 1000L);
      return metricMessage;
    }
  }

  private static class DistributionMetricMessage extends MetricMessage {

    private String valueType;
    private MetricResult<DistributionResult> distribution;
    private long metricTimestamp;

    public DistributionMetricMessage(
        MetricResult<DistributionResult> distribution, String valueType, long metricTimestamp) {
      this.valueType = valueType;
      this.distribution = distribution;
      this.metricTimestamp = metricTimestamp;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createCommittedMessage() {
      Number value = null;
      switch (valueType) {
        case "min":
          value = distribution.getCommitted().getMin();
          break;
        case "max":
          value = distribution.getCommitted().getMax();
          break;
        case "count":
          value = distribution.getCommitted().getCount();
          break;
        case "sum":
          value = distribution.getCommitted().getSum();
          break;
        case "mean":
          value = distribution.getCommitted().getMean();
          break;
        default:
          break;
      }
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", valueType, CommittedOrAttemped.COMMITTED),
              value,
              metricTimestamp);
      return metricMessage;
    }

    @SuppressFBWarnings(
        value = "VA_FORMAT_STRING_USES_NEWLINE",
        justification = "\\n is part of graphite protocol")
    @Override
    protected String createAttemptedMessage() {
      Number value = null;
      switch (valueType) {
        case "min":
          value = distribution.getAttempted().getMin();
          break;
        case "max":
          value = distribution.getAttempted().getMax();
          break;
        case "count":
          value = distribution.getAttempted().getCount();
          break;
        case "sum":
          value = distribution.getAttempted().getSum();
          break;
        case "mean":
          value = distribution.getAttempted().getMean();
          break;
        default:
          break;
      }
      String metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", valueType, CommittedOrAttemped.ATTEMPTED),
              value,
              metricTimestamp);
      return metricMessage;
    }
  }

  private static <T> String createNormalizedMetricName(
      MetricResult<T> metric,
      String metricType,
      String valueType,
      CommittedOrAttemped committedOrAttemped) {
    String metricName =
        String.format(
            "beam.%s.%s.%s.%s.%s.%s",
            metricType,
            metric.getName().getNamespace(),
            metric.getName().getName(),
            metric.getKey().stepName(),
            committedOrAttemped,
            valueType);

    return WHITESPACE.matcher(metricName).replaceAll(SPACE_REPLACEMENT);
  }

  private enum CommittedOrAttemped {
    COMMITTED("committed"),
    ATTEMPTED("attempted");

    private final String committedOrAttempted;

    CommittedOrAttemped(String committedOrAttempted) {
      this.committedOrAttempted = committedOrAttempted;
    }

    @Override
    public String toString() {
      return committedOrAttempted;
    }
  }
}
