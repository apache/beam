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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsSink;
import org.apache.beam.sdk.options.PipelineOptions;

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

  public MetricsGraphiteSink(PipelineOptions pipelineOptions) {
    this.address = pipelineOptions.getMetricsGraphiteHost();
    this.port = pipelineOptions.getMetricsGraphitePort();
    this.charset = UTF_8;
  }

  @Experimental(Experimental.Kind.METRICS)
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
      // if committed metrics are not supported, exception is thrown and we don't append the message
      try {
        messagePayload.append(createCounterGraphiteMessage(metricTimestamp, counter, true));
      } catch (UnsupportedOperationException e) {
        if (!e.getMessage().contains("committed metrics")) {
          throw e;
        }
      }
      messagePayload.append(createCounterGraphiteMessage(metricTimestamp, counter, false));
    }

    for (MetricResult<GaugeResult> gauge : gauges) {
      try {
        messagePayload.append(createGaugeGraphiteMessage(gauge, true));
      } catch (UnsupportedOperationException e) {
        if (!e.getMessage().contains("committed metrics")) {
          throw e;
        }
      }
      messagePayload.append(createGaugeGraphiteMessage(gauge, false));
    }

    for (MetricResult<DistributionResult> distribution : distributions) {
      try {
        messagePayload.append(
            createDistributionGraphiteMessage(metricTimestamp, distribution, true));
      } catch (UnsupportedOperationException e) {
        if (!e.getMessage().contains("committed metrics")) {
          throw e;
        }
      }
      messagePayload.append(
          createDistributionGraphiteMessage(metricTimestamp, distribution, false));
    }
    writer.write(messagePayload.toString());
    writer.flush();
    writer.close();
    socket.close();
  }

  @SuppressFBWarnings(
    value = "VA_FORMAT_STRING_USES_NEWLINE",
    justification = "\\n is part of graphite protocol"
  )
  private String createCounterGraphiteMessage(
      long metricTimestamp, MetricResult<Long> counter, boolean committedValue) {
    String metricMessage;
    if (committedValue) {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  counter, "counter", "value", CommittedOrAttemped.COMMITTED),
              counter.getCommitted(),
              metricTimestamp);
    } else {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  counter, "counter", "value", CommittedOrAttemped.ATTEMPTED),
              counter.getAttempted(),
              metricTimestamp);
    }
    return metricMessage;
  }

  @SuppressFBWarnings(
    value = "VA_FORMAT_STRING_USES_NEWLINE",
    justification = "\\n is part of graphite protocol"
  )
  private String createGaugeGraphiteMessage(
      MetricResult<GaugeResult> gauge, boolean committedValue) {
    String metricMessage;
    if (committedValue) {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(gauge, "gauge", "value", CommittedOrAttemped.COMMITTED),
              gauge.getCommitted().getValue(),
              gauge.getCommitted().getTimestamp().getMillis() / 1000L);
    } else {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(gauge, "gauge", "value", CommittedOrAttemped.ATTEMPTED),
              gauge.getAttempted().getValue(),
              gauge.getAttempted().getTimestamp().getMillis() / 1000L);
    }
    return metricMessage;
  }

  @SuppressFBWarnings(
    value = "VA_FORMAT_STRING_USES_NEWLINE",
    justification = "\\n is part of graphite protocol"
  )
  private String createDistributionGraphiteMessage(
      long metricTimestamp, MetricResult<DistributionResult> distribution, boolean committedValue) {
    StringBuilder messagePayload = new StringBuilder();
    String metricMessage;
    if (committedValue) {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "min", CommittedOrAttemped.COMMITTED),
              distribution.getCommitted().getMin(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "max", CommittedOrAttemped.COMMITTED),
              distribution.getCommitted().getMax(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "count", CommittedOrAttemped.COMMITTED),
              distribution.getCommitted().getCount(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "sum", CommittedOrAttemped.COMMITTED),
              distribution.getCommitted().getSum(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "mean", CommittedOrAttemped.COMMITTED),
              distribution.getCommitted().getMean(),
              metricTimestamp);
      messagePayload.append(metricMessage);
    } else {
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "min", CommittedOrAttemped.ATTEMPTED),
              distribution.getAttempted().getMin(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "max", CommittedOrAttemped.ATTEMPTED),
              distribution.getAttempted().getMax(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "count", CommittedOrAttemped.ATTEMPTED),
              distribution.getAttempted().getCount(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "sum", CommittedOrAttemped.ATTEMPTED),
              distribution.getAttempted().getSum(),
              metricTimestamp);
      messagePayload.append(metricMessage);
      metricMessage =
          String.format(
              Locale.US,
              "%s %s %s\n",
              createNormalizedMetricName(
                  distribution, "distribution", "mean", CommittedOrAttemped.ATTEMPTED),
              distribution.getAttempted().getMean(),
              metricTimestamp);
      messagePayload.append(metricMessage);
    }
    return messagePayload.toString();
  }

  private <T> String createNormalizedMetricName(
      MetricResult<T> metric,
      String metricType,
      String valueType,
      CommittedOrAttemped committedOrAttemped) {
    String metricName =
        String.format(
            "beam.%s.%s.%s.%s.%s",
            metricType,
            metric.getName().getNamespace(),
            metric.getName().getName(),
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
