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
package org.apache.beam.sdk.testutils.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/** Provides methods for querying metrics from {@link PipelineResult} per namespace. */
public class MetricsReader {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MetricsReader.class);

  private enum DistributionType {
    MIN,
    MAX
  }

  private final PipelineResult result;

  private final String namespace;

  public MetricsReader(PipelineResult result, String namespace) {
    this.result = result;
    this.namespace = namespace;
  }

  /**
   * Return the current value for a long counter, or a default value if can't be retrieved. Note
   * this uses only attempted metrics because some runners don't support committed metrics.
   */
  public long getCounterMetric(String name, long defaultValue) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    Iterable<MetricResult<Long>> counters = metrics.getCounters();

    checkIfMetricResultIsUnique(name, counters);

    try {
      MetricResult<Long> metricResult = counters.iterator().next();
      return metricResult.getAttempted();
    } catch (NoSuchElementException e) {
      LOG.error("Failed to get metric {}, from namespace {}", name, namespace);
    }
    return defaultValue;
  }

  /**
   * Return start time metric by counting the difference between "now" and min value from a
   * distribution metric.
   */
  public long getStartTimeMetric(long now, String name) {
    return this.getTimestampMetric(now, this.getDistributionMetric(name, DistributionType.MIN, -1));
  }

  /**
   * Return end time metric by counting the difference between "now" and MAX value from a
   * distribution metric.
   */
  public long getEndTimeMetric(long now, String name) {
    return this.getTimestampMetric(now, this.getDistributionMetric(name, DistributionType.MAX, -1));
  }

  /**
   * Return the current value for a long counter, or a default value if can't be retrieved. Note
   * this uses only attempted metrics because some runners don't support committed metrics.
   */
  private long getDistributionMetric(String name, DistributionType distType, long defaultValue) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    Iterable<MetricResult<DistributionResult>> distributions = metrics.getDistributions();

    checkIfMetricResultIsUnique(name, distributions);

    try {
      MetricResult<DistributionResult> distributionResult = distributions.iterator().next();
      switch (distType) {
        case MIN:
          return distributionResult.getAttempted().getMin();
        case MAX:
          return distributionResult.getAttempted().getMax();
        default:
          return defaultValue;
      }
    } catch (NoSuchElementException e) {
      LOG.error("Failed to get distribution metric {} for namespace {}", name, namespace);
    }
    return defaultValue;
  }

  private <T> void checkIfMetricResultIsUnique(String name, Iterable<MetricResult<T>> metricResult)
      throws IllegalStateException {

    Preconditions.checkState(
        Iterables.size(metricResult) == 1,
        String.format("More than one metric matches name: %s in namespace %s.", name, namespace));
  }

  /** Return the current value for a time counter, or -1 if can't be retrieved. */
  private long getTimestampMetric(long now, long value) {
    // timestamp metrics are used to monitor time of execution of transforms.
    // If result timestamp metric is too far from now, consider that metric is erroneous

    if (Math.abs(value - now) > Duration.standardDays(10000).getMillis()) {
      return -1;
    }
    return value;
  }
}
