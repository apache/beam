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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/** Provides methods for querying metrics from {@link PipelineResult} per namespace. */
public class MetricsReader {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MetricsReader.class);

  private static final long ERRONEOUS_METRIC_VALUE = -1;

  private final PipelineResult result;

  private final String namespace;

  private final long now;

  @VisibleForTesting
  MetricsReader(PipelineResult result, String defaultNamespace, long now) {
    this.result = result;
    this.namespace = defaultNamespace;
    this.now = now;
  }

  public MetricsReader(PipelineResult result, String namespace) {
    this(result, namespace, System.currentTimeMillis());
  }

  public MetricsReader withNamespace(String namespace) {
    return new MetricsReader(result, namespace, now);
  }

  /**
   * Return the current value for a long counter, or -1 if can't be retrieved. Note this uses only
   * attempted metrics because some runners don't support committed metrics.
   */
  public long getCounterMetric(String name) {
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
    return ERRONEOUS_METRIC_VALUE;
  }

  /**
   * Return start time metric by counting the difference between "now" and min value from a
   * distribution metric.
   */
  public long getStartTimeMetric(String name) {
    Iterable<MetricResult<DistributionResult>> timeDistributions = getDistributions(name);
    return getLowestMin(timeDistributions);
  }

  public Collection<NamedTestResult> readAll(
      Set<Function<MetricsReader, NamedTestResult>> suppliers) {
    return suppliers.stream().map(supp -> supp.apply(this)).collect(Collectors.toSet());
  }

  private Long getLowestMin(Iterable<MetricResult<DistributionResult>> distributions) {
    Optional<Long> lowestMin =
        StreamSupport.stream(distributions.spliterator(), true)
            .map(element -> element.getAttempted().getMin())
            .filter(this::isCredible)
            .min(Long::compareTo);

    return lowestMin.orElse(ERRONEOUS_METRIC_VALUE);
  }

  /**
   * Return end time metric by counting the difference between "now" and MAX value from a
   * distribution metric.
   */
  public long getEndTimeMetric(String name) {
    Iterable<MetricResult<DistributionResult>> timeDistributions = getDistributions(name);
    return getGreatestMax(timeDistributions);
  }

  private Long getGreatestMax(Iterable<MetricResult<DistributionResult>> distributions) {
    Optional<Long> greatestMax =
        StreamSupport.stream(distributions.spliterator(), true)
            .map(element -> element.getAttempted().getMax())
            .filter(this::isCredible)
            .max(Long::compareTo);

    return greatestMax.orElse(ERRONEOUS_METRIC_VALUE);
  }

  private Iterable<MetricResult<DistributionResult>> getDistributions(String name) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(namespace, name))
                    .build());
    return metrics.getDistributions();
  }

  private <T> void checkIfMetricResultIsUnique(String name, Iterable<MetricResult<T>> metricResult)
      throws IllegalStateException {

    int resultCount = Iterables.size(metricResult);
    Preconditions.checkState(
        resultCount <= 1,
        "More than one metric result matches name: %s in namespace %s. Metric results count: %s",
        name,
        namespace,
        resultCount);
  }

  /**
   * timestamp metrics are used to monitor time of execution of transforms. If result timestamp
   * metric is too far from now, consider that metric is erroneous private boolean isCredible(long
   * value) {
   */
  private boolean isCredible(long value) {
    return (Math.abs(value - now) <= Duration.standardDays(10000).getMillis());
  }

  /**
   * Factory method. Returns instance of {@link MetricsReader}.
   *
   * @param results pipeline from which metrics are read from
   * @param namespace namespace is required due to fact methods uses it internally
   * @return instance of {@link MetricsReader}
   */
  public static MetricsReader ofResults(final PipelineResult results, final String namespace) {
    checkNotNull(results);
    checkNotNull(namespace);
    return new MetricsReader(results, namespace);
  }
}
