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
package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link org.apache.beam.sdk.metrics.MetricResults}, which takes static
 * {@link Iterable}s of counters, distributions, gauges, and stringsets, and serves queries by
 * applying {@link org.apache.beam.sdk.metrics.MetricsFilter}s linearly to them.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DefaultMetricResults extends MetricResults {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricResults.class);

  private final Iterable<MetricResult<Long>> counters;
  private final Iterable<MetricResult<DistributionResult>> distributions;
  private final Iterable<MetricResult<GaugeResult>> gauges;
  private final Iterable<MetricResult<StringSetResult>> stringSets;
  private final Iterable<MetricResult<HistogramData>> perWorkerHistograms;

  public DefaultMetricResults(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges,
      Iterable<MetricResult<StringSetResult>> stringSets,
      Iterable<MetricResult<HistogramData>> perWorkerHistograms) {
    LOG.info("xxx does this get here? DefaultMetricResults ");
    this.counters = counters;
    this.distributions = distributions;
    this.gauges = gauges;
    this.stringSets = stringSets;
    this.perWorkerHistograms = perWorkerHistograms;
  }

  @Override
  public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    return MetricQueryResults.create(
        Iterables.filter(counters, counter -> MetricFiltering.matches(filter, counter.getKey())),
        Iterables.filter(
            distributions, distribution -> MetricFiltering.matches(filter, distribution.getKey())),
        Iterables.filter(gauges, gauge -> MetricFiltering.matches(filter, gauge.getKey())),
        Iterables.filter(
            stringSets, stringSets -> MetricFiltering.matches(filter, stringSets.getKey())),
        Iterables.filter(
            perWorkerHistograms,
            perWorkerHistogram -> MetricFiltering.matches(filter, perWorkerHistogram.getKey())));
  }
}
