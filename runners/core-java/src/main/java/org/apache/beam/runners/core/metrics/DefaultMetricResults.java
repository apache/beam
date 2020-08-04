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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Default implementation of {@link org.apache.beam.sdk.metrics.MetricResults}, which takes static
 * {@link Iterable}s of counters, distributions, and gauges, and serves queries by applying {@link
 * org.apache.beam.sdk.metrics.MetricsFilter}s linearly to them.
 */
public class DefaultMetricResults extends MetricResults {

  private final Iterable<MetricResult<Long>> counters;
  private final Iterable<MetricResult<DistributionResult>> distributions;
  private final Iterable<MetricResult<GaugeResult>> gauges;

  public DefaultMetricResults(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges) {
    this.counters = counters;
    this.distributions = distributions;
    this.gauges = gauges;
  }

  @Override
  public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
    return MetricQueryResults.create(
        Iterables.filter(counters, counter -> MetricFiltering.matches(filter, counter.getKey())),
        Iterables.filter(
            distributions, distribution -> MetricFiltering.matches(filter, distribution.getKey())),
        Iterables.filter(gauges, gauge -> MetricFiltering.matches(filter, gauge.getKey())));
  }
}
