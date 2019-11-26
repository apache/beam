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
package org.apache.beam.runners.spark.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSortedMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricRegistry} decorator-like that supports {@link AggregatorMetric} and {@link
 * SparkBeamMetric} as {@link Gauge Gauges}.
 *
 * <p>{@link MetricRegistry} is not an interface, so this is not a by-the-book decorator. That said,
 * it delegates all metric related getters to the "decorated" instance.
 */
public class WithMetricsSupport extends MetricRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(WithMetricsSupport.class);

  private final MetricRegistry internalMetricRegistry;

  private WithMetricsSupport(final MetricRegistry internalMetricRegistry) {
    this.internalMetricRegistry = internalMetricRegistry;
  }

  public static WithMetricsSupport forRegistry(final MetricRegistry metricRegistry) {
    return new WithMetricsSupport(metricRegistry);
  }

  @Override
  public SortedMap<String, Timer> getTimers(final MetricFilter filter) {
    return internalMetricRegistry.getTimers(filter);
  }

  @Override
  public SortedMap<String, Meter> getMeters(final MetricFilter filter) {
    return internalMetricRegistry.getMeters(filter);
  }

  @Override
  public SortedMap<String, Histogram> getHistograms(final MetricFilter filter) {
    return internalMetricRegistry.getHistograms(filter);
  }

  @Override
  public SortedMap<String, Counter> getCounters(final MetricFilter filter) {
    return internalMetricRegistry.getCounters(filter);
  }

  @Override
  public SortedMap<String, Gauge> getGauges(final MetricFilter filter) {
    return new ImmutableSortedMap.Builder<String, Gauge>(
            Ordering.from(String.CASE_INSENSITIVE_ORDER))
        .putAll(internalMetricRegistry.getGauges(filter))
        .putAll(extractGauges(internalMetricRegistry, filter))
        .build();
  }

  private Map<String, Gauge> extractGauges(
      final MetricRegistry metricRegistry, final MetricFilter filter) {
    Map<String, Gauge> gauges = new HashMap<>();

    // find the AggregatorMetric metrics from within all currently registered metrics
    final Optional<Map<String, Gauge>> aggregatorMetrics =
        FluentIterable.from(metricRegistry.getMetrics().entrySet())
            .firstMatch(isAggregatorMetric())
            .transform(aggregatorMetricToGauges());

    // find the SparkBeamMetric metrics from within all currently registered metrics
    final Optional<Map<String, Gauge>> beamMetrics =
        FluentIterable.from(metricRegistry.getMetrics().entrySet())
            .firstMatch(isSparkBeamMetric())
            .transform(beamMetricToGauges());

    if (aggregatorMetrics.isPresent()) {
      gauges.putAll(Maps.filterEntries(aggregatorMetrics.get(), matches(filter)));
    }

    if (beamMetrics.isPresent()) {
      gauges.putAll(Maps.filterEntries(beamMetrics.get(), matches(filter)));
    }

    return gauges;
  }

  private Function<Map.Entry<String, Metric>, Map<String, Gauge>> aggregatorMetricToGauges() {
    return entry -> {
      final NamedAggregators agg = ((AggregatorMetric) entry.getValue()).getNamedAggregators();
      final String parentName = entry.getKey();
      final Map<String, Gauge> gaugeMap = Maps.transformEntries(agg.renderAll(), toGauge());
      final Map<String, Gauge> fullNameGaugeMap = Maps.newLinkedHashMap();
      for (Map.Entry<String, Gauge> gaugeEntry : gaugeMap.entrySet()) {
        fullNameGaugeMap.put(parentName + "." + gaugeEntry.getKey(), gaugeEntry.getValue());
      }
      return Maps.filterValues(fullNameGaugeMap, Predicates.notNull());
    };
  }

  private Function<Map.Entry<String, Metric>, Map<String, Gauge>> beamMetricToGauges() {
    return entry -> {
      final Map<String, ?> metrics = ((SparkBeamMetric) entry.getValue()).renderAll();
      final String parentName = entry.getKey();
      final Map<String, Gauge> gaugeMap = Maps.transformEntries(metrics, toGauge());
      final Map<String, Gauge> fullNameGaugeMap = Maps.newLinkedHashMap();
      for (Map.Entry<String, Gauge> gaugeEntry : gaugeMap.entrySet()) {
        fullNameGaugeMap.put(parentName + "." + gaugeEntry.getKey(), gaugeEntry.getValue());
      }
      return Maps.filterValues(fullNameGaugeMap, Predicates.notNull());
    };
  }

  private Maps.EntryTransformer<String, Object, Gauge> toGauge() {
    return (name, rawValue) ->
        () -> {
          // at the moment the metric's type is assumed to be
          // compatible with Double. While far from perfect, it seems reasonable at
          // this point in time
          try {
            return Double.parseDouble(rawValue.toString());
          } catch (final Exception e) {
            LOG.warn(
                "Failed reporting metric with name [{}], of type [{}], since it could not be"
                    + " converted to double",
                name,
                rawValue.getClass().getSimpleName(),
                e);
            return null;
          }
        };
  }

  private Predicate<Map.Entry<String, Gauge>> matches(final MetricFilter filter) {
    return entry -> filter.matches(entry.getKey(), entry.getValue());
  }

  private Predicate<Map.Entry<String, Metric>> isAggregatorMetric() {
    return metricEntry -> (metricEntry.getValue() instanceof AggregatorMetric);
  }

  private Predicate<Map.Entry<String, Metric>> isSparkBeamMetric() {
    return metricEntry -> (metricEntry.getValue() instanceof SparkBeamMetric);
  }
}
