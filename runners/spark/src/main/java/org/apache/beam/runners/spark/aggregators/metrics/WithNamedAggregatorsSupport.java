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

package org.apache.beam.runners.spark.aggregators.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

import java.util.Map;
import java.util.SortedMap;

import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MetricRegistry} decorator-like* that supports {@link AggregatorMetric} by exposing
 * the underlying * {@link org.apache.beam.runners.spark.aggregators.NamedAggregators}'
 * aggregators as {@link Gauge}s.
 * <p>
 * *{@link MetricRegistry} is not an interface, so this is not a by-the-book decorator.
 * That said, it delegates all metric related getters to the "decorated" instance.
 * </p>
 */
public class WithNamedAggregatorsSupport extends MetricRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(WithNamedAggregatorsSupport.class);

  private MetricRegistry internalMetricRegistry;

  private WithNamedAggregatorsSupport(final MetricRegistry internalMetricRegistry) {
    this.internalMetricRegistry = internalMetricRegistry;
  }

  public static WithNamedAggregatorsSupport forRegistry(final MetricRegistry metricRegistry) {
    return new WithNamedAggregatorsSupport(metricRegistry);
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
    return
        new ImmutableSortedMap.Builder<String, Gauge>(
            Ordering.from(String.CASE_INSENSITIVE_ORDER))
            .putAll(internalMetricRegistry.getGauges(filter))
            .putAll(extractGauges(internalMetricRegistry, filter))
            .build();
  }

  private Map<String, Gauge> extractGauges(final MetricRegistry metricRegistry,
                                           final MetricFilter filter) {

    // find the AggregatorMetric metrics from within all currently registered metrics
    final Optional<Map<String, Gauge>> gauges =
        FluentIterable
            .from(metricRegistry.getMetrics().entrySet())
            .firstMatch(isAggregatorMetric())
            .transform(toGauges());

    return
        gauges.isPresent()
            ? Maps.filterEntries(gauges.get(), matches(filter))
            : ImmutableMap.<String, Gauge>of();
  }

  private Function<Map.Entry<String, Metric>, Map<String, Gauge>> toGauges() {
    return new Function<Map.Entry<String, Metric>, Map<String, Gauge>>() {
      @Override
      public Map<String, Gauge> apply(final Map.Entry<String, Metric> entry) {
        final NamedAggregators agg = ((AggregatorMetric) entry.getValue()).getNamedAggregators();
        final String parentName = entry.getKey();
        final Map<String, Gauge> gaugeMap = Maps.transformEntries(agg.renderAll(), toGauge());
        final Map<String, Gauge> fullNameGaugeMap = Maps.newLinkedHashMap();
        for (Map.Entry<String, Gauge> gaugeEntry : gaugeMap.entrySet()) {
          fullNameGaugeMap.put(parentName + "." + gaugeEntry.getKey(), gaugeEntry.getValue());
        }
        return Maps.filterValues(fullNameGaugeMap, Predicates.notNull());
      }
    };
  }

  private Maps.EntryTransformer<String, Object, Gauge> toGauge() {
    return new Maps.EntryTransformer<String, Object, Gauge>() {

      @Override
      public Gauge transformEntry(final String name, final Object rawValue) {
        return new Gauge<Double>() {

          @Override
          public Double getValue() {
            // at the moment the metric's type is assumed to be
            // compatible with Double. While far from perfect, it seems reasonable at
            // this point in time
            try {
              return Double.parseDouble(rawValue.toString());
            } catch (final Exception e) {
              LOG.warn("Failed reporting metric with name [{}], of type [{}], since it could not be"
                  + " converted to double", name, rawValue.getClass().getSimpleName(), e);
              return null;
            }
          }
        };
      }
    };
  }

  private Predicate<Map.Entry<String, Gauge>> matches(final MetricFilter filter) {
    return new Predicate<Map.Entry<String, Gauge>>() {
      @Override
      public boolean apply(final Map.Entry<String, Gauge> entry) {
        return filter.matches(entry.getKey(), entry.getValue());
      }
    };
  }

  private Predicate<Map.Entry<String, Metric>> isAggregatorMetric() {
    return new Predicate<Map.Entry<String, Metric>>() {
      @Override
      public boolean apply(final Map.Entry<String, Metric> metricEntry) {
        return (metricEntry.getValue() instanceof AggregatorMetric);
      }
    };
  }
}
