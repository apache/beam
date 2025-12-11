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
package org.apache.beam.runners.spark.structuredstreaming.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.Map;
import java.util.SortedMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSortedMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;

/**
 * A {@link MetricRegistry} decorator-like that supports {@link BeamMetricSet}s as {@link Gauge
 * Gauges}.
 *
 * <p>{@link MetricRegistry} is not an interface, so this is not a by-the-book decorator. That said,
 * it delegates all metric related getters to the "decorated" instance.
 */
public class WithMetricsSupport extends MetricRegistry {

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
  @SuppressWarnings({"rawtypes"}) // required by interface
  public SortedMap<String, Gauge> getGauges(final MetricFilter filter) {
    ImmutableSortedMap.Builder<String, Gauge> builder =
        new ImmutableSortedMap.Builder<>(Ordering.from(String.CASE_INSENSITIVE_ORDER));

    Map<String, Gauge> gauges =
        internalMetricRegistry.getGauges(
            (n, m) -> filter.matches(n, m) || m instanceof BeamMetricSet);

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      Gauge gauge = entry.getValue();
      if (gauge instanceof BeamMetricSet) {
        builder.putAll(((BeamMetricSet) gauge).getValue(entry.getKey(), filter));
      } else {
        builder.put(entry.getKey(), gauge);
      }
    }
    return builder.build();
  }
}
