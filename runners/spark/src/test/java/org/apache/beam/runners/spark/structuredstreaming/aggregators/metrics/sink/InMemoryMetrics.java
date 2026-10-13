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
package org.apache.beam.runners.spark.structuredstreaming.aggregators.metrics.sink;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.spark.structuredstreaming.metrics.WithMetricsSupport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.metrics.sink.Sink;

/** An in-memory {@link Sink} implementation for tests. */
public class InMemoryMetrics implements Sink {

  private static final AtomicReference<WithMetricsSupport> extendedMetricsRegistry =
      new AtomicReference<>();
  private static final AtomicReference<MetricRegistry> internalMetricRegistry =
      new AtomicReference<>();

  // Constructor for Spark 3.1
  @SuppressWarnings("UnusedParameters")
  public InMemoryMetrics(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    extendedMetricsRegistry.set(WithMetricsSupport.forRegistry(metricRegistry));
    internalMetricRegistry.set(metricRegistry);
  }

  // Constructor for Spark >= 3.2
  @SuppressWarnings("UnusedParameters")
  public InMemoryMetrics(final Properties properties, final MetricRegistry metricRegistry) {
    extendedMetricsRegistry.set(WithMetricsSupport.forRegistry(metricRegistry));
    internalMetricRegistry.set(metricRegistry);
  }

  @SuppressWarnings({"TypeParameterUnusedInFormals", "rawtypes"}) // because of getGauges
  public static <T> T valueOf(final String name) {
    // this might fail in case we have multiple aggregators with the same suffix after
    // the last dot, but it should be good enough for tests.
    WithMetricsSupport extended = extendedMetricsRegistry.get();
    if (extended != null) {
      Collection<Gauge> matches = extended.getGauges((n, m) -> n.endsWith(name)).values();
      return matches.isEmpty() ? null : (T) Iterables.getOnlyElement(matches).getValue();
    } else {
      return null;
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static void clearAll() {
    MetricRegistry internal = internalMetricRegistry.get();
    if (internal != null) {
      internal.removeMatching(MetricFilter.ALL);
    }
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void report() {}
}
