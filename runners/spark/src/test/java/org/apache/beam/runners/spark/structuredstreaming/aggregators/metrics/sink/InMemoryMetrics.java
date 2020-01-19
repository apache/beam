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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import java.util.Properties;
import org.apache.beam.runners.spark.structuredstreaming.metrics.WithMetricsSupport;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.spark.metrics.sink.Sink;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** An in-memory {@link Sink} implementation for tests. */
@RunWith(JUnit4.class)
public class InMemoryMetrics implements Sink {

  private static WithMetricsSupport extendedMetricsRegistry;
  private static MetricRegistry internalMetricRegistry;

  @SuppressWarnings("UnusedParameters")
  public InMemoryMetrics(
      final Properties properties,
      final MetricRegistry metricRegistry,
      final org.apache.spark.SecurityManager securityMgr) {
    extendedMetricsRegistry = WithMetricsSupport.forRegistry(metricRegistry);
    internalMetricRegistry = metricRegistry;
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <T> T valueOf(final String name) {
    final T retVal;

    // this might fail in case we have multiple aggregators with the same suffix after
    // the last dot, but it should be good enough for tests.
    if (extendedMetricsRegistry != null
        && extendedMetricsRegistry.getGauges().keySet().stream()
            .anyMatch(Predicates.containsPattern(name + "$")::apply)) {
      String key =
          extendedMetricsRegistry.getGauges().keySet().stream()
              .filter(Predicates.containsPattern(name + "$")::apply)
              .findFirst()
              .get();
      retVal = (T) extendedMetricsRegistry.getGauges().get(key).getValue();
    } else {
      retVal = null;
    }

    return retVal;
  }

  @SuppressWarnings("WeakerAccess")
  public static void clearAll() {
    if (internalMetricRegistry != null) {
      internalMetricRegistry.removeMatching(MetricFilter.ALL);
    }
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void report() {}
}
