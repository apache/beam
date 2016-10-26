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

import com.codahale.metrics.MetricRegistry;

import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.spark.metrics.source.Source;

/**
 * A Spark {@link Source} that is tailored to expose an {@link AggregatorMetric},
 * wrapping an underlying {@link NamedAggregators} instance.
 */
public class AggregatorMetricSource implements Source {

  private final String sourceName;

  private final MetricRegistry metricRegistry = new MetricRegistry();

  public AggregatorMetricSource(final String appName, final NamedAggregators aggregators) {
    sourceName = appName;
    metricRegistry.register("Beam", AggregatorMetric.of(aggregators));
  }

  @Override
  public String sourceName() {
    return sourceName;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
