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

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.metrics.source.Source;

/**
 * A Spark {@link Source} that is tailored to expose a {@link SparkBeamMetric}, wrapping an
 * underlying {@link org.apache.beam.sdk.metrics.MetricResults} instance.
 */
public class SparkBeamMetricSource implements Source {
  private static final String METRIC_NAME = "Metrics";

  private final String name;

  private final MetricRegistry metricRegistry = new MetricRegistry();

  public SparkBeamMetricSource(final String name) {
    this.name = name;
    metricRegistry.register(METRIC_NAME, new SparkBeamMetric());
  }

  @Override
  public String sourceName() {
    return name;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }
}
