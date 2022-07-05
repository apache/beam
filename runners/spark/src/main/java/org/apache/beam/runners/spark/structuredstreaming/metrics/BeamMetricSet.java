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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import java.util.Map;

/**
 * Map of Beam metrics available from {@link Gauge#getValue()}.
 *
 * <p>Note: Recent versions of Dropwizard {@link com.codahale.metrics.MetricRegistry MetricRegistry}
 * do not allow registering arbitrary implementations of {@link com.codahale.metrics.Metric
 * Metrics}.
 */
public abstract class BeamMetricSet implements Gauge<Map<String, Gauge<Double>>> {

  @Override
  public final Map<String, Gauge<Double>> getValue() {
    return getValue("", MetricFilter.ALL);
  }

  protected abstract Map<String, Gauge<Double>> getValue(String prefix, MetricFilter filter);

  protected Gauge<Double> staticGauge(Number number) {
    return new StaticGauge(number.doubleValue());
  }

  private static class StaticGauge implements Gauge<Double> {
    double value;

    StaticGauge(double value) {
      this.value = value;
    }

    @Override
    public Double getValue() {
      return value;
    }
  }
}
