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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An adapter between the {@link NamedAggregators} and the Dropwizard {@link Metric} interface. */
public class AggregatorMetric extends BeamMetricSet {

  private static final Logger LOG = LoggerFactory.getLogger(AggregatorMetric.class);

  private final NamedAggregators namedAggregators;

  private AggregatorMetric(NamedAggregators namedAggregators) {
    this.namedAggregators = namedAggregators;
  }

  public static AggregatorMetric of(NamedAggregators namedAggregators) {
    return new AggregatorMetric(namedAggregators);
  }

  @Override
  public Map<String, Gauge<Double>> getValue(String prefix, MetricFilter filter) {
    Map<String, Gauge<Double>> metrics = new HashMap<>();
    for (Map.Entry<String, ?> entry : namedAggregators.renderAll().entrySet()) {
      String name = prefix + "." + entry.getKey();
      Object rawValue = entry.getValue();
      if (rawValue != null) {
        try {
          Gauge<Double> gauge = staticGauge(rawValue);
          if (filter.matches(name, gauge)) {
            metrics.put(name, gauge);
          }
        } catch (NumberFormatException e) {
          LOG.warn(
              "Metric `{}` of type {} can't be reported, conversion to double failed.",
              name,
              rawValue.getClass().getSimpleName(),
              e);
        }
      }
    }
    return metrics;
  }

  // Metric type is assumed to be compatible with Double
  protected Gauge<Double> staticGauge(Object rawValue) throws NumberFormatException {
    return rawValue instanceof Number
        ? super.staticGauge((Number) rawValue)
        : super.staticGauge(Double.parseDouble(rawValue.toString()));
  }
}
