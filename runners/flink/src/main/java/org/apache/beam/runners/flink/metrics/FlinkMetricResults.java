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
package org.apache.beam.runners.flink.metrics;


import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.COUNTER_PREFIX;
import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.DISTRIBUTION_PREFIX;
import static org.apache.beam.runners.flink.metrics.FlinkMetricContainer.GAUGE_PREFIX;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.DistributionData;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeData;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricFiltering;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;

/**
 * Implementation of {@link MetricResults} for the Flink Runner.
 */
public class FlinkMetricResults extends MetricResults {

  private Map<String, Object> accumulators;

  public FlinkMetricResults(Map<String, Object> accumulators) {
    this.accumulators = accumulators;
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return new FlinkMetricQueryResults(filter);
  }

  private class FlinkMetricQueryResults implements MetricQueryResults {

    private MetricsFilter filter;

    FlinkMetricQueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      List<MetricResult<Long>> result = new ArrayList<>();
      for (Map.Entry<String, Object> accumulator : accumulators.entrySet()) {
        if (accumulator.getKey().startsWith(COUNTER_PREFIX)) {
          MetricKey metricKey = FlinkMetricContainer.parseMetricKey(accumulator.getKey());
          if (MetricFiltering.matches(filter, metricKey)) {
            result.add(new FlinkMetricResult<>(
                metricKey.metricName(), metricKey.stepName(), (Long) accumulator.getValue()));
          }
        }
      }
      return result;
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      List<MetricResult<DistributionResult>> result = new ArrayList<>();
      for (Map.Entry<String, Object> accumulator : accumulators.entrySet()) {
        if (accumulator.getKey().startsWith(DISTRIBUTION_PREFIX)) {
          MetricKey metricKey = FlinkMetricContainer.parseMetricKey(accumulator.getKey());
          DistributionData data = (DistributionData) accumulator.getValue();
          if (MetricFiltering.matches(filter, metricKey)) {
            result.add(new FlinkMetricResult<>(
                metricKey.metricName(), metricKey.stepName(), data.extractResult()));
          }
        }
      }
      return result;
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> gauges() {
      List<MetricResult<GaugeResult>> result = new ArrayList<>();
      for (Map.Entry<String, Object> accumulator : accumulators.entrySet()) {
        if (accumulator.getKey().startsWith(GAUGE_PREFIX)) {
          MetricKey metricKey = FlinkMetricContainer.parseMetricKey(accumulator.getKey());
          GaugeData data = (GaugeData) accumulator.getValue();
          if (MetricFiltering.matches(filter, metricKey)) {
            result.add(new FlinkMetricResult<>(
                metricKey.metricName(), metricKey.stepName(), data.extractResult()));
          }
        }
      }
      return result;
    }

  }

  private static class FlinkMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    FlinkMetricResult(MetricName name, String step, T result) {
      this.name = name;
      this.step = step;
      this.result = result;
    }

    @Override
    public MetricName name() {
      return name;
    }

    @Override
    public String step() {
      return step;
    }

    @Override
    public T committed() {
      throw new UnsupportedOperationException("Flink runner does not currently support committed"
          + " metrics results. Please use 'attempted' instead.");
    }

    @Override
    public T attempted() {
      return result;
    }
  }

}
