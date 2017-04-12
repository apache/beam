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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
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
import org.apache.beam.sdk.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricsFilter;


/**
 * Implementation of {@link MetricResults} for the Spark Runner.
 */
public class SparkMetricResults extends MetricResults {

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    return new SparkMetricQueryResults(filter);
  }

  private static class SparkMetricQueryResults implements MetricQueryResults {
    private final MetricsFilter filter;

    SparkMetricQueryResults(MetricsFilter filter) {
      this.filter = filter;
    }

    @Override
    public Iterable<MetricResult<Long>> counters() {
      return
          FluentIterable
              .from(SparkMetricsContainer.getCounters())
              .filter(matchesFilter(filter))
              .transform(TO_COUNTER_RESULT)
              .toList();
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> distributions() {
      return
          FluentIterable
              .from(SparkMetricsContainer.getDistributions())
              .filter(matchesFilter(filter))
              .transform(TO_DISTRIBUTION_RESULT)
              .toList();
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> gauges() {
      return
          FluentIterable
              .from(SparkMetricsContainer.getGauges())
              .filter(matchesFilter(filter))
              .transform(TO_GAUGE_RESULT)
              .toList();
    }

    private Predicate<MetricUpdate<?>> matchesFilter(final MetricsFilter filter) {
      return new Predicate<MetricUpdate<?>>() {
        @Override
        public boolean apply(MetricUpdate<?> metricResult) {
          return MetricFiltering.matches(filter, metricResult.getKey());
        }
      };
    }
  }

  private static final Function<MetricUpdate<DistributionData>, MetricResult<DistributionResult>>
      TO_DISTRIBUTION_RESULT =
      new Function<MetricUpdate<DistributionData>, MetricResult<DistributionResult>>() {
        @Override
        public MetricResult<DistributionResult> apply(MetricUpdate<DistributionData> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new SparkMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate().extractResult());
          } else {
            return null;
          }
        }
      };

  private static final Function<MetricUpdate<Long>, MetricResult<Long>>
      TO_COUNTER_RESULT =
      new Function<MetricUpdate<Long>, MetricResult<Long>>() {
        @Override
        public MetricResult<Long> apply(MetricUpdate<Long> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new SparkMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate());
          } else {
            return null;
          }
        }
      };

  private static final Function<MetricUpdate<GaugeData>, MetricResult<GaugeResult>>
      TO_GAUGE_RESULT =
      new Function<MetricUpdate<GaugeData>, MetricResult<GaugeResult>>() {
        @Override
        public MetricResult<GaugeResult> apply(MetricUpdate<GaugeData> metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new SparkMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getUpdate().extractResult());
          } else {
            return null;
          }
        }
      };

  private static class SparkMetricResult<T> implements MetricResult<T> {
    private final MetricName name;
    private final String step;
    private final T result;

    SparkMetricResult(MetricName name, String step, T result) {
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
      throw new UnsupportedOperationException("Spark runner does not currently support committed"
          + " metrics results. Please use 'attempted' instead.");
    }

    @Override
    public T attempted() {
      return result;
    }
  }
}
