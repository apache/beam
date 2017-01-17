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
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import java.util.Set;
import org.apache.beam.runners.spark.metrics.MetricAggregator.CounterAggregator;
import org.apache.beam.runners.spark.metrics.MetricAggregator.DistributionAggregator;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
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

    private Predicate<MetricAggregator<?>> matchesFilter(final MetricsFilter filter) {
      return new Predicate<MetricAggregator<?>>() {
        @Override
        public boolean apply(MetricAggregator<?> metricResult) {
          return matches(filter, metricResult.getKey());
        }
      };
    }

    private boolean matches(MetricsFilter filter, MetricKey key) {
      return matchesName(key.metricName(), filter.names())
          && matchesScope(key.stepName(), filter.steps());
    }

    private boolean matchesName(MetricName metricName, Set<MetricNameFilter> nameFilters) {
      if (nameFilters.isEmpty()) {
        return true;
      }

      for (MetricNameFilter nameFilter : nameFilters) {
        if ((nameFilter.getName() == null || nameFilter.getName().equals(metricName.name()))
            && Objects.equal(metricName.namespace(), nameFilter.getNamespace())) {
          return true;
        }
      }

      return false;
    }

    private boolean matchesScope(String actualScope, Set<String> scopes) {
      if (scopes.isEmpty() || scopes.contains(actualScope)) {
        return true;
      }

      for (String scope : scopes) {
        if (actualScope.startsWith(scope)) {
          return true;
        }
      }

      return false;
    }
  }

  private static final Function<DistributionAggregator, MetricResult<DistributionResult>>
      TO_DISTRIBUTION_RESULT =
      new Function<DistributionAggregator, MetricResult<DistributionResult>>() {
        @Override
        public MetricResult<DistributionResult>
        apply(DistributionAggregator metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new SparkMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getValue().extractResult());
          } else {
            return null;
          }
        }
      };

  private static final Function<CounterAggregator, MetricResult<Long>>
      TO_COUNTER_RESULT =
      new Function<CounterAggregator, MetricResult<Long>>() {
        @Override
        public MetricResult<Long>
        apply(CounterAggregator metricResult) {
          if (metricResult != null) {
            MetricKey key = metricResult.getKey();
            return new SparkMetricResult<>(key.metricName(), key.stepName(),
                metricResult.getValue());
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
      return result;
    }

    @Override
    public T attempted() {
      return result;
    }
  }
}
