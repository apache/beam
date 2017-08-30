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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.metrics.MetricFiltering;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

/**
 * Implementation of {@link MetricResults} for the MapReduce Runner.
 */
public class MapReduceMetricResults extends MetricResults {

  private final List<Job> jobs;

  public MapReduceMetricResults(List<Job> jobs) {
    this.jobs = checkNotNull(jobs, "jobs");
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    List<MetricResult<Long>> counters = new ArrayList<>();
    for (Job job : jobs) {
      Iterable<CounterGroup> groups;
      try {
        groups = job.getCounters();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      for (CounterGroup group : groups) {
        String groupName = group.getName();
        for (Counter counter : group) {
          MetricKey metricKey = MetricsReporter.toMetricKey(groupName, counter.getName());
          if (!MetricFiltering.matches(filter, metricKey)) {
            continue;
          }
          counters.add(
              MapReduceMetricResult.create(
                  metricKey.metricName(),
                  metricKey.stepName(),
                  counter.getValue()));
        }
      }
    }
    return MapReduceMetricQueryResults.create(counters);
  }


  @AutoValue
  abstract static class MapReduceMetricQueryResults implements MetricQueryResults {

    public abstract @Nullable Iterable<MetricResult<DistributionResult>> distributions();
    public abstract @Nullable Iterable<MetricResult<GaugeResult>> gauges();

    public static MetricQueryResults create(Iterable<MetricResult<Long>> counters) {
      return new AutoValue_MapReduceMetricResults_MapReduceMetricQueryResults(
          counters, null, null);
    }
  }

  @AutoValue
  abstract static class MapReduceMetricResult<T> implements MetricResult<T> {
    // need to define these here so they appear in the correct order
    // and the generated constructor is usable and consistent
    public abstract MetricName name();
    public abstract String step();
    public abstract @Nullable T committed();
    public abstract T attempted();

    public static <T> MetricResult<T> create(MetricName name, String step, T attempted) {
      return new AutoValue_MapReduceMetricResults_MapReduceMetricResult<T>(
          name, step, null, attempted);
    }
  }
}
