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
package org.apache.beam.sdk.metrics;

import com.google.auto.value.AutoValue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.util.HistogramData;

/** The results of a query for metrics. Allows accessing all the metrics that matched the filter. */
@AutoValue
public abstract class MetricQueryResults {
  /** Return the metric results for the counters that matched the filter. */
  public abstract Iterable<MetricResult<Long>> getCounters();

  /** Return the metric results for the distributions that matched the filter. */
  public abstract Iterable<MetricResult<DistributionResult>> getDistributions();

  /** Return the metric results for the gauges that matched the filter. */
  public abstract Iterable<MetricResult<GaugeResult>> getGauges();

  /** Return the metric results for the sets that matched the filter. */
  public abstract Iterable<MetricResult<StringSetResult>> getStringSets();

  /** Return the metric results for the sets that matched the filter. */
  public abstract Iterable<MetricResult<HistogramData>> getPerWorkerHistograms();

  static <T> void printMetrics(String type, Iterable<MetricResult<T>> metrics, StringBuilder sb) {
    List<MetricResult<T>> metricsList = ImmutableList.copyOf(metrics);
    if (!metricsList.isEmpty()) {
      sb.append(type).append("(");
      boolean first = true;
      for (MetricResult<T> metricResult : metricsList) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(metricResult.getKey()).append(": ").append(metricResult.getAttempted());
        if (metricResult.hasCommitted()) {
          T committed = metricResult.getCommitted();
          sb.append(", ").append(committed);
        }
      }
      sb.append(")");
    }
  }

  @Override
  public final String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MetricQueryResults(");
    printMetrics("Counters", getCounters(), sb);
    printMetrics("Distributions", getDistributions(), sb);
    printMetrics("Gauges", getGauges(), sb);
    printMetrics("StringSets", getStringSets(), sb);
    printMetrics("perWorkerHistograms", getPerWorkerHistograms(), sb);
    sb.append(")");
    return sb.toString();
  }

  public static MetricQueryResults create(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges,
      Iterable<MetricResult<StringSetResult>> stringSets,
      Iterable<MetricResult<HistogramData>> perWorkerHistograms) {
    return new AutoValue_MetricQueryResults(counters, distributions, gauges, stringSets, perWorkerHistograms);
  }

  public static MetricQueryResults create(
    Iterable<MetricResult<Long>> counters,
    Iterable<MetricResult<DistributionResult>> distributions,
    Iterable<MetricResult<GaugeResult>> gauges,
    Iterable<MetricResult<StringSetResult>> stringSets) {
  return new AutoValue_MetricQueryResults(counters, distributions, gauges, stringSets, Collections.emptyList());
}
}
