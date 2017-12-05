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
package org.apache.beam.runners.jstorm.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.alibaba.jstorm.common.metric.AsmCounter;
import com.alibaba.jstorm.common.metric.AsmGauge;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.common.metric.snapshot.AsmHistogramSnapshot;
import com.alibaba.jstorm.metric.AsmWindow;
import com.alibaba.jstorm.metrics.Snapshot;
import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.joda.time.Instant;

/**
 * Implementation of {@link MetricResults} for the JStorm Runner.
 */
public class JStormMetricResults extends MetricResults {

  private final Map<String, AsmCounter> counterMap;
  private final Map<String, AsmGauge> gaugeMap;
  private final Map<String, AsmHistogram> histogramMap;

  public JStormMetricResults(
      Map<String, AsmCounter> counterMap,
      Map<String, AsmGauge> gaugeMap,
      Map<String, AsmHistogram> histogramMap) {
    this.counterMap = checkNotNull(counterMap, "counterMap");
    this.gaugeMap = checkNotNull(gaugeMap, "gaugeMap");
    this.histogramMap = checkNotNull(histogramMap, "histogramMap");
  }

  @Override
  public MetricQueryResults queryMetrics(MetricsFilter filter) {
    List<MetricResult<Long>> counters = new ArrayList<>();
    for (Map.Entry<String, AsmCounter> entry : counterMap.entrySet()) {
      MetricKey metricKey = MetricsReporter.toMetricKey(entry.getKey());
      if (!MetricFiltering.matches(filter, metricKey)) {
        continue;
      }
      counters.add(
          JStormMetricResult.create(
              metricKey.metricName(),
              metricKey.stepName(),
              (Long) entry.getValue().getValue(AsmWindow.M10_WINDOW)));
    }

    List<MetricResult<GaugeResult>> gauges = new ArrayList<>();
    for (Map.Entry<String, AsmGauge> entry : gaugeMap.entrySet()) {
      MetricKey metricKey = MetricsReporter.toMetricKey(entry.getKey());
      if (!MetricFiltering.matches(filter, metricKey)) {
        continue;
      }
      gauges.add(
          JStormMetricResult.create(
              metricKey.metricName(),
              metricKey.stepName(),
              GaugeResult.create(
                  ((Double) entry.getValue().getValue(AsmWindow.M10_WINDOW)).longValue(),
                  new Instant(0))));
    }

    List<MetricResult<DistributionResult>> distributions = new ArrayList<>();
    for (Map.Entry<String, AsmHistogram> entry : histogramMap.entrySet()) {
      MetricKey metricKey = MetricsReporter.toMetricKey(entry.getKey());
      if (!MetricFiltering.matches(filter, metricKey)) {
        continue;
      }
      AsmHistogram histogram = entry.getValue();
      histogram.forceFlush();

      Snapshot snapshot =
          ((AsmHistogramSnapshot) histogram.getSnapshots().get(AsmWindow.M10_WINDOW)).getSnapshot();
      // TODO: Sum and count might be under estimated, because JStorm histogram only store a fixed
      // number of values.
      long sum = 0;
      for (long v : snapshot.getValues()) {
        sum += v;
      }
      distributions.add(
          JStormMetricResult.create(
              metricKey.metricName(),
              metricKey.stepName(),
              DistributionResult.create(
                  sum,
                  snapshot.size(),
                  snapshot.getMin(),
                  snapshot.getMax())));
    }

    return JStormMetricQueryResults.create(counters, gauges, distributions);
  }

  @AutoValue
  abstract static class JStormMetricQueryResults implements MetricQueryResults {

    public abstract @Nullable Iterable<MetricResult<DistributionResult>> distributions();

    public static MetricQueryResults create(
        Iterable<MetricResult<Long>> counters,
        Iterable<MetricResult<GaugeResult>> gauges,
        Iterable<MetricResult<DistributionResult>> distributions) {
      return new AutoValue_JStormMetricResults_JStormMetricQueryResults(
          counters, gauges, distributions);
    }
  }

  @AutoValue
  abstract static class JStormMetricResult<T> implements MetricResult<T> {
    // need to define these here so they appear in the correct order
    // and the generated constructor is usable and consistent
    public abstract MetricName name();
    public abstract String step();
    public abstract @Nullable T committed();
    public abstract T attempted();

    public static <T> MetricResult<T> create(MetricName name, String step, T attempted) {
      return new AutoValue_JStormMetricResults_JStormMetricResult<>(name, step, null, attempted);
    }
  }
}
