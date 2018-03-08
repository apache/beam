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

package org.apache.beam.runners.core.metrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.MetricName;

/** Translation utilities for metrics classes to/from Fn API. */
@Experimental(Kind.METRICS)
public abstract class MetricsTranslation {

  private MetricsTranslation() {}

  public static MetricUpdates metricUpdatesFromProto(
      String ptransformName, Collection<BeamFnApi.Metrics.User> userMetricUpdates) {
    List<MetricUpdates.MetricUpdate<Long>> counterUpdates = new ArrayList<>();
    List<MetricUpdates.MetricUpdate<DistributionData>> distributionUpdates = new ArrayList<>();
    List<MetricUpdates.MetricUpdate<GaugeData>> gaugeUpdates = new ArrayList<>();

    for (BeamFnApi.Metrics.User userMetricUpdate : userMetricUpdates) {
      MetricKey metricKey =
          MetricKey.create(ptransformName, metricNameFromProto(userMetricUpdate.getMetricName()));
      switch (userMetricUpdate.getDataCase()) {
        case COUNTER_DATA:
          counterUpdates.add(
              MetricUpdates.MetricUpdate.create(
                  metricKey, userMetricUpdate.getCounterData().getValue()));
          break;
        case DISTRIBUTION_DATA:
          distributionUpdates.add(
              MetricUpdates.MetricUpdate.create(
                  metricKey,
                  DistributionData.create(
                      userMetricUpdate.getDistributionData().getSum(),
                      userMetricUpdate.getDistributionData().getCount(),
                      userMetricUpdate.getDistributionData().getMin(),
                      userMetricUpdate.getDistributionData().getMax())));
          break;
        case GAUGE_DATA:
          gaugeUpdates.add(
              MetricUpdates.MetricUpdate.create(
                  metricKey,
                  GaugeData.create(
                      userMetricUpdate.getGaugeData().getValue())));
          break;
        case DATA_NOT_SET:
          continue;
      }
    }
    return MetricUpdates.create(counterUpdates, distributionUpdates, gaugeUpdates);
  }

  public static Map<String, Collection<BeamFnApi.Metrics.User>> metricUpdatesToProto(
      MetricUpdates metricUpdates) {
    LoadingCache<String, Collection<BeamFnApi.Metrics.User>> fnMetrics =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, Collection<BeamFnApi.Metrics.User>>() {
                  @Override
                  public Collection<BeamFnApi.Metrics.User> load(String ptransformName) {
                    return new ArrayList<>();
                  }
                });

    for (MetricUpdates.MetricUpdate<Long> counterUpdate : metricUpdates.counterUpdates()) {
      fnMetrics
          .getUnchecked(counterUpdate.getKey().stepName())
          .add(
              BeamFnApi.Metrics.User.newBuilder()
                  .setMetricName(metricNameToProto(counterUpdate.getKey().metricName()))
                  .setCounterData(
                      BeamFnApi.Metrics.User.CounterData.newBuilder()
                          .setValue(counterUpdate.getUpdate()))
                  .build());
    }

    for (MetricUpdates.MetricUpdate<GaugeData> gaugeUpdate : metricUpdates.gaugeUpdates()) {
      fnMetrics
          .getUnchecked(gaugeUpdate.getKey().stepName())
          .add(
              BeamFnApi.Metrics.User.newBuilder()
                  .setMetricName(metricNameToProto(gaugeUpdate.getKey().metricName()))
                  .setGaugeData(
                      BeamFnApi.Metrics.User.GaugeData.newBuilder()
                          .setValue(gaugeUpdate.getUpdate().value()))
                  .build());
    }

    for (MetricUpdates.MetricUpdate<DistributionData> distributionUpdate :
        metricUpdates.distributionUpdates()) {
      fnMetrics
          .getUnchecked(distributionUpdate.getKey().stepName())
          .add(
              BeamFnApi.Metrics.User.newBuilder()
                  .setMetricName(metricNameToProto(distributionUpdate.getKey().metricName()))
                  .setDistributionData(
                      BeamFnApi.Metrics.User.DistributionData.newBuilder()
                          .setCount(distributionUpdate.getUpdate().count())
                          .setMax(distributionUpdate.getUpdate().max())
                          .setMin(distributionUpdate.getUpdate().min())
                          .setSum(distributionUpdate.getUpdate().sum()))
                  .build());
    }

    return fnMetrics.asMap();
  }

  public static MetricName metricNameFromProto(BeamFnApi.Metrics.User.MetricName protoMetricName) {
    return MetricName.named(protoMetricName.getNamespace(), protoMetricName.getName());
  }

  public static BeamFnApi.Metrics.User.MetricName metricNameToProto(MetricName metricName) {
    return BeamFnApi.Metrics.User.MetricName.newBuilder()
        .setNamespace(metricName.namespace())
        .setName(metricName.name())
        .build();
  }
}
