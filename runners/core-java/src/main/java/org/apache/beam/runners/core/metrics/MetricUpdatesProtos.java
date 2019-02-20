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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.MetricKey;

/** Convert {@link MetricUpdates} to a {@link List} of {@link MonitoringInfo}. */
public class MetricUpdatesProtos {
  private static <T> MonitoringInfo addMetric(
      MetricUpdates.MetricUpdate<T> metricUpdate, BiConsumer<SimpleMonitoringInfoBuilder, T> fn) {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(true);
    MetricKey metricKey = metricUpdate.getKey();
    builder.handleMetricKey(metricKey);

    fn.accept(builder, metricUpdate.getUpdate());
    return builder.setTimestampToNow().build();
  }

  private static <T> void fromMetrics(
      ArrayList<MonitoringInfo> monitoringInfos,
      Iterable<MetricUpdates.MetricUpdate<T>> metrics,
      BiConsumer<SimpleMonitoringInfoBuilder, T> fn) {
    for (MetricUpdates.MetricUpdate<T> metric : metrics) {
      MonitoringInfo monitoringInfo = addMetric(metric, fn);
      if (monitoringInfo != null) {
        monitoringInfos.add(monitoringInfo);
      }
    }
  }

  public static List<MonitoringInfo> toProto(MetricUpdates metricUpdates) {
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<>();

    fromMetrics(
        monitoringInfos,
        metricUpdates.counterUpdates(),
        SimpleMonitoringInfoBuilder::setInt64Value);
    fromMetrics(
        monitoringInfos,
        metricUpdates.distributionUpdates(),
        SimpleMonitoringInfoBuilder::setIntDistributionValue);
    fromMetrics(
        monitoringInfos, metricUpdates.gaugeUpdates(), SimpleMonitoringInfoBuilder::setGaugeValue);

    return monitoringInfos;
  }
}
