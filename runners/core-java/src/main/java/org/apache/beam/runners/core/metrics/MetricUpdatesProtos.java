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
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MetricUpdates.MetricUpdate;
import org.apache.beam.sdk.metrics.MetricKey;

/** Convert {@link MetricUpdates} to a {@link List} of {@link MonitoringInfo}. */
public class MetricUpdatesProtos {
  public static List<MonitoringInfo> toProto(MetricUpdates metricUpdates) {
    ArrayList<MonitoringInfo> monitoringInfos = new ArrayList<>();
    addMetricUpdates(
        monitoringInfos,
        metricUpdates.counterUpdates(),
        SimpleMonitoringInfoBuilder::setInt64Value);
    addMetricUpdates(
        monitoringInfos,
        metricUpdates.distributionUpdates(),
        SimpleMonitoringInfoBuilder::setIntDistributionValue);
    addMetricUpdates(
        monitoringInfos, metricUpdates.gaugeUpdates(), SimpleMonitoringInfoBuilder::setGaugeValue);
    return monitoringInfos;
  }

  /**
   * Convert {@link MetricUpdate}s of a given type to {@link MonitoringInfo}s, and add them to a
   * given list.
   */
  private static <T> void addMetricUpdates(
      ArrayList<MonitoringInfo> monitoringInfos,
      Iterable<MetricUpdate<T>> metrics,
      BiConsumer<SimpleMonitoringInfoBuilder, T> setValue) {
    for (MetricUpdate<T> metric : metrics) {
      MonitoringInfo monitoringInfo = updateToProto(metric, setValue);
      if (monitoringInfo != null) {
        monitoringInfos.add(monitoringInfo);
      }
    }
  }

  /** Convert a {@link MetricUpdate} to a {@link MonitoringInfo}. */
  private static <T> MonitoringInfo updateToProto(
      MetricUpdate<T> metricUpdate, BiConsumer<SimpleMonitoringInfoBuilder, T> setValue) {
    MetricKey metricKey = metricUpdate.getKey();
    SimpleMonitoringInfoBuilder builder =
        new SimpleMonitoringInfoBuilder().setLabelsAndUrnFrom(metricKey);

    setValue.accept(builder, metricUpdate.getUpdate());
    return builder.setTimestampToNow().build();
  }
}
