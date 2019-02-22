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

import static org.apache.beam.runners.core.metrics.DistributionProtos.fromProto;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.LATEST_INT64_TYPE_URN;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL;
import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.SUM_INT64_TYPE_URN;

import java.util.function.Consumer;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.model.pipeline.v1.MetricsApi.Metric;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for working with {@link Metric}s and converting {@link MonitoringInfo}s to {@link
 * MetricKey}s.
 */
public class MonitoringInfos {
  private static final Logger LOG = LoggerFactory.getLogger(MonitoringInfos.class);

  /**
   * Helper for handling each case of a {@link Metric}'s "oneof" value field (counter, distribution,
   * or gauge), and gracefully dropping / warning about unexpected and unsupported states.
   */
  public static void forEachMetricType(
      Metric metric,
      String type,
      Timestamp timestamp,
      Consumer<Long> counterFn,
      Consumer<DistributionResult> distributionFn,
      Consumer<GaugeResult> gaugeFn) {
    Metric.DataCase dataCase = metric.getDataCase();
    switch (dataCase) {
      case COUNTER_DATA:
        MetricsApi.CounterData counterData = metric.getCounterData();
        switch (counterData.getValueCase()) {
          case INT64_VALUE:
            long value = counterData.getInt64Value();
            if (type.equals(SUM_INT64_TYPE_URN)) {
              counterFn.accept(value);
            } else if (type.equals(LATEST_INT64_TYPE_URN)) {
              gaugeFn.accept(GaugeProtos.fromProto(value, timestamp));
            } else {
              LOG.warn("Unsupported counter metric type ({}): {}", type, metric);
            }
            break;
          case DOUBLE_VALUE:
          case STRING_VALUE:
            LOG.warn("Unsupported metric type: {}", metric);
            break;
          case VALUE_NOT_SET:
            throw new IllegalStateException("Counter value not set: " + metric);
        }
        break;
      case DISTRIBUTION_DATA:
        MetricsApi.DistributionData distributionData = metric.getDistributionData();
        switch (distributionData.getDistributionCase()) {
          case INT_DISTRIBUTION_DATA:
            distributionFn.accept(fromProto(distributionData.getIntDistributionData()));
            break;
          case DOUBLE_DISTRIBUTION_DATA:
            LOG.warn("Unsupported metric type (double distribution): {}", metric);
            break;
          case DISTRIBUTION_NOT_SET:
            throw new IllegalStateException("Distribution value not set: " + metric);
        }
        break;
      case EXTREMA_DATA:
        LOG.warn("Unsupported metric type (extrema): {}", metric);
        break;
      case DATA_NOT_SET:
        throw new IllegalStateException("Metric value not set: " + metric);
    }
  }

  /**
   * Create a {@link MetricKey} from a {@link MonitoringInfo}
   *
   * <p>Could be moved to {@link MetricKey}, but kept separate here for now while some metrics
   * package/module-dependency questions are being resolved.
   */
  public static MetricKey keyFromMonitoringInfo(MonitoringInfo monitoringInfo) {
    String ptransform = monitoringInfo.getLabelsMap().get(PTRANSFORM_LABEL);
    return MetricKey.create(ptransform, MonitoringInfoMetricName.create(monitoringInfo));
  }
}
