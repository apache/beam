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

import static org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder.PTRANSFORM_LABEL;

import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Metric;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
import org.apache.beam.runners.core.construction.metrics.MetricKey;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;

/**
 * Helpers for working with {@link Metric}s and converting {@link MonitoringInfo}s to {@link
 * MetricKey}s.
 */
public class MonitoringInfos {

  /**
   * Helper for handling each case of a {@link Metric}'s "oneof" value field (counter, distribution,
   * or gauge).
   */
  public static void processMetric(
      Metric metric,
      Consumer<Long> counterFn,
      Consumer<DistributionResult> distributionFn,
      Consumer<GaugeResult> gaugeFn) {
    Metric.DataCase dataCase = metric.getDataCase();
    switch (dataCase) {
      case COUNTER:
        counterFn.accept(metric.getCounter());
        break;
      case DISTRIBUTION:
        distributionFn.accept(DistributionProtos.fromProto(metric.getDistribution()));
        break;
      case GAUGE:
        gaugeFn.accept(GaugeProtos.fromProto(metric.getGauge()));
        break;
      case DATA_NOT_SET:
        throw new IllegalStateException("Metric value not set: " + metric);
    }
  }

  public static MetricKey keyFromMonitoringInfo(MonitoringInfo monitoringInfo) {
    String ptransform = monitoringInfo.getLabelsMap().get(PTRANSFORM_LABEL);
    return MetricKey.create(ptransform, MonitoringInfoMetricName.create(monitoringInfo));
  }
}
