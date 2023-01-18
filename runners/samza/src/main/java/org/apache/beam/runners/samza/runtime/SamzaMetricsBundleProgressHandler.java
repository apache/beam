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
package org.apache.beam.runners.samza.runtime;

import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.DISTRIBUTION_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.LATEST_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns.SUM_INT64_TYPE;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;

import java.util.List;
import java.util.Map;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SamzaMetricsBundleProgressHandler implements BundleProgressHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SamzaMetricsBundleProgressHandler.class);
  private final String stepName;

  private final SamzaMetricsContainer samzaMetricsContainer;
  private final Map<String, String> transformIdToUniqueName;

  public SamzaMetricsBundleProgressHandler(
      String stepName,
      SamzaMetricsContainer samzaMetricsContainer,
      Map<String, String> transformIdToUniqueName) {
    this.stepName = stepName;
    this.samzaMetricsContainer = samzaMetricsContainer;
    this.transformIdToUniqueName = transformIdToUniqueName;
  }

  @Override
  public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {}

  @Override
  public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
    handleMonitoringInfos(response.getMonitoringInfosList());
  }

  private void handleMonitoringInfos(List<MetricsApi.MonitoringInfo> monitoringInfos) {
    for (MetricsApi.MonitoringInfo monitoringInfo : monitoringInfos) {
      if (monitoringInfo.getPayload().isEmpty()) {
        return;
      }

      String pTransformId =
          monitoringInfo.getLabelsOrDefault(
              MonitoringInfoConstants.Labels.PTRANSFORM, this.stepName);
      String stepName = transformIdToUniqueName.getOrDefault(pTransformId, pTransformId);
      MetricsContainer metricsContainer = samzaMetricsContainer.getContainer(stepName);
      String namespace =
          monitoringInfo.getLabelsOrDefault(
              MonitoringInfoConstants.Labels.NAMESPACE, monitoringInfo.getUrn());
      String counterName =
          monitoringInfo.getLabelsOrDefault(
              MonitoringInfoConstants.Labels.NAME, monitoringInfo.getLabelsMap().toString());
      MetricName metricName = MetricName.named(namespace, counterName);

      switch (monitoringInfo.getType()) {
        case SUM_INT64_TYPE:
          Counter counter = metricsContainer.getCounter(metricName);
          counter.inc(decodeInt64Counter(monitoringInfo.getPayload()));
          break;

        case DISTRIBUTION_INT64_TYPE:
          Distribution distribution = metricsContainer.getDistribution(metricName);
          DistributionData data = decodeInt64Distribution(monitoringInfo.getPayload());
          distribution.update(data.sum(), data.count(), data.min(), data.max());
          break;

        case LATEST_INT64_TYPE:
          Gauge gauge = metricsContainer.getGauge(metricName);
          // Gauge doesn't expose update as public. This will reset the timestamp.
          gauge.set(decodeInt64Gauge(monitoringInfo.getPayload()).value());
          break;

        default:
          LOG.warn("Unsupported metric type {}", monitoringInfo.getType());
      }
    }
  }
}
