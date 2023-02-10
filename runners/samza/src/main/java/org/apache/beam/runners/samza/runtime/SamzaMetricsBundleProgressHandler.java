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

/**
 * {@inheritDoc} Parses metrics information contained in the bundle progress messages. Passed the
 * updated metrics to the provided SamzaMetricsContainer.
 */
class SamzaMetricsBundleProgressHandler implements BundleProgressHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(SamzaMetricsBundleProgressHandler.class);
  private final String stepName;

  private final SamzaMetricsContainer samzaMetricsContainer;
  private final Map<String, String> transformIdToUniqueName;

  /**
   * Constructor of a SamzaMetricsBundleProgressHandler.
   *
   * <p>The full metric names in classic mode is {transformUniqueName}:{className}:{metricName}. We
   * attempt to follow the same format in portable mode, but the monitoringInfos returned by the
   * worker only contains the transformId. The current solution is to provide a mapping from
   * transformId back to uniqueName. A future improvement would be making the monitoring infos
   * contain the uniqueName.
   *
   * @param stepName Default stepName provided by the runner.
   * @param samzaMetricsContainer The destination for publishing the metrics.
   * @param transformIdToUniqueName A mapping from transformId to uniqueName for pTransforms.
   */
  public SamzaMetricsBundleProgressHandler(
      String stepName,
      SamzaMetricsContainer samzaMetricsContainer,
      Map<String, String> transformIdToUniqueName) {
    this.stepName = stepName;
    this.samzaMetricsContainer = samzaMetricsContainer;
    this.transformIdToUniqueName = transformIdToUniqueName;
  }

  @Override
  /**
   * {@inheritDoc} Handles a progress report from the bundle while it is executing. We choose to
   * ignore the progress report. The metrics do not have to be updated on every progress report, so
   * we save computation resources by ignoring it.
   */
  public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {}

  @Override
  /**
   * {@inheritDoc} Handles the bundle's completion report. Parses the monitoringInfos in the
   * response, then updates the MetricsRegistry.
   */
  public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
    response.getMonitoringInfosList().stream()
        .filter(monitoringInfo -> !monitoringInfo.getPayload().isEmpty())
        .map(this::parseAndUpdateMetric)
        .distinct()
        .forEach(samzaMetricsContainer::updateMetrics);
  }

  /**
   * Parses the metric contained in monitoringInfo, then publishes the metric to the
   * metricContainer.
   *
   * <p>We attempt to construct a classic mode metricName
   * ({transformUniqueName}:{className}:{metricName}). All the info should be in the labels, but we
   * have fallbacks in case the labels don't exist.
   *
   * <p>Priorities for the transformUniqueName 1. Obtained transformUniqueName using the
   * transformIdToUniqueName 2. The transformId provided by the monitoringInfo 3. The stepName
   * provided by the runner, which maybe a result of fusing.
   *
   * <p>Priorities for the className 1. The namespace label 2. The monitoringInfo urn. Copying the
   * implementation in MonitoringInfoMetricName.
   *
   * <p>Priorities for the metricName 1. The name label 2. The monitoringInfo urn. Copying the
   * implementation in MonitoringInfoMetricName.
   *
   * @see
   *     org.apache.beam.runners.core.metrics.MonitoringInfoMetricName#of(MetricsApi.MonitoringInfo)
   * @return the final transformUniqueName for the metric
   */
  private String parseAndUpdateMetric(MetricsApi.MonitoringInfo monitoringInfo) {
    String pTransformId =
        monitoringInfo.getLabelsOrDefault(MonitoringInfoConstants.Labels.PTRANSFORM, stepName);
    String transformUniqueName = transformIdToUniqueName.getOrDefault(pTransformId, pTransformId);
    String className =
        monitoringInfo.getLabelsOrDefault(
            MonitoringInfoConstants.Labels.NAMESPACE, monitoringInfo.getUrn());
    String userMetricName =
        monitoringInfo.getLabelsOrDefault(
            MonitoringInfoConstants.Labels.NAME, monitoringInfo.getLabelsMap().toString());

    MetricsContainer metricsContainer = samzaMetricsContainer.getContainer(transformUniqueName);
    MetricName metricName = MetricName.named(className, userMetricName);

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
        LOG.debug("Unsupported metric type {}", monitoringInfo.getType());
    }
    return transformUniqueName;
  }
}
