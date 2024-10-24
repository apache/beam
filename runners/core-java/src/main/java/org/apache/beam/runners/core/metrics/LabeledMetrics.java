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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.DelegatingCounter;
import org.apache.beam.sdk.metrics.DelegatingDistribution;
import org.apache.beam.sdk.metrics.DelegatingHistogram;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.util.HistogramData;

/**
 * Define a metric on the current MetricContainer with a specific URN and a set of labels. This is a
 * more convenient way to collect the necessary fields to repackage the metric into a MonitoringInfo
 * proto later. Intended for internal use only (SDK and RunnerHarness developement).
 */
public class LabeledMetrics {
  /**
   * Create a metric that can be incremented and decremented, and is aggregated by taking the sum.
   */
  public static Counter counter(MonitoringInfoMetricName metricName) {
    return new DelegatingCounter(metricName);
  }

  public static Counter counter(MonitoringInfoMetricName metricName, boolean processWideContainer) {
    return new DelegatingCounter(metricName, processWideContainer);
  }

  public static Distribution distribution(MonitoringInfoMetricName metricName) {
    return new DelegatingDistribution(metricName);
  }

  public static Histogram histogram(
      MonitoringInfoMetricName metricName,
      HistogramData.BucketType bucketType,
      boolean processWideContainer) {
    return new DelegatingHistogram(metricName, bucketType, processWideContainer);
  }

  public static Histogram histogram(
      MonitoringInfoMetricName metricName,
      HistogramData.BucketType bucketType,
      boolean processWideContainer,
      boolean perWorkerHistogram) {
    return new DelegatingHistogram(
        metricName, bucketType, processWideContainer, perWorkerHistogram);
  }
}
