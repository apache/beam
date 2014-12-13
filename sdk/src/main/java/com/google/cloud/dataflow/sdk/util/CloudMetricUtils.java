/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util;

import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.Metric.DoubleMetric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for working with Dataflow API Metrics.
 */
public class CloudMetricUtils {
  // Do not instantiate.
  private CloudMetricUtils() {}

  /**
   * Returns a List of {@link MetricUpdate}s representing the given Metrics.
   */
  public static List<MetricUpdate> extractCloudMetrics(
      Collection<Metric<?>> metrics,
      String workerId) {
    List<MetricUpdate> cloudMetrics = new ArrayList<>(metrics.size());
    for (Metric<?> metric : metrics) {
      cloudMetrics.add(extractCloudMetric(metric, workerId));
    }
    return cloudMetrics;
  }

  /**
   * Returns a {@link MetricUpdate} representing the given Metric.
   */
  public static MetricUpdate extractCloudMetric(Metric<?> metric, String workerId) {
    if (metric instanceof DoubleMetric) {
      return extractCloudMetric(
          metric,
          ((DoubleMetric) metric).getValue(),
          workerId);
    } else {
      throw new IllegalArgumentException("unexpected kind of Metric");
    }
  }

  private static MetricUpdate extractCloudMetric(
      Metric<?> metric, Double value, String workerId) {
    MetricStructuredName name = new MetricStructuredName();
    name.setName(metric.getName());
    Map<String, String> context = new HashMap<>();
    context.put("workerId", workerId);
    name.setContext(context);
    return new MetricUpdate().setName(name).setScalar(CloudObject.forFloat(value));
  }
}
