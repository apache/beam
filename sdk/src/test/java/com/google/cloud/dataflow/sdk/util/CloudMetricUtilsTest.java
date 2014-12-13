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

import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.Metric.DoubleMetric;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link CloudMetricUtils}. */
@RunWith(JUnit4.class)
public class CloudMetricUtilsTest {
  private void addDoubleMetric(String name, double value, String workerId,
                                List<Metric<?>> metrics,
                                List<MetricUpdate> cloudMetrics) {
    metrics.add(new DoubleMetric(name, value));
    MetricStructuredName structuredName = new MetricStructuredName();
    structuredName.setName(name);
    Map<String, String> context = new HashMap<>();
    context.put("workerId", workerId);
    structuredName.setContext(context);
    cloudMetrics.add(new MetricUpdate()
        .setName(structuredName)
        .setScalar(CloudObject.forFloat(value)));
  }

  @Test
  public void testExtractCloudMetrics() {
    List<Metric<?>> metrics = new ArrayList<>();
    List<MetricUpdate> expected = new ArrayList<>();
    String workerId = "worker-id";

    addDoubleMetric("m1", 3.14, workerId, metrics, expected);
    addDoubleMetric("m2", 2.17, workerId, metrics, expected);
    addDoubleMetric("m3", -66.666, workerId, metrics, expected);

    List<MetricUpdate> actual = CloudMetricUtils.extractCloudMetrics(metrics, workerId);

    assertEquals(expected, actual);
  }
}
