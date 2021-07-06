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

import java.util.HashMap;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.junit.Assert;
import org.junit.Test;

public class ServiceCallMetricTest {

  @Test
  public void testCall() {
    // Test that its on the ProcessWideMetricContainer.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);

    String urn = MonitoringInfoConstants.Urns.API_REQUEST_COUNT;
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put("key", "value");
    ServiceCallMetric metric =
        new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);

    // Http Status Code
    metric.call(200);
    labels.put(MonitoringInfoConstants.Labels.STATUS, "ok");
    MonitoringInfoMetricName name = MonitoringInfoMetricName.named(urn, labels);
    Assert.assertEquals(1, (long) container.getCounter(name).getCumulative());

    // Normalize the status by lower casing and mapping to a canonical name with underscores.
    metric.call("notFound");
    labels.put(MonitoringInfoConstants.Labels.STATUS, "not_found");
    name = MonitoringInfoMetricName.named(urn, labels);
    Assert.assertEquals(1, (long) container.getCounter(name).getCumulative());

    // Normalize the status by lower casing and mapping to a canonical name with underscores.
    metric.call("PERMISSIONDENIED");
    labels.put(MonitoringInfoConstants.Labels.STATUS, "permission_denied");
    name = MonitoringInfoMetricName.named(urn, labels);
    Assert.assertEquals(1, (long) container.getCounter(name).getCumulative());

    // Accept other string codes passed in, even if they aren't in the canonical map.
    metric.call("something_else");
    labels.put(MonitoringInfoConstants.Labels.STATUS, "something_else");
    name = MonitoringInfoMetricName.named(urn, labels);
    Assert.assertEquals(1, (long) container.getCounter(name).getCumulative());

    // Map unknown numeric codes to "unknown"
    metric.call(123);
    labels.put(MonitoringInfoConstants.Labels.STATUS, "unknown");
    name = MonitoringInfoMetricName.named(urn, labels);
    Assert.assertEquals(1, (long) container.getCounter(name).getCumulative());
  }
}
