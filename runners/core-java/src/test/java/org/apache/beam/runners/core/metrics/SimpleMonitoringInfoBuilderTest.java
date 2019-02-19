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

import static junit.framework.TestCase.assertEquals;
import static org.apache.beam.sdk.metrics.MetricUrns.ELEMENT_COUNT_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.PCOLLECTION_LABEL;
import static org.apache.beam.sdk.metrics.MetricUrns.PTRANSFORM_LABEL;
import static org.apache.beam.sdk.metrics.MetricUrns.SUM_INT64_TYPE_URN;
import static org.apache.beam.sdk.metrics.MetricUrns.USER_COUNTER_URN_PREFIX;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SimpleMonitoringInfoBuilder}. */
@RunWith(JUnit4.class)
public class SimpleMonitoringInfoBuilderTest {

  @Test
  public void testReturnsNullIfSpecRequirementsNotMet() {
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(ELEMENT_COUNT_URN);
    assertNull(builder.build());

    builder.setInt64Value(1);
    assertNull(builder.build());

    builder.setPCollectionLabel("myPcollection");
    // Pass now that the spec is fully met.
    MonitoringInfo monitoringInfo = builder.build();
    assertTrue(monitoringInfo != null);
    assertEquals("myPcollection", monitoringInfo.getLabelsOrDefault(PCOLLECTION_LABEL, null));
    assertEquals(ELEMENT_COUNT_URN, monitoringInfo.getUrn());
    assertEquals(SUM_INT64_TYPE_URN, monitoringInfo.getType());
    assertEquals(1, monitoringInfo.getMetric().getCounterData().getInt64Value());
  }

  @Test
  public void testUserCounter() {
    SimpleMonitoringInfoBuilder builder =
        new SimpleMonitoringInfoBuilder().userMetric("step", "myNamespace", "myName");
    assertNull(builder.build());

    builder.setInt64Value(1);
    // Pass now that the spec is fully met.
    MonitoringInfo monitoringInfo = builder.build();
    assertNotNull(monitoringInfo);
    assertEquals(USER_COUNTER_URN_PREFIX + "myNamespace:myName", monitoringInfo.getUrn());
    assertEquals(SUM_INT64_TYPE_URN, monitoringInfo.getType());
    assertEquals("step", monitoringInfo.getLabelsOrThrow(PTRANSFORM_LABEL));
    assertEquals(1, monitoringInfo.getMetric().getCounterData().getInt64Value());
  }

  @Test
  public void testUserMetricWithInvalidDelimiterCharacterIsReplaced() {
    MonitoringInfo monitoringInfo =
        new SimpleMonitoringInfoBuilder()
            .userMetric("step", "myNamespace:withInvalidChar", "myName")
            .setInt64Value(1)
            .build();
    // Pass now that the spec is fully met.
    assertNotNull(monitoringInfo);
    assertEquals(
        USER_COUNTER_URN_PREFIX + "myNamespace_withInvalidChar:myName", monitoringInfo.getUrn());
    assertEquals(SUM_INT64_TYPE_URN, monitoringInfo.getType());
    assertEquals("step", monitoringInfo.getLabelsOrThrow(PTRANSFORM_LABEL));
    assertEquals(1, monitoringInfo.getMetric().getCounterData().getInt64Value());
  }
}
