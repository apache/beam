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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.sdk.values.KV;
import org.junit.Test;

public class ShortIdMapTest {

  @Test
  public void testShortIdAssignment() throws Exception {
    ShortIdMap shortIdMap = new ShortIdMap();
    List<KV<String, MetricsApi.MonitoringInfo>> testCases = new ArrayList<>();

    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64);
    testCases.add(KV.of("metric0", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
    testCases.add(KV.of("metric1", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_DOUBLE);
    testCases.add(KV.of("metric2", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn("TestingSentinelUrn");
    builder.setType("TestingSentinelType");
    testCases.add(KV.of("metric3", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS);
    testCases.add(KV.of("metric4", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
    testCases.add(KV.of("metric5", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
    builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber7");
    builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
    testCases.add(KV.of("metric6", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
    builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber8");
    builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
    testCases.add(KV.of("metric7", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.SERVICE, "BigQuery");
    testCases.add(KV.of("metric8", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
    builder.setLabel(MonitoringInfoConstants.Labels.SERVICE, "Storage");
    testCases.add(KV.of("metric9", builder.build()));

    // Validate that modifying the payload, but using the same URN/labels
    // does not change the shortId assignment.
    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_DOUBLE);
    testCases.add(KV.of("metric2", builder.build()));

    builder = new SimpleMonitoringInfoBuilder(false);
    builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
    builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber7");
    builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
    testCases.add(KV.of("metric6", builder.build()));

    // Verify each short ID is assigned properly.
    Set<String> expectedShortIds = new HashSet<String>();
    for (KV<String, MetricsApi.MonitoringInfo> entry : testCases) {
      assertEquals(entry.getKey(), shortIdMap.getOrCreateShortId(entry.getValue()));
      expectedShortIds.add(entry.getKey());
    }

    HashMap<String, MetricsApi.MonitoringInfo> actualRecoveredInfos = new HashMap<>();
    for (String expectedShortId : expectedShortIds) {
      actualRecoveredInfos.put(expectedShortId, shortIdMap.get(expectedShortId));
    }
    // Retrieve all of the MonitoringInfos by short id, and verify that the
    // metadata (everything but the payload) matches the originals
    assertEquals(expectedShortIds, actualRecoveredInfos.keySet());
    for (KV<String, MetricsApi.MonitoringInfo> entry : testCases) {
      // Clear payloads of both expected and actual before comparing
      MetricsApi.MonitoringInfo expectedMonitoringInfo = entry.getValue();
      MetricsApi.MonitoringInfo.Builder expected =
          MetricsApi.MonitoringInfo.newBuilder(expectedMonitoringInfo);
      expected.clearPayload();

      MetricsApi.MonitoringInfo.Builder actual =
          MetricsApi.MonitoringInfo.newBuilder(actualRecoveredInfos.get(entry.getKey()));
      actual.clearPayload();
      assertEquals(expected.build(), actual.build());
    }

    // Verify each short ID is assigned properly, in reverse.
    for (int i = testCases.size() - 1; i > 0; i--) {
      assertEquals(
          testCases.get(i).getKey(), shortIdMap.getOrCreateShortId(testCases.get(i).getValue()));
    }
  }
}
