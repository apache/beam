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
package org.apache.beam.fn.harness.control;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class MonitoringInfoShortIdCacheTest {

  @Test
  public void testShortIdAssignment() throws Exception {
    try (MonitoringInfoShortIdCache c = MonitoringInfoShortIdCache.newShortIdCache()) {
      List<Pair<String, MetricsApi.MonitoringInfo>> testCases =
          new ArrayList<Pair<String, MetricsApi.MonitoringInfo>>();

      SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64);
      builder.setInt64DistributionValue(DistributionData.create(1, 1, 1, 1));
      testCases.add(Pair.of("1", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
      builder.setInt64SumValue(2);
      testCases.add(Pair.of("2", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_DOUBLE);
      builder.setDoubleDistributionValue(1, 1, 1, 1);
      testCases.add(Pair.of("3", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn("TestingSentinelUrn");
      builder.setType("TestingSentinelType");
      testCases.add(Pair.of("4", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS);
      builder.setInt64SumValue(2);
      testCases.add(Pair.of("5", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
      builder.setInt64SumValue(3);
      testCases.add(Pair.of("6", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
      builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber7");
      builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
      builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
      builder.setInt64SumValue(4);
      testCases.add(Pair.of("7", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
      builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber8");
      builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
      builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
      builder.setInt64SumValue(4);
      testCases.add(Pair.of("8", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
      builder.setLabel(MonitoringInfoConstants.Labels.SERVICE, "BigQuery");
      builder.setInt64SumValue(4);
      testCases.add(Pair.of("9", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.API_REQUEST_COUNT);
      builder.setLabel(MonitoringInfoConstants.Labels.SERVICE, "Storage");
      builder.setInt64SumValue(4);
      testCases.add(Pair.of("a", builder.build()));

      // Validate that modifying the payload, but using the same URN/labels
      // does not change the shortId assignment.
      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_DOUBLE);
      builder.setDoubleDistributionValue(2, 2, 1, 1);
      testCases.add(Pair.of("3", builder.build()));

      builder = new SimpleMonitoringInfoBuilder(false);
      builder.setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64);
      builder.setLabel(MonitoringInfoConstants.Labels.NAME, "metricNumber7");
      builder.setLabel(MonitoringInfoConstants.Labels.NAMESPACE, "myNamespace");
      builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, "myPtransform");
      builder.setInt64SumValue(8);
      testCases.add(Pair.of("7", builder.build()));

      // Verify each short ID is assigned properly.
      Set<String> expectedShortIds = new HashSet<String>();
      for (Pair<String, MetricsApi.MonitoringInfo> entry : testCases) {
        MonitoringInfoShortIdCache shortIdCache = MonitoringInfoShortIdCache.getShortIdCache();
        assertEquals(entry.getKey(), shortIdCache.getShortId(entry.getValue()));
        expectedShortIds.add(entry.getKey());
      }

      HashMap<String, MetricsApi.MonitoringInfo> actualRecoveredInfos =
          MonitoringInfoShortIdCache.getShortIdCache().getInfos(expectedShortIds);
      // Retrieve all of the MonitoringInfos by short id, and verify that the
      // metadata (everything but the payload) matches the originals
      assertEquals(expectedShortIds, actualRecoveredInfos.keySet());
      for (Pair<String, MetricsApi.MonitoringInfo> entry : testCases) {
        MetricsApi.MonitoringInfo expectedMonitoringInfo = entry.getValue();
        MetricsApi.MonitoringInfo.Builder b =
            MetricsApi.MonitoringInfo.newBuilder(expectedMonitoringInfo);
        b.clearPayload();
        assertEquals(b.build(), actualRecoveredInfos.get(entry.getKey()));
      }

      // Verify each short ID is assigned properly, in reverse.
      for (int i = testCases.size() - 1; i > 0; i--) {
        MonitoringInfoShortIdCache shortIdCache = MonitoringInfoShortIdCache.getShortIdCache();
        assertEquals(
            testCases.get(i).getKey(), shortIdCache.getShortId(testCases.get(i).getValue()));
      }
    }
  }
}
