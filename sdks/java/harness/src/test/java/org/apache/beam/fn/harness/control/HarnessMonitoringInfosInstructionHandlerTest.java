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
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.core.metrics.LabeledMetrics;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.junit.Test;

public class HarnessMonitoringInfosInstructionHandlerTest {

  @Test
  public void testReturnsProcessWideMonitoringInfos() {
    MetricsEnvironment.setProcessWideContainer(MetricsContainerImpl.createProcessWideContainer());

    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "service");
    labels.put(MonitoringInfoConstants.Labels.METHOD, "method");
    labels.put(MonitoringInfoConstants.Labels.RESOURCE, "resource");
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "transform");
    labels.put(MonitoringInfoConstants.Labels.STATUS, "ok");
    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);
    Counter counter = LabeledMetrics.counter(name, true);
    counter.inc(7);

    ShortIdMap metricsShortIds = new ShortIdMap();
    HarnessMonitoringInfosInstructionHandler testObject =
        new HarnessMonitoringInfosInstructionHandler(metricsShortIds);

    BeamFnApi.InstructionRequest.Builder builder = BeamFnApi.InstructionRequest.newBuilder();
    BeamFnApi.InstructionResponse.Builder responseBuilder =
        testObject.harnessMonitoringInfos(builder.build());

    BeamFnApi.InstructionResponse response = responseBuilder.build();
    assertEquals(1, response.getHarnessMonitoringInfos().getMonitoringDataMap().size());

    // Expect a payload to be set for "metric0".
    assertTrue(
        !response.getHarnessMonitoringInfos().getMonitoringDataMap().get("metric0").isEmpty());
  }
}
