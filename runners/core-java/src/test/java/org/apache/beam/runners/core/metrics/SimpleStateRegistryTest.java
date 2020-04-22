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

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

/** Tests for {@link SimpleStateRegistryTest}. */
public class SimpleStateRegistryTest {

  @Test
  public void testExecutionTimeUrnsBuildMonitoringInfos() throws Exception {
    String testPTransformId = "pTransformId";
    HashMap<String, String> labelsMetadata = new HashMap<String, String>();
    labelsMetadata.put(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
    SimpleExecutionState startState =
        new SimpleExecutionState(
            ExecutionStateTracker.START_STATE_NAME,
            MonitoringInfoConstants.Urns.START_BUNDLE_MSECS,
            labelsMetadata);
    SimpleExecutionState processState =
        new SimpleExecutionState(
            ExecutionStateTracker.PROCESS_STATE_NAME,
            MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS,
            labelsMetadata);
    SimpleExecutionState finishState =
        new SimpleExecutionState(
            ExecutionStateTracker.FINISH_STATE_NAME,
            MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS,
            labelsMetadata);

    SimpleStateRegistry testObject = new SimpleStateRegistry();
    testObject.register(startState);
    testObject.register(processState);
    testObject.register(finishState);
    List<MonitoringInfo> testOutput = testObject.getExecutionTimeMonitoringInfos();

    List<Matcher<MonitoringInfo>> matchers = new ArrayList<Matcher<MonitoringInfo>>();
    SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.START_BUNDLE_MSECS);
    builder.setInt64SumValue(0);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    // Check for execution time metrics for the testPTransformId
    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.PROCESS_BUNDLE_MSECS);
    builder.setInt64SumValue(0);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    builder = new SimpleMonitoringInfoBuilder();
    builder.setUrn(MonitoringInfoConstants.Urns.FINISH_BUNDLE_MSECS);
    builder.setInt64SumValue(0);
    builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
    matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

    for (Matcher<MonitoringInfo> matcher : matchers) {
      assertThat(testOutput, Matchers.hasItem(matcher));
    }
  }

  @Test
  public void testResetRegistry() {
    SimpleExecutionState state1 = mock(SimpleExecutionState.class);
    SimpleExecutionState state2 = mock(SimpleExecutionState.class);

    SimpleStateRegistry testObject = new SimpleStateRegistry();
    testObject.register(state1);
    testObject.register(state2);

    testObject.reset();
    verify(state1, times(1)).reset();
    verify(state2, times(1)).reset();
  }
}
