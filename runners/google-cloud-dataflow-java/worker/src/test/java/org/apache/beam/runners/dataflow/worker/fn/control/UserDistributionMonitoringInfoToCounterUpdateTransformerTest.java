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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.runners.dataflow.worker.testing.GenericJsonAssert.assertEqualsAsJson;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.SpecMonitoringInfoValidator;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class UserDistributionMonitoringInfoToCounterUpdateTransformerTest {

  @Rule public final ExpectedException exception = ExpectedException.none();

  @Mock private SpecMonitoringInfoValidator mockSpecValidator;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testTransformReturnsNullIfSpecValidationFails() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    UserDistributionMonitoringInfoToCounterUpdateTransformer testObject =
        new UserDistributionMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, stepContextMapping);
    Optional<String> error = Optional.of("Error text");
    when(mockSpecValidator.validate(any())).thenReturn(error);
    assertNull(testObject.transform(null));
  }

  @Test
  public void testTransformThrowsIfMonitoringInfoWithWrongUrnPrefixReceived() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(Urns.ELEMENT_COUNT)
            .setType(TypeUrns.SUM_INT64_TYPE)
            .build();
    UserDistributionMonitoringInfoToCounterUpdateTransformer testObject =
        new UserDistributionMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    exception.expect(RuntimeException.class);
    testObject.transform(monitoringInfo);
  }

  @Test
  public void testTransformReturnsNullIfMonitoringInfoWithUnknownPTransformLabelPresent() {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(Urns.USER_DISTRIBUTION_INT64)
            .setType(TypeUrns.DISTRIBUTION_INT64_TYPE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, "anyName")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "anyNamespace")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .build();
    UserDistributionMonitoringInfoToCounterUpdateTransformer testObject =
        new UserDistributionMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());
    assertNull(testObject.transform(monitoringInfo));
  }

  @Test
  public void testTransformReturnsValidCounterUpdateWhenValidUserMonitoringInfoReceived()
      throws Exception {
    Map<String, DataflowStepContext> stepContextMapping = new HashMap<>();
    NameContext nc =
        NameContext.create("anyStageName", "anyOriginalName", "anySystemName", "anyUserName");
    DataflowStepContext dsc = mock(DataflowStepContext.class);
    when(dsc.getNameContext()).thenReturn(nc);
    stepContextMapping.put("anyValue", dsc);

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(Urns.USER_DISTRIBUTION_INT64)
            .setType(TypeUrns.DISTRIBUTION_INT64_TYPE)
            .putLabels(MonitoringInfoConstants.Labels.NAME, "anyName")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "anyNamespace")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .setPayload(
                encodeInt64Distribution(
                    DistributionData.create(
                        2L /* sum */, 1L /* count */, 3L /* min */, 4L /* max */)))
            .build();
    UserDistributionMonitoringInfoToCounterUpdateTransformer testObject =
        new UserDistributionMonitoringInfoToCounterUpdateTransformer(
            mockSpecValidator, stepContextMapping);
    when(mockSpecValidator.validate(any())).thenReturn(Optional.empty());

    CounterUpdate result = testObject.transform(monitoringInfo);
    assertNotNull(result);

    assertEqualsAsJson(
        "{cumulative:true, distribution:{count:{highBits:0, lowBits:1}, "
            + "max:{highBits:0, lowBits:4}, min:{highBits:0, lowBits:3}, "
            + "sum:{highBits:0, lowBits:2}}, "
            + "structuredNameAndMetadata:{metadata:{kind:'DISTRIBUTION'}, "
            + "name:{name:'anyName', origin:'USER', originNamespace:'anyNamespace', "
            + "originalStepName:'anyOriginalName'}}}",
        result);
  }
}
