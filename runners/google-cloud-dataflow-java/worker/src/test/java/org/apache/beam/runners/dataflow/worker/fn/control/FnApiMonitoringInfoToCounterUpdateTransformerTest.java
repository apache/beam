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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class FnApiMonitoringInfoToCounterUpdateTransformerTest {

  @Mock private UserMonitoringInfoToCounterUpdateTransformer mockTransformer2;
  @Mock private UserMonitoringInfoToCounterUpdateTransformer mockTransformer1;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testTransformUtilizesRelevantCounterTransformer() {
    Map<String, MonitoringInfoToCounterUpdateTransformer> genericTransformers = new HashMap<>();

    final String validUrn = "urn1";
    final String validType = "type1";
    genericTransformers.put(validUrn, mockTransformer1);
    genericTransformers.put("any:other:urn", mockTransformer2);

    FnApiMonitoringInfoToCounterUpdateTransformer testObject =
        new FnApiMonitoringInfoToCounterUpdateTransformer(genericTransformers);

    CounterUpdate expectedResult = new CounterUpdate();
    when(mockTransformer1.transform(any())).thenReturn(expectedResult);

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(validUrn)
            .setType(validType)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .build();

    CounterUpdate result = testObject.transform(monitoringInfo);

    assertSame(expectedResult, result);
    verifyZeroInteractions(mockTransformer2);
  }

  @Test
  public void testTransformReturnsNullOnUnknownUrn() {
    Map<String, MonitoringInfoToCounterUpdateTransformer> genericTransformers = new HashMap<>();

    final String validUrn = "known:urn";
    final String validType = "known:type";
    genericTransformers.put(validUrn, mockTransformer2);

    FnApiMonitoringInfoToCounterUpdateTransformer testObject =
        new FnApiMonitoringInfoToCounterUpdateTransformer(genericTransformers);

    MonitoringInfo unknownUrn =
        MonitoringInfo.newBuilder()
            .setUrn("unknown:urn")
            .setType(validType)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .build();
    assertNull(testObject.transform(unknownUrn));

    verifyZeroInteractions(mockTransformer1, mockTransformer2);
  }
}
