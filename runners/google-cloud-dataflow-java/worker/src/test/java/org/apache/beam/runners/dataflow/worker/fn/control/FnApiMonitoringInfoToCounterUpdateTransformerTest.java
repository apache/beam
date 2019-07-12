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

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
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
    genericTransformers.put(validUrn, mockTransformer1);
    genericTransformers.put("any:other:urn", mockTransformer2);

    FnApiMonitoringInfoToCounterUpdateTransformer testObject =
        new FnApiMonitoringInfoToCounterUpdateTransformer(genericTransformers);

    CounterUpdate expectedResult = new CounterUpdate();
    when(mockTransformer1.transform(any())).thenReturn(expectedResult);

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn(validUrn)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .build();

    CounterUpdate result = testObject.transform(monitoringInfo);

    assertSame(expectedResult, result);
  }

  @Test
  public void testTransformReturnsNullOnUnknownUrn() {
    Map<String, MonitoringInfoToCounterUpdateTransformer> genericTransformers = new HashMap<>();

    genericTransformers.put("beam:metric:user", mockTransformer2);

    FnApiMonitoringInfoToCounterUpdateTransformer testObject =
        new FnApiMonitoringInfoToCounterUpdateTransformer(genericTransformers);

    CounterUpdate expectedResult = new CounterUpdate();
    when(mockTransformer1.transform(any())).thenReturn(expectedResult);

    MonitoringInfo monitoringInfo =
        MonitoringInfo.newBuilder()
            .setUrn("any:other:urn")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyValue")
            .build();

    CounterUpdate result = testObject.transform(monitoringInfo);

    assertSame(null, result);
  }
}
