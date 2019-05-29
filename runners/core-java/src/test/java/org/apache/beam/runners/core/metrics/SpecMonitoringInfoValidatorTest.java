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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.junit.Before;
import org.junit.Test;

/** Relevant tests. */
public class SpecMonitoringInfoValidatorTest {

  SpecMonitoringInfoValidator testObject = null;

  @Before
  public void setUp() throws Exception {
    testObject = new SpecMonitoringInfoValidator();
  }

  @Test
  public void validateReturnsErrorOnInvalidMonitoringInfoType() {
    MonitoringInfo testInput =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:user")
            .putLabels(MonitoringInfoConstants.Labels.NAME, "anyCounter")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "namespace")
            .setType("beam:metrics:bad_value")
            .build();
    assertTrue(testObject.validate(testInput).isPresent());
  }

  @Test
  public void validateReturnsNoErrorOnValidMonitoringInfo() {
    MonitoringInfo testInput =
        MonitoringInfo.newBuilder()
            .setUrn(Urns.USER_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAME, "anyCounter")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyString")
            .setType(TypeUrns.SUM_INT64)
            .build();
    assertFalse(testObject.validate(testInput).isPresent());

    testInput =
        MonitoringInfo.newBuilder()
            .setUrn(Urns.USER_DISTRIBUTION_COUNTER)
            .putLabels(MonitoringInfoConstants.Labels.NAME, "anyDistribution")
            .putLabels(MonitoringInfoConstants.Labels.NAMESPACE, "namespace")
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "anyString")
            .setType(TypeUrns.DISTRIBUTION_INT64)
            .putLabels("dummy", "value")
            .build();
    assertFalse(testObject.validate(testInput).isPresent());

    testInput =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT)
            .setType(TypeUrns.SUM_INT64)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "value")
            .putLabels(MonitoringInfoConstants.Labels.PCOLLECTION, "anotherValue")
            .build();
    assertFalse(testObject.validate(testInput).isPresent());
  }

  @Test
  public void validateReturnsErrorOnInvalidMonitoringInfoLabels() {
    MonitoringInfo testInput =
        MonitoringInfo.newBuilder()
            .setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT)
            .setType(TypeUrns.SUM_INT64)
            .putLabels(MonitoringInfoConstants.Labels.PTRANSFORM, "unexpectedLabel")
            .build();
    assertTrue(testObject.validate(testInput).isPresent());
  }
}
