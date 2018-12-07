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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.MonitoringInfo;
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
            .setUrn("beam:metric:user:someCounter")
            .setType("beam:metrics:bad_value")
            .build();
    assertTrue(testObject.validate(testInput).isPresent());
  }

  @Test
  public void validateReturnsNoErrorOnValidMonitoringInfo() {
    MonitoringInfo testInput =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:user:someCounter")
            .setType("beam:metrics:sum_int_64")
            .putLabels("dummy", "value")
            .build();
    assertFalse(testObject.validate(testInput).isPresent());

    testInput =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .setType("beam:metrics:sum_int_64")
            .putLabels("PTRANSFORM", "value")
            .putLabels("PCOLLECTION", "anotherValue")
            .build();
    assertFalse(testObject.validate(testInput).isPresent());
  }

  @Test
  public void validateReturnsErrorOnInvalidMonitoringInfoLabels() {
    MonitoringInfo testInput =
        MonitoringInfo.newBuilder()
            .setUrn("beam:metric:element_count:v1")
            .setType("beam:metrics:sum_int_64")
            .putLabels("PCOLLECTION", "anotherValue")
            .build();
    assertTrue(testObject.validate(testInput).isPresent());
  }
}
