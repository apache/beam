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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BundleProgressReporterTest {
  @Test
  public void testInMemory() {
    Map<String, ByteString> valuesToReport = new HashMap<>();
    BundleProgressReporter reporter = mock(BundleProgressReporter.class);
    BundleProgressReporter.InMemory value = new BundleProgressReporter.InMemory();

    value.register(reporter);
    verifyNoInteractions(reporter);

    value.updateIntermediateMonitoringData(valuesToReport);
    verify(reporter).updateIntermediateMonitoringData(valuesToReport);
    verifyNoMoreInteractions(reporter);

    value.updateFinalMonitoringData(valuesToReport);
    verify(reporter).updateFinalMonitoringData(valuesToReport);
    verifyNoMoreInteractions(reporter);

    value.reset();
    verify(reporter).reset();
    verifyNoMoreInteractions(reporter);
  }
}
