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
package org.apache.beam.fn.harness.jmh.logging;

import org.apache.beam.fn.harness.jmh.logging.BeamFnLoggingClientBenchmark.ManageExecutionState;
import org.apache.beam.fn.harness.jmh.logging.BeamFnLoggingClientBenchmark.ManyExpectedCallsLoggingClientAndService;
import org.apache.beam.fn.harness.jmh.logging.BeamFnLoggingClientBenchmark.ZeroExpectedCallsLoggingClientAndService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamFnLoggingClientBenchmark}. */
@RunWith(JUnit4.class)
public class BeamFnLoggingClientBenchmarkTest {
  @Test
  public void testLogging() throws Exception {
    ManyExpectedCallsLoggingClientAndService service =
        new ManyExpectedCallsLoggingClientAndService();
    new BeamFnLoggingClientBenchmark().testLogging(service);
    service.tearDown();
  }

  @Test
  public void testLoggingWithAllOptionalParameters() throws Exception {
    ManyExpectedCallsLoggingClientAndService service =
        new ManyExpectedCallsLoggingClientAndService();
    ManageExecutionState state = new ManageExecutionState();
    new BeamFnLoggingClientBenchmark().testLoggingWithAllOptionalParameters(service, state);
    state.tearDown();
    service.tearDown();
  }

  @Test
  public void testSkippedLogging() throws Exception {
    ZeroExpectedCallsLoggingClientAndService service =
        new ZeroExpectedCallsLoggingClientAndService();
    new BeamFnLoggingClientBenchmark().testSkippedLogging(service);
    service.tearDown();
  }
}
