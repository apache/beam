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
package org.apache.beam.runners.fnexecution.environment;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProcessEnvironment}. */
@RunWith(JUnit4.class)
public class ProcessEnvironmentTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Test
  public void closeClosesInstructionRequestHandler() throws Exception {
    InstructionRequestHandler handler = mock(InstructionRequestHandler.class);
    RemoteEnvironment env =
        ProcessEnvironment.create(
            mock(ProcessManager.class), Environment.getDefaultInstance(), "1", handler);

    env.close();
    verify(handler).close();
  }
}
