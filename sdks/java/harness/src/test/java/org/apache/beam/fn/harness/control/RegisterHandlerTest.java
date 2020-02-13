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

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RegisterHandler}. */
@RunWith(JUnit4.class)
public class RegisterHandlerTest {
  @Rule public TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  private static final BeamFnApi.InstructionRequest REGISTER_REQUEST =
      BeamFnApi.InstructionRequest.newBuilder()
          .setInstructionId("1L")
          .setRegister(
              BeamFnApi.RegisterRequest.newBuilder()
                  .addProcessBundleDescriptor(
                      BeamFnApi.ProcessBundleDescriptor.newBuilder()
                          .setId("1L")
                          .putCoders(
                              "10L",
                              RunnerApi.Coder.newBuilder()
                                  .setSpec(
                                      RunnerApi.FunctionSpec.newBuilder()
                                          .setUrn("testUrn1")
                                          .build())
                                  .build())
                          .build())
                  .addProcessBundleDescriptor(
                      BeamFnApi.ProcessBundleDescriptor.newBuilder()
                          .setId("2L")
                          .putCoders(
                              "20L",
                              RunnerApi.Coder.newBuilder()
                                  .setSpec(
                                      RunnerApi.FunctionSpec.newBuilder()
                                          .setUrn("testUrn2")
                                          .build())
                                  .build())
                          .build())
                  .build())
          .build();
  private static final BeamFnApi.InstructionResponse REGISTER_RESPONSE =
      BeamFnApi.InstructionResponse.newBuilder()
          .setRegister(RegisterResponse.getDefaultInstance())
          .build();

  @Test
  public void testRegistration() throws Exception {
    RegisterHandler handler = new RegisterHandler();
    Future<BeamFnApi.InstructionResponse> responseFuture =
        executor.submit(
            () -> {
              // Purposefully wait a small amount of time making it likely that
              // a downstream caller needs to block.
              Thread.sleep(100);
              return handler.register(REGISTER_REQUEST).build();
            });
    assertEquals(
        REGISTER_REQUEST.getRegister().getProcessBundleDescriptor(0), handler.getById("1L"));
    assertEquals(
        REGISTER_REQUEST.getRegister().getProcessBundleDescriptor(1), handler.getById("2L"));
    assertEquals(
        REGISTER_REQUEST.getRegister().getProcessBundleDescriptor(0).getCodersOrThrow("10L"),
        handler.getById("10L"));
    assertEquals(
        REGISTER_REQUEST.getRegister().getProcessBundleDescriptor(1).getCodersOrThrow("20L"),
        handler.getById("20L"));
    assertEquals(REGISTER_RESPONSE, responseFuture.get());
  }
}
