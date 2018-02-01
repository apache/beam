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

package org.apache.beam.fn.harness;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FnHarness}. */
@RunWith(JUnit4.class)
public class FnHarnessTest {
  private static final BeamFnApi.InstructionRequest INSTRUCTION_REQUEST =
      BeamFnApi.InstructionRequest.newBuilder()
      .setInstructionId("999L")
      .setRegister(BeamFnApi.RegisterRequest.getDefaultInstance())
      .build();
  private static final BeamFnApi.InstructionResponse INSTRUCTION_RESPONSE =
      BeamFnApi.InstructionResponse.newBuilder()
      .setInstructionId("999L")
      .setRegister(BeamFnApi.RegisterResponse.getDefaultInstance())
      .build();

  @Test
  public void testLaunchFnHarnessAndTeardownCleanly() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    List<BeamFnApi.LogEntry> logEntries = new ArrayList<>();
    List<BeamFnApi.InstructionResponse> instructionResponses = new ArrayList<>();

    BeamFnLoggingGrpc.BeamFnLoggingImplBase loggingService =
        new BeamFnLoggingGrpc.BeamFnLoggingImplBase() {
          @Override
          public StreamObserver<BeamFnApi.LogEntry.List> logging(
              StreamObserver<LogControl> responseObserver) {
            return TestStreams.withOnNext(
                    (BeamFnApi.LogEntry.List entries) ->
                        logEntries.addAll(entries.getLogEntriesList()))
                .withOnCompleted(responseObserver::onCompleted)
                .build();
          }
        };

    BeamFnControlGrpc.BeamFnControlImplBase controlService =
        new BeamFnControlGrpc.BeamFnControlImplBase() {
          @Override
          public StreamObserver<InstructionResponse> control(
              StreamObserver<InstructionRequest> responseObserver) {
            CountDownLatch waitForResponses =
                new CountDownLatch(1 /* number of responses expected */);
            options
                .as(GcsOptions.class)
                .getExecutorService()
                .submit(
                    () -> {
                      responseObserver.onNext(INSTRUCTION_REQUEST);
                      Uninterruptibles.awaitUninterruptibly(waitForResponses);
                      responseObserver.onCompleted();
                    });
            return TestStreams.withOnNext(
                    (InstructionResponse t) -> {
                      instructionResponses.add(t);
                      waitForResponses.countDown();
                    })
                .withOnCompleted(waitForResponses::countDown)
                .build();
          }
        };

    Server loggingServer = ServerBuilder.forPort(0).addService(loggingService).build();
    loggingServer.start();
    try {
      Server controlServer = ServerBuilder.forPort(0).addService(controlService).build();
      controlServer.start();
      try {
        Endpoints.ApiServiceDescriptor loggingDescriptor = Endpoints.ApiServiceDescriptor
            .newBuilder()
            .setUrl("localhost:" + loggingServer.getPort())
            .build();
        Endpoints.ApiServiceDescriptor controlDescriptor = Endpoints.ApiServiceDescriptor
            .newBuilder()
            .setUrl("localhost:" + controlServer.getPort())
            .build();

        FnHarness.main(options, loggingDescriptor, controlDescriptor);
        assertThat(instructionResponses, contains(INSTRUCTION_RESPONSE));
      } finally {
        controlServer.shutdownNow();
      }
    } finally {
      loggingServer.shutdownNow();
    }
  }
}

