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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;

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

  private static @Mock Runnable onStartupMock = mock(Runnable.class);
  private static @Mock Consumer<PipelineOptions> beforeProcessingMock = mock(Consumer.class);

  @Rule
  public Timeout timeout =
      Timeout.builder().withLookingForStuckThread(true).withTimeout(1, TimeUnit.MINUTES).build();

  /**
   * Fake JvmInitializer that simply forwards calls to mocked functions so that they can be observed
   * in tests.
   */
  @AutoService(JvmInitializer.class)
  public static class FnHarnessTestInitializer implements JvmInitializer {
    @Override
    public void onStartup() {
      onStartupMock.run();
    }

    @Override
    public void beforeProcessing(PipelineOptions options) {
      beforeProcessingMock.accept(options);
    }
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored") // failure will cause test to timeout.
  public void testLaunchFnHarnessAndTeardownCleanly() throws Exception {
    Function<String, String> environmentVariableMock = mock(Function.class);

    PipelineOptions options = PipelineOptionsFactory.create();

    when(environmentVariableMock.apply("HARNESS_ID")).thenReturn("id");
    when(environmentVariableMock.apply("PIPELINE_OPTIONS"))
        .thenReturn(PipelineOptionsTranslation.toJson(options));

    List<BeamFnApi.LogEntry> logEntries = new ArrayList<>();
    List<BeamFnApi.InstructionResponse> instructionResponses = mock(List.class);

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
        Endpoints.ApiServiceDescriptor loggingDescriptor =
            Endpoints.ApiServiceDescriptor.newBuilder()
                .setUrl("localhost:" + loggingServer.getPort())
                .build();
        Endpoints.ApiServiceDescriptor controlDescriptor =
            Endpoints.ApiServiceDescriptor.newBuilder()
                .setUrl("localhost:" + controlServer.getPort())
                .build();

        when(environmentVariableMock.apply("LOGGING_API_SERVICE_DESCRIPTOR"))
            .thenReturn(TextFormat.printToString(loggingDescriptor));
        when(environmentVariableMock.apply("CONTROL_API_SERVICE_DESCRIPTOR"))
            .thenReturn(TextFormat.printToString(controlDescriptor));

        FnHarness.main(environmentVariableMock);
      } finally {
        controlServer.shutdownNow();
      }
    } finally {
      loggingServer.shutdownNow();
    }

    // Verify that we first run onStartup functions before even reading the environment, and that
    // we then call beforeProcessing functions before executing instructions.
    InOrder inOrder =
        inOrder(onStartupMock, beforeProcessingMock, environmentVariableMock, instructionResponses);
    inOrder.verify(onStartupMock).run();
    inOrder.verify(environmentVariableMock, atLeastOnce()).apply(any());
    inOrder.verify(beforeProcessingMock).accept(any());
    inOrder.verify(instructionResponses).add(INSTRUCTION_RESPONSE);
  }
}
