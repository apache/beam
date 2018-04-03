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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingConsumer;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ProcessBundleHandler}. */
@RunWith(JUnit4.class)
public class ProcessBundleHandlerTest {
  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";
  private static final String DATA_OUTPUT_URN = "urn:org.apache.beam:sink:runner:0.1";

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BeamFnDataClient beamFnDataClient;
  @Captor private ArgumentCaptor<ThrowingConsumer<WindowedValue<String>>> consumerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testOrderOfStartAndFinishCalls() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .putOutputs("2L-output", "2L-output-pc")
                .build())
            .putTransforms("3L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_OUTPUT_URN).build())
                .putInputs("3L-input", "2L-output-pc")
                .build())
            .putPcollections("2L-output-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    List<RunnerApi.PTransform> transformsProcessed = new ArrayList<>();
    List<String> orderOfOperations = new ArrayList<>();

    PTransformRunnerFactory<Object> startFinishRecorder =
        (pipelineOptions,
            beamFnDataClient,
            beamFnStateClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            pCollections,
            coders,
            windowingStrategies,
            pCollectionIdsToConsumers,
            addStartFunction,
            addFinishFunction) -> {
          assertThat(processBundleInstructionId.get(), equalTo("999L"));

          transformsProcessed.add(pTransform);
          addStartFunction.accept(() -> orderOfOperations.add("Start" + pTransformId));
          addFinishFunction.accept(() -> orderOfOperations.add("Finish" + pTransformId));
          return null;
        };

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        null /* beamFnStateClient */,
        ImmutableMap.of(
            DATA_INPUT_URN, startFinishRecorder,
            DATA_OUTPUT_URN, startFinishRecorder));

    handler.processBundle(BeamFnApi.InstructionRequest.newBuilder()
        .setInstructionId("999L")
        .setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
        .build());

    // Processing of transforms is performed in reverse order.
    assertThat(transformsProcessed, contains(
        processBundleDescriptor.getTransformsMap().get("3L"),
        processBundleDescriptor.getTransformsMap().get("2L")));
    // Start should occur in reverse order while finish calls should occur in forward order
    assertThat(orderOfOperations, contains("Start3L", "Start2L", "Finish2L", "Finish3L"));
  }

  @Test
  public void testCreatingPTransformExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (pipelineOptions,
                    beamFnDataClient,
                    beamFnStateClient,
                    pTransformId,
                    pTransform,
                    processBundleInstructionId,
                    pCollections,
                    coders,
                    windowingStrategies,
                    pCollectionIdsToConsumers,
                    addStartFunction,
                    addFinishFunction) -> {
                  thrown.expect(IllegalStateException.class);
                  thrown.expectMessage("TestException");
                  throw new IllegalStateException("TestException");
                }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformStartExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionIdsToConsumers,
                        addStartFunction,
                        addFinishFunction) -> {
                      thrown.expect(IllegalStateException.class);
                      thrown.expectMessage("TestException");
                      addStartFunction.accept(ProcessBundleHandlerTest::throwException);
                      return null;
                    }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder()
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder()
                    .setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPTransformFinishExceptionsArePropagated() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler =
        new ProcessBundleHandler(
            PipelineOptionsFactory.create(),
            fnApiRegistry::get,
            beamFnDataClient,
            null /* beamFnStateGrpcClientCache */,
            ImmutableMap.of(
                DATA_INPUT_URN,
                (PTransformRunnerFactory<Object>)
                    (pipelineOptions,
                        beamFnDataClient,
                        beamFnStateClient,
                        pTransformId,
                        pTransform,
                        processBundleInstructionId,
                        pCollections,
                        coders,
                        windowingStrategies,
                        pCollectionIdsToConsumers,
                        addStartFunction,
                        addFinishFunction) -> {
                      thrown.expect(IllegalStateException.class);
                      thrown.expectMessage("TestException");
                      addFinishFunction.accept(ProcessBundleHandlerTest::throwException);
                      return null;
                    }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }

  @Test
  public void testPendingStateCallsBlockTillCompletion() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .setStateApiServiceDescriptor(ApiServiceDescriptor.getDefaultInstance())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    CompletableFuture<StateResponse> successfulResponse = new CompletableFuture<>();
    CompletableFuture<StateResponse> unsuccessfulResponse = new CompletableFuture<>();

    BeamFnStateGrpcClientCache mockBeamFnStateGrpcClient =
        Mockito.mock(BeamFnStateGrpcClientCache.class);
    BeamFnStateClient mockBeamFnStateClient = Mockito.mock(BeamFnStateClient.class);
    when(mockBeamFnStateGrpcClient.forApiServiceDescriptor(any()))
        .thenReturn(mockBeamFnStateClient);

    doAnswer(
            invocation -> {
              StateRequest.Builder stateRequestBuilder =
                  (StateRequest.Builder) invocation.getArguments()[0];
              CompletableFuture<StateResponse> completableFuture =
                  (CompletableFuture<StateResponse>) invocation.getArguments()[1];
              new Thread(
                      () -> {
                        // Simulate sleeping which introduces a race which most of the time requires
                        // the ProcessBundleHandler to block.
                        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                        switch (stateRequestBuilder.getInstructionReference()) {
                          case "SUCCESS":
                            completableFuture.complete(StateResponse.getDefaultInstance());
                            break;
                          case "FAIL":
                            completableFuture.completeExceptionally(
                                new RuntimeException("TEST ERROR"));
                        }
                      })
                  .start();
              return null;
            })
        .when(mockBeamFnStateClient)
        .handle(any(), any());

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        mockBeamFnStateGrpcClient,
        ImmutableMap.of(DATA_INPUT_URN, new PTransformRunnerFactory<Object>() {
          @Override
          public Object createRunnerForPTransform(
              PipelineOptions pipelineOptions,
              BeamFnDataClient beamFnDataClient,
              BeamFnStateClient beamFnStateClient,
              String pTransformId,
              PTransform pTransform,
              Supplier<String> processBundleInstructionId,
              Map<String, PCollection> pCollections,
              Map<String, Coder> coders,
              Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
              Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
              Consumer<ThrowingRunnable> addStartFunction,
              Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
            addStartFunction.accept(() -> doStateCalls(beamFnStateClient));
            return null;
          }

          private void doStateCalls(BeamFnStateClient beamFnStateClient) {
            beamFnStateClient.handle(StateRequest.newBuilder().setInstructionReference("SUCCESS"),
                successfulResponse);
            beamFnStateClient.handle(StateRequest.newBuilder().setInstructionReference("FAIL"),
                unsuccessfulResponse);
          }
        }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder()
                .setProcessBundleDescriptorReference("1L"))
            .build());

    assertTrue(successfulResponse.isDone());
    assertTrue(unsuccessfulResponse.isDone());
  }

  @Test
  public void testStateCallsFailIfNoStateApiServiceDescriptorSpecified() throws Exception {
    BeamFnApi.ProcessBundleDescriptor processBundleDescriptor =
        BeamFnApi.ProcessBundleDescriptor.newBuilder()
            .putTransforms("2L", RunnerApi.PTransform.newBuilder()
                .setSpec(RunnerApi.FunctionSpec.newBuilder().setUrn(DATA_INPUT_URN).build())
                .build())
            .build();
    Map<String, Message> fnApiRegistry = ImmutableMap.of("1L", processBundleDescriptor);

    ProcessBundleHandler handler = new ProcessBundleHandler(
        PipelineOptionsFactory.create(),
        fnApiRegistry::get,
        beamFnDataClient,
        null /* beamFnStateGrpcClientCache */,
        ImmutableMap.of(DATA_INPUT_URN, new PTransformRunnerFactory<Object>() {
          @Override
          public Object createRunnerForPTransform(
              PipelineOptions pipelineOptions,
              BeamFnDataClient beamFnDataClient,
              BeamFnStateClient beamFnStateClient,
              String pTransformId,
              PTransform pTransform,
              Supplier<String> processBundleInstructionId,
              Map<String, PCollection> pCollections,
              Map<String, Coder> coders,
              Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
              Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
              Consumer<ThrowingRunnable> addStartFunction,
              Consumer<ThrowingRunnable> addFinishFunction) throws IOException {
            addStartFunction.accept(() -> doStateCalls(beamFnStateClient));
            return null;
          }

          private void doStateCalls(BeamFnStateClient beamFnStateClient) {
            thrown.expect(IllegalStateException.class);
            thrown.expectMessage("State API calls are unsupported");
            beamFnStateClient.handle(StateRequest.newBuilder().setInstructionReference("SUCCESS"),
                new CompletableFuture<>());
          }
        }));
    handler.processBundle(
        BeamFnApi.InstructionRequest.newBuilder().setProcessBundle(
            BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorReference("1L"))
            .build());
  }


  private static void throwException() {
    throw new IllegalStateException("TestException");
  }
}
