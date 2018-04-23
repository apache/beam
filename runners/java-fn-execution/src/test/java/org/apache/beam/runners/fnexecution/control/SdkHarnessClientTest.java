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
package org.apache.beam.runners.fnexecution.control;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.fnexecution.InProcessSdkHarness;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.RemoteOutputReceiver;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortRead;
import org.apache.beam.sdk.fn.data.RemoteGrpcPortWrite;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link SdkHarnessClient}. */
@RunWith(JUnit4.class)
public class SdkHarnessClientTest {

  @Mock public FnApiControlClient fnApiControlClient;
  @Mock public FnDataService dataService;

  @Rule public InProcessSdkHarness harness = InProcessSdkHarness.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private SdkHarnessClient sdkHarnessClient;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    sdkHarnessClient = SdkHarnessClient.usingFnApiClient(fnApiControlClient, dataService);
  }

  @Test
  public void testRegisterCachesBundleProcessors() throws Exception {
    CompletableFuture<InstructionResponse> registerResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(registerResponseFuture);

    ProcessBundleDescriptor descriptor1 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor1").build();
    ProcessBundleDescriptor descriptor2 =
        ProcessBundleDescriptor.newBuilder().setId("descriptor2").build();

    RemoteInputDestination<WindowedValue<Integer>> remoteInputs =
        RemoteInputDestination.of(
            FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
            Target.getDefaultInstance());

    BundleProcessor<?> processor1 = sdkHarnessClient.getProcessor(descriptor1, remoteInputs);
    BundleProcessor<?> processor2 = sdkHarnessClient.getProcessor(descriptor2, remoteInputs);

    assertNotSame(processor1, processor2);

    // Ensure that caching works.
    assertSame(processor1, sdkHarnessClient.getProcessor(descriptor1, remoteInputs));
  }

  @Test
  public void testRegisterWithStateRequiresStateDelegator() throws Exception {
    CompletableFuture<InstructionResponse> registerResponseFuture = new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(registerResponseFuture);

    ProcessBundleDescriptor descriptor = ProcessBundleDescriptor.newBuilder()
        .setId("test")
        .setStateApiServiceDescriptor(ApiServiceDescriptor.newBuilder().setUrl("foo"))
        .build();

    RemoteInputDestination<WindowedValue<Integer>> remoteInputs =
        RemoteInputDestination.of(
            FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE),
            Target.getDefaultInstance());

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("containing a state");
    BundleProcessor<?> processor = sdkHarnessClient.getProcessor(descriptor, remoteInputs);
  }

  @Test
  public void testNewBundleNoDataDoesNotCrash() throws Exception {
    String descriptorId1 = "descriptor1";

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor, RemoteInputDestination.of(coder, Target.getDefaultInstance()));
    when(dataService.send(any(), eq(coder))).thenReturn(mock(CloseableFnDataReceiver.class));

    try (ActiveBundle<String> activeBundle = processor.newBundle(Collections.emptyMap())) {
      // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
      // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
      // the response.
      //
      // Currently there are no fields so there's nothing to check. This test is formulated
      // to match the pattern it should have if/when the response is meaningful.
      BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse
          .getDefaultInstance();
      processBundleResponseFuture.complete(
          BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
    }
  }

  @Test
  public void testNewBundleAndProcessElements() throws Exception {
    ProcessBundleDescriptor processBundleDescriptor =
        getProcessBundleDescriptor(harness.dataEndpoint());

    BeamFnApi.Target sdkGrpcReadTarget =
        BeamFnApi.Target.newBuilder()
            .setName(
                getOnlyElement(
                    processBundleDescriptor.getTransformsOrThrow("read").getOutputsMap().keySet()))
            .setPrimitiveTransformReference("read")
            .build();
    BeamFnApi.Target sdkGrpcWriteTarget =
        BeamFnApi.Target.newBuilder()
            .setName(
                getOnlyElement(
                    processBundleDescriptor.getTransformsOrThrow("write").getInputsMap().keySet()))
            .setPrimitiveTransformReference("write")
            .build();

    SdkHarnessClient client = harness.client();
    BundleProcessor<String> processor =
        client.getProcessor(
            processBundleDescriptor,
            RemoteInputDestination.of(
                FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE),
                sdkGrpcReadTarget));

    Collection<WindowedValue<String>> outputs = new ArrayList<>();
    try (ActiveBundle<String> activeBundle =
        processor.newBundle(
            Collections.singletonMap(
                sdkGrpcWriteTarget,
                RemoteOutputReceiver.of(
                    FullWindowedValueCoder.of(
                        LengthPrefixCoder.of(StringUtf8Coder.of()), Coder.INSTANCE),
                    outputs::add)))) {
      FnDataReceiver<WindowedValue<String>> bundleInputReceiver = activeBundle.getInputReceiver();
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("foo"));
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("bar"));
      bundleInputReceiver.accept(WindowedValue.valueInGlobalWindow("baz"));
    }

    // The bundle can be a simple function of some sort, but needs to be complete.
    assertThat(
        outputs,
        containsInAnyOrder(
            WindowedValue.valueInGlobalWindow("spam"),
            WindowedValue.valueInGlobalWindow("ham"),
            WindowedValue.valueInGlobalWindow("eggs")));
  }

  @Test
  public void handleCleanupWhenInputSenderFails() throws Exception {
    String descriptorId1 = "descriptor1";
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            RemoteInputDestination.of(coder, Target.getDefaultInstance()));
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);

    doThrow(testException).when(mockInputSender).close();

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver))) {
        // We shouldn't be required to complete the process bundle response future.
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);

      verify(mockOutputReceiver).cancel();
      verifyNoMoreInteractions(mockOutputReceiver);
    }
  }

  @Test
  public void handleCleanupWithStateWhenInputSenderFails() throws Exception {
    String descriptorId1 = "descriptor1";
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);

    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            RemoteInputDestination.of(coder, Target.getDefaultInstance()),
            mockStateDelegator);
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);

    doThrow(testException).when(mockInputSender).close();

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver),
          mockStateHandler)) {
        // We shouldn't be required to complete the process bundle response future.
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);

      verify(mockStateRegistration).abort();
      verify(mockOutputReceiver).cancel();
      verifyNoMoreInteractions(mockStateRegistration, mockOutputReceiver);
    }
  }

  @Test
  public void handleCleanupWhenProcessingBundleFails() throws Exception {
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId("descriptor1").build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            RemoteInputDestination.of(coder, Target.getDefaultInstance()));
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver))) {
        processBundleResponseFuture.completeExceptionally(testException);
      }
      fail("Exception expected");
    } catch (ExecutionException e) {
      assertEquals(testException, e.getCause());

      verify(mockOutputReceiver).cancel();
      verifyNoMoreInteractions(mockOutputReceiver);
    }
  }

  @Test
  public void handleCleanupWithStateWhenProcessingBundleFails() throws Exception {
    String descriptorId1 = "descriptor1";
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);
    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            RemoteInputDestination.of(coder, Target.getDefaultInstance()),
            mockStateDelegator);
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver),
          mockStateHandler)) {
        processBundleResponseFuture.completeExceptionally(testException);
      }
      fail("Exception expected");
    } catch (ExecutionException e) {
      assertEquals(testException, e.getCause());

      verify(mockStateRegistration).abort();
      verify(mockOutputReceiver).cancel();
      verifyNoMoreInteractions(mockStateRegistration, mockOutputReceiver);
    }
  }

  @Test
  public void handleCleanupWhenAwaitingOnClosingOutputReceivers() throws Exception {
    String descriptorId1 = "descriptor1";
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor, RemoteInputDestination.of(coder, Target.getDefaultInstance()));
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);
    doThrow(testException).when(mockOutputReceiver).awaitCompletion();

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver))) {
        // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
        // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
        // the response.
        //
        // Currently there are no fields so there's nothing to check. This test is formulated
        // to match the pattern it should have if/when the response is meaningful.
        BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse
            .getDefaultInstance();
        processBundleResponseFuture.complete(
            BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);
    }
  }

  @Test
  public void handleCleanupWithStateWhenAwaitingOnClosingOutputReceivers() throws Exception {
    String descriptorId1 = "descriptor1";
    Exception testException = new Exception();

    InboundDataClient mockOutputReceiver = mock(InboundDataClient.class);
    CloseableFnDataReceiver mockInputSender = mock(CloseableFnDataReceiver.class);
    StateDelegator mockStateDelegator = mock(StateDelegator.class);
    StateDelegator.Registration mockStateRegistration = mock(StateDelegator.Registration.class);
    when(mockStateDelegator.registerForProcessBundleInstructionId(any(), any()))
        .thenReturn(mockStateRegistration);
    StateRequestHandler mockStateHandler = mock(StateRequestHandler.class);

    ProcessBundleDescriptor descriptor =
        ProcessBundleDescriptor.newBuilder().setId(descriptorId1).build();
    CompletableFuture<InstructionResponse> processBundleResponseFuture =
        new CompletableFuture<>();
    when(fnApiControlClient.handle(any(BeamFnApi.InstructionRequest.class)))
        .thenReturn(new CompletableFuture<>())
        .thenReturn(processBundleResponseFuture);

    FullWindowedValueCoder<String> coder =
        FullWindowedValueCoder.of(StringUtf8Coder.of(), Coder.INSTANCE);
    BundleProcessor<String> processor =
        sdkHarnessClient.getProcessor(
            descriptor,
            RemoteInputDestination.of(coder, Target.getDefaultInstance()),
            mockStateDelegator);
    when(dataService.receive(any(), any(), any())).thenReturn(mockOutputReceiver);
    when(dataService.send(any(), eq(coder))).thenReturn(mockInputSender);
    doThrow(testException).when(mockOutputReceiver).awaitCompletion();

    RemoteOutputReceiver mockRemoteOutputReceiver = mock(RemoteOutputReceiver.class);

    try {
      try (ActiveBundle<String> activeBundle = processor.newBundle(
          ImmutableMap.of(Target.getDefaultInstance(), mockRemoteOutputReceiver),
          mockStateHandler)) {
        // Correlating the ProcessBundleRequest and ProcessBundleResponse is owned by the underlying
        // FnApiControlClient. The SdkHarnessClient owns just wrapping the request and unwrapping
        // the response.
        //
        // Currently there are no fields so there's nothing to check. This test is formulated
        // to match the pattern it should have if/when the response is meaningful.
        BeamFnApi.ProcessBundleResponse response = BeamFnApi.ProcessBundleResponse
            .getDefaultInstance();
        processBundleResponseFuture.complete(
            BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response).build());
      }
      fail("Exception expected");
    } catch (Exception e) {
      assertEquals(testException, e);
    }
  }

  private BeamFnApi.ProcessBundleDescriptor getProcessBundleDescriptor(
      Endpoints.ApiServiceDescriptor endpoint) throws IOException {
    Pipeline userPipeline = Pipeline.create();
    TupleTag<String> outputTag = new TupleTag<>();
    userPipeline
        .apply("create", Create.of("foo"))
        .apply("proc", ParDo.of(new TestFn()).withOutputTags(outputTag, TupleTagList.empty()));
    RunnerApi.Pipeline userProto = PipelineTranslation.toProto(userPipeline);

    ProcessBundleDescriptor.Builder pbdBuilder =
        ProcessBundleDescriptor.newBuilder()
            .setId("my_id")
            .putAllEnvironments(userProto.getComponents().getEnvironmentsMap())
            .putAllWindowingStrategies(userProto.getComponents().getWindowingStrategiesMap())
            .putAllCoders(userProto.getComponents().getCodersMap());
    RunnerApi.Coder fullValueCoder =
        CoderTranslation.toProto(WindowedValue.getFullCoder(StringUtf8Coder.of(), Coder.INSTANCE))
            .getCoder();
    pbdBuilder.putCoders("wire_coder", fullValueCoder);

    PTransform targetProcessor = userProto.getComponents().getTransformsOrThrow("proc");
    RemoteGrpcPort port =
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(endpoint)
            .setCoderId("wire_coder")
            .build();
    RemoteGrpcPortRead readNode =
        RemoteGrpcPortRead.readFromPort(
            port, getOnlyElement(targetProcessor.getInputsMap().values()));
    RemoteGrpcPortWrite writeNode =
        RemoteGrpcPortWrite.writeToPort(
            getOnlyElement(targetProcessor.getOutputsMap().values()), port);
    // TODO: Ensure cross-env (Runner <-> SDK GRPC Read/Write Node) coders are length-prefixed
    for (String pc : targetProcessor.getInputsMap().values()) {
      pbdBuilder.putPcollections(pc, userProto.getComponents().getPcollectionsOrThrow(pc));
    }
    for (String pc : targetProcessor.getOutputsMap().values()) {
      pbdBuilder.putPcollections(pc, userProto.getComponents().getPcollectionsOrThrow(pc));
    }
    pbdBuilder
        .putTransforms("proc", targetProcessor)
        .putTransforms("read", readNode.toPTransform())
        .putTransforms("write", writeNode.toPTransform());
    return pbdBuilder
        .build();
  }

  private static class TestFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      if ("foo".equals(context.element())) {
        context.output("spam");
      } else if ("bar".equals(context.element())) {
        context.output("ham");
      } else {
        context.output("eggs");
      }
    }
  }
}
