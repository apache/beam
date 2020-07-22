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
package org.apache.beam.runners.dataflow.worker.fn.control;

import static org.apache.beam.runners.dataflow.worker.fn.control.RegisterAndProcessBundleOperation.encodeAndConcat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest.RequestCase;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateAppendRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateClearRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateGetRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.DataflowPortabilityPCollectionView;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.ThrowingRunnable;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow.Coder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for {@link RegisterAndProcessBundleOperation}. */
@RunWith(JUnit4.class)
@SuppressWarnings("FutureReturnValueIgnored")
public class RegisterAndProcessBundleOperationTest {
  private static final BeamFnApi.RegisterRequest REGISTER_REQUEST =
      BeamFnApi.RegisterRequest.newBuilder()
          .addProcessBundleDescriptor(BeamFnApi.ProcessBundleDescriptor.newBuilder().setId("555"))
          .build();
  @Mock private OperationContext mockContext;
  @Mock private StateDelegator mockBeamFnStateDelegator;

  @Captor private ArgumentCaptor<StateRequestHandler> stateHandlerCaptor;

  private AtomicInteger stateServiceRegisterCounter;
  private AtomicInteger stateServiceDeregisterCounter;
  private AtomicInteger stateServiceAbortCounter;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    stateServiceRegisterCounter = new AtomicInteger();
    stateServiceDeregisterCounter = new AtomicInteger();
    stateServiceAbortCounter = new AtomicInteger();
    when(mockBeamFnStateDelegator.registerForProcessBundleInstructionId(
            any(String.class), any(StateRequestHandler.class)))
        .thenAnswer(
            new Answer<StateDelegator.Registration>() {
              @Override
              public StateDelegator.Registration answer(InvocationOnMock invocationOnMock)
                  throws Throwable {
                stateServiceRegisterCounter.incrementAndGet();
                return new StateDelegator.Registration() {

                  @Override
                  public void deregister() {
                    stateServiceDeregisterCounter.incrementAndGet();
                  }

                  @Override
                  public void abort() {
                    stateServiceAbortCounter.incrementAndGet();
                  }
                };
              }
            });
  }

  private IdGenerator makeIdGeneratorStartingFrom(long initialValue) {
    return new IdGenerator() {
      AtomicLong longs = new AtomicLong(initialValue);

      @Override
      public String getId() {
        return Long.toString(longs.getAndIncrement());
      }
    };
  }

  @Test
  public void testSupportsRestart() {
    new RegisterAndProcessBundleOperation(
            IdGenerators.decrementingLongs(),
            new TestInstructionRequestHandler() {
              @Override
              public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
                CompletableFuture<InstructionResponse> responseFuture = new CompletableFuture<>();
                completeFuture(request, responseFuture);
                return responseFuture;
              }
            },
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext)
        .supportsRestart();
  }

  @Test
  public void testRegisterOnlyOnFirstBundle() throws Exception {
    List<BeamFnApi.InstructionRequest> requests = new ArrayList<>();
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            new TestInstructionRequestHandler() {
              @Override
              public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
                requests.add(request);
                switch (request.getRequestCase()) {
                  case REGISTER:
                  case PROCESS_BUNDLE:
                    return CompletableFuture.completedFuture(responseFor(request).build());
                  default:
                    // block forever on other requests
                    return new CompletableFuture<>();
                }
              }
            },
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);

    // Ensure that the first time we start we send the register and process bundle requests
    assertThat(requests, empty());
    operation.start();
    assertEquals(
        requests.get(0),
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("777")
            .setRegister(REGISTER_REQUEST)
            .build());
    assertEquals(
        requests.get(1),
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("778")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("555"))
            .build());
    operation.finish();

    // Ensure on restart that we only send the process bundle request
    operation.start();
    assertEquals(
        requests.get(2),
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("779")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("555"))
            .build());
    operation.finish();
  }

  @Test
  public void testProcessingBundleBlocksOnFinish() throws Exception {
    List<BeamFnApi.InstructionRequest> requests = new ArrayList<>();
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    ExecutorService executorService = Executors.newCachedThreadPool();
    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            new TestInstructionRequestHandler() {
              @Override
              public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
                requests.add(request);
                switch (request.getRequestCase()) {
                  case REGISTER:
                    return CompletableFuture.completedFuture(responseFor(request).build());
                  case PROCESS_BUNDLE:
                    CompletableFuture<InstructionResponse> responseFuture =
                        new CompletableFuture<>();
                    executorService.submit(
                        () -> {
                          // Purposefully sleep simulating SDK harness doing work
                          Thread.sleep(100);
                          responseFuture.complete(responseFor(request).build());
                          completeFuture(request, responseFuture);
                          return null;
                        });
                    return responseFuture;
                  default:
                    // Anything else hangs; nothing else should be blocking
                    return new CompletableFuture<>();
                }
              }
            },
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);

    operation.start();
    // This method blocks till the requests are completed
    operation.finish();

    // Ensure that the messages were received
    assertEquals(
        requests.get(0),
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("777")
            .setRegister(REGISTER_REQUEST)
            .build());
    assertEquals(
        requests.get(1),
        BeamFnApi.InstructionRequest.newBuilder()
            .setInstructionId("778")
            .setProcessBundle(
                BeamFnApi.ProcessBundleRequest.newBuilder().setProcessBundleDescriptorId("555"))
            .build());
  }

  @Test
  public void testProcessingBundleHandlesUserStateRequests() throws Exception {
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    ExecutorService executorService = Executors.newCachedThreadPool();

    InMemoryStateInternals<ByteString> stateInternals =
        InMemoryStateInternals.forKey(ByteString.EMPTY);
    DataflowStepContext mockStepContext = mock(DataflowStepContext.class);
    DataflowStepContext mockUserStepContext = mock(DataflowStepContext.class);
    when(mockStepContext.namespacedToUser()).thenReturn(mockUserStepContext);
    when(mockUserStepContext.stateInternals()).thenReturn(stateInternals);

    InstructionRequestHandler instructionRequestHandler =
        new TestInstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      StateRequest partialRequest =
                          StateRequest.newBuilder()
                              .setStateKey(
                                  StateKey.newBuilder()
                                      .setBagUserState(
                                          StateKey.BagUserState.newBuilder()
                                              .setTransformId("testPTransformId")
                                              .setWindow(ByteString.EMPTY)
                                              .setUserStateId("testUserStateId")))
                              .buildPartial();
                      StateRequest get =
                          partialRequest
                              .toBuilder()
                              .setGet(StateGetRequest.getDefaultInstance())
                              .build();
                      StateRequest clear =
                          partialRequest
                              .toBuilder()
                              .setClear(StateClearRequest.getDefaultInstance())
                              .build();
                      StateRequest append =
                          partialRequest
                              .toBuilder()
                              .setAppend(
                                  StateAppendRequest.newBuilder()
                                      .setData(ByteString.copyFromUtf8("ABC")))
                              .build();

                      StateRequestHandler stateHandler = stateHandlerCaptor.getValue();

                      StateResponse.Builder getWhenEmptyResponse =
                          MoreFutures.get(stateHandler.handle(get));
                      assertEquals(ByteString.EMPTY, getWhenEmptyResponse.getGet().getData());

                      StateResponse.Builder appendWhenEmptyResponse =
                          MoreFutures.get(stateHandler.handle(append));
                      assertNotNull(appendWhenEmptyResponse);
                      StateResponse.Builder appendWhenEmptyResponse2 =
                          MoreFutures.get(stateHandler.handle(append));
                      assertNotNull(appendWhenEmptyResponse2);

                      StateResponse.Builder getWhenHasValueResponse =
                          MoreFutures.get(stateHandler.handle(get));
                      assertEquals(
                          ByteString.copyFromUtf8("ABC").concat(ByteString.copyFromUtf8("ABC")),
                          getWhenHasValueResponse.getGet().getData());

                      StateResponse.Builder clearResponse =
                          MoreFutures.get(stateHandler.handle(clear));
                      assertNotNull(clearResponse);

                      return responseFor(request).build();
                    });
              default:
                // block forever
                return new CompletableFuture<>();
            }
          }
        };

    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            instructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of("testPTransformId", mockStepContext),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);

    operation.start();
    verify(mockBeamFnStateDelegator)
        .registerForProcessBundleInstructionId(eq("778"), stateHandlerCaptor.capture());

    // This method blocks till the requests are completed
    operation.finish();
    // Ensure that the number of reigstrations matches the number of deregistrations
    assertEquals(stateServiceRegisterCounter.get(), stateServiceDeregisterCounter.get());
    assertEquals(0, stateServiceAbortCounter.get());
  }

  @Test
  public void testProcessingBundleHandlesMultimapSideInputRequests() throws Exception {
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    ExecutorService executorService = Executors.newCachedThreadPool();

    DataflowStepContext mockStepContext = mock(DataflowStepContext.class);
    DataflowStepContext mockUserStepContext = mock(DataflowStepContext.class);
    when(mockStepContext.namespacedToUser()).thenReturn(mockUserStepContext);

    CountDownLatch waitForStateHandler = new CountDownLatch(1);
    // Issues state calls to the Runner after a process bundle request is sent.
    InstructionRequestHandler fakeClient =
        new TestInstructionRequestHandler() {
          @Override
          public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            switch (request.getRequestCase()) {
              case REGISTER:
                return CompletableFuture.completedFuture(responseFor(request).build());
              case PROCESS_BUNDLE:
                return MoreFutures.supplyAsync(
                    () -> {
                      StateKey getKey =
                          StateKey.newBuilder()
                              .setMultimapSideInput(
                                  StateKey.MultimapSideInput.newBuilder()
                                      .setTransformId("testPTransformId")
                                      .setSideInputId("testSideInputId")
                                      .setWindow(
                                          ByteString.copyFrom(
                                              CoderUtils.encodeToByteArray(
                                                  GlobalWindow.Coder.INSTANCE,
                                                  GlobalWindow.INSTANCE)))
                                      .setKey(
                                          ByteString.copyFrom(
                                              CoderUtils.encodeToByteArray(
                                                  ByteArrayCoder.of(),
                                                  "ABC".getBytes(StandardCharsets.UTF_8),
                                                  Coder.Context.NESTED))))
                              .build();

                      StateRequest getRequest =
                          StateRequest.newBuilder()
                              .setStateKey(getKey)
                              .setGet(StateGetRequest.getDefaultInstance())
                              .build();

                      waitForStateHandler.await();
                      StateRequestHandler stateHandler = stateHandlerCaptor.getValue();

                      StateResponse.Builder getResponse =
                          MoreFutures.get(stateHandler.handle(getRequest));

                      assertEquals(
                          encodeAndConcat(Arrays.asList("X", "Y", "Z"), StringUtf8Coder.of()),
                          getResponse.getGet().getData());

                      return responseFor(request).build();
                    });
              default:
                // block forever on other request types
                return new CompletableFuture<>();
            }
          }
        };

    SideInputReader fakeSideInputReader =
        new SideInputReader() {
          @Nullable
          @Override
          public <T> T get(PCollectionView<T> view, BoundedWindow window) {
            assertEquals(GlobalWindow.INSTANCE, window);
            assertEquals("testSideInputId", view.getTagInternal().getId());
            return (T)
                InMemoryMultimapSideInputView.fromIterable(
                    ByteArrayCoder.of(),
                    ImmutableList.of(
                        KV.of("ABC".getBytes(StandardCharsets.UTF_8), "X"),
                        KV.of("ABC".getBytes(StandardCharsets.UTF_8), "Y"),
                        KV.of("ABC".getBytes(StandardCharsets.UTF_8), "Z")));
          }

          @Override
          public <T> boolean contains(PCollectionView<T> view) {
            return "testSideInputId".equals(view.getTagInternal().getId());
          }

          @Override
          public boolean isEmpty() {
            return false;
          }
        };

    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            fakeClient,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of("testPTransformId", mockStepContext),
            ImmutableMap.of("testPTransformId", fakeSideInputReader),
            ImmutableTable.of(
                "testPTransformId",
                "testSideInputId",
                DataflowPortabilityPCollectionView.with(
                    new TupleTag<>("testSideInputId"),
                    FullWindowedValueCoder.of(
                        KvCoder.of(ByteArrayCoder.of(), StringUtf8Coder.of()),
                        GlobalWindow.Coder.INSTANCE))),
            ImmutableMap.of(),
            mockContext);

    operation.start();
    verify(mockBeamFnStateDelegator)
        .registerForProcessBundleInstructionId(eq("778"), stateHandlerCaptor.capture());
    waitForStateHandler.countDown();

    // This method blocks till the requests are completed
    operation.finish();
    // Ensure that the number of reigstrations matches the number of deregistrations
    assertEquals(stateServiceRegisterCounter.get(), stateServiceDeregisterCounter.get());
    assertEquals(0, stateServiceAbortCounter.get());
  }

  @Test
  public void testAbortCancelsAndCleansUpDuringRegister() throws Exception {
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    ExecutorService executorService = Executors.newCachedThreadPool();

    CountDownLatch waitForAbortToComplete = new CountDownLatch(1);
    AtomicReference<ThrowingRunnable> abortReference = new AtomicReference<>();
    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            new TestInstructionRequestHandler() {
              @Override
              public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
                CompletableFuture<InstructionResponse> responseFuture = new CompletableFuture<>();
                if (request.getRequestCase() == RequestCase.PROCESS_BUNDLE) {
                  executorService.submit(
                      (Callable<Void>)
                          () -> {
                            abortReference.get().run();
                            waitForAbortToComplete.countDown();
                            return null;
                          });
                } else {
                  completeFuture(request, responseFuture);
                }
                return responseFuture;
              }
            },
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);
    abortReference.set(operation::abort);
    operation.start();

    waitForAbortToComplete.await();
    // Ensure that the number of registrations matches the number of aborts
    assertEquals(stateServiceRegisterCounter.get(), stateServiceAbortCounter.get());
    assertEquals(0, stateServiceDeregisterCounter.get());
  }

  @Test
  public void testAbortCancelsAndCleansUpDuringProcessBundle() throws Exception {
    IdGenerator idGenerator = makeIdGeneratorStartingFrom(777L);
    ExecutorService executorService = Executors.newCachedThreadPool();

    CountDownLatch waitForAbortToComplete = new CountDownLatch(1);
    AtomicReference<ThrowingRunnable> abortReference = new AtomicReference<>();
    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            idGenerator,
            new TestInstructionRequestHandler() {
              @Override
              public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
                CompletableFuture<InstructionResponse> responseFuture = new CompletableFuture<>();
                if (request.getRequestCase() == RequestCase.PROCESS_BUNDLE) {
                  executorService.submit(
                      (Callable<Void>)
                          () -> {
                            abortReference.get().run();
                            waitForAbortToComplete.countDown();
                            return null;
                          });
                } else {
                  completeFuture(request, responseFuture);
                }
                return responseFuture;
              }
            },
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);
    abortReference.set(operation::abort);
    operation.start();

    waitForAbortToComplete.await();
    // Ensure that the number of registrations matches the number of aborts
    assertEquals(stateServiceRegisterCounter.get(), stateServiceAbortCounter.get());
    assertEquals(0, stateServiceDeregisterCounter.get());
  }

  private InstructionResponse.Builder responseFor(BeamFnApi.InstructionRequest request) {
    BeamFnApi.InstructionResponse.Builder response =
        BeamFnApi.InstructionResponse.newBuilder().setInstructionId(request.getInstructionId());
    if (request.hasRegister()) {
      response.setRegister(BeamFnApi.RegisterResponse.getDefaultInstance());
    } else if (request.hasProcessBundle()) {
      response.setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance());
    } else if (request.hasFinalizeBundle()) {
      response.setFinalizeBundle(BeamFnApi.FinalizeBundleResponse.getDefaultInstance());
    } else if (request.hasProcessBundleProgress()) {
      response.setProcessBundleProgress(
          BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    } else if (request.hasProcessBundleSplit()) {
      response.setProcessBundleSplit(BeamFnApi.ProcessBundleSplitResponse.getDefaultInstance());
    }
    return response;
  }

  private void completeFuture(
      BeamFnApi.InstructionRequest request, CompletableFuture<InstructionResponse> response) {
    response.complete(responseFor(request).build());
  }

  @Test
  public void testGetProcessBundleProgressReturnsDefaultInstanceIfNoBundleIdCached()
      throws Exception {
    InstructionRequestHandler mockInstructionRequestHandler = mock(InstructionRequestHandler.class);

    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            IdGenerators.decrementingLongs(),
            mockInstructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);

    assertEquals(
        ProcessBundleProgressResponse.getDefaultInstance(),
        MoreFutures.get(operation.getProcessBundleProgress()));
  }

  @Test
  public void testGetProcessBundleProgressFetchesProgressResponseWhenBundleIdCached()
      throws Exception {
    InstructionRequestHandler mockInstructionRequestHandler = mock(InstructionRequestHandler.class);

    RegisterAndProcessBundleOperation operation =
        new RegisterAndProcessBundleOperation(
            IdGenerators.decrementingLongs(),
            mockInstructionRequestHandler,
            mockBeamFnStateDelegator,
            REGISTER_REQUEST,
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableTable.of(),
            ImmutableMap.of(),
            mockContext);

    operation.getProcessBundleInstructionId(); // this generates and caches bundleId

    ProcessBundleProgressResponse expectedResult =
        ProcessBundleProgressResponse.newBuilder().build();
    InstructionResponse instructionResponse =
        InstructionResponse.newBuilder().setProcessBundleProgress(expectedResult).build();
    CompletableFuture resultFuture = CompletableFuture.completedFuture(instructionResponse);
    when(mockInstructionRequestHandler.handle(any())).thenReturn(resultFuture);

    final ProcessBundleProgressResponse result =
        MoreFutures.get(operation.getProcessBundleProgress());

    assertSame("Return value from mockInstructionRequestHandler", expectedResult, result);
  }

  private abstract static class TestInstructionRequestHandler implements InstructionRequestHandler {
    @Override
    public void registerProcessBundleDescriptor(ProcessBundleDescriptor descriptor) {}

    @Override
    public void close() {}
  }
}
