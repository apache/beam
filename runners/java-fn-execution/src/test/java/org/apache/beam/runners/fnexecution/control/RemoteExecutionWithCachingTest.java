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

import static org.apache.beam.sdk.options.ExperimentalOptions.addExperiment;
import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.FusedPipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.SideInputReference;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.BagUserStateHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.BagUserStateHandlerFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.IterableSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.InProcessServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the execution of a pipeline from specification time to executing a single fused stage,
 * going through pipeline fusion.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "keyfor"
})
public class RemoteExecutionWithCachingTest implements Serializable {
  @Rule public transient ResetDateTimeProvider resetDateTimeProvider = new ResetDateTimeProvider();

  private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutionWithCachingTest.class);

  private transient GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private transient GrpcFnServer<GrpcDataService> dataServer;
  private transient GrpcFnServer<GrpcStateService> stateServer;
  private transient GrpcFnServer<GrpcLoggingService> loggingServer;
  private transient GrpcStateService stateDelegator;
  private transient SdkHarnessClient controlClient;

  private transient ExecutorService serverExecutor;
  private transient ExecutorService sdkHarnessExecutor;
  private transient Future<?> sdkHarnessExecutorFuture;

  @Before
  public void setup() throws Exception {
    // Setup execution-time servers
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();
    serverExecutor = Executors.newCachedThreadPool(threadFactory);
    InProcessServerFactory serverFactory = InProcessServerFactory.create();
    dataServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcDataService.create(
                PipelineOptionsFactory.create(),
                serverExecutor,
                OutboundObserverFactory.serverDirect()),
            serverFactory);
    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(Slf4jLogWriter.getDefault()), serverFactory);
    stateDelegator = GrpcStateService.create();
    stateServer = GrpcFnServer.allocatePortAndCreateFor(stateDelegator, serverFactory);

    ControlClientPool clientPool = MapControlClientPool.create();
    controlServer =
        GrpcFnServer.allocatePortAndCreateFor(
            FnApiControlClientPoolService.offeringClientsToPool(
                clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
            serverFactory);

    PipelineOptions options = PipelineOptionsFactory.create();
    addExperiment(options.as(ExperimentalOptions.class), "cross_bundle_caching");

    // Create the SDK harness, and wait until it connects
    sdkHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
    sdkHarnessExecutorFuture =
        sdkHarnessExecutor.submit(
            () -> {
              try {
                FnHarness.main(
                    "id",
                    options,
                    Collections.emptySet(), // Runner capabilities.
                    loggingServer.getApiServiceDescriptor(),
                    controlServer.getApiServiceDescriptor(),
                    null,
                    InProcessManagedChannelFactory.create(),
                    OutboundObserverFactory.clientDirect());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Use proper worker id.
    InstructionRequestHandler controlClient =
        clientPool.getSource().take("", java.time.Duration.ofSeconds(2));
    this.controlClient = SdkHarnessClient.usingFnApiClient(controlClient, dataServer.getService());
  }

  @After
  public void tearDown() throws Exception {
    controlServer.close();
    stateServer.close();
    dataServer.close();
    loggingServer.close();
    controlClient.close();
    sdkHarnessExecutor.shutdownNow();
    serverExecutor.shutdownNow();
    try {
      sdkHarnessExecutorFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof RuntimeException
          && e.getCause().getCause() instanceof InterruptedException) {
        // expected
      } else {
        throw e;
      }
    }
  }

  @Test
  public void testExecutionWithSideInputCaching() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");
    PCollection<String> input =
        p.apply("impulse", Impulse.create())
            .apply(
                "create",
                ParDo.of(
                    new DoFn<byte[], String>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output("zero");
                        ctxt.output("one");
                        ctxt.output("two");
                      }
                    }))
            .setCoder(StringUtf8Coder.of());
    PCollectionView<Iterable<String>> view = input.apply("createSideInput", View.asIterable());

    input
        .apply(
            "readSideInput",
            ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        for (String value : context.sideInput(view)) {
                          context.output(KV.of(context.element(), value));
                        }
                      }
                    })
                .withSideInputs(view))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(), (ExecutableStage stage) -> !stage.getSideInputs().isEmpty());
    checkState(optionalStage.isPresent(), "Expected a stage with side inputs.");
    ExecutableStage stage = optionalStage.get();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "test_stage",
            stage,
            dataServer.getApiServiceDescriptor(),
            stateServer.getApiServiceDescriptor());

    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(),
            descriptor.getRemoteInputDestinations(),
            stateDelegator);
    Map<String, Coder> remoteOutputCoders = descriptor.getRemoteOutputCoders();
    Map<String, Collection<WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, Coder> remoteOutputCoder : remoteOutputCoders.entrySet()) {
      List<WindowedValue<?>> outputContents = Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder<WindowedValue<?>>) remoteOutputCoder.getValue(), outputContents::add));
    }

    StoringStateRequestHandler stateRequestHandler =
        new StoringStateRequestHandler(
            StateRequestHandlers.forSideInputHandlerFactory(
                descriptor.getSideInputSpecs(),
                new SideInputHandlerFactory() {
                  @Override
                  public <V, W extends BoundedWindow>
                      IterableSideInputHandler<V, W> forIterableSideInput(
                          String pTransformId,
                          String sideInputId,
                          Coder<V> elementCoder,
                          Coder<W> windowCoder) {
                    return new IterableSideInputHandler<V, W>() {
                      @Override
                      public Iterable<V> get(W window) {
                        return (Iterable) Arrays.asList("A", "B", "C");
                      }

                      @Override
                      public Coder<V> elementCoder() {
                        return elementCoder;
                      }
                    };
                  }

                  @Override
                  public <K, V, W extends BoundedWindow>
                      MultimapSideInputHandler<K, V, W> forMultimapSideInput(
                          String pTransformId,
                          String sideInputId,
                          KvCoder<K, V> elementCoder,
                          Coder<W> windowCoder) {
                    throw new UnsupportedOperationException();
                  }
                }));
    SideInputReference sideInputReference = stage.getSideInputs().iterator().next();
    String transformId = sideInputReference.transform().getId();
    String sideInputId = sideInputReference.localName();
    stateRequestHandler.addCacheToken(
        BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder()
            .setSideInput(
                BeamFnApi.ProcessBundleRequest.CacheToken.SideInput.newBuilder()
                    .setSideInputId(sideInputId)
                    .setTransformId(transformId)
                    .build())
            .setToken(ByteString.copyFromUtf8("SideInputToken"))
            .build());
    BundleProgressHandler progressHandler = BundleProgressHandler.ignored();

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, stateRequestHandler, progressHandler)) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow("X"));
    }

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, stateRequestHandler, progressHandler)) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow("X"));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C")),
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C"))));
    }

    // Only expect one read to the sideInput
    assertEquals(1, stateRequestHandler.receivedRequests.size());
    BeamFnApi.StateRequest receivedRequest = stateRequestHandler.receivedRequests.get(0);
    assertEquals(
        receivedRequest.getStateKey().getIterableSideInput(),
        BeamFnApi.StateKey.IterableSideInput.newBuilder()
            .setSideInputId(sideInputId)
            .setTransformId(transformId)
            .build());
  }

  @Test
  public void testExecutionWithUserStateCaching() throws Exception {
    Pipeline p = Pipeline.create();
    final String stateId = "foo";
    final String stateId2 = "bar";

    p.apply("impulse", Impulse.create())
        .apply(
            "create",
            ParDo.of(
                new DoFn<byte[], KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {}
                }))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply(
            "userState",
            ParDo.of(
                new DoFn<KV<String, String>, KV<String, String>>() {

                  @StateId(stateId)
                  private final StateSpec<BagState<String>> bufferState =
                      StateSpecs.bag(StringUtf8Coder.of());

                  @StateId(stateId2)
                  private final StateSpec<BagState<String>> bufferState2 =
                      StateSpecs.bag(StringUtf8Coder.of());

                  @ProcessElement
                  public void processElement(
                      @Element KV<String, String> element,
                      @StateId(stateId) BagState<String> state,
                      @StateId(stateId2) BagState<String> state2,
                      OutputReceiver<KV<String, String>> r) {
                    for (String value : state.read()) {
                      r.output(KV.of(element.getKey(), value));
                    }
                    ReadableState<Boolean> isEmpty = state2.isEmpty();
                    if (isEmpty.read()) {
                      r.output(KV.of(element.getKey(), "Empty"));
                    } else {
                      state2.clear();
                    }
                  }
                }))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(), (ExecutableStage stage) -> !stage.getUserStates().isEmpty());
    checkState(optionalStage.isPresent(), "Expected a stage with user state.");
    ExecutableStage stage = optionalStage.get();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "test_stage",
            stage,
            dataServer.getApiServiceDescriptor(),
            stateServer.getApiServiceDescriptor());

    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(),
            descriptor.getRemoteInputDestinations(),
            stateDelegator);
    Map<String, Coder> remoteOutputCoders = descriptor.getRemoteOutputCoders();
    Map<String, Collection<WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, Coder> remoteOutputCoder : remoteOutputCoders.entrySet()) {
      List<WindowedValue<?>> outputContents = Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder<WindowedValue<?>>) remoteOutputCoder.getValue(), outputContents::add));
    }

    Map<String, List<ByteString>> userStateData =
        ImmutableMap.of(
            stateId,
            new ArrayList(
                Arrays.asList(
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            StringUtf8Coder.of(), "A", Coder.Context.NESTED)),
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            StringUtf8Coder.of(), "B", Coder.Context.NESTED)),
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            StringUtf8Coder.of(), "C", Coder.Context.NESTED)))),
            stateId2,
            new ArrayList(
                Arrays.asList(
                    ByteString.copyFrom(
                        CoderUtils.encodeToByteArray(
                            StringUtf8Coder.of(), "D", Coder.Context.NESTED)))));

    StoringStateRequestHandler stateRequestHandler =
        new StoringStateRequestHandler(
            StateRequestHandlers.forBagUserStateHandlerFactory(
                descriptor,
                new BagUserStateHandlerFactory<ByteString, Object, BoundedWindow>() {
                  @Override
                  public BagUserStateHandler<ByteString, Object, BoundedWindow> forUserState(
                      String pTransformId,
                      String userStateId,
                      Coder<ByteString> keyCoder,
                      Coder<Object> valueCoder,
                      Coder<BoundedWindow> windowCoder) {
                    return new BagUserStateHandler<ByteString, Object, BoundedWindow>() {
                      @Override
                      public Iterable<Object> get(ByteString key, BoundedWindow window) {
                        return (Iterable) userStateData.get(userStateId);
                      }

                      @Override
                      public void append(
                          ByteString key, BoundedWindow window, Iterator<Object> values) {
                        Iterators.addAll(userStateData.get(userStateId), (Iterator) values);
                      }

                      @Override
                      public void clear(ByteString key, BoundedWindow window) {
                        userStateData.get(userStateId).clear();
                      }
                    };
                  }
                }));

    try (RemoteBundle bundle =
        processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of("X", "Y")));
    }
    try (RemoteBundle bundle2 =
        processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle2.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of("X", "Z")));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C")),
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C")),
              valueInGlobalWindow(KV.of("X", "Empty"))));
    }
    assertThat(
        userStateData.get(stateId),
        IsIterableContainingInOrder.contains(
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "A", Coder.Context.NESTED)),
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "B", Coder.Context.NESTED)),
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "C", Coder.Context.NESTED))));
    assertThat(userStateData.get(stateId2), IsEmptyIterable.emptyIterable());

    // 3 Requests expected: state read, state2 read, and state2 clear
    assertEquals(3, stateRequestHandler.getRequestCount());
    ByteString.Output out = ByteString.newOutput();
    StringUtf8Coder.of().encode("X", out);

    assertEquals(
        stateId,
        stateRequestHandler
            .receivedRequests
            .get(0)
            .getStateKey()
            .getBagUserState()
            .getUserStateId());
    assertEquals(
        stateRequestHandler.receivedRequests.get(0).getStateKey().getBagUserState().getKey(),
        out.toByteString());
    assertTrue(stateRequestHandler.receivedRequests.get(0).hasGet());

    assertEquals(
        stateId2,
        stateRequestHandler
            .receivedRequests
            .get(1)
            .getStateKey()
            .getBagUserState()
            .getUserStateId());
    assertEquals(
        stateRequestHandler.receivedRequests.get(1).getStateKey().getBagUserState().getKey(),
        out.toByteString());
    assertTrue(stateRequestHandler.receivedRequests.get(1).hasGet());

    assertEquals(
        stateId2,
        stateRequestHandler
            .receivedRequests
            .get(2)
            .getStateKey()
            .getBagUserState()
            .getUserStateId());
    assertEquals(
        stateRequestHandler.receivedRequests.get(2).getStateKey().getBagUserState().getKey(),
        out.toByteString());
    assertTrue(stateRequestHandler.receivedRequests.get(2).hasClear());
  }

  /**
   * A state handler that stores each state request made - used to validate that cached requests are
   * not forwarded to the state client.
   */
  private static class StoringStateRequestHandler implements StateRequestHandler {

    private StateRequestHandler stateRequestHandler;
    private ArrayList<BeamFnApi.StateRequest> receivedRequests;
    private ArrayList<BeamFnApi.ProcessBundleRequest.CacheToken> cacheTokens;

    StoringStateRequestHandler(StateRequestHandler delegate) {
      stateRequestHandler = delegate;
      receivedRequests = new ArrayList<>();
      cacheTokens = new ArrayList<>();
    }

    @Override
    public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
        throws Exception {
      receivedRequests.add(request);
      return stateRequestHandler.handle(request);
    }

    @Override
    public Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> getCacheTokens() {
      return Iterables.concat(stateRequestHandler.getCacheTokens(), cacheTokens);
    }

    public int getRequestCount() {
      return receivedRequests.size();
    }

    public void addCacheToken(BeamFnApi.ProcessBundleRequest.CacheToken token) {
      cacheTokens.add(token);
    }
  }
}
