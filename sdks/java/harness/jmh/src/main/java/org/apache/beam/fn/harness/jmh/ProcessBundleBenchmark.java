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
package org.apache.beam.fn.harness.jmh;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateKey;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRunnerProtocols;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.ControlClientPool;
import org.apache.beam.runners.fnexecution.control.FnApiControlClientPoolService;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.MapControlClientPool;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.RemoteOutputReceiver;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.FusedPipeline;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/** Benchmarks for processing a bundle end to end. */
public class ProcessBundleBenchmark {

  private static final String WORKER_ID = "benchmark_worker";

  /** Sets up the {@link ExecutionStateTracker} and an execution state. */
  @State(Scope.Benchmark)
  public static class SdkHarness {
    @Param({"true", "false"})
    public String elementsEmbedding = "false";

    final GrpcFnServer<FnApiControlClientPoolService> controlServer;
    final GrpcFnServer<GrpcDataService> dataServer;
    final GrpcFnServer<GrpcStateService> stateServer;
    final GrpcFnServer<GrpcLoggingService> loggingServer;
    final GrpcStateService stateDelegator;
    final SdkHarnessClient controlClient;

    final ExecutorService serverExecutor;
    final ExecutorService sdkHarnessExecutor;
    final Future<?> sdkHarnessExecutorFuture;

    public SdkHarness() {
      Set<String> runnerCapabilities = new HashSet<>();
      if (Boolean.parseBoolean(elementsEmbedding)) {
        runnerCapabilities.add(
            BeamUrns.getUrn(StandardRunnerProtocols.Enum.CONTROL_RESPONSE_ELEMENTS_EMBEDDING));
      }
      try {
        // Setup execution-time servers
        ThreadFactory threadFactory =
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("ProcessBundlesBenchmark-thread")
                .build();
        serverExecutor = Executors.newCachedThreadPool(threadFactory);
        ServerFactory serverFactory = ServerFactory.createDefault();
        dataServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcDataService.create(
                    PipelineOptionsFactory.create(),
                    serverExecutor,
                    OutboundObserverFactory.serverDirect()),
                serverFactory);
        loggingServer =
            GrpcFnServer.allocatePortAndCreateFor(
                GrpcLoggingService.forWriter(
                    new LogWriter() {
                      @Override
                      public void log(LogEntry entry) {
                        // no-op
                      }
                    }),
                serverFactory);
        stateDelegator = GrpcStateService.create();
        stateServer = GrpcFnServer.allocatePortAndCreateFor(stateDelegator, serverFactory);

        ControlClientPool clientPool = MapControlClientPool.create();
        controlServer =
            GrpcFnServer.allocatePortAndCreateFor(
                FnApiControlClientPoolService.offeringClientsToPool(
                    clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
                serverFactory);

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        // Create the SDK harness, and wait until it connects
        sdkHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
        sdkHarnessExecutorFuture =
            sdkHarnessExecutor.submit(
                () -> {
                  try {
                    FnHarness.main(
                        WORKER_ID,
                        pipelineOptions,
                        runnerCapabilities,
                        loggingServer.getApiServiceDescriptor(),
                        controlServer.getApiServiceDescriptor(),
                        null,
                        ManagedChannelFactory.createDefault(),
                        OutboundObserverFactory.clientDirect(),
                        Caches.fromOptions(pipelineOptions));
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
        InstructionRequestHandler controlClient =
            clientPool.getSource().take(WORKER_ID, java.time.Duration.ofSeconds(60));
        this.controlClient =
            SdkHarnessClient.usingFnApiClient(controlClient, dataServer.getService());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @TearDown
    public void tearDown() {
      try {
        controlServer.close();
        stateServer.close();
        dataServer.close();
        loggingServer.close();
        controlClient.close();
        sdkHarnessExecutorFuture.get();
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        sdkHarnessExecutor.shutdownNow();
        serverExecutor.shutdownNow();
      }
    }
  }

  @State(Scope.Benchmark)
  public static class TrivialTransform extends SdkHarness {
    final BundleProcessor processor;
    final ExecutableProcessBundleDescriptor descriptor;

    private static class OutputZeroOneTwo extends DoFn<byte[], String> {
      @ProcessElement
      public void process(ProcessContext ctxt) {
        ctxt.output("zero");
        ctxt.output("one");
        ctxt.output("two");
      }
    }

    private static class ConvertLength extends DoFn<String, Long> {
      @ProcessElement
      public void process(ProcessContext ctxt) {
        ctxt.output((long) ctxt.element().length());
      }
    }

    public TrivialTransform() {
      try {
        Pipeline p = Pipeline.create();
        p.apply("impulse", Impulse.create())
            .apply("create", ParDo.of(new OutputZeroOneTwo()))
            .apply("len", ParDo.of(new ConvertLength()))
            .apply("addKeys", WithKeys.of("foo"))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
            // Force the output to be materialized
            .apply("gbk", GroupByKey.create());

        RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
        // The fuser will create one stage (SDK responsible portion in []):
        // (Impulse + [create + len + addKeys] + GBK write)
        //
        // We use the one stage containing the DoFns and run a benchmark expecting the SDK
        // to accept byte[] and output KV<String, Long>.
        FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
        checkState(fused.getFusedStages().size() == 1, "Expected exactly one fused stage");
        ExecutableStage stage = fused.getFusedStages().iterator().next();

        this.descriptor =
            ProcessBundleDescriptors.fromExecutableStage(
                "my_stage", stage, dataServer.getApiServiceDescriptor());

        this.processor =
            controlClient.getProcessor(
                descriptor.getProcessBundleDescriptor(), descriptor.getRemoteInputDestinations());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testTinyBundle(TrivialTransform trivialTransform) throws Exception {
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        trivialTransform.descriptor.getRemoteOutputCoders();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    AtomicInteger outputValuesCount = new AtomicInteger();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>)
                  (WindowedValue<?> value) -> outputValuesCount.incrementAndGet()));
    }
    try (RemoteBundle bundle =
        trivialTransform.processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(new byte[0]));
    }
    assertEquals(3, outputValuesCount.getAndSet(0));
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testLargeBundle(TrivialTransform trivialTransform) throws Exception {
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        trivialTransform.descriptor.getRemoteOutputCoders();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    AtomicInteger outputValuesCount = new AtomicInteger();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>)
                  (WindowedValue<?> value) -> outputValuesCount.incrementAndGet()));
    }
    try (RemoteBundle bundle =
        trivialTransform.processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      for (int i = 0; i < 1_000; i++) {
        Iterables.getOnlyElement(bundle.getInputReceivers().values())
            .accept(valueInGlobalWindow(new byte[0]));
      }
    }
    assertEquals(3_000, outputValuesCount.getAndSet(0));
  }

  @State(Scope.Benchmark)
  public static class StatefulTransform extends SdkHarness {
    final BundleProcessor processor;
    final ExecutableProcessBundleDescriptor descriptor;
    final StateRequestHandler nonCachingStateRequestHandler;
    final StateRequestHandler cachingStateRequestHandler;

    @SuppressWarnings({
      // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
      // errorprone is released (2.11.0)
      "unused"
    })
    private static class StatefulOutputZeroOneTwo
        extends DoFn<KV<String, String>, KV<String, String>> {
      private static final String STATE_ID = "bagState";

      @StateId(STATE_ID)
      private final StateSpec<BagState<String>> bagStateSpec = StateSpecs.bag(StringUtf8Coder.of());

      @ProcessElement
      public void process(ProcessContext ctxt, @StateId(STATE_ID) BagState<String> state) {
        int size = Iterables.size(state.read());
        if (size == 3) {
          for (String value : state.read()) {
            ctxt.output(KV.of(ctxt.element().getKey(), value));
          }
          state.clear();
        } else {
          state.add(ctxt.element().getValue());
        }
      }
    }

    private static class ToKeyAndValueDoFn extends DoFn<byte[], KV<String, String>> {
      @ProcessElement
      public void process(ProcessContext ctxt) {
        ctxt.output(KV.of("key", "value"));
      }
    }

    public StatefulTransform() {
      try {
        Pipeline p = Pipeline.create();
        p.apply("impulse", Impulse.create())
            .apply("toKeyAndValue", ParDo.of(new ToKeyAndValueDoFn()))
            .apply("stateful", ParDo.of(new StatefulOutputZeroOneTwo()))
            // Force the output to be materialized
            .apply("gbk", GroupByKey.create());

        RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
        // The fuser will break up the pipeline into two stages (SDK responsible portion in []):
        // (Impulse + [toKeyAndValue]) -> ([stateful] + GBK write)
        //
        // We pull out the stage containing the stateful DoFn and run a benchmark expecting the SDK
        // to accept KV<String, String> and output KV<String, String>.
        FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
        checkState(fused.getFusedStages().size() == 2, "Expected exactly two fused stages");
        ExecutableStage stage = null;
        for (ExecutableStage value : fused.getFusedStages()) {
          if (!value.getUserStates().isEmpty()) {
            stage = value;
            break;
          }
        }
        if (stage == null) {
          throw new IllegalStateException("Stage with stateful DoFn not found.");
        }

        this.descriptor =
            ProcessBundleDescriptors.fromExecutableStage(
                "my_stage",
                stage,
                dataServer.getApiServiceDescriptor(),
                stateServer.getApiServiceDescriptor());

        this.processor =
            controlClient.getProcessor(
                descriptor.getProcessBundleDescriptor(),
                descriptor.getRemoteInputDestinations(),
                stateDelegator);
        this.nonCachingStateRequestHandler = new InMemoryBagUserStateHandler();

        List<CacheToken> cacheTokens = new ArrayList<>();
        cacheTokens.add(
            CacheToken.newBuilder()
                .setUserState(CacheToken.UserState.newBuilder())
                .setToken(ByteString.copyFromUtf8("cacheMe"))
                .build());
        this.cachingStateRequestHandler =
            new InMemoryBagUserStateHandler() {
              @Override
              public Iterable<CacheToken> getCacheTokens() {
                return cacheTokens;
              }
            };
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class InMemoryBagUserStateHandler implements StateRequestHandler {
    private final Map<ByteString, ByteString> bagState = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<StateResponse.Builder> handle(StateRequest request) throws Exception {
      if (!request.getStateKey().hasBagUserState()) {
        throw new IllegalStateException(
            "Unknown state key type " + request.getStateKey().getTypeCase());
      }
      StateResponse.Builder response = StateResponse.newBuilder();
      StateKey.BagUserState stateKey = request.getStateKey().getBagUserState();
      switch (request.getRequestCase()) {
        case APPEND:
          {
            ByteString data =
                bagState.computeIfAbsent(stateKey.getKey(), (unused) -> ByteString.EMPTY);
            bagState.put(stateKey.getKey(), data.concat(request.getAppend().getData()));
            response.getAppendBuilder().build();
            break;
          }
        case CLEAR:
          {
            bagState.remove(stateKey.getKey());
            response.getClearBuilder().build();
            break;
          }
        case GET:
          {
            ByteString data =
                bagState.computeIfAbsent(stateKey.getKey(), (unused) -> ByteString.EMPTY);
            response.getGetBuilder().setData(data).build();
            break;
          }
        default:
          throw new IllegalStateException("Unknown request type " + request.getRequestCase());
      }
      return CompletableFuture.completedFuture(response);
    }
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testStateWithoutCaching(StatefulTransform statefulTransform) throws Exception {
    testState(statefulTransform, statefulTransform.nonCachingStateRequestHandler);
  }

  @Benchmark
  @Threads(16) // Use several threads since we expect contention during bundle processing.
  public void testStateWithCaching(StatefulTransform statefulTransform) throws Exception {
    testState(statefulTransform, statefulTransform.cachingStateRequestHandler);
  }

  private static void testState(
      StatefulTransform statefulTransform, StateRequestHandler stateRequestHandler)
      throws Exception {
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        statefulTransform.descriptor.getRemoteOutputCoders();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    AtomicInteger outputValuesCount = new AtomicInteger();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>)
                  (WindowedValue<?> value) -> outputValuesCount.incrementAndGet()));
    }
    String key = Strings.padStart(Long.toHexString(Thread.currentThread().getId()), 16, '0');
    try (RemoteBundle bundle =
        statefulTransform.processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of(key, "zero")));
    }
    try (RemoteBundle bundle =
        statefulTransform.processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of(key, "one")));
    }
    try (RemoteBundle bundle =
        statefulTransform.processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of(key, "two")));
    }
    try (RemoteBundle bundle =
        statefulTransform.processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of(key, "flush")));
    }
    assertEquals(3, outputValuesCount.getAndSet(0));
  }
}
