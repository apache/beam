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

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.FusedPipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.InProcessServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.openjdk.jmh.annotations.Benchmark;
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
      try {
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

        // Create the SDK harness, and wait until it connects
        sdkHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
        sdkHarnessExecutorFuture =
            sdkHarnessExecutor.submit(
                () -> {
                  try {
                    FnHarness.main(
                        WORKER_ID,
                        PipelineOptionsFactory.create(),
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
        InstructionRequestHandler controlClient =
            clientPool.getSource().take(WORKER_ID, java.time.Duration.ofSeconds(2));
        this.controlClient =
            SdkHarnessClient.usingFnApiClient(controlClient, dataServer.getService());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @TearDown
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
}
