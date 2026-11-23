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

import static org.apache.beam.sdk.values.WindowedValues.valueInGlobalWindow;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.pipeline.v1.RunnerApi;
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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.ServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.FusedPipeline;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** JMH benchmarks for deep fused chains in the Java Fn API SDK Harness. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@SuppressWarnings({"rawtypes", "unchecked"})
public class FusionChainBenchmark {

  private static final int BUNDLE_SIZE = 1000;
  private static final byte[] PAYLOAD = "fusion-harness-payload".getBytes(StandardCharsets.UTF_8);

  public static final class BytesToStringDoFn extends DoFn<byte[], String> {
    @ProcessElement
    public void process(@Element byte[] in, OutputReceiver<String> out) {
      out.output(new String(in, StandardCharsets.UTF_8));
    }
  }

  public static final class StepFn0 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (!e.isEmpty()) {
        out.output(e);
      }
    }
  }

  public static final class StepFn1 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() > 0) {
        out.output(e);
      }
    }
  }

  public static final class StepFn2 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.charAt(0) != 0) {
        out.output(e);
      }
    }
  }

  public static final class StepFn3 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e != null) {
        out.output(e);
      }
    }
  }

  public static final class StepFn4 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() >= 1) {
        out.output(e);
      }
    }
  }

  public static final class StepFn5 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.hashCode() != 123456789) {
        out.output(e);
      }
    }
  }

  public static final class StepFn6 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.charAt(0) >= ' ') {
        out.output(e);
      }
    }
  }

  public static final class StepFn7 extends DoFn<String, String> {
    @ProcessElement
    public void process(@Element String e, OutputReceiver<String> out) {
      if (e.length() < 1_000_000) {
        out.output(e);
      }
    }
  }

  private static DoFn<String, String> stepFn(int idx) {
    switch (idx & 7) {
      case 0:
        return new StepFn0();
      case 1:
        return new StepFn1();
      case 2:
        return new StepFn2();
      case 3:
        return new StepFn3();
      case 4:
        return new StepFn4();
      case 5:
        return new StepFn5();
      case 6:
        return new StepFn6();
      default:
        return new StepFn7();
    }
  }

  @State(Scope.Benchmark)
  @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
  public static class DeepChainHarness {
    private static final String WORKER_ID = "benchmark_worker";

    @Param({"true", "false"})
    public String elementsEmbedding = "false";

    @Param({"2", "4", "8", "16", "32"})
    public int depth;

    @Param({"false"})
    public boolean disableTrampoline;

    @Param({"false"})
    public boolean disableCodegen;

    GrpcFnServer<FnApiControlClientPoolService> controlServer;
    GrpcFnServer<GrpcDataService> dataServer;
    GrpcFnServer<GrpcStateService> stateServer;
    GrpcFnServer<GrpcLoggingService> loggingServer;
    SdkHarnessClient controlClient;
    ExecutorService serverExecutor;
    ExecutorService sdkHarnessExecutor;
    Future<?> sdkHarnessExecutorFuture;

    BundleProcessor heterogeneousProcessor;
    ExecutableProcessBundleDescriptor heterogeneousDescriptor;

    BundleProcessor mapFilterProcessor;
    ExecutableProcessBundleDescriptor mapFilterDescriptor;

    @Setup
    public void setupChains() throws Exception {
      Set<String> runnerCapabilities = new HashSet<>();
      ThreadFactory threadFactory =
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("FusionChainBenchmark-thread")
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
                    public void log(LogEntry entry) {}
                  }),
              serverFactory);
      GrpcStateService stateDelegator = GrpcStateService.create();
      stateServer = GrpcFnServer.allocatePortAndCreateFor(stateDelegator, serverFactory);
      ControlClientPool clientPool = MapControlClientPool.create();
      controlServer =
          GrpcFnServer.allocatePortAndCreateFor(
              FnApiControlClientPoolService.offeringClientsToPool(
                  clientPool.getSink(), GrpcContextHeaderAccessorProvider.getHeaderAccessor()),
              serverFactory);

      PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
      if (disableTrampoline || disableCodegen) {
        java.util.List<String> exps = new java.util.ArrayList<>();
        if (disableTrampoline) {
          exps.add("disable_fusion_trampoline");
        }
        if (disableCodegen) {
          exps.add("disable_codegen_fused_segments");
        }
        pipelineOptions.as(ExperimentalOptions.class).setExperiments(exps);
      }
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
      InstructionRequestHandler requestHandler =
          clientPool.getSource().take(WORKER_ID, java.time.Duration.ofSeconds(60));
      this.controlClient =
          SdkHarnessClient.usingFnApiClient(requestHandler, dataServer.getService());
      // 1. Heterogeneous DoFn chain
      Pipeline p1 = Pipeline.create();
      PCollection<String> cur1 =
          p1.apply("impulse", Impulse.create()).apply("decode", ParDo.of(new BytesToStringDoFn()));
      for (int i = 0; i < depth; i++) {
        cur1 = cur1.apply("step_" + i, ParDo.of(stepFn(i)));
      }
      cur1.apply("key", WithKeys.of("k"))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .apply("gbk", GroupByKey.create());

      RunnerApi.Pipeline proto1 = PipelineTranslation.toProto(p1);
      FusedPipeline fused1 = GreedyPipelineFuser.fuse(proto1);
      checkState(fused1.getFusedStages().size() == 1, "Expected 1 fused stage");
      ExecutableStage stage1 = fused1.getFusedStages().iterator().next();
      this.heterogeneousDescriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              "heterogeneous_" + depth, stage1, dataServer.getApiServiceDescriptor());
      this.heterogeneousProcessor =
          controlClient.getProcessor(
              heterogeneousDescriptor.getProcessBundleDescriptor(),
              heterogeneousDescriptor.getRemoteInputDestinations());

      // 2. MapElements & Filter chain
      Pipeline p2 = Pipeline.create();
      PCollection<String> cur2 =
          p2.apply("impulse", Impulse.create()).apply("decode", ParDo.of(new BytesToStringDoFn()));
      for (int i = 0; i < depth; i++) {
        if ((i & 1) == 0) {
          final int step = i;
          cur2 =
              cur2.apply(
                  "map_" + i,
                  MapElements.into(TypeDescriptors.strings())
                      .via(s -> s.length() >= (step & 0) ? s : ""));
        } else {
          cur2 = cur2.apply("filter_" + i, Filter.by(s -> !s.isEmpty()));
        }
      }
      cur2.apply("key", WithKeys.of("k"))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .apply("gbk", GroupByKey.create());

      RunnerApi.Pipeline proto2 = PipelineTranslation.toProto(p2);
      FusedPipeline fused2 = GreedyPipelineFuser.fuse(proto2);
      checkState(fused2.getFusedStages().size() == 1, "Expected 1 fused stage");
      ExecutableStage stage2 = fused2.getFusedStages().iterator().next();
      this.mapFilterDescriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              "mapfilter_" + depth, stage2, dataServer.getApiServiceDescriptor());
      this.mapFilterProcessor =
          controlClient.getProcessor(
              mapFilterDescriptor.getProcessBundleDescriptor(),
              mapFilterDescriptor.getRemoteInputDestinations());
    }

    @TearDown
    public void tearDown() {
      try {
        controlServer.close();
        sdkHarnessExecutorFuture.get();
        stateServer.close();
        dataServer.close();
        loggingServer.close();
        controlClient.close();
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

  private static void runBundle(
      ExecutableProcessBundleDescriptor descriptor, BundleProcessor processor) throws Exception {
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        descriptor.getRemoteOutputCoders();
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
        processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      FnDataReceiver<WindowedValue<?>> inputReceiver =
          Iterables.getOnlyElement(bundle.getInputReceivers().values());
      WindowedValue<byte[]> elem = valueInGlobalWindow(PAYLOAD);
      for (int i = 0; i < BUNDLE_SIZE; i++) {
        inputReceiver.accept(elem);
      }
    }
    assertEquals(BUNDLE_SIZE, outputValuesCount.get());
  }

  @Benchmark
  public void heterogeneousDoFnChain(DeepChainHarness state) throws Exception {
    runBundle(state.heterogeneousDescriptor, state.heterogeneousProcessor);
  }

  @Benchmark
  public void mapAndFilterChain(DeepChainHarness state) throws Exception {
    runBundle(state.mapFilterDescriptor, state.mapFilterProcessor);
  }
}
