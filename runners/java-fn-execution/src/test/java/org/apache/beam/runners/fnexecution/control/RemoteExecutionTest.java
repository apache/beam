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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry.Severity;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse.ChannelSplit;
import org.apache.beam.model.pipeline.v1.MetricsApi.MonitoringInfo;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.TypeUrns;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.MonitoringInfoMatchers;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.LogWriter;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.BagUserStateHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.BagUserStateHandlerFactory;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.IterableSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.MultimapSideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.server.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.sdk.fn.server.InProcessServerFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.testing.ResetDateTimeProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.FusedPipeline;
import org.apache.beam.sdk.util.construction.graph.GreedyPipelineFuser;
import org.apache.beam.sdk.util.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.util.construction.graph.ProtoOverrides;
import org.apache.beam.sdk.util.construction.graph.SplittableParDoExpander;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsEmptyIterable;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

/**
 * Tests the execution of a pipeline from specification time to executing a single fused stage,
 * going through pipeline fusion.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "keyfor",
  // TODO(https://github.com/apache/beam/issues/21230): Remove when new version of
  // errorprone is released (2.11.0)
  "unused"
})
public class RemoteExecutionTest implements Serializable {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Rule public transient ResetDateTimeProvider resetDateTimeProvider = new ResetDateTimeProvider();

  private static final String WORKER_ID = "remote_test";

  private transient GrpcFnServer<FnApiControlClientPoolService> controlServer;
  private transient GrpcFnServer<GrpcDataService> dataServer;
  private transient GrpcFnServer<GrpcStateService> stateServer;
  private transient LogCapturer logCapturer;
  private transient GrpcFnServer<GrpcLoggingService> loggingServer;
  private transient GrpcStateService stateDelegator;
  private transient SdkHarnessClient controlClient;

  private transient ExecutorService serverExecutor;
  private transient ExecutorService sdkHarnessExecutor;
  private transient Future<?> sdkHarnessExecutorFuture;

  private static class LogCapturer implements LogWriter {
    List<BeamFnApi.LogEntry> capturedLogs = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void log(BeamFnApi.LogEntry entry) {
      capturedLogs.add(entry);
    }
  }

  public void launchSdkHarness(PipelineOptions options) throws Exception {
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
    logCapturer = new LogCapturer();
    loggingServer =
        GrpcFnServer.allocatePortAndCreateFor(
            GrpcLoggingService.forWriter(logCapturer), serverFactory);
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
                    options,
                    Collections.emptySet(), // Runner capabilities.
                    loggingServer.getApiServiceDescriptor(),
                    controlServer.getApiServiceDescriptor(),
                    null,
                    ManagedChannelFactory.createInProcess(),
                    OutboundObserverFactory.clientDirect(),
                    Caches.eternal());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    InstructionRequestHandler controlClient =
        clientPool.getSource().take(WORKER_ID, java.time.Duration.ofSeconds(10));
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
      Throwable ex = e.getCause();
      while (ex instanceof RuntimeException) {
        ex = ex.getCause();
      }
      // InterruptedException at call tree is expected
      if (!(ex instanceof InterruptedException)) {
        throw e;
      }
    }
    logCapturer = null;
  }

  @Test
  public void testExecution() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();
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
        .apply(
            "len",
            ParDo.of(
                new DoFn<String, Long>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {
                    ctxt.output((long) ctxt.element().length());
                  }
                }))
        .apply("addKeys", WithKeys.of("foo"))
        // Use some unknown coders
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    checkState(fused.getFusedStages().size() == 1, "Expected exactly one fused stage");
    ExecutableStage stage = fused.getFusedStages().iterator().next();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "my_stage", stage, dataServer.getApiServiceDescriptor());

    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(), descriptor.getRemoteInputDestinations());
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        descriptor.getRemoteOutputCoders();
    Map<String, Collection<? super WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      List<? super WindowedValue<?>> outputContents =
          Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>) outputContents::add));
    }

    // The impulse example
    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(new byte[0]));
    }

    for (Collection<? super WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(byteValueOf("foo", 4)),
              valueInGlobalWindow(byteValueOf("foo", 3)),
              valueInGlobalWindow(byteValueOf("foo", 3))));
    }
  }

  @Test
  public void testLogging() throws Exception {
    long startTime = System.currentTimeMillis();
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();
    p.apply("impulse", Impulse.create())
        .apply(
            "create",
            ParDo.of(
                new DoFn<byte[], String>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {
                    ctxt.output("zero");
                  }
                }))
        .apply(
            "len",
            ParDo.of(
                new DoFn<String, Long>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {
                    org.slf4j.Logger logger = LoggerFactory.getLogger(RemoteExecutionTest.class);
                    logger.warn("TEST" + ctxt.element());
                    logger.error("TEST_EXCEPTION" + ctxt.element(), new Exception());
                  }
                }))
        .apply("addKeys", WithKeys.of("foo"))
        // Use some unknown coders
        .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of()))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    checkState(fused.getFusedStages().size() == 1, "Expected exactly one fused stage");
    ExecutableStage stage = fused.getFusedStages().iterator().next();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "my_stage", stage, dataServer.getApiServiceDescriptor());
    String ptransformId = null;
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        descriptor.getProcessBundleDescriptor().getTransformsMap().entrySet()) {
      if (entry.getValue().getUniqueName().contains("len")) {
        ptransformId = entry.getKey();
      }
    }
    assertNotNull(ptransformId);
    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(), descriptor.getRemoteInputDestinations());
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        descriptor.getRemoteOutputCoders();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      List<? super WindowedValue<?>> outputContents =
          Collections.synchronizedList(new ArrayList<>());
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>) outputContents::add));
    }

    String instructionId;
    // Execute a bundle that logs.
    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      instructionId = bundle.getId();
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(new byte[0]));
    }

    while (System.currentTimeMillis() - startTime < 30_000L) {
      BeamFnApi.LogEntry[] logs = logCapturer.capturedLogs.toArray(new BeamFnApi.LogEntry[0]);
      boolean foundPTransformLog = false;
      boolean foundExceptionLog = false;
      for (BeamFnApi.LogEntry log : logs) {
        assertThat(
            log.getTimestamp().getSeconds() * 1000 + log.getTimestamp().getNanos() / 1_000_000,
            allOf(greaterThanOrEqualTo(startTime), lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(log.getThread(), not(""));
        assertThat(log.getLogLocation(), not(""));

        if ("TESTzero".equals(log.getMessage())) {
          assertThat(log.getSeverity(), equalTo(Severity.Enum.WARN));
          assertThat(log.getInstructionId(), equalTo(instructionId));
          assertThat(log.getLogLocation(), equalTo(RemoteExecutionTest.class.getCanonicalName()));
          assertThat(log.getTransformId(), equalTo(ptransformId));
          assertThat(log.getTrace(), equalTo(""));
          foundPTransformLog = true;
        } else if ("TEST_EXCEPTIONzero".equals(log.getMessage())) {
          assertThat(log.getSeverity(), equalTo(Severity.Enum.ERROR));
          assertThat(log.getInstructionId(), equalTo(instructionId));
          assertThat(log.getLogLocation(), equalTo(RemoteExecutionTest.class.getCanonicalName()));
          assertThat(log.getTransformId(), equalTo(ptransformId));
          assertThat(log.getTrace(), containsString("RemoteExecutionTest"));
          foundExceptionLog = true;
        }
      }
      if (foundPTransformLog && foundExceptionLog) {
        break;
      }
      // Wait till we get more logs from the SDK.
      Thread.sleep(500);
    }
  }

  @Test
  public void testBundleProcessorThrowsExecutionExceptionWhenUserCodeThrows() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();
    p.apply("impulse", Impulse.create())
        .apply(
            "create",
            ParDo.of(
                new DoFn<byte[], KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) throws Exception {
                    String element =
                        CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), ctxt.element());
                    if (element.equals("X")) {
                      throw new Exception("testBundleExecutionFailure");
                    }
                    ctxt.output(KV.of(element, element));
                  }
                }))
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    checkState(fused.getFusedStages().size() == 1, "Expected exactly one fused stage");
    ExecutableStage stage = fused.getFusedStages().iterator().next();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "my_stage", stage, dataServer.getApiServiceDescriptor());

    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(), descriptor.getRemoteInputDestinations());
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        descriptor.getRemoteOutputCoders();
    Map<String, Collection<? super WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      List<? super WindowedValue<?>> outputContents =
          Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>) outputContents::add));
    }

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "Y")));
    }

    try {
      try (RemoteBundle bundle =
          processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
        Iterables.getOnlyElement(bundle.getInputReceivers().values())
            .accept(valueInGlobalWindow(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "X")));
      }
      // Fail the test if we reach this point and never threw the exception.
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getMessage().contains("testBundleExecutionFailure"));
    }

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "Z")));
    }

    for (Collection<? super WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("Y", "Y")), valueInGlobalWindow(KV.of("Z", "Z"))));
    }
  }

  @Test
  public void testExecutionWithSideInput() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
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
    PCollectionView<Iterable<String>> iterableView =
        input.apply("createIterableSideInput", View.asIterable());
    PCollectionView<Map<String, Iterable<String>>> multimapView =
        input.apply(WithKeys.of("key")).apply("createMultimapSideInput", View.asMultimap());

    input
        .apply(
            "readSideInput",
            ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        for (String value : context.sideInput(iterableView)) {
                          context.output(KV.of(context.element(), value));
                        }
                        for (Map.Entry<String, Iterable<String>> entry :
                            context.sideInput(multimapView).entrySet()) {
                          for (String value : entry.getValue()) {
                            context.output(KV.of(context.element(), entry.getKey() + ":" + value));
                          }
                        }
                      }
                    })
                .withSideInputs(iterableView, multimapView))
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

    StateRequestHandler stateRequestHandler =
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
                return new MultimapSideInputHandler<K, V, W>() {
                  @Override
                  public Iterable<K> get(W window) {
                    return (Iterable) Arrays.asList("key1", "key2");
                  }

                  @Override
                  public Iterable<V> get(K key, W window) {
                    if ("key1".equals(key)) {
                      return (Iterable) Arrays.asList("H", "I", "J");
                    } else if ("key2".equals(key)) {
                      return (Iterable) Arrays.asList("M", "N", "O");
                    }
                    return Collections.emptyList();
                  }

                  @Override
                  public Coder<K> keyCoder() {
                    return elementCoder.getKeyCoder();
                  }

                  @Override
                  public Coder<V> valueCoder() {
                    return elementCoder.getValueCoder();
                  }
                };
              }
            });
    BundleProgressHandler progressHandler = BundleProgressHandler.ignored();

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, stateRequestHandler, progressHandler)) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow("X"));
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow("Y"));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C")),
              valueInGlobalWindow(KV.of("X", "key1:H")),
              valueInGlobalWindow(KV.of("X", "key1:I")),
              valueInGlobalWindow(KV.of("X", "key1:J")),
              valueInGlobalWindow(KV.of("X", "key2:M")),
              valueInGlobalWindow(KV.of("X", "key2:N")),
              valueInGlobalWindow(KV.of("X", "key2:O")),
              valueInGlobalWindow(KV.of("Y", "A")),
              valueInGlobalWindow(KV.of("Y", "B")),
              valueInGlobalWindow(KV.of("Y", "C")),
              valueInGlobalWindow(KV.of("Y", "key1:H")),
              valueInGlobalWindow(KV.of("Y", "key1:I")),
              valueInGlobalWindow(KV.of("Y", "key1:J")),
              valueInGlobalWindow(KV.of("Y", "key2:M")),
              valueInGlobalWindow(KV.of("Y", "key2:N")),
              valueInGlobalWindow(KV.of("Y", "key2:O"))));
    }
  }

  @Test
  public void testExecutionWithSideInputCaching() throws Exception {
    Pipeline p = Pipeline.create();
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "beam_fn_api");
    // TODO(BEAM-10097): Remove experiment once all portable runners support this view type
    addExperiment(p.getOptions().as(ExperimentalOptions.class), "use_runner_v2");

    launchSdkHarness(p.getOptions());

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
    PCollectionView<Iterable<String>> iterableView =
        input.apply("createIterableSideInput", View.asIterable());
    PCollectionView<Map<String, Iterable<String>>> multimapView =
        input.apply(WithKeys.of("key")).apply("createMultimapSideInput", View.asMultimap());

    input
        .apply(
            "readSideInput",
            ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        for (String value : context.sideInput(iterableView)) {
                          context.output(KV.of(context.element(), value));
                        }
                        for (Map.Entry<String, Iterable<String>> entry :
                            context.sideInput(multimapView).entrySet()) {
                          for (String value : entry.getValue()) {
                            context.output(KV.of(context.element(), entry.getKey() + ":" + value));
                          }
                        }
                      }
                    })
                .withSideInputs(iterableView, multimapView))
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
                    return new MultimapSideInputHandler<K, V, W>() {
                      @Override
                      public Iterable<K> get(W window) {
                        return (Iterable) Arrays.asList("key1", "key2");
                      }

                      @Override
                      public Iterable<V> get(K key, W window) {
                        if ("key1".equals(key)) {
                          return (Iterable) Arrays.asList("H", "I", "J");
                        } else if ("key2".equals(key)) {
                          return (Iterable) Arrays.asList("M", "N", "O");
                        }
                        return Collections.emptyList();
                      }

                      @Override
                      public Coder<K> keyCoder() {
                        return elementCoder.getKeyCoder();
                      }

                      @Override
                      public Coder<V> valueCoder() {
                        return elementCoder.getValueCoder();
                      }
                    };
                  }
                }));
    String transformId = Iterables.get(stage.getSideInputs(), 0).transform().getId();
    stateRequestHandler.addCacheToken(
        BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder()
            .setSideInput(
                BeamFnApi.ProcessBundleRequest.CacheToken.SideInput.newBuilder()
                    .setSideInputId(iterableView.getTagInternal().getId())
                    .setTransformId(transformId)
                    .build())
            .setToken(ByteString.copyFromUtf8("IterableSideInputToken"))
            .build());
    stateRequestHandler.addCacheToken(
        BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder()
            .setSideInput(
                BeamFnApi.ProcessBundleRequest.CacheToken.SideInput.newBuilder()
                    .setSideInputId(multimapView.getTagInternal().getId())
                    .setTransformId(transformId)
                    .build())
            .setToken(ByteString.copyFromUtf8("MulitmapSideInputToken"))
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
          .accept(valueInGlobalWindow("Y"));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C")),
              valueInGlobalWindow(KV.of("X", "key1:H")),
              valueInGlobalWindow(KV.of("X", "key1:I")),
              valueInGlobalWindow(KV.of("X", "key1:J")),
              valueInGlobalWindow(KV.of("X", "key2:M")),
              valueInGlobalWindow(KV.of("X", "key2:N")),
              valueInGlobalWindow(KV.of("X", "key2:O")),
              valueInGlobalWindow(KV.of("Y", "A")),
              valueInGlobalWindow(KV.of("Y", "B")),
              valueInGlobalWindow(KV.of("Y", "C")),
              valueInGlobalWindow(KV.of("Y", "key1:H")),
              valueInGlobalWindow(KV.of("Y", "key1:I")),
              valueInGlobalWindow(KV.of("Y", "key1:J")),
              valueInGlobalWindow(KV.of("Y", "key2:M")),
              valueInGlobalWindow(KV.of("Y", "key2:N")),
              valueInGlobalWindow(KV.of("Y", "key2:O"))));
    }

    // Expect the following requests for the first bundle:
    //   * one to read iterable side input
    //   * one to read keys from multimap side input
    //   * one to read key1 iterable from multimap side input
    //   * one to read key2 iterable from multimap side input
    assertEquals(4, stateRequestHandler.receivedRequests.size());
    assertEquals(
        stateRequestHandler.receivedRequests.get(0).getStateKey().getIterableSideInput(),
        BeamFnApi.StateKey.IterableSideInput.newBuilder()
            .setSideInputId(iterableView.getTagInternal().getId())
            .setTransformId(transformId)
            .build());
    assertEquals(
        stateRequestHandler.receivedRequests.get(1).getStateKey().getMultimapKeysSideInput(),
        BeamFnApi.StateKey.MultimapKeysSideInput.newBuilder()
            .setSideInputId(multimapView.getTagInternal().getId())
            .setTransformId(transformId)
            .build());
    assertEquals(
        stateRequestHandler.receivedRequests.get(2).getStateKey().getMultimapSideInput(),
        BeamFnApi.StateKey.MultimapSideInput.newBuilder()
            .setSideInputId(multimapView.getTagInternal().getId())
            .setTransformId(transformId)
            .setKey(encode("key1"))
            .build());
    assertEquals(
        stateRequestHandler.receivedRequests.get(3).getStateKey().getMultimapSideInput(),
        BeamFnApi.StateKey.MultimapSideInput.newBuilder()
            .setSideInputId(multimapView.getTagInternal().getId())
            .setTransformId(transformId)
            .setKey(encode("key2"))
            .build());
  }

  private static ByteString encode(String value) throws Exception {
    ByteStringOutputStream output = new ByteStringOutputStream();
    StringUtf8Coder.of().encode(value, output);
    return output.toByteString();
  }

  /**
   * A {@link DoFn} that uses static maps of {@link CountDownLatch}es to block execution allowing
   * for synchronization during test execution. The expected flow is:
   *
   * <ol>
   *   <li>Runner -> wait for AFTER_PROCESS
   *   <li>SDK -> unlock AFTER_PROCESS and wait for ALLOW_COMPLETION
   *   <li>Runner -> issue progress request and on response unlock ALLOW_COMPLETION
   * </ol>
   */
  private static class MetricsDoFn extends DoFn<byte[], String> {
    private static final String PROCESS_USER_COUNTER_NAME = "processUserCounter";
    private static final String START_USER_COUNTER_NAME = "startUserCounter";
    private static final String FINISH_USER_COUNTER_NAME = "finishUserCounter";
    private static final String PROCESS_USER_DISTRIBUTION_NAME = "processUserDistribution";
    private static final String START_USER_DISTRIBUTION_NAME = "startUserDistribution";
    private static final String FINISH_USER_DISTRIBUTION_NAME = "finishUserDistribution";
    private static final ConcurrentMap<String, CountDownLatch> AFTER_PROCESS =
        new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, CountDownLatch> ALLOW_COMPLETION =
        new ConcurrentHashMap<>();

    private final String uuid = UUID.randomUUID().toString();

    public MetricsDoFn() {
      AFTER_PROCESS.put(uuid, new CountDownLatch(1));
      ALLOW_COMPLETION.put(uuid, new CountDownLatch(1));
    }

    @StartBundle
    public void startBundle() throws InterruptedException {
      Metrics.counter(RemoteExecutionTest.class, START_USER_COUNTER_NAME).inc(10);
      Metrics.distribution(RemoteExecutionTest.class, START_USER_DISTRIBUTION_NAME).update(10);
      Thread.sleep(500);
    }

    @ProcessElement
    public void processElement(ProcessContext ctxt) throws InterruptedException {
      ctxt.output("zero");
      ctxt.output("one");
      ctxt.output("two");
      Metrics.counter(RemoteExecutionTest.class, PROCESS_USER_COUNTER_NAME).inc();
      Metrics.distribution(RemoteExecutionTest.class, PROCESS_USER_DISTRIBUTION_NAME).update(1);
      Thread.sleep(500);
      AFTER_PROCESS.get(uuid).countDown();
      checkState(
          ALLOW_COMPLETION.get(uuid).await(60, TimeUnit.SECONDS),
          "Failed to wait for DoFn to be allowed to complete.");
    }

    @FinishBundle
    public void finishBundle() throws InterruptedException {
      Metrics.counter(RemoteExecutionTest.class, FINISH_USER_COUNTER_NAME).inc(100);
      Metrics.distribution(RemoteExecutionTest.class, FINISH_USER_DISTRIBUTION_NAME).update(100);
      Thread.sleep(500);
    }
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testMetrics() throws Exception {
    launchSdkHarness(
        PipelineOptionsFactory.fromArgs("--experiments=state_sampling_period_millis=10").create());
    MetricsDoFn metricsDoFn = new MetricsDoFn();
    Pipeline p = Pipeline.create();

    PCollection<String> input =
        p.apply("impulse", Impulse.create())
            .apply("create", ParDo.of(metricsDoFn))
            .setCoder(StringUtf8Coder.of());

    SingleOutput<String, String> pardo =
        ParDo.of(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(ProcessContext ctxt) {
                // Output the element twice to keep unique numbers in asserts, 6 output elements.
                ctxt.output(ctxt.element());
                ctxt.output(ctxt.element());
              }
            });
    input.apply("processA", pardo).setCoder(StringUtf8Coder.of());
    input.apply("processB", pardo).setCoder(StringUtf8Coder.of());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(fused.getFusedStages(), (ExecutableStage stage) -> true);
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
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, Coder> remoteOutputCoder : remoteOutputCoders.entrySet()) {
      List<WindowedValue<?>> outputContents = Collections.synchronizedList(new ArrayList<>());
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder<WindowedValue<?>>) remoteOutputCoder.getValue(), outputContents::add));
    }

    AtomicReference<List<MonitoringInfo>> progressMonitoringInfos = new AtomicReference<>();

    final String testPTransformId = "create-ParMultiDo-Metrics-";
    BundleProgressHandler progressHandler =
        new BundleProgressHandler() {
          @Override
          public void onProgress(ProcessBundleProgressResponse response) {
            progressMonitoringInfos.set(response.getMonitoringInfosList());
            MetricsDoFn.ALLOW_COMPLETION.get(metricsDoFn.uuid).countDown();
            List<Matcher<MonitoringInfo>> matchers = new ArrayList<>();

            // We expect all user counters except for the ones in @FinishBundle
            // Since non-user metrics are registered at bundle creation time, they will still report
            // values most of which will be 0.

            SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.PROCESS_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64SumValue(1);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(MonitoringInfoConstants.Labels.NAME, MetricsDoFn.START_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64SumValue(10);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.FINISH_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            matchers.add(not(MonitoringInfoMatchers.matchSetFields(builder.build())));

            // User Distributions.
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME,
                    MetricsDoFn.PROCESS_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64DistributionValue(DistributionData.create(1, 1, 1, 1));
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.START_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64DistributionValue(DistributionData.create(10, 1, 10, 10));
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.FINISH_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            matchers.add(not(MonitoringInfoMatchers.matchSetFields(builder.build())));

            assertThat(
                response.getMonitoringInfosList(),
                Matchers.hasItems(matchers.toArray(new Matcher[0])));
          }

          @Override
          public void onCompleted(ProcessBundleResponse response) {
            List<Matcher<MonitoringInfo>> matchers = new ArrayList<>();
            // User Counters.
            SimpleMonitoringInfoBuilder builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.PROCESS_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64SumValue(1);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(MonitoringInfoConstants.Labels.NAME, MetricsDoFn.START_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64SumValue(10);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_SUM_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.FINISH_USER_COUNTER_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64SumValue(100);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            // User Distributions.
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME,
                    MetricsDoFn.PROCESS_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64DistributionValue(DistributionData.create(1, 1, 1, 1));
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.START_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64DistributionValue(DistributionData.create(10, 1, 10, 10));
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder
                .setUrn(MonitoringInfoConstants.Urns.USER_DISTRIBUTION_INT64)
                .setLabel(
                    MonitoringInfoConstants.Labels.NAMESPACE, RemoteExecutionTest.class.getName())
                .setLabel(
                    MonitoringInfoConstants.Labels.NAME, MetricsDoFn.FINISH_USER_DISTRIBUTION_NAME);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            builder.setInt64DistributionValue(DistributionData.create(100, 1, 100, 100));
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            // The element counter should be counted only once for the pcollection.
            // So there should be only two elements.
            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
            builder.setLabel(MonitoringInfoConstants.Labels.PCOLLECTION, "impulse.out");
            builder.setInt64SumValue(1);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
            builder.setLabel(
                MonitoringInfoConstants.Labels.PCOLLECTION, "create/ParMultiDo(Metrics).output");
            builder.setInt64SumValue(3);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            // Verify that the element count is not double counted if two PCollections consume it.
            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
            builder.setLabel(
                MonitoringInfoConstants.Labels.PCOLLECTION,
                "processA/ParMultiDo(Anonymous).output");
            builder.setInt64SumValue(6);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(MonitoringInfoConstants.Urns.ELEMENT_COUNT);
            builder.setLabel(
                MonitoringInfoConstants.Labels.PCOLLECTION,
                "processB/ParMultiDo(Anonymous).output");
            builder.setInt64SumValue(6);
            matchers.add(MonitoringInfoMatchers.matchSetFields(builder.build()));

            // Check for execution time metrics for the testPTransformId
            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(MonitoringInfoConstants.Urns.START_BUNDLE_MSECS);
            builder.setType(TypeUrns.SUM_INT64_TYPE);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            matchers.add(
                allOf(
                    MonitoringInfoMatchers.matchSetFields(builder.build()),
                    MonitoringInfoMatchers.counterValueGreaterThanOrEqualTo(1)));

            // Check for execution time metrics for the testPTransformId
            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(Urns.PROCESS_BUNDLE_MSECS);
            builder.setType(TypeUrns.SUM_INT64_TYPE);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            matchers.add(
                allOf(
                    MonitoringInfoMatchers.matchSetFields(builder.build()),
                    MonitoringInfoMatchers.counterValueGreaterThanOrEqualTo(1)));

            builder = new SimpleMonitoringInfoBuilder();
            builder.setUrn(Urns.FINISH_BUNDLE_MSECS);
            builder.setType(TypeUrns.SUM_INT64_TYPE);
            builder.setLabel(MonitoringInfoConstants.Labels.PTRANSFORM, testPTransformId);
            matchers.add(
                allOf(
                    MonitoringInfoMatchers.matchSetFields(builder.build()),
                    MonitoringInfoMatchers.counterValueGreaterThanOrEqualTo(1)));

            List<MonitoringInfo> oldMonitoringInfos = progressMonitoringInfos.get();
            if (oldMonitoringInfos == null) {
              throw new IllegalStateException(
                  "Progress request did not complete before timeout allowing for bundle to complete.");
            }
            List<MonitoringInfo> mergedMonitoringInfos =
                mergeMonitoringInfos(oldMonitoringInfos, response.getMonitoringInfosList());

            assertThat(mergedMonitoringInfos, Matchers.hasItems(matchers.toArray(new Matcher[0])));
          }
        };

    ExecutorService executor = Executors.newSingleThreadExecutor();

    try (RemoteBundle bundle =
        processor.newBundle(outputReceivers, StateRequestHandler.unsupported(), progressHandler)) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "X")));

      executor.submit(
          () -> {
            checkState(
                MetricsDoFn.AFTER_PROCESS.get(metricsDoFn.uuid).await(60, TimeUnit.SECONDS),
                "Runner waited too long for DoFn to get to AFTER_PROCESS.");
            bundle.requestProgress();
            return (Void) null;
          });
    }
    executor.shutdown();
  }

  private static List<MonitoringInfo> mergeMonitoringInfos(
      List<MonitoringInfo> oldMonitoringInfos, List<MonitoringInfo> newMonitoringInfos) {
    Map<MonitoringInfo, MonitoringInfo> miKeyToMiWithPayload = new HashMap<>();
    for (MonitoringInfo monitoringInfo : oldMonitoringInfos) {
      miKeyToMiWithPayload.put(monitoringInfo.toBuilder().clearPayload().build(), monitoringInfo);
    }
    for (MonitoringInfo monitoringInfo : newMonitoringInfos) {
      miKeyToMiWithPayload.put(monitoringInfo.toBuilder().clearPayload().build(), monitoringInfo);
    }
    return new ArrayList<>(miKeyToMiWithPayload.values());
  }

  @Test
  public void testExecutionWithUserState() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();
    final String stateId = "foo";
    final String stateId2 = "foo2";

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
                    state.add(element.getValue());
                    state2.clear();
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
    StateRequestHandler stateRequestHandler =
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
            });

    try (RemoteBundle bundle =
        processor.newBundle(
            outputReceivers, stateRequestHandler, BundleProgressHandler.ignored())) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(valueInGlobalWindow(KV.of("X", "Y")));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("X", "A")),
              valueInGlobalWindow(KV.of("X", "B")),
              valueInGlobalWindow(KV.of("X", "C"))));
    }
    assertThat(
        userStateData.get(stateId),
        IsIterableContainingInOrder.contains(
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "A", Coder.Context.NESTED)),
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "B", Coder.Context.NESTED)),
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "C", Coder.Context.NESTED)),
            ByteString.copyFrom(
                CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "Y", Coder.Context.NESTED))));
    assertThat(userStateData.get(stateId2), IsEmptyIterable.emptyIterable());
  }

  @Test
  public void testExecutionWithUserStateCaching() throws Exception {
    Pipeline p = Pipeline.create();
    launchSdkHarness(p.getOptions());

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
    ByteStringOutputStream out = new ByteStringOutputStream();
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

  @Test
  public void testExecutionWithTimer() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();

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
            "timer",
            ParDo.of(
                new DoFn<KV<String, String>, KV<String, String>>() {

                  @TimerId("event")
                  private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                  @TimerId("processing")
                  private final TimerSpec processingTimerSpec =
                      TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

                  @ProcessElement
                  public void processElement(
                      ProcessContext context,
                      @TimerId("event") Timer eventTimeTimer,
                      @TimerId("processing") Timer processingTimeTimer) {
                    context.output(KV.of("main" + context.element().getKey(), ""));
                    eventTimeTimer
                        .withOutputTimestamp(context.timestamp())
                        .set(context.timestamp().plus(Duration.millis(1L)));
                    processingTimeTimer.offset(Duration.millis(2L));
                    processingTimeTimer.setRelative();
                  }

                  @OnTimer("event")
                  public void eventTimer(
                      OnTimerContext context,
                      @Key String key,
                      @TimerId("event") Timer eventTimeTimer,
                      @TimerId("processing") Timer processingTimeTimer) {
                    context.output(KV.of("event", key));
                    eventTimeTimer
                        .withOutputTimestamp(context.timestamp())
                        .set(context.fireTimestamp().plus(Duration.millis(11L)));
                    processingTimeTimer.offset(Duration.millis(12L));
                    processingTimeTimer.setRelative();
                  }

                  @OnTimer("processing")
                  public void processingTimer(
                      OnTimerContext context,
                      @Key String key,
                      @TimerId("event") Timer eventTimeTimer,
                      @TimerId("processing") Timer processingTimeTimer) {
                    context.output(KV.of("processing", key));
                    eventTimeTimer
                        .withOutputTimestamp(context.timestamp())
                        .set(context.fireTimestamp().plus(Duration.millis(21L)));
                    processingTimeTimer.offset(Duration.millis(22L));
                    processingTimeTimer.setRelative();
                  }

                  @OnWindowExpiration
                  public void onWindowExpiration(
                      @Key String key, OutputReceiver<KV<String, String>> outputReceiver) {
                    outputReceiver.output(KV.of("onWindowExpiration", key));
                  }
                }))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(), (ExecutableStage stage) -> !stage.getTimers().isEmpty());
    checkState(optionalStage.isPresent(), "Expected a stage with timers.");
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
            stateDelegator,
            descriptor.getTimerSpecs());
    Map<String, Collection<WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, Coder> remoteOutputCoder : descriptor.getRemoteOutputCoders().entrySet()) {
      List<WindowedValue<?>> outputContents = Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder<WindowedValue<?>>) remoteOutputCoder.getValue(), outputContents::add));
    }
    Map<KV<String, String>, Collection<org.apache.beam.sdk.util.construction.Timer<?>>>
        timerValues = new HashMap<>();
    Map<KV<String, String>, RemoteOutputReceiver<org.apache.beam.sdk.util.construction.Timer<?>>>
        timerReceivers = new HashMap<>();
    for (Map.Entry<String, Map<String, ProcessBundleDescriptors.TimerSpec>> transformTimerSpecs :
        descriptor.getTimerSpecs().entrySet()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : transformTimerSpecs.getValue().values()) {
        KV<String, String> key = KV.of(timerSpec.transformId(), timerSpec.timerId());
        List<org.apache.beam.sdk.util.construction.Timer<?>> outputContents =
            Collections.synchronizedList(new ArrayList<>());
        timerValues.put(key, outputContents);
        timerReceivers.put(
            key,
            RemoteOutputReceiver.of(
                (Coder<org.apache.beam.sdk.util.construction.Timer<?>>) timerSpec.coder(),
                outputContents::add));
      }
    }

    ProcessBundleDescriptors.TimerSpec eventTimerSpec = null;
    ProcessBundleDescriptors.TimerSpec processingTimerSpec = null;
    ProcessBundleDescriptors.TimerSpec onWindowExpirationSpec = null;
    for (Map<String, ProcessBundleDescriptors.TimerSpec> timerSpecs :
        descriptor.getTimerSpecs().values()) {
      for (ProcessBundleDescriptors.TimerSpec timerSpec : timerSpecs.values()) {
        if ("onWindowExpiration0".equals(timerSpec.timerId())) {
          onWindowExpirationSpec = timerSpec;
        } else if (TimeDomain.EVENT_TIME.equals(timerSpec.getTimerSpec().getTimeDomain())) {
          eventTimerSpec = timerSpec;
        } else if (TimeDomain.PROCESSING_TIME.equals(timerSpec.getTimerSpec().getTimeDomain())) {
          processingTimerSpec = timerSpec;
        } else {
          fail(String.format("Unknown timer specification %s", timerSpec));
        }
      }
    }

    // Set the current system time to a fixed value to get stable values for processing time timer
    // output.
    DateTimeUtils.setCurrentMillisFixed(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis() + 10000L);
    try {
      try (RemoteBundle bundle =
          processor.newBundle(
              outputReceivers,
              timerReceivers,
              StateRequestHandler.unsupported(),
              BundleProgressHandler.ignored(),
              null,
              null)) {
        Iterables.getOnlyElement(bundle.getInputReceivers().values())
            .accept(valueInGlobalWindow(KV.of("X", "X")));
        bundle
            .getTimerReceivers()
            .get(KV.of(eventTimerSpec.transformId(), eventTimerSpec.timerId()))
            .accept(timerForTest("Y", 1000L, 100L));
        bundle
            .getTimerReceivers()
            .get(KV.of(processingTimerSpec.transformId(), processingTimerSpec.timerId()))
            .accept(timerForTest("Z", 2000L, 200L));
        bundle
            .getTimerReceivers()
            .get(KV.of(onWindowExpirationSpec.transformId(), onWindowExpirationSpec.timerId()))
            // Normally fireTimestamp and holdTimestamp would be the same in window expirations but
            // we specifically set them to different values to ensure that they are used correctly.
            .accept(timerForTest("key", 5001L, 5000L));
      }
      String mainOutputTransform =
          Iterables.getOnlyElement(descriptor.getRemoteOutputCoders().keySet());
      assertThat(
          outputValues.get(mainOutputTransform),
          containsInAnyOrder(
              valueInGlobalWindow(KV.of("mainX", "")),
              WindowedValue.timestampedValueInGlobalWindow(
                  KV.of("event", "Y"),
                  BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(100L))),
              WindowedValue.timestampedValueInGlobalWindow(
                  KV.of("processing", "Z"),
                  BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(200L))),
              WindowedValue.timestampedValueInGlobalWindow(
                  KV.of("onWindowExpiration", "key"),
                  BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(5000L)))));
      assertThat(
          timerValues.get(KV.of(eventTimerSpec.transformId(), eventTimerSpec.timerId())),
          containsInAnyOrder(
              timerForTest("X", 1L, 0L),
              timerForTest("Y", 1011L, 100L),
              timerForTest("Z", 2021L, 200L)));
      assertThat(
          timerValues.get(KV.of(processingTimerSpec.transformId(), processingTimerSpec.timerId())),
          containsInAnyOrder(
              timerForTest("X", 10002L, 0L),
              timerForTest("Y", 10012L, 100L),
              timerForTest("Z", 10022L, 200L)));
    } finally {
      DateTimeUtils.setCurrentMillisSystem();
    }
  }

  @Test
  public void testExecutionWithMultipleStages() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();

    Function<String, PCollection<String>> pCollectionGenerator =
        suffix ->
            p.apply("impulse" + suffix, Impulse.create())
                .apply(
                    "create" + suffix,
                    ParDo.of(
                        new DoFn<byte[], String>() {
                          @ProcessElement
                          public void process(ProcessContext c) {
                            try {
                              c.output(
                                  CoderUtils.decodeFromByteArray(
                                      StringUtf8Coder.of(), c.element()));
                            } catch (CoderException e) {
                              throw new RuntimeException(e);
                            }
                          }
                        }))
                .setCoder(StringUtf8Coder.of())
                .apply(
                    ParDo.of(
                        new DoFn<String, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            c.output("stream" + suffix + c.element());
                          }
                        }));
    PCollection<String> input1 = pCollectionGenerator.apply("1");
    PCollection<String> input2 = pCollectionGenerator.apply("2");

    PCollection<String> outputMerged =
        PCollectionList.of(input1).and(input2).apply(Flatten.pCollections());
    outputMerged
        .apply(
            "createKV",
            ParDo.of(
                new DoFn<String, KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    c.output(KV.of(c.element(), ""));
                  }
                }))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineProto);
    Set<ExecutableStage> stages = fused.getFusedStages();

    assertThat(stages.size(), equalTo(2));

    List<WindowedValue<?>> outputValues = Collections.synchronizedList(new ArrayList<>());

    for (ExecutableStage stage : stages) {
      ExecutableProcessBundleDescriptor descriptor =
          ProcessBundleDescriptors.fromExecutableStage(
              stage.toString(),
              stage,
              dataServer.getApiServiceDescriptor(),
              stateServer.getApiServiceDescriptor());

      BundleProcessor processor =
          controlClient.getProcessor(
              descriptor.getProcessBundleDescriptor(),
              descriptor.getRemoteInputDestinations(),
              stateDelegator);
      Map<String, Coder> remoteOutputCoders = descriptor.getRemoteOutputCoders();
      Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
      for (Entry<String, Coder> remoteOutputCoder : remoteOutputCoders.entrySet()) {
        outputReceivers.putIfAbsent(
            remoteOutputCoder.getKey(),
            RemoteOutputReceiver.of(
                (Coder<WindowedValue<?>>) remoteOutputCoder.getValue(), outputValues::add));
      }

      try (RemoteBundle bundle =
          processor.newBundle(
              outputReceivers,
              StateRequestHandler.unsupported(),
              BundleProgressHandler.ignored())) {
        Iterables.getOnlyElement(bundle.getInputReceivers().values())
            .accept(valueInGlobalWindow(CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "X")));
      }
    }
    assertThat(
        outputValues,
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("stream1X", "")),
            valueInGlobalWindow(KV.of("stream2X", ""))));
  }

  /**
   * A restriction tracker that will block making progress on {@link #WAIT_TILL_SPLIT} until a try
   * split is invoked.
   */
  private static class WaitingTillSplitRestrictionTracker extends RestrictionTracker<String, Void> {
    private static final String WAIT_TILL_SPLIT = "WaitTillSplit";
    private static final String PRIMARY = "Primary";
    private static final String RESIDUAL = "Residual";

    private String currentRestriction;

    private WaitingTillSplitRestrictionTracker(String restriction) {
      this.currentRestriction = restriction;
    }

    @Override
    public boolean tryClaim(Void position) {
      return needsSplitting();
    }

    @Override
    public String currentRestriction() {
      return currentRestriction;
    }

    @Override
    public SplitResult<String> trySplit(double fractionOfRemainder) {
      if (!needsSplitting()) {
        return null;
      }
      this.currentRestriction = PRIMARY;
      return SplitResult.of(currentRestriction, RESIDUAL);
    }

    private boolean needsSplitting() {
      return WAIT_TILL_SPLIT.equals(currentRestriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {
      checkState(!needsSplitting(), "Expected for this restriction to have been split.");
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  @Test(timeout = 60000L)
  public void testSplit() throws Exception {
    launchSdkHarness(PipelineOptionsFactory.create());
    Pipeline p = Pipeline.create();
    p.apply("impulse", Impulse.create())
        .apply(
            "create",
            ParDo.of(
                new DoFn<byte[], String>() {
                  @ProcessElement
                  public void process(ProcessContext ctxt) {
                    ctxt.output("zero");
                    ctxt.output(WaitingTillSplitRestrictionTracker.WAIT_TILL_SPLIT);
                    ctxt.output("two");
                  }
                }))
        .apply(
            "forceSplit",
            ParDo.of(
                new DoFn<String, String>() {
                  @GetInitialRestriction
                  public String getInitialRestriction(@Element String element) {
                    return element;
                  }

                  @NewTracker
                  public WaitingTillSplitRestrictionTracker newTracker(
                      @Restriction String restriction) {
                    return new WaitingTillSplitRestrictionTracker(restriction);
                  }

                  @ProcessElement
                  public void process(
                      RestrictionTracker<String, Void> tracker, ProcessContext context) {
                    while (tracker.tryClaim(null)) {}
                    context.output(tracker.currentRestriction());
                  }
                }))
        .apply("addKeys", WithKeys.of("foo"))
        // Use some unknown coders
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
        // Force the output to be materialized
        .apply("gbk", GroupByKey.create());

    RunnerApi.Pipeline pipeline = PipelineTranslation.toProto(p);
    // Expand any splittable DoFns within the graph to enable sizing and splitting of bundles.
    RunnerApi.Pipeline pipelineWithSdfExpanded =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            pipeline,
            SplittableParDoExpander.createSizedReplacement());
    FusedPipeline fused = GreedyPipelineFuser.fuse(pipelineWithSdfExpanded);

    // Find the fused stage with the SDF ProcessSizedElementAndRestriction transform
    Optional<ExecutableStage> optionalStage =
        Iterables.tryFind(
            fused.getFusedStages(),
            (ExecutableStage stage) ->
                Iterables.filter(
                        stage.getTransforms(),
                        (PTransformNode node) ->
                            PTransformTranslation
                                .SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN
                                .equals(node.getTransform().getSpec().getUrn()))
                    .iterator()
                    .hasNext());
    checkState(
        optionalStage.isPresent(), "Expected a stage with SDF ProcessSizedElementAndRestriction.");
    ExecutableStage stage = optionalStage.get();

    ExecutableProcessBundleDescriptor descriptor =
        ProcessBundleDescriptors.fromExecutableStage(
            "my_stage", stage, dataServer.getApiServiceDescriptor());

    BundleProcessor processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(), descriptor.getRemoteInputDestinations());
    Map<String, ? super Coder<WindowedValue<?>>> remoteOutputCoders =
        descriptor.getRemoteOutputCoders();
    Map<String, Collection<? super WindowedValue<?>>> outputValues = new HashMap<>();
    Map<String, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<String, ? super Coder<WindowedValue<?>>> remoteOutputCoder :
        remoteOutputCoders.entrySet()) {
      List<? super WindowedValue<?>> outputContents =
          Collections.synchronizedList(new ArrayList<>());
      outputValues.put(remoteOutputCoder.getKey(), outputContents);
      outputReceivers.put(
          remoteOutputCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) remoteOutputCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>) outputContents::add));
    }

    List<ProcessBundleSplitResponse> splitResponses = new ArrayList<>();
    List<ProcessBundleResponse> checkpointResponses = new ArrayList<>();
    List<String> requestsFinalization = new ArrayList<>();

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<Object> future;

    // Execute the remote bundle.
    try (RemoteBundle bundle =
        processor.newBundle(
            outputReceivers,
            Collections.emptyMap(),
            StateRequestHandler.unsupported(),
            BundleProgressHandler.ignored(),
            splitResponses::add,
            checkpointResponses::add,
            requestsFinalization::add)) {
      Iterables.getOnlyElement(bundle.getInputReceivers().values())
          .accept(
              valueInGlobalWindow(
                  sdfSizedElementAndRestrictionForTest(
                      WaitingTillSplitRestrictionTracker.WAIT_TILL_SPLIT)));
      // Keep sending splits until the bundle terminates.
      future =
          (ScheduledFuture)
              executor.scheduleWithFixedDelay(
                  () -> bundle.split(0.5), 0L, 100L, TimeUnit.MILLISECONDS);
    }
    future.cancel(false);
    executor.shutdown();

    assertTrue(requestsFinalization.isEmpty());
    assertTrue(checkpointResponses.isEmpty());

    // We only validate the last split response since it is the only one that could possibly
    // contain the SDF split, all others will be a reduction in the ChannelSplit range.
    assertFalse(splitResponses.isEmpty());
    ProcessBundleSplitResponse splitResponse = splitResponses.get(splitResponses.size() - 1);
    ChannelSplit channelSplit = Iterables.getOnlyElement(splitResponse.getChannelSplitsList());

    // There is only one outcome for the final split that can happen since the SDF is blocking the
    // bundle from completing and hence needed to be split.
    assertEquals(-1L, channelSplit.getLastPrimaryElement());
    assertEquals(1L, channelSplit.getFirstResidualElement());
    assertEquals(1, splitResponse.getPrimaryRootsCount());
    assertEquals(1, splitResponse.getResidualRootsCount());
    assertThat(
        Iterables.getOnlyElement(outputValues.values()),
        containsInAnyOrder(
            valueInGlobalWindow(KV.of("foo", WaitingTillSplitRestrictionTracker.PRIMARY))));
  }

  /**
   * The SDF ProcessSizedElementAndRestriction expansion expects {@code KV<KV<Element, Restriction>,
   * Size>} where {@code Restriction} in Java SDFs is represented as {@code KV<Restriction,
   * WatermarkEstimatorState>} and the default {@code WatermarkEstimatorState} is {@code Void} which
   * always encodes to an empty byte array.
   */
  private KV<KV<String, KV<String, byte[]>>, Double> sdfSizedElementAndRestrictionForTest(
      String element) {
    return KV.of(KV.of(element, KV.of(element, new byte[0])), 0.0);
  }

  private KV<String, byte[]> byteValueOf(String key, long value) throws CoderException {
    return KV.of(key, CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), value));
  }

  private org.apache.beam.sdk.util.construction.Timer<String> timerForTest(
      String key, long fireTimestamp, long holdTimestamp) {
    return org.apache.beam.sdk.util.construction.Timer.of(
        key,
        "",
        Collections.singletonList(GlobalWindow.INSTANCE),
        BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(fireTimestamp)),
        BoundedWindow.TIMESTAMP_MIN_VALUE.plus(Duration.millis(holdTimestamp)),
        PaneInfo.NO_FIRING);
  }
}
