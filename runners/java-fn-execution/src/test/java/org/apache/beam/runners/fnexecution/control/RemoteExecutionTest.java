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

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.apache.beam.fn.harness.FnHarness;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.FusedPipeline;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.ExecutableProcessBundleDescriptor;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.ActiveBundle;
import org.apache.beam.runners.fnexecution.control.SdkHarnessClient.BundleProcessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.logging.GrpcLoggingService;
import org.apache.beam.runners.fnexecution.logging.Slf4jLogWriter;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandler;
import org.apache.beam.runners.fnexecution.state.StateRequestHandlers.SideInputHandlerFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the execution of a pipeline from specification time to executing a single fused stage,
 * going through pipeline fusion.
 */
@RunWith(JUnit4.class)
public class RemoteExecutionTest implements Serializable {
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
            GrpcDataService.create(serverExecutor, OutboundObserverFactory.serverDirect()),
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

    // Create the SDK harness, and wait until it connects
    sdkHarnessExecutor = Executors.newSingleThreadExecutor(threadFactory);
    sdkHarnessExecutorFuture =
        sdkHarnessExecutor.submit(
            () -> {
              try {
                FnHarness.main(
                    "id",
                    PipelineOptionsFactory.create(),
                    loggingServer.getApiServiceDescriptor(),
                    controlServer.getApiServiceDescriptor(),
                    InProcessManagedChannelFactory.create(),
                    OutboundObserverFactory.clientDirect());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    // TODO: https://issues.apache.org/jira/browse/BEAM-4149 Use proper worker id.
    InstructionRequestHandler controlClient =
        clientPool.getSource().take("", Duration.ofSeconds(2));
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
  public void testExecution() throws Exception {
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
    // TODO: This cast is nonsense
    RemoteInputDestination<WindowedValue<byte[]>> remoteDestination =
        (RemoteInputDestination<WindowedValue<byte[]>>)
            (RemoteInputDestination) descriptor.getRemoteInputDestination();

    BundleProcessor<byte[]> processor =
        controlClient.getProcessor(descriptor.getProcessBundleDescriptor(), remoteDestination);
    Map<Target, ? super Coder<WindowedValue<?>>> outputTargets = descriptor.getOutputTargetCoders();
    Map<Target, Collection<? super WindowedValue<?>>> outputValues = new HashMap<>();
    Map<Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<Target, ? super Coder<WindowedValue<?>>> targetCoder : outputTargets.entrySet()) {
      List<? super WindowedValue<?>> outputContents =
          Collections.synchronizedList(new ArrayList<>());
      outputValues.put(targetCoder.getKey(), outputContents);
      outputReceivers.put(
          targetCoder.getKey(),
          RemoteOutputReceiver.of(
              (Coder) targetCoder.getValue(),
              (FnDataReceiver<? super WindowedValue<?>>) outputContents::add));
    }
    // The impulse example

    try (ActiveBundle<byte[]> bundle =
        processor.newBundle(outputReceivers, BundleProgressHandler.unsupported())) {
      bundle.getInputReceiver().accept(WindowedValue.valueInGlobalWindow(new byte[0]));
    }

    for (Collection<? super WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              WindowedValue.valueInGlobalWindow(kvBytes("foo", 4)),
              WindowedValue.valueInGlobalWindow(kvBytes("foo", 3)),
              WindowedValue.valueInGlobalWindow(kvBytes("foo", 3))));
    }
  }

  @Test
  public void testExecutionWithSideInput() throws Exception {
    Pipeline p = Pipeline.create();
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
    // TODO: This cast is nonsense
    RemoteInputDestination<WindowedValue<byte[]>> remoteDestination =
        (RemoteInputDestination<WindowedValue<byte[]>>)
            (RemoteInputDestination) descriptor.getRemoteInputDestination();

    BundleProcessor<byte[]> processor =
        controlClient.getProcessor(
            descriptor.getProcessBundleDescriptor(), remoteDestination, stateDelegator);
    Map<Target, Coder<WindowedValue<?>>> outputTargets = descriptor.getOutputTargetCoders();
    Map<Target, Collection<WindowedValue<?>>> outputValues = new HashMap<>();
    Map<Target, RemoteOutputReceiver<?>> outputReceivers = new HashMap<>();
    for (Entry<Target, Coder<WindowedValue<?>>> targetCoder : outputTargets.entrySet()) {
      List<WindowedValue<?>> outputContents = Collections.synchronizedList(new ArrayList<>());
      outputValues.put(targetCoder.getKey(), outputContents);
      outputReceivers.put(
          targetCoder.getKey(),
          RemoteOutputReceiver.of(targetCoder.getValue(), outputContents::add));
    }

    Iterable<byte[]> sideInputData =
        Arrays.asList(
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "A"),
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "B"),
            CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "C"));
    StateRequestHandler stateRequestHandler =
        StateRequestHandlers.forSideInputHandlerFactory(
            descriptor.getSideInputSpecs(),
            new SideInputHandlerFactory() {
              @Override
              public <T, V, W extends BoundedWindow> SideInputHandler<V, W> forSideInput(
                  String pTransformId,
                  String sideInputId,
                  RunnerApi.FunctionSpec accessPattern,
                  Coder<T> elementCoder,
                  Coder<W> windowCoder) {
                return new SideInputHandler<V, W>() {
                  @Override
                  public Iterable<V> get(byte[] key, W window) {
                    return (Iterable) sideInputData;
                  }

                  @Override
                  public Coder<V> resultCoder() {
                    return ((KvCoder) elementCoder).getValueCoder();
                  }
                };
              }
            });
    BundleProgressHandler progressHandler = BundleProgressHandler.unsupported();

    try (ActiveBundle<byte[]> bundle =
        processor.newBundle(outputReceivers, stateRequestHandler, progressHandler)) {
      bundle
          .getInputReceiver()
          .accept(
              WindowedValue.valueInGlobalWindow(
                  CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "X")));
      bundle
          .getInputReceiver()
          .accept(
              WindowedValue.valueInGlobalWindow(
                  CoderUtils.encodeToByteArray(StringUtf8Coder.of(), "Y")));
    }
    for (Collection<WindowedValue<?>> windowedValues : outputValues.values()) {
      assertThat(
          windowedValues,
          containsInAnyOrder(
              WindowedValue.valueInGlobalWindow(kvBytes("X", "A")),
              WindowedValue.valueInGlobalWindow(kvBytes("X", "B")),
              WindowedValue.valueInGlobalWindow(kvBytes("X", "C")),
              WindowedValue.valueInGlobalWindow(kvBytes("Y", "A")),
              WindowedValue.valueInGlobalWindow(kvBytes("Y", "B")),
              WindowedValue.valueInGlobalWindow(kvBytes("Y", "C"))));
    }
  }

  private KV<byte[], byte[]> kvBytes(String key, long value) throws CoderException {
    return KV.of(
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), key),
        CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), value));
  }

  private KV<byte[], byte[]> kvBytes(String key, String value) throws CoderException {
    return KV.of(
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), key),
        CoderUtils.encodeToByteArray(StringUtf8Coder.of(), value));
  }
}
