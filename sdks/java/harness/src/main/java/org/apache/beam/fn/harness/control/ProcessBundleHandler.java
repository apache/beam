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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.BeamFnDataReadRunner;
import org.apache.beam.fn.harness.Cache;
import org.apache.beam.fn.harness.Caches;
import org.apache.beam.fn.harness.Caches.ClearableCache;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Context;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.debug.DataSampler;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest.CacheToken;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.StandardRunnerProtocols;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Urns;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.DataEndpoint;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.TimerEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsEnvironment.MetricsEnvironmentState;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.SetMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes {@link BeamFnApi.ProcessBundleRequest}s and {@link
 * BeamFnApi.ProcessBundleSplitRequest}s.
 *
 * <p>{@link BeamFnApi.ProcessBundleSplitRequest}s use a {@link BundleProcessorCache cache} to
 * find/create a {@link BundleProcessor}. The creation of a {@link BundleProcessor} uses the
 * associated {@link BeamFnApi.ProcessBundleDescriptor} definition; creating runners for each {@link
 * RunnerApi.FunctionSpec}; wiring them together based upon the {@code input} and {@code output} map
 * definitions. The {@link BundleProcessor} executes the DAG based graph by starting all runners in
 * reverse topological order, and finishing all runners in forward topological order.
 *
 * <p>{@link BeamFnApi.ProcessBundleSplitRequest}s finds an {@code active} {@link BundleProcessor}
 * associated with a currently processing {@link BeamFnApi.ProcessBundleRequest} and uses it to
 * perform a split request. See <a href="https://s.apache.org/beam-breaking-fusion">breaking the
 * fusion barrier</a> for further details.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class ProcessBundleHandler {

  // TODO: What should the initial set of URNs be?
  private static final String DATA_INPUT_URN = "beam:runner:source:v1";
  private static final String DATA_OUTPUT_URN = "beam:runner:sink:v1";
  public static final String JAVA_SOURCE_URN = "beam:source:java:0.1";

  private static final Logger LOG = LoggerFactory.getLogger(ProcessBundleHandler.class);
  @VisibleForTesting static final Map<String, PTransformRunnerFactory> REGISTERED_RUNNER_FACTORIES;

  static {
    Set<Registrar> pipelineRunnerRegistrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    pipelineRunnerRegistrars.addAll(
        Lists.newArrayList(ServiceLoader.load(Registrar.class, ReflectHelpers.findClassLoader())));

    // Load all registered PTransform runner factories.
    ImmutableMap.Builder<String, PTransformRunnerFactory> builder = ImmutableMap.builder();
    for (Registrar registrar : pipelineRunnerRegistrars) {
      builder.putAll(registrar.getPTransformRunnerFactories());
    }
    REGISTERED_RUNNER_FACTORIES = builder.build();
  }

  private final PipelineOptions options;
  private final Function<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry;
  private final BeamFnDataClient beamFnDataClient;
  private final BeamFnStateGrpcClientCache beamFnStateGrpcClientCache;
  private final FinalizeBundleHandler finalizeBundleHandler;
  private final ShortIdMap shortIds;
  private final boolean runnerAcceptsShortIds;
  private final ExecutionStateSampler executionStateSampler;
  private final Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap;
  private final PTransformRunnerFactory defaultPTransformRunnerFactory;
  private final Cache<Object, Object> processWideCache;
  @VisibleForTesting final BundleProcessorCache bundleProcessorCache;
  private final Set<String> runnerCapabilities;
  private final @Nullable DataSampler dataSampler;

  public ProcessBundleHandler(
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Function<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler,
      ShortIdMap shortIds,
      ExecutionStateSampler executionStateSampler,
      Cache<Object, Object> processWideCache,
      @Nullable DataSampler dataSampler) {
    this(
        options,
        runnerCapabilities,
        fnApiRegistry,
        beamFnDataClient,
        beamFnStateGrpcClientCache,
        finalizeBundleHandler,
        shortIds,
        executionStateSampler,
        REGISTERED_RUNNER_FACTORIES,
        processWideCache,
        new BundleProcessorCache(),
        dataSampler);
  }

  @VisibleForTesting
  ProcessBundleHandler(
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Function<String, BeamFnApi.ProcessBundleDescriptor> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler,
      ShortIdMap shortIds,
      ExecutionStateSampler executionStateSampler,
      Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap,
      Cache<Object, Object> processWideCache,
      BundleProcessorCache bundleProcessorCache,
      @Nullable DataSampler dataSampler) {
    this.options = options;
    this.fnApiRegistry = fnApiRegistry;
    this.beamFnDataClient = beamFnDataClient;
    this.beamFnStateGrpcClientCache = beamFnStateGrpcClientCache;
    this.finalizeBundleHandler = finalizeBundleHandler;
    this.shortIds = shortIds;
    this.runnerCapabilities = runnerCapabilities;
    this.runnerAcceptsShortIds =
        runnerCapabilities.contains(
            BeamUrns.getUrn(RunnerApi.StandardRunnerProtocols.Enum.MONITORING_INFO_SHORT_IDS));
    this.executionStateSampler = executionStateSampler;
    this.urnToPTransformRunnerFactoryMap = urnToPTransformRunnerFactoryMap;
    this.defaultPTransformRunnerFactory =
        new UnknownPTransformRunnerFactory(urnToPTransformRunnerFactoryMap.keySet());
    this.processWideCache = processWideCache;
    this.bundleProcessorCache = bundleProcessorCache;
    this.dataSampler = dataSampler;
  }

  private void createRunnerAndConsumersForPTransformRecursively(
      BeamFnStateClient beamFnStateClient,
      BeamFnDataClient queueingClient,
      String pTransformId,
      PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      Supplier<List<BeamFnApi.ProcessBundleRequest.CacheToken>> cacheTokens,
      Supplier<Cache<?, ?>> bundleCache,
      ProcessBundleDescriptor processBundleDescriptor,
      SetMultimap<String, String> pCollectionIdsToConsumingPTransforms,
      PCollectionConsumerRegistry pCollectionConsumerRegistry,
      Set<String> processedPTransformIds,
      PTransformFunctionRegistry startFunctionRegistry,
      PTransformFunctionRegistry finishFunctionRegistry,
      Consumer<ThrowingRunnable> addResetFunction,
      Consumer<ThrowingRunnable> addTearDownFunction,
      BiConsumer<Endpoints.ApiServiceDescriptor, DataEndpoint<?>> addDataEndpoint,
      Consumer<TimerEndpoint<?>> addTimerEndpoint,
      Consumer<BundleProgressReporter> addBundleProgressReporter,
      BundleSplitListener splitListener,
      BundleFinalizer bundleFinalizer,
      Collection<BeamFnDataReadRunner> channelRoots,
      Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> outboundAggregatorMap,
      Set<String> runnerCapabilities)
      throws IOException {

    // Recursively ensure that all consumers of the output PCollection have been created.
    // Since we are creating the consumers first, we know that the we are building the DAG
    // in reverse topological order.
    for (String pCollectionId : pTransform.getOutputsMap().values()) {

      for (String consumingPTransformId : pCollectionIdsToConsumingPTransforms.get(pCollectionId)) {
        createRunnerAndConsumersForPTransformRecursively(
            beamFnStateClient,
            queueingClient,
            consumingPTransformId,
            processBundleDescriptor.getTransformsMap().get(consumingPTransformId),
            processBundleInstructionId,
            cacheTokens,
            bundleCache,
            processBundleDescriptor,
            pCollectionIdsToConsumingPTransforms,
            pCollectionConsumerRegistry,
            processedPTransformIds,
            startFunctionRegistry,
            finishFunctionRegistry,
            addResetFunction,
            addTearDownFunction,
            addDataEndpoint,
            addTimerEndpoint,
            addBundleProgressReporter,
            splitListener,
            bundleFinalizer,
            channelRoots,
            outboundAggregatorMap,
            runnerCapabilities);
      }
    }

    if (!pTransform.hasSpec()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot process transform with no spec: %s",
              TextFormat.printer().printToString(pTransform)));
    }

    if (pTransform.getSubtransformsCount() > 0) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot process composite transform: %s",
              TextFormat.printer().printToString(pTransform)));
    }

    // Skip reprocessing processed pTransforms.
    if (!processedPTransformIds.contains(pTransformId)) {
      Object runner =
          urnToPTransformRunnerFactoryMap
              .getOrDefault(pTransform.getSpec().getUrn(), defaultPTransformRunnerFactory)
              .createRunnerForPTransform(
                  new Context() {
                    @Override
                    public PipelineOptions getPipelineOptions() {
                      return options;
                    }

                    @Override
                    public ShortIdMap getShortIdMap() {
                      return shortIds;
                    }

                    @Override
                    public BeamFnDataClient getBeamFnDataClient() {
                      return queueingClient;
                    }

                    @Override
                    public BeamFnStateClient getBeamFnStateClient() {
                      return beamFnStateClient;
                    }

                    @Override
                    public String getPTransformId() {
                      return pTransformId;
                    }

                    @Override
                    public PTransform getPTransform() {
                      return pTransform;
                    }

                    @Override
                    public Supplier<String> getProcessBundleInstructionIdSupplier() {
                      return processBundleInstructionId;
                    }

                    @Override
                    public Supplier<List<CacheToken>> getCacheTokensSupplier() {
                      return cacheTokens;
                    }

                    @Override
                    public Supplier<Cache<?, ?>> getBundleCacheSupplier() {
                      return bundleCache;
                    }

                    @Override
                    public Cache<?, ?> getProcessWideCache() {
                      return processWideCache;
                    }

                    @Override
                    public Map<String, PCollection> getPCollections() {
                      return processBundleDescriptor.getPcollectionsMap();
                    }

                    @Override
                    public Map<String, Coder> getCoders() {
                      return processBundleDescriptor.getCodersMap();
                    }

                    @Override
                    public Map<String, WindowingStrategy> getWindowingStrategies() {
                      return processBundleDescriptor.getWindowingStrategiesMap();
                    }

                    @Override
                    public Set<String> getRunnerCapabilities() {
                      return runnerCapabilities;
                    }

                    @Override
                    public <T> void addPCollectionConsumer(
                        String pCollectionId, FnDataReceiver<WindowedValue<T>> consumer) {
                      pCollectionConsumerRegistry.register(
                          pCollectionId, pTransformId, pTransform.getUniqueName(), consumer);
                    }

                    @Override
                    public <T> FnDataReceiver<T> addOutgoingDataEndpoint(
                        ApiServiceDescriptor apiServiceDescriptor,
                        org.apache.beam.sdk.coders.Coder<T> coder) {
                      BeamFnDataOutboundAggregator aggregator =
                          outboundAggregatorMap.computeIfAbsent(
                              apiServiceDescriptor,
                              asd ->
                                  queueingClient.createOutboundAggregator(
                                      asd,
                                      processBundleInstructionId,
                                      runnerCapabilities.contains(
                                          BeamUrns.getUrn(
                                              StandardRunnerProtocols.Enum
                                                  .CONTROL_RESPONSE_ELEMENTS_EMBEDDING))));
                      return aggregator.registerOutputDataLocation(pTransformId, coder);
                    }

                    @Override
                    public <T> FnDataReceiver<Timer<T>> addOutgoingTimersEndpoint(
                        String timerFamilyId, org.apache.beam.sdk.coders.Coder<Timer<T>> coder) {
                      BeamFnDataOutboundAggregator aggregator;
                      if (!processBundleDescriptor.hasTimerApiServiceDescriptor()) {
                        throw new IllegalStateException(
                            String.format(
                                "Timers are unsupported because the "
                                    + "ProcessBundleRequest %s does not provide a timer ApiServiceDescriptor.",
                                processBundleInstructionId.get()));
                      }
                      aggregator =
                          outboundAggregatorMap.computeIfAbsent(
                              processBundleDescriptor.getTimerApiServiceDescriptor(),
                              asd ->
                                  queueingClient.createOutboundAggregator(
                                      asd,
                                      processBundleInstructionId,
                                      runnerCapabilities.contains(
                                          BeamUrns.getUrn(
                                              StandardRunnerProtocols.Enum
                                                  .CONTROL_RESPONSE_ELEMENTS_EMBEDDING))));
                      return aggregator.registerOutputTimersLocation(
                          pTransformId, timerFamilyId, coder);
                    }

                    @Override
                    public FnDataReceiver<?> getPCollectionConsumer(String pCollectionId) {
                      return pCollectionConsumerRegistry.getMultiplexingConsumer(pCollectionId);
                    }

                    @Override
                    public void addStartBundleFunction(ThrowingRunnable startFunction) {
                      startFunctionRegistry.register(
                          pTransformId, pTransform.getUniqueName(), startFunction);
                    }

                    @Override
                    public void addFinishBundleFunction(ThrowingRunnable finishFunction) {
                      finishFunctionRegistry.register(
                          pTransformId, pTransform.getUniqueName(), finishFunction);
                    }

                    @Override
                    public <T> void addIncomingDataEndpoint(
                        ApiServiceDescriptor apiServiceDescriptor,
                        org.apache.beam.sdk.coders.Coder<T> coder,
                        FnDataReceiver<T> receiver) {
                      addDataEndpoint.accept(
                          apiServiceDescriptor, DataEndpoint.create(pTransformId, coder, receiver));
                    }

                    @Override
                    public <T> void addIncomingTimerEndpoint(
                        String timerFamilyId,
                        org.apache.beam.sdk.coders.Coder<Timer<T>> coder,
                        FnDataReceiver<Timer<T>> receiver) {
                      addTimerEndpoint.accept(
                          TimerEndpoint.create(pTransformId, timerFamilyId, coder, receiver));
                    }

                    @Override
                    public void addResetFunction(ThrowingRunnable resetFunction) {
                      addResetFunction.accept(resetFunction);
                    }

                    @Override
                    public void addTearDownFunction(ThrowingRunnable tearDownFunction) {
                      addTearDownFunction.accept(tearDownFunction);
                    }

                    @Override
                    public void addBundleProgressReporter(
                        BundleProgressReporter bundleProgressReporter) {
                      addBundleProgressReporter.accept(bundleProgressReporter);
                    }

                    @Override
                    public BundleSplitListener getSplitListener() {
                      return splitListener;
                    }

                    @Override
                    public BundleFinalizer getBundleFinalizer() {
                      return bundleFinalizer;
                    }
                  });
      if (runner instanceof BeamFnDataReadRunner) {
        channelRoots.add((BeamFnDataReadRunner) runner);
      }
      processedPTransformIds.add(pTransformId);
    }
  }

  /**
   * Processes a bundle, running the start(), process(), and finish() functions. This function is
   * required to be reentrant.
   */
  public BeamFnApi.InstructionResponse.Builder processBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    BeamFnApi.ProcessBundleResponse.Builder response = BeamFnApi.ProcessBundleResponse.newBuilder();

    BundleProcessor bundleProcessor =
        bundleProcessorCache.get(
            request,
            () -> {
              try {
                return createBundleProcessor(
                    request.getProcessBundle().getProcessBundleDescriptorId(),
                    request.getProcessBundle());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    try {
      PTransformFunctionRegistry startFunctionRegistry = bundleProcessor.getStartFunctionRegistry();
      PTransformFunctionRegistry finishFunctionRegistry =
          bundleProcessor.getFinishFunctionRegistry();
      ExecutionStateTracker stateTracker = bundleProcessor.getStateTracker();

      try (HandleStateCallsForBundle beamFnStateClient = bundleProcessor.getBeamFnStateClient()) {
        stateTracker.start(request.getInstructionId());
        try {
          // Already in reverse topological order so we don't need to do anything.
          for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
            LOG.debug("Starting function {}", startFunction);
            startFunction.run();
          }

          if (request.getProcessBundle().hasElements()) {
            boolean inputFinished =
                bundleProcessor
                    .getInboundObserver()
                    .multiplexElements(request.getProcessBundle().getElements());
            if (!inputFinished) {
              throw new RuntimeException(
                  "Elements embedded in ProcessBundleRequest do not contain stream terminators for "
                      + "all data and timer inputs. Unterminated endpoints: "
                      + bundleProcessor.getInboundObserver().getUnfinishedEndpoints());
            }
          } else if (!bundleProcessor.getInboundEndpointApiServiceDescriptors().isEmpty()) {
            BeamFnDataInboundObserver observer = bundleProcessor.getInboundObserver();
            beamFnDataClient.registerReceiver(
                request.getInstructionId(),
                bundleProcessor.getInboundEndpointApiServiceDescriptors(),
                observer);
            observer.awaitCompletion();
            beamFnDataClient.unregisterReceiver(
                request.getInstructionId(),
                bundleProcessor.getInboundEndpointApiServiceDescriptors());
          }

          // Need to reverse this since we want to call finish in topological order.
          for (ThrowingRunnable finishFunction :
              Lists.reverse(finishFunctionRegistry.getFunctions())) {
            LOG.debug("Finishing function {}", finishFunction);
            finishFunction.run();
          }

          // If bundleProcessor has not flushed any elements, embed them in response.
          embedOutboundElementsIfApplicable(response, bundleProcessor);

          // Add all checkpointed residuals to the response.
          response.addAllResidualRoots(bundleProcessor.getSplitListener().getResidualRoots());

          // Add all metrics to the response.
          bundleProcessor.getProgressRequestLock().lock();
          Map<String, ByteString> monitoringData = finalMonitoringData(bundleProcessor);
          if (runnerAcceptsShortIds) {
            response.putAllMonitoringData(monitoringData);
          } else {
            for (Map.Entry<String, ByteString> metric : monitoringData.entrySet()) {
              response.addMonitoringInfos(
                  shortIds.get(metric.getKey()).toBuilder().setPayload(metric.getValue()));
            }
          }

          if (!bundleProcessor.getBundleFinalizationCallbackRegistrations().isEmpty()) {
            finalizeBundleHandler.registerCallbacks(
                bundleProcessor.getInstructionId(),
                ImmutableList.copyOf(bundleProcessor.getBundleFinalizationCallbackRegistrations()));
            response.setRequiresFinalization(true);
          }
        } finally {
          // We specifically deactivate state tracking while we are holding the progress request and
          // sampling locks.
          stateTracker.reset();
        }
      }

      // Mark the bundle processor as re-usable.
      bundleProcessorCache.release(
          request.getProcessBundle().getProcessBundleDescriptorId(), bundleProcessor);
      return BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response);
    } catch (Exception e) {
      // Make sure we clean up from the active set of bundle processors.
      LOG.debug(
          "Discard bundleProcessor for {} after exception: {}",
          request.getProcessBundle().getProcessBundleDescriptorId(),
          e.getMessage());
      bundleProcessorCache.discard(bundleProcessor);
      throw e;
    }
  }

  private void embedOutboundElementsIfApplicable(
      ProcessBundleResponse.Builder response, BundleProcessor bundleProcessor) {
    if (bundleProcessor.getOutboundAggregators().isEmpty()) {
      return;
    }
    List<Elements> collectedElements =
        new ArrayList<>(bundleProcessor.getOutboundAggregators().size());
    boolean hasFlushedAggregator = false;
    for (BeamFnDataOutboundAggregator aggregator :
        bundleProcessor.getOutboundAggregators().values()) {
      Elements elements = aggregator.sendOrCollectBufferedDataAndFinishOutboundStreams();
      if (elements == null) {
        hasFlushedAggregator = true;
      }
      collectedElements.add(elements);
    }
    if (!hasFlushedAggregator) {
      Elements.Builder elementsToEmbed = Elements.newBuilder();
      for (Elements collectedElement : collectedElements) {
        elementsToEmbed.mergeFrom(collectedElement);
      }
      response.setElements(elementsToEmbed.build());
    } else {
      // Since there was at least one flushed aggregator, we have to use the aggregators that were
      // able to successfully collect their elements to emit them and can not send them as part of
      // the ProcessBundleResponse.
      int i = 0;
      for (BeamFnDataOutboundAggregator aggregator :
          bundleProcessor.getOutboundAggregators().values()) {
        Elements elements = collectedElements.get(i++);
        if (elements != null) {
          aggregator.sendElements(elements);
        }
      }
    }
  }

  public BeamFnApi.InstructionResponse.Builder progress(BeamFnApi.InstructionRequest request)
      throws Exception {
    BundleProcessor bundleProcessor =
        bundleProcessorCache.find(request.getProcessBundleProgress().getInstructionId());

    if (bundleProcessor == null) {
      // We might be unable to find an active bundle if ProcessBundleProgressRequest is received by
      // the SDK before the ProcessBundleRequest. In this case, we send an empty response instead of
      // failing so that the runner does not fail/timeout.
      return BeamFnApi.InstructionResponse.newBuilder()
          .setProcessBundleProgress(BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    }

    // Try to capture the progress lock, the lock will only be held if the bundle is
    // being finished or another progress request is in progress.
    if (!bundleProcessor.getProgressRequestLock().tryLock()) {
      return BeamFnApi.InstructionResponse.newBuilder()
          .setProcessBundleProgress(BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    }

    Map<String, ByteString> monitoringData;
    try {
      // While holding the lock we check to see if this bundle processor is still active.
      if (bundleProcessorCache.find(request.getProcessBundleProgress().getInstructionId())
          == null) {
        return BeamFnApi.InstructionResponse.newBuilder()
            .setProcessBundleProgress(BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
      }

      monitoringData = intermediateMonitoringData(bundleProcessor);
    } finally {
      bundleProcessor.getProgressRequestLock().unlock();
    }

    BeamFnApi.ProcessBundleProgressResponse.Builder response =
        BeamFnApi.ProcessBundleProgressResponse.newBuilder();
    if (runnerAcceptsShortIds) {
      response.putAllMonitoringData(monitoringData);
    } else {
      for (Map.Entry<String, ByteString> metric : monitoringData.entrySet()) {
        response.addMonitoringInfos(
            shortIds.get(metric.getKey()).toBuilder().setPayload(metric.getValue()));
      }
    }

    response.setConsumingReceivedData(
        bundleProcessor.getInboundObserver().isConsumingReceivedData());

    return BeamFnApi.InstructionResponse.newBuilder().setProcessBundleProgress(response);
  }

  private Map<String, ByteString> intermediateMonitoringData(BundleProcessor bundleProcessor)
      throws Exception {
    Map<String, ByteString> monitoringData = new HashMap<>();
    // Extract MonitoringInfos that come from the metrics container registry.
    monitoringData.putAll(
        bundleProcessor
            .getStateTracker()
            .getMetricsContainerRegistry()
            .getMonitoringData(shortIds));
    // Add any additional monitoring infos that the "runners" report explicitly.
    bundleProcessor
        .getBundleProgressReporterAndRegistrar()
        .updateIntermediateMonitoringData(monitoringData);
    return monitoringData;
  }

  private Map<String, ByteString> finalMonitoringData(BundleProcessor bundleProcessor)
      throws Exception {
    HashMap<String, ByteString> monitoringData = new HashMap<>();
    // Extract MonitoringInfos that come from the metrics container registry.
    monitoringData.putAll(
        bundleProcessor
            .getStateTracker()
            .getMetricsContainerRegistry()
            .getMonitoringData(shortIds));
    // Add any additional monitoring infos that the "runners" report explicitly.
    bundleProcessor
        .getBundleProgressReporterAndRegistrar()
        .updateFinalMonitoringData(monitoringData);
    return monitoringData;
  }

  /** Splits an active bundle. */
  public BeamFnApi.InstructionResponse.Builder trySplit(BeamFnApi.InstructionRequest request) {
    BundleProcessor bundleProcessor =
        bundleProcessorCache.find(request.getProcessBundleSplit().getInstructionId());
    BeamFnApi.ProcessBundleSplitResponse.Builder response =
        BeamFnApi.ProcessBundleSplitResponse.newBuilder();

    if (bundleProcessor == null) {
      // We might be unable to find an active bundle if ProcessBundleSplitRequest is received by
      // the SDK before the ProcessBundleRequest. In this case, we send an empty response instead of
      // failing so that the runner does not fail/timeout.
      return BeamFnApi.InstructionResponse.newBuilder()
          .setProcessBundleSplit(BeamFnApi.ProcessBundleSplitResponse.getDefaultInstance());
    }

    for (BeamFnDataReadRunner channelRoot : bundleProcessor.getChannelRoots()) {
      channelRoot.trySplit(request.getProcessBundleSplit(), response);
    }
    return BeamFnApi.InstructionResponse.newBuilder().setProcessBundleSplit(response);
  }

  /** Shutdown the bundles, running the tearDown() functions. */
  public void shutdown() throws Exception {
    bundleProcessorCache.shutdown();
  }

  @VisibleForTesting
  static class MetricsEnvironmentStateForBundle {
    private @Nullable MetricsEnvironmentState currentThreadState;

    public void start(MetricsContainer container) {
      currentThreadState = MetricsEnvironment.getMetricsEnvironmentStateForCurrentThread();
      currentThreadState.activate(container);
    }

    public void reset() {
      currentThreadState.activate(null);
      currentThreadState = null;
    }

    public void discard() {
      currentThreadState.activate(null);
    }
  }

  private BundleProcessor createBundleProcessor(
      String bundleId, BeamFnApi.ProcessBundleRequest processBundleRequest) throws IOException {
    BeamFnApi.ProcessBundleDescriptor bundleDescriptor = fnApiRegistry.apply(bundleId);

    SetMultimap<String, String> pCollectionIdsToConsumingPTransforms = HashMultimap.create();
    BundleProgressReporter.InMemory bundleProgressReporterAndRegistrar =
        new BundleProgressReporter.InMemory();
    MetricsEnvironmentStateForBundle metricsEnvironmentStateForBundle =
        new MetricsEnvironmentStateForBundle();
    ExecutionStateTracker stateTracker = executionStateSampler.create();
    bundleProgressReporterAndRegistrar.register(stateTracker);
    PCollectionConsumerRegistry pCollectionConsumerRegistry =
        new PCollectionConsumerRegistry(
            stateTracker,
            shortIds,
            bundleProgressReporterAndRegistrar,
            bundleDescriptor,
            dataSampler);
    HashSet<String> processedPTransformIds = new HashSet<>();

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(shortIds, stateTracker, Urns.START_BUNDLE_MSECS);
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(shortIds, stateTracker, Urns.FINISH_BUNDLE_MSECS);
    List<ThrowingRunnable> resetFunctions = new ArrayList<>();
    List<ThrowingRunnable> tearDownFunctions = new ArrayList<>();

    // Build a multimap of PCollection ids to PTransform ids which consume said PCollections
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        bundleDescriptor.getTransformsMap().entrySet()) {
      for (String pCollectionId : entry.getValue().getInputsMap().values()) {
        pCollectionIdsToConsumingPTransforms.put(pCollectionId, entry.getKey());
      }
    }

    // Instantiate a State API call handler depending on whether a State ApiServiceDescriptor was
    // specified.
    HandleStateCallsForBundle beamFnStateClient;
    if (bundleDescriptor.hasStateApiServiceDescriptor()) {
      BeamFnStateClient underlyingClient =
          beamFnStateGrpcClientCache.forApiServiceDescriptor(
              bundleDescriptor.getStateApiServiceDescriptor());
      beamFnStateClient = new BlockTillStateCallsFinish(underlyingClient);
    } else {
      beamFnStateClient = new FailAllStateCallsForBundle(processBundleRequest);
    }

    BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();

    Collection<CallbackRegistration> bundleFinalizationCallbackRegistrations = new ArrayList<>();
    BundleFinalizer bundleFinalizer =
        new BundleFinalizer() {
          @Override
          public void afterBundleCommit(Instant callbackExpiry, Callback callback) {
            bundleFinalizationCallbackRegistrations.add(
                CallbackRegistration.create(callbackExpiry, callback));
          }
        };

    BundleProcessor bundleProcessor =
        BundleProcessor.create(
            processWideCache,
            bundleProgressReporterAndRegistrar,
            bundleDescriptor,
            startFunctionRegistry,
            finishFunctionRegistry,
            resetFunctions,
            tearDownFunctions,
            splitListener,
            pCollectionConsumerRegistry,
            metricsEnvironmentStateForBundle,
            stateTracker,
            beamFnStateClient,
            bundleFinalizationCallbackRegistrations,
            runnerCapabilities);

    // Create a BeamFnStateClient
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        bundleDescriptor.getTransformsMap().entrySet()) {

      // Skip anything which isn't a root.
      // Also force data output transforms to be unconditionally instantiated (see BEAM-10450).
      // TODO: Remove source as a root and have it be triggered by the Runner.
      if (!DATA_INPUT_URN.equals(entry.getValue().getSpec().getUrn())
          && !DATA_OUTPUT_URN.equals(entry.getValue().getSpec().getUrn())
          && !JAVA_SOURCE_URN.equals(entry.getValue().getSpec().getUrn())
          && !PTransformTranslation.READ_TRANSFORM_URN.equals(
              entry.getValue().getSpec().getUrn())) {
        continue;
      }

      createRunnerAndConsumersForPTransformRecursively(
          beamFnStateClient,
          beamFnDataClient,
          entry.getKey(),
          entry.getValue(),
          bundleProcessor::getInstructionId,
          bundleProcessor::getCacheTokens,
          bundleProcessor::getBundleCache,
          bundleDescriptor,
          pCollectionIdsToConsumingPTransforms,
          pCollectionConsumerRegistry,
          processedPTransformIds,
          startFunctionRegistry,
          finishFunctionRegistry,
          resetFunctions::add,
          tearDownFunctions::add,
          (apiServiceDescriptor, dataEndpoint) -> {
            if (!bundleProcessor
                .getInboundEndpointApiServiceDescriptors()
                .contains(apiServiceDescriptor)) {
              bundleProcessor.getInboundEndpointApiServiceDescriptors().add(apiServiceDescriptor);
            }
            bundleProcessor.getInboundDataEndpoints().add(dataEndpoint);
          },
          (timerEndpoint) -> {
            if (!bundleDescriptor.hasTimerApiServiceDescriptor()) {
              throw new IllegalStateException(
                  String.format(
                      "Timers are unsupported because the "
                          + "ProcessBundleRequest %s does not provide a timer ApiServiceDescriptor.",
                      bundleId));
            }
            bundleProcessor.getTimerEndpoints().add(timerEndpoint);
          },
          bundleProgressReporterAndRegistrar::register,
          splitListener,
          bundleFinalizer,
          bundleProcessor.getChannelRoots(),
          bundleProcessor.getOutboundAggregators(),
          bundleProcessor.getRunnerCapabilities());
    }
    bundleProcessor.finish();

    return bundleProcessor;
  }

  public BundleProcessorCache getBundleProcessorCache() {
    return bundleProcessorCache;
  }

  /** A cache for {@link BundleProcessor}s. */
  public static class BundleProcessorCache {

    private final LoadingCache<String, ConcurrentLinkedQueue<BundleProcessor>>
        cachedBundleProcessors;
    private final Map<String, BundleProcessor> activeBundleProcessors;

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    BundleProcessorCache() {
      this.cachedBundleProcessors =
          CacheBuilder.newBuilder()
              .expireAfterAccess(Duration.ofMinutes(1L))
              .removalListener(
                  removalNotification -> {
                    ((ConcurrentLinkedQueue<BundleProcessor>) removalNotification.getValue())
                        .forEach(
                            bundleProcessor -> {
                              bundleProcessor.shutdown();
                            });
                  })
              .build(
                  new CacheLoader<String, ConcurrentLinkedQueue<BundleProcessor>>() {
                    @Override
                    public ConcurrentLinkedQueue<BundleProcessor> load(String s) throws Exception {
                      return new ConcurrentLinkedQueue<>();
                    }
                  });
      // We specifically use a weak hash map so that references will automatically go out of scope
      // and not need to be freed explicitly from the cache.
      this.activeBundleProcessors = Collections.synchronizedMap(new WeakHashMap<>());
    }

    @VisibleForTesting
    Map<String, ConcurrentLinkedQueue<BundleProcessor>> getCachedBundleProcessors() {
      return ImmutableMap.copyOf(cachedBundleProcessors.asMap());
    }

    public Map<String, BundleProcessor> getActiveBundleProcessors() {
      return ImmutableMap.copyOf(activeBundleProcessors);
    }

    /**
     * Get a {@link BundleProcessor} from the cache if it's available. Otherwise, create one using
     * the specified {@code bundleProcessorSupplier}. The {@link BundleProcessor} that is returned
     * can be {@link #find found} using the specified method.
     *
     * <p>The caller is responsible for calling {@link #release} to return the bundle processor back
     * to this cache if and only if the bundle processor successfully processed a bundle.
     */
    BundleProcessor get(
        InstructionRequest processBundleRequest,
        Supplier<BundleProcessor> bundleProcessorSupplier) {
      ConcurrentLinkedQueue<BundleProcessor> bundleProcessors =
          cachedBundleProcessors.getUnchecked(
              processBundleRequest.getProcessBundle().getProcessBundleDescriptorId());
      BundleProcessor bundleProcessor = bundleProcessors.poll();
      if (bundleProcessor == null) {
        bundleProcessor = bundleProcessorSupplier.get();
      }

      bundleProcessor.setupForProcessBundleRequest(processBundleRequest);
      activeBundleProcessors.put(processBundleRequest.getInstructionId(), bundleProcessor);
      return bundleProcessor;
    }

    /**
     * Finds an active bundle processor for the specified {@code instructionId} or null if one could
     * not be found.
     */
    public BundleProcessor find(String instructionId) {
      return activeBundleProcessors.get(instructionId);
    }

    /**
     * Add a {@link BundleProcessor} to cache. The {@link BundleProcessor} will be marked as
     * inactive and reset before being added to the cache.
     */
    void release(String bundleDescriptorId, BundleProcessor bundleProcessor) {
      activeBundleProcessors.remove(bundleProcessor.getInstructionId());
      try {
        bundleProcessor.reset();
        cachedBundleProcessors.get(bundleDescriptorId).add(bundleProcessor);
      } catch (Exception e) {
        LOG.warn(
            "Was unable to reset bundle processor safely. Bundle processor will be discarded and re-instantiated on next bundle for descriptor {}.",
            bundleDescriptorId,
            e);
      }
    }

    /** Discard an active {@link BundleProcessor} instead of being re-used. */
    void discard(BundleProcessor bundleProcessor) {
      bundleProcessor.discard();
      activeBundleProcessors.remove(bundleProcessor.getInstructionId());
    }

    /** Shutdown all the cached {@link BundleProcessor}s, running the tearDown() functions. */
    void shutdown() throws Exception {
      cachedBundleProcessors.invalidateAll();
    }
  }

  /** A container for the reusable information used to process a bundle. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class BundleProcessor {
    public static BundleProcessor create(
        Cache<Object, Object> processWideCache,
        BundleProgressReporter.InMemory bundleProgressReporterAndRegistrar,
        ProcessBundleDescriptor processBundleDescriptor,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        List<ThrowingRunnable> resetFunctions,
        List<ThrowingRunnable> tearDownFunctions,
        BundleSplitListener.InMemory splitListener,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        MetricsEnvironmentStateForBundle metricsEnvironmentStateForBundle,
        ExecutionStateTracker stateTracker,
        HandleStateCallsForBundle beamFnStateClient,
        Collection<CallbackRegistration> bundleFinalizationCallbackRegistrations,
        Set<String> runnerCapabilities) {
      return new AutoValue_ProcessBundleHandler_BundleProcessor(
          processWideCache,
          bundleProgressReporterAndRegistrar,
          processBundleDescriptor,
          startFunctionRegistry,
          finishFunctionRegistry,
          resetFunctions,
          tearDownFunctions,
          splitListener,
          pCollectionConsumerRegistry,
          metricsEnvironmentStateForBundle,
          stateTracker,
          beamFnStateClient,
          /*inboundEndpointApiServiceDescriptors=*/ new ArrayList<>(),
          /*inboundDataEndpoints=*/ new ArrayList<>(),
          /*timerEndpoints=*/ new ArrayList<>(),
          bundleFinalizationCallbackRegistrations,
          /*channelRoots=*/ new ArrayList<>(),
          // We rely on the stable iteration order of outboundAggregators, thus using LinkedHashMap.
          /*outboundAggregators=*/ new LinkedHashMap<>(),
          runnerCapabilities,
          new ReentrantLock());
    }

    private String instructionId;
    private List<CacheToken> cacheTokens;
    private ClearableCache<Object, Object> bundleCache;

    abstract Cache<?, ?> getProcessWideCache();

    abstract BundleProgressReporter.InMemory getBundleProgressReporterAndRegistrar();

    abstract ProcessBundleDescriptor getProcessBundleDescriptor();

    abstract PTransformFunctionRegistry getStartFunctionRegistry();

    abstract PTransformFunctionRegistry getFinishFunctionRegistry();

    abstract List<ThrowingRunnable> getResetFunctions();

    abstract List<ThrowingRunnable> getTearDownFunctions();

    abstract BundleSplitListener.InMemory getSplitListener();

    abstract PCollectionConsumerRegistry getpCollectionConsumerRegistry();

    abstract MetricsEnvironmentStateForBundle getMetricsEnvironmentStateForBundle();

    public abstract ExecutionStateTracker getStateTracker();

    abstract HandleStateCallsForBundle getBeamFnStateClient();

    abstract List<Endpoints.ApiServiceDescriptor> getInboundEndpointApiServiceDescriptors();

    abstract List<DataEndpoint<?>> getInboundDataEndpoints();

    abstract List<TimerEndpoint<?>> getTimerEndpoints();

    abstract Collection<CallbackRegistration> getBundleFinalizationCallbackRegistrations();

    abstract Collection<BeamFnDataReadRunner> getChannelRoots();

    abstract Map<ApiServiceDescriptor, BeamFnDataOutboundAggregator> getOutboundAggregators();

    abstract Set<String> getRunnerCapabilities();

    abstract Lock getProgressRequestLock();

    synchronized String getInstructionId() {
      return this.instructionId;
    }

    synchronized List<CacheToken> getCacheTokens() {
      return this.cacheTokens;
    }

    synchronized Cache<Object, Object> getBundleCache() {
      if (this.bundleCache == null) {
        this.bundleCache =
            new Caches.ClearableCache<>(
                Caches.subCache(getProcessWideCache(), "Bundle", this.instructionId));
      }
      return this.bundleCache;
    }

    private BeamFnDataInboundObserver inboundObserver2;

    BeamFnDataInboundObserver getInboundObserver() {
      return inboundObserver2;
    }

    /** Finishes construction of the {@link BundleProcessor}. */
    void finish() {
      inboundObserver2 =
          BeamFnDataInboundObserver.forConsumers(getInboundDataEndpoints(), getTimerEndpoints());
      for (BeamFnDataOutboundAggregator aggregator : getOutboundAggregators().values()) {
        aggregator.start();
      }
    }

    synchronized void setupForProcessBundleRequest(InstructionRequest request) {
      this.instructionId = request.getInstructionId();
      this.cacheTokens = request.getProcessBundle().getCacheTokensList();
      getMetricsEnvironmentStateForBundle().start(getStateTracker().getMetricsContainer());
    }

    void reset() throws Exception {
      synchronized (this) {
        this.instructionId = null;
        this.cacheTokens = null;
        if (this.bundleCache != null) {
          this.bundleCache.clear();
          this.bundleCache = null;
        }
      }
      getSplitListener().clear();
      getMetricsEnvironmentStateForBundle().reset();
      getStateTracker().reset();
      getBundleFinalizationCallbackRegistrations().clear();
      for (ThrowingRunnable resetFunction : getResetFunctions()) {
        resetFunction.run();
      }
      getInboundObserver().reset();
      getBundleProgressReporterAndRegistrar().reset();
      getProgressRequestLock().unlock();
    }

    void discard() {
      synchronized (this) {
        this.instructionId = null;
        this.cacheTokens = null;
        if (this.bundleCache != null) {
          this.bundleCache.clear();
        }
        // setupFunctions are invoked in createBundleProcessor. Invoke teardownFunction here as the
        // BundleProcessor is already removed from cache and won't be re-used.
        for (ThrowingRunnable teardownFunction : Lists.reverse(this.getTearDownFunctions())) {
          try {
            teardownFunction.run();
          } catch (Throwable e) {
            LOG.warn(
                "Exceptions are thrown from DoFn.teardown method when trying to discard "
                    + "ProcessBundleHandler",
                e);
          }
        }
        getMetricsEnvironmentStateForBundle().discard();
        for (BeamFnDataOutboundAggregator aggregator : getOutboundAggregators().values()) {
          aggregator.discard();
        }
      }
    }

    // this is called in cachedBundleProcessors removal listener
    void shutdown() {
      for (ThrowingRunnable tearDownFunction : getTearDownFunctions()) {
        LOG.debug("Tearing down function {}", tearDownFunction);
        try {
          tearDownFunction.run();
        } catch (Exception e) {
          LOG.error(
              "Exceptions are thrown from DoFn.teardown method. Note that it will not fail the"
                  + " pipeline execution,",
              e);
        }
      }
    }
  }

  /**
   * A {@link BeamFnStateClient} which counts the number of outstanding {@link StateRequest}s and
   * blocks till they are all finished.
   */
  private static class BlockTillStateCallsFinish extends HandleStateCallsForBundle {
    private final BeamFnStateClient beamFnStateClient;
    private final Phaser phaser;
    private int currentPhase;

    private BlockTillStateCallsFinish(BeamFnStateClient beamFnStateClient) {
      this.beamFnStateClient = beamFnStateClient;
      this.phaser = new Phaser(1 /* initial party is the process bundle handler */);
      this.currentPhase = phaser.getPhase();
    }

    @Override
    public void close() throws Exception {
      int unarrivedParties = phaser.getUnarrivedParties();
      if (unarrivedParties > 0) {
        LOG.debug(
            "Waiting for {} parties to arrive before closing, current phase {}.",
            unarrivedParties,
            currentPhase);
      }
      currentPhase = phaser.arriveAndAwaitAdvance();
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored") // async arriveAndDeregister task doesn't need
    // monitoring.
    public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
      // Register each request with the phaser and arrive and deregister each time a request
      // completes.
      CompletableFuture<StateResponse> response = beamFnStateClient.handle(requestBuilder);
      phaser.register();
      response.whenComplete((stateResponse, throwable) -> phaser.arriveAndDeregister());
      return response;
    }
  }

  /**
   * A {@link BeamFnStateClient} which fails all requests because the {@link ProcessBundleRequest}
   * does not contain a State {@link ApiServiceDescriptor}.
   */
  private static class FailAllStateCallsForBundle extends HandleStateCallsForBundle {
    private final ProcessBundleRequest request;

    private FailAllStateCallsForBundle(ProcessBundleRequest request) {
      this.request = request;
    }

    @Override
    public void close() throws Exception {
      // no-op
    }

    @Override
    public CompletableFuture<StateResponse> handle(BeamFnApi.StateRequest.Builder requestBuilder) {
      throw new IllegalStateException(
          String.format(
              "State API calls are unsupported because the "
                  + "ProcessBundleRequest %s does not support state.",
              request));
    }
  }

  abstract static class HandleStateCallsForBundle implements AutoCloseable, BeamFnStateClient {}

  private static class UnknownPTransformRunnerFactory implements PTransformRunnerFactory<Object> {
    private final Set<String> knownUrns;

    private UnknownPTransformRunnerFactory(Set<String> knownUrns) {
      this.knownUrns = knownUrns;
    }

    @Override
    public Object createRunnerForPTransform(Context context) {
      String message =
          String.format(
              "No factory registered for %s, known factories %s",
              context.getPTransform().getSpec().getUrn(), knownUrns);
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }
}
