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
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Phaser;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.BeamFnDataReadRunner;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.PTransformRunnerFactory.ProgressRequestCallback;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements.Data;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessDataBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.BeamUrns;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.ShortIdMap;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.Message;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.SetMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes {@link ProcessDataBundleRequest}s
 *
 * <p>{@link BeamFnApi.ProcessBundleSplitRequest}s use a {@link DataBundleProcessorCache cache} to
 * find/create a {@link DataBundleProcessor}. The creation of a {@link DataBundleProcessor} uses the
 * associated {@link ProcessBundleDescriptor} definition; creating runners for each {@link
 * RunnerApi.FunctionSpec}; wiring them together based upon the {@code input} and {@code output} map
 * definitions. The {@link DataBundleProcessor} executes the DAG based graph by starting all runners
 * in reverse topological order, and finishing all runners in forward topological order.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness",
  "keyfor"
}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class ProcessDataBundleHandler {

  // TODO: What should the initial set of URNs be?
  private static final String DATA_INPUT_URN = "beam:runner:source:v1";
  private static final String DATA_OUTPUT_URN = "beam:runner:sink:v1";
  public static final String JAVA_SOURCE_URN = "beam:source:java:0.1";

  private static final Logger LOG = LoggerFactory.getLogger(ProcessDataBundleHandler.class);
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
  private final Function<String, Message> fnApiRegistry;
  private final BeamFnStateGrpcClientCache beamFnStateGrpcClientCache;
  private final FinalizeBundleHandler finalizeBundleHandler;
  private final ShortIdMap shortIds;
  private final boolean runnerAcceptsShortIds;
  private final Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap;
  private final PTransformRunnerFactory defaultPTransformRunnerFactory;
  @VisibleForTesting final DataBundleProcessorCache bundleProcessorCache;

  public ProcessDataBundleHandler(
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Function<String, Message> fnApiRegistry,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler,
      ShortIdMap shortIds) {
    this(
        options,
        runnerCapabilities,
        fnApiRegistry,
        beamFnStateGrpcClientCache,
        finalizeBundleHandler,
        shortIds,
        REGISTERED_RUNNER_FACTORIES,
        new DataBundleProcessorCache());
  }

  @VisibleForTesting
  ProcessDataBundleHandler(
      PipelineOptions options,
      Set<String> runnerCapabilities,
      Function<String, Message> fnApiRegistry,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler,
      ShortIdMap shortIds,
      Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap,
      DataBundleProcessorCache bundleProcessorCache) {
    this.options = options;
    this.fnApiRegistry = fnApiRegistry;
    this.beamFnStateGrpcClientCache = beamFnStateGrpcClientCache;
    this.finalizeBundleHandler = finalizeBundleHandler;
    this.shortIds = shortIds;
    this.runnerAcceptsShortIds =
        runnerCapabilities.contains(
            BeamUrns.getUrn(RunnerApi.StandardRunnerProtocols.Enum.MONITORING_INFO_SHORT_IDS));
    this.urnToPTransformRunnerFactoryMap = urnToPTransformRunnerFactoryMap;
    this.defaultPTransformRunnerFactory =
        new UnknownPTransformRunnerFactory(urnToPTransformRunnerFactoryMap.keySet());
    this.bundleProcessorCache = bundleProcessorCache;
  }

  private void createRunnerAndConsumersForDataPTransformRecursively(
      Supplier<List<Data>> inputSupplier,
      Consumer<Data> outputConsumer,
      BeamFnStateClient beamFnStateClient,
      String pTransformId,
      PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      ProcessBundleDescriptor processBundleDescriptor,
      SetMultimap<String, String> pCollectionIdsToConsumingPTransforms,
      PCollectionConsumerRegistry pCollectionConsumerRegistry,
      Set<String> processedPTransformIds,
      PTransformFunctionRegistry startFunctionRegistry,
      PTransformFunctionRegistry finishFunctionRegistry,
      Consumer<ThrowingRunnable> addResetFunction,
      Consumer<ThrowingRunnable> addTearDownFunction,
      Consumer<ProgressRequestCallback> addProgressRequestCallback,
      BundleFinalizer bundleFinalizer,
      Collection<BeamFnDataReadRunner> channelRoots)
      throws IOException {

    // Recursively ensure that all consumers of the output PCollection have been created.
    // Since we are creating the consumers first, we know that the we are building the DAG
    // in reverse topological order.
    for (String pCollectionId : pTransform.getOutputsMap().values()) {

      for (String consumingPTransformId : pCollectionIdsToConsumingPTransforms.get(pCollectionId)) {
        createRunnerAndConsumersForDataPTransformRecursively(
            inputSupplier,
            outputConsumer,
            beamFnStateClient,
            consumingPTransformId,
            processBundleDescriptor.getTransformsMap().get(consumingPTransformId),
            processBundleInstructionId,
            processBundleDescriptor,
            pCollectionIdsToConsumingPTransforms,
            pCollectionConsumerRegistry,
            processedPTransformIds,
            startFunctionRegistry,
            finishFunctionRegistry,
            addResetFunction,
            addTearDownFunction,
            addProgressRequestCallback,
            bundleFinalizer,
            channelRoots);
      }
    }

    if (!pTransform.hasSpec()) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot process transform with no spec: %s", TextFormat.printToString(pTransform)));
    }

    if (pTransform.getSubtransformsCount() > 0) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot process composite transform: %s", TextFormat.printToString(pTransform)));
    }

    // Skip reprocessing processed pTransforms.
    if (!processedPTransformIds.contains(pTransformId)) {
      Object runner =
          urnToPTransformRunnerFactoryMap
              .getOrDefault(pTransform.getSpec().getUrn(), defaultPTransformRunnerFactory)
              .createRunnerForDataPTransform(
                  options,
                  beamFnStateClient,
                  pTransformId,
                  pTransform,
                  processBundleInstructionId,
                  processBundleDescriptor.getPcollectionsMap(),
                  processBundleDescriptor.getCodersMap(),
                  processBundleDescriptor.getWindowingStrategiesMap(),
                  pCollectionConsumerRegistry,
                  startFunctionRegistry,
                  finishFunctionRegistry,
                  addResetFunction,
                  addTearDownFunction,
                  addProgressRequestCallback,
                  bundleFinalizer,
                  inputSupplier,
                  outputConsumer);
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
  public BeamFnApi.InstructionResponse.Builder processDataBundle(
      BeamFnApi.InstructionRequest request) throws Exception {
    BeamFnApi.ProcessDataBundleResponse.Builder response =
        BeamFnApi.ProcessDataBundleResponse.newBuilder();

    DataBundleProcessor bundleProcessor =
        bundleProcessorCache.get(
            request.getProcessDataBundle().getProcessBundleDescriptorId(),
            request.getInstructionId(),
            () -> {
              try {
                return createDataBundleProcessor(
                    request.getProcessDataBundle().getProcessBundleDescriptorId(),
                    request.getProcessDataBundle());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    PTransformFunctionRegistry startFunctionRegistry = bundleProcessor.getStartFunctionRegistry();
    PTransformFunctionRegistry finishFunctionRegistry = bundleProcessor.getFinishFunctionRegistry();
    ExecutionStateTracker stateTracker = bundleProcessor.getStateTracker();

    try {
      try (HandleStateCallsForBundle beamFnStateClient = bundleProcessor.getBeamFnStateClient()) {
        try (Closeable closeTracker = stateTracker.activate()) {
          bundleProcessor
              .getDataInput()
              .addAll(request.getProcessDataBundle().getElements().getDataList());

          // Already in reverse topological order so we don't need to do anything.
          for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
            LOG.debug("Starting function {}", startFunction);
            startFunction.run();
          }

          // Need to reverse this since we want to call finish in topological order.
          for (ThrowingRunnable finishFunction :
              Lists.reverse(finishFunctionRegistry.getFunctions())) {
            LOG.debug("Finishing function {}", finishFunction);
            finishFunction.run();
          }
        }

        Elements.Builder builder = Elements.newBuilder();
        builder.addAllData(bundleProcessor.getDataOutput());
        response.setElements(builder.build());

        // Add all metrics to the response.
        Map<String, ByteString> monitoringData = monitoringData(bundleProcessor);
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
      }
      bundleProcessorCache.release(
          request.getProcessDataBundle().getProcessBundleDescriptorId(), bundleProcessor);
    } catch (Exception e) {
      bundleProcessorCache.release(
          request.getProcessDataBundle().getProcessBundleDescriptorId(), bundleProcessor);
      throw e;
    }
    return BeamFnApi.InstructionResponse.newBuilder().setProcessDataBundle(response);
  }

  public BeamFnApi.InstructionResponse.Builder progress(BeamFnApi.InstructionRequest request)
      throws Exception {
    DataBundleProcessor bundleProcessor =
        bundleProcessorCache.find(request.getProcessBundleProgress().getInstructionId());
    BeamFnApi.ProcessBundleProgressResponse.Builder response =
        BeamFnApi.ProcessBundleProgressResponse.newBuilder();

    if (bundleProcessor == null) {
      // We might be unable to find an active bundle if ProcessDataBundleProgressRequest is received
      // by
      // the SDK before the ProcessDataBundleRequest. In this case, we send an empty response
      // instead of
      // failing so that the runner does not fail/timeout.
      return BeamFnApi.InstructionResponse.newBuilder()
          .setProcessBundleProgress(BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    }

    Map<String, ByteString> monitoringData = monitoringData(bundleProcessor);
    if (runnerAcceptsShortIds) {
      response.putAllMonitoringData(monitoringData);
    } else {
      for (Map.Entry<String, ByteString> metric : monitoringData.entrySet()) {
        response.addMonitoringInfos(
            shortIds.get(metric.getKey()).toBuilder().setPayload(metric.getValue()));
      }
    }

    return BeamFnApi.InstructionResponse.newBuilder().setProcessBundleProgress(response);
  }

  private ImmutableMap<String, ByteString> monitoringData(DataBundleProcessor bundleProcessor)
      throws Exception {
    ImmutableMap.Builder<String, ByteString> result = ImmutableMap.builder();
    // Get start bundle Execution Time Metrics.
    result.putAll(
        bundleProcessor.getStartFunctionRegistry().getExecutionTimeMonitoringData(shortIds));
    // Get process bundle Execution Time Metrics.
    result.putAll(
        bundleProcessor.getpCollectionConsumerRegistry().getExecutionTimeMonitoringData(shortIds));
    // Get finish bundle Execution Time Metrics.
    result.putAll(
        bundleProcessor.getFinishFunctionRegistry().getExecutionTimeMonitoringData(shortIds));
    // Extract MonitoringInfos that come from the metrics container registry.
    result.putAll(bundleProcessor.getMetricsContainerRegistry().getMonitoringData(shortIds));
    // Add any additional monitoring infos that the "runners" report explicitly.
    for (ProgressRequestCallback progressRequestCallback :
        bundleProcessor.getProgressRequestCallbacks()) {
      // TODO(BEAM-6597): Plumb reporting monitoring infos using the short id system upstream.
      for (MetricsApi.MonitoringInfo monitoringInfo :
          progressRequestCallback.getMonitoringInfos()) {
        ByteString payload = monitoringInfo.getPayload();
        String shortId =
            shortIds.getOrCreateShortId(monitoringInfo.toBuilder().clearPayload().build());
        result.put(shortId, payload);
      }
    }
    return result.build();
  }

  /** Shutdown the bundles, running the tearDown() functions. */
  public void shutdown() throws Exception {
    bundleProcessorCache.shutdown();
  }

  private DataBundleProcessor createDataBundleProcessor(
      String bundleId, ProcessDataBundleRequest processBundleRequest) throws IOException {
    // Note: We must create one instance of the QueueingBeamFnDataClient as it is designed to
    // handle the life of a bundle. It will insert elements onto a queue and drain them off so all
    // process() calls will execute on this thread when queueingClient.drainAndBlock() is called.
    // QueueingBeamFnDataClient queueingClient =
    //     new QueueingBeamFnDataClient(this.beamFnDataClient, DATA_QUEUE_SIZE);

    ProcessBundleDescriptor bundleDescriptor =
        (ProcessBundleDescriptor) fnApiRegistry.apply(bundleId);

    SetMultimap<String, String> pCollectionIdsToConsumingPTransforms = HashMultimap.create();
    MetricsContainerStepMap metricsContainerRegistry = new MetricsContainerStepMap();
    ExecutionStateTracker stateTracker =
        new ExecutionStateTracker(ExecutionStateSampler.instance());
    PCollectionConsumerRegistry pCollectionConsumerRegistry =
        new PCollectionConsumerRegistry(metricsContainerRegistry, stateTracker);
    HashSet<String> processedPTransformIds = new HashSet<>();

    PTransformFunctionRegistry startFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, stateTracker, ExecutionStateTracker.START_STATE_NAME);
    PTransformFunctionRegistry finishFunctionRegistry =
        new PTransformFunctionRegistry(
            metricsContainerRegistry, stateTracker, ExecutionStateTracker.FINISH_STATE_NAME);
    List<ThrowingRunnable> resetFunctions = new ArrayList<>();
    List<ThrowingRunnable> tearDownFunctions = new ArrayList<>();
    List<ProgressRequestCallback> progressRequestCallbacks = new ArrayList<>();

    // Build a multimap of PCollection ids to PTransform ids which consume said PCollections
    for (Map.Entry<String, PTransform> entry : bundleDescriptor.getTransformsMap().entrySet()) {
      for (String pCollectionId : entry.getValue().getInputsMap().values()) {
        pCollectionIdsToConsumingPTransforms.put(pCollectionId, entry.getKey());
      }
    }

    // Instantiate a State API call handler depending on whether a State ApiServiceDescriptor
    // was specified.
    HandleStateCallsForBundle beamFnStateClient =
        new BlockTillStateCallsFinish(
            beamFnStateGrpcClientCache.forApiServiceDescriptor(
                bundleDescriptor.getStateApiServiceDescriptor()));

    // Instantiate a Timer client registration handler depending on whether a Timer
    // ApiServiceDescriptor was specified.
    // BeamFnTimerClient beamFnTimerClient =
    //     bundleDescriptor.hasTimerApiServiceDescriptor()
    //         ? new BeamFnTimerGrpcClient(
    //             queueingClient, bundleDescriptor.getTimerApiServiceDescriptor())
    //         : new FailAllTimerRegistrations(processBundleRequest);

    Collection<CallbackRegistration> bundleFinalizationCallbackRegistrations = new ArrayList<>();
    BundleFinalizer bundleFinalizer =
        new BundleFinalizer() {
          @Override
          public void afterBundleCommit(Instant callbackExpiry, Callback callback) {
            bundleFinalizationCallbackRegistrations.add(
                CallbackRegistration.create(callbackExpiry, callback));
          }
        };

    BundleSplitListener.InMemory splitListener = BundleSplitListener.InMemory.create();
    DataBundleProcessor bundleProcessor =
        DataBundleProcessor.create(
            new ArrayList<>(),
            new ArrayList<>(),
            startFunctionRegistry,
            finishFunctionRegistry,
            resetFunctions,
            tearDownFunctions,
            progressRequestCallbacks,
            splitListener,
            pCollectionConsumerRegistry,
            metricsContainerRegistry,
            stateTracker,
            beamFnStateClient,
            bundleFinalizationCallbackRegistrations);

    // Create a BeamFnStateClient
    for (Map.Entry<String, PTransform> entry : bundleDescriptor.getTransformsMap().entrySet()) {

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

      createRunnerAndConsumersForDataPTransformRecursively(
          bundleProcessor::getDataInput,
          bundleProcessor::storeOutputData,
          beamFnStateClient,
          entry.getKey(),
          entry.getValue(),
          bundleProcessor::getInstructionId,
          bundleDescriptor,
          pCollectionIdsToConsumingPTransforms,
          pCollectionConsumerRegistry,
          processedPTransformIds,
          startFunctionRegistry,
          finishFunctionRegistry,
          resetFunctions::add,
          tearDownFunctions::add,
          progressRequestCallbacks::add,
          bundleFinalizer,
          bundleProcessor.getChannelRoots());
    }
    return bundleProcessor;
  }

  public DataBundleProcessorCache getDataBundleProcessorCache() {
    return bundleProcessorCache;
  }

  /** A cache for {@link DataBundleProcessor}s. */
  public static class DataBundleProcessorCache {

    private final LoadingCache<String, ConcurrentLinkedQueue<DataBundleProcessor>>
        cachedDataBundleProcessors;
    private final Map<String, DataBundleProcessor> activeDataBundleProcessors;

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    DataBundleProcessorCache() {
      this.cachedDataBundleProcessors =
          CacheBuilder.newBuilder()
              .expireAfterAccess(Duration.ofMinutes(1L))
              .removalListener(
                  removalNotification -> {
                    ((ConcurrentLinkedQueue<DataBundleProcessor>) removalNotification.getValue())
                        .forEach(
                            bundleProcessor -> {
                              bundleProcessor.shutdown();
                            });
                  })
              .build(
                  new CacheLoader<String, ConcurrentLinkedQueue<DataBundleProcessor>>() {
                    @Override
                    public ConcurrentLinkedQueue<DataBundleProcessor> load(String s)
                        throws Exception {
                      return new ConcurrentLinkedQueue<>();
                    }
                  });
      // We specifically use a weak hash map so that references will automatically go out of scope
      // and not need to be freed explicitly from the cache.
      this.activeDataBundleProcessors = Collections.synchronizedMap(new WeakHashMap<>());
    }

    @VisibleForTesting
    Map<String, ConcurrentLinkedQueue<DataBundleProcessor>> getCachedDataBundleProcessors() {
      return ImmutableMap.copyOf(cachedDataBundleProcessors.asMap());
    }

    public Map<String, DataBundleProcessor> getActiveDataBundleProcessors() {
      return ImmutableMap.copyOf(activeDataBundleProcessors);
    }

    /**
     * Get a {@link DataBundleProcessor} from the cache if it's available. Otherwise, create one
     * using the specified {@code bundleProcessorSupplier}. The {@link DataBundleProcessor} that is
     * returned can be {@link #find found} using the specified method.
     *
     * <p>The caller is responsible for calling {@link #release} to return the bundle processor back
     * to this cache if and only if the bundle processor successfully processed a bundle.
     */
    DataBundleProcessor get(
        String bundleDescriptorId,
        String instructionId,
        Supplier<DataBundleProcessor> bundleProcessorSupplier) {
      ConcurrentLinkedQueue<DataBundleProcessor> bundleProcessors =
          cachedDataBundleProcessors.getUnchecked(bundleDescriptorId);
      DataBundleProcessor bundleProcessor = bundleProcessors.poll();
      if (bundleProcessor == null) {
        bundleProcessor = bundleProcessorSupplier.get();
      }

      bundleProcessor.setInstructionId(instructionId);
      activeDataBundleProcessors.put(instructionId, bundleProcessor);
      return bundleProcessor;
    }

    /**
     * Finds an active bundle processor for the specified {@code instructionId} or null if one could
     * not be found.
     */
    public DataBundleProcessor find(String instructionId) {
      return activeDataBundleProcessors.get(instructionId);
    }

    /**
     * Add a {@link DataBundleProcessor} to cache. The {@link DataBundleProcessor} will be reset
     * before being added to the cache and will be marked as inactive.
     */
    void release(String bundleDescriptorId, DataBundleProcessor bundleProcessor) {
      activeDataBundleProcessors.remove(bundleProcessor.getInstructionId());
      try {
        bundleProcessor.setInstructionId(null);
        bundleProcessor.reset();
        cachedDataBundleProcessors.get(bundleDescriptorId).add(bundleProcessor);
      } catch (Exception e) {
        LOG.warn(
            "Was unable to reset bundle processor safely. Bundle processor will be discarded and re-instantiated on next bundle for descriptor {}.",
            bundleDescriptorId,
            e);
      }
    }

    /** Shutdown all the cached {@link DataBundleProcessor}s, running the tearDown() functions. */
    void shutdown() throws Exception {
      cachedDataBundleProcessors.invalidateAll();
    }
  }

  /** A container for the reusable information used to process a bundle. */
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  public abstract static class DataBundleProcessor {
    public static DataBundleProcessor create(
        List<Data> dataInput,
        List<Data> dataOutput,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        List<ThrowingRunnable> resetFunctions,
        List<ThrowingRunnable> tearDownFunctions,
        List<ProgressRequestCallback> progressRequestCallbacks,
        BundleSplitListener.InMemory splitListener,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        MetricsContainerStepMap metricsContainerRegistry,
        ExecutionStateTracker stateTracker,
        HandleStateCallsForBundle beamFnStateClient,
        Collection<CallbackRegistration> bundleFinalizationCallbackRegistrations) {
      return new AutoValue_ProcessDataBundleHandler_DataBundleProcessor(
          dataInput,
          dataOutput,
          startFunctionRegistry,
          finishFunctionRegistry,
          resetFunctions,
          tearDownFunctions,
          progressRequestCallbacks,
          splitListener,
          pCollectionConsumerRegistry,
          metricsContainerRegistry,
          stateTracker,
          beamFnStateClient,
          bundleFinalizationCallbackRegistrations,
          new ArrayList<>());
    }

    private String instructionId;

    abstract List<Data> getDataInput();

    abstract List<Data> getDataOutput();

    abstract PTransformFunctionRegistry getStartFunctionRegistry();

    abstract PTransformFunctionRegistry getFinishFunctionRegistry();

    abstract List<ThrowingRunnable> getResetFunctions();

    abstract List<ThrowingRunnable> getTearDownFunctions();

    abstract List<ProgressRequestCallback> getProgressRequestCallbacks();

    abstract BundleSplitListener.InMemory getSplitListener();

    abstract PCollectionConsumerRegistry getpCollectionConsumerRegistry();

    abstract MetricsContainerStepMap getMetricsContainerRegistry();

    public abstract ExecutionStateTracker getStateTracker();

    abstract HandleStateCallsForBundle getBeamFnStateClient();

    abstract Collection<CallbackRegistration> getBundleFinalizationCallbackRegistrations();

    abstract Collection<BeamFnDataReadRunner> getChannelRoots();

    synchronized String getInstructionId() {
      return this.instructionId;
    }

    synchronized void setInstructionId(String instructionId) {
      this.instructionId = instructionId;
    }

    synchronized void storeOutputData(Data output) {
      this.getDataOutput().add(output);
    }

    void reset() throws Exception {
      getDataInput().clear();
      getDataOutput().clear();
      getStartFunctionRegistry().reset();
      getFinishFunctionRegistry().reset();
      getSplitListener().clear();
      getpCollectionConsumerRegistry().reset();
      getMetricsContainerRegistry().reset();
      getStateTracker().reset();
      for (ThrowingRunnable resetFunction : getResetFunctions()) {
        resetFunction.run();
      }
    }

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
    public void handle(Builder requestBuilder, CompletableFuture<StateResponse> response) {
      // Register each request with the phaser and arrive and deregister each time a request
      // completes.
      phaser.register();
      response.whenComplete((stateResponse, throwable) -> phaser.arriveAndDeregister());
      beamFnStateClient.handle(requestBuilder, response);
    }
  }

  abstract static class HandleStateCallsForBundle implements AutoCloseable, BeamFnStateClient {}

  private static class UnknownPTransformRunnerFactory implements PTransformRunnerFactory<Object> {
    private final Set<String> knownUrns;

    private UnknownPTransformRunnerFactory(Set<String> knownUrns) {
      this.knownUrns = knownUrns;
    }

    @Override
    public Object createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        BeamFnTimerClient beamFnTimerClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, Coder> coders,
        Map<String, WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> addResetFunction,
        Consumer<ThrowingRunnable> tearDownFunctions,
        Consumer<ProgressRequestCallback> addProgressRequestCallback,
        BundleSplitListener splitListener,
        BundleFinalizer bundleFinalizer) {
      String message =
          String.format(
              "No factory registered for %s, known factories %s",
              pTransform.getSpec().getUrn(), knownUrns);
      LOG.error(message);
      throw new IllegalStateException(message);
    }

    @Override
    public Object createRunnerForDataPTransform(
        PipelineOptions pipelineOptions,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, Coder> coders,
        Map<String, WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> addResetFunction,
        Consumer<ThrowingRunnable> tearDownFunctions,
        Consumer<ProgressRequestCallback> addProgressRequestCallback,
        BundleFinalizer bundleFinalizer,
        Supplier<List<Data>> inputSupplier,
        Consumer<Data> outputConsumer) {
      String message =
          String.format(
              "No factory registered for %s, known factories %s",
              pTransform.getSpec().getUrn(), knownUrns);
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }
}
