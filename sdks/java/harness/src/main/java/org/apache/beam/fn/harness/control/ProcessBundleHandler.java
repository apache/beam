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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.beam.fn.harness.data.BeamFnTimerGrpcClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.data.QueueingBeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Message;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.SetMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
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
  private final Function<String, Message> fnApiRegistry;
  private final BeamFnDataClient beamFnDataClient;
  private final BeamFnStateGrpcClientCache beamFnStateGrpcClientCache;
  private final FinalizeBundleHandler finalizeBundleHandler;
  private final Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap;
  private final PTransformRunnerFactory defaultPTransformRunnerFactory;
  @VisibleForTesting final BundleProcessorCache bundleProcessorCache;

  public ProcessBundleHandler(
      PipelineOptions options,
      Function<String, Message> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler) {
    this(
        options,
        fnApiRegistry,
        beamFnDataClient,
        beamFnStateGrpcClientCache,
        finalizeBundleHandler,
        REGISTERED_RUNNER_FACTORIES,
        new BundleProcessorCache());
  }

  @VisibleForTesting
  ProcessBundleHandler(
      PipelineOptions options,
      Function<String, Message> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      FinalizeBundleHandler finalizeBundleHandler,
      Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap,
      BundleProcessorCache bundleProcessorCache) {
    this.options = options;
    this.fnApiRegistry = fnApiRegistry;
    this.beamFnDataClient = beamFnDataClient;
    this.beamFnStateGrpcClientCache = beamFnStateGrpcClientCache;
    this.finalizeBundleHandler = finalizeBundleHandler;
    this.urnToPTransformRunnerFactoryMap = urnToPTransformRunnerFactoryMap;
    this.defaultPTransformRunnerFactory =
        new UnknownPTransformRunnerFactory(urnToPTransformRunnerFactoryMap.keySet());
    this.bundleProcessorCache = bundleProcessorCache;
  }

  private void createRunnerAndConsumersForPTransformRecursively(
      BeamFnStateClient beamFnStateClient,
      BeamFnTimerClient beamFnTimerClient,
      BeamFnDataClient queueingClient,
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
      BundleSplitListener splitListener,
      BundleFinalizer bundleFinalizer,
      Collection<BeamFnDataReadRunner> channelRoots)
      throws IOException {

    // Recursively ensure that all consumers of the output PCollection have been created.
    // Since we are creating the consumers first, we know that the we are building the DAG
    // in reverse topological order.
    for (String pCollectionId : pTransform.getOutputsMap().values()) {

      for (String consumingPTransformId : pCollectionIdsToConsumingPTransforms.get(pCollectionId)) {
        createRunnerAndConsumersForPTransformRecursively(
            beamFnStateClient,
            beamFnTimerClient,
            queueingClient,
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
            splitListener,
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
              .createRunnerForPTransform(
                  options,
                  queueingClient,
                  beamFnStateClient,
                  beamFnTimerClient,
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
                  splitListener,
                  bundleFinalizer);
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
            request.getProcessBundle().getProcessBundleDescriptorId(),
            request.getInstructionId(),
            () -> {
              try {
                return createBundleProcessor(
                    request.getProcessBundle().getProcessBundleDescriptorId(),
                    request.getProcessBundle());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
    PTransformFunctionRegistry startFunctionRegistry = bundleProcessor.getStartFunctionRegistry();
    PTransformFunctionRegistry finishFunctionRegistry = bundleProcessor.getFinishFunctionRegistry();
    ExecutionStateTracker stateTracker = bundleProcessor.getStateTracker();
    QueueingBeamFnDataClient queueingClient = bundleProcessor.getQueueingClient();

    try (HandleStateCallsForBundle beamFnStateClient = bundleProcessor.getBeamFnStateClient()) {
      try (Closeable closeTracker = stateTracker.activate()) {
        // Already in reverse topological order so we don't need to do anything.
        for (ThrowingRunnable startFunction : startFunctionRegistry.getFunctions()) {
          LOG.debug("Starting function {}", startFunction);
          startFunction.run();
        }

        queueingClient.drainAndBlock();

        // Need to reverse this since we want to call finish in topological order.
        for (ThrowingRunnable finishFunction :
            Lists.reverse(finishFunctionRegistry.getFunctions())) {
          LOG.debug("Finishing function {}", finishFunction);
          finishFunction.run();
        }
      }

      // Add all checkpointed residuals to the response.
      response.addAllResidualRoots(bundleProcessor.getSplitListener().getResidualRoots());

      // TODO(BEAM-6597): This should be reporting monitoring infos using the short id system.
      // Get start bundle Execution Time Metrics.
      response.addAllMonitoringInfos(
          bundleProcessor.getStartFunctionRegistry().getExecutionTimeMonitoringInfos());
      // Get process bundle Execution Time Metrics.
      response.addAllMonitoringInfos(
          bundleProcessor.getpCollectionConsumerRegistry().getExecutionTimeMonitoringInfos());
      // Get finish bundle Execution Time Metrics.
      response.addAllMonitoringInfos(
          bundleProcessor.getFinishFunctionRegistry().getExecutionTimeMonitoringInfos());
      // Extract MonitoringInfos that come from the metrics container registry.
      response.addAllMonitoringInfos(
          bundleProcessor.getMetricsContainerRegistry().getMonitoringInfos());
      // Add any additional monitoring infos that the "runners" report explicitly.
      for (ProgressRequestCallback progressRequestCallback :
          bundleProcessor.getProgressRequestCallbacks()) {
        response.addAllMonitoringInfos(progressRequestCallback.getMonitoringInfos());
      }

      if (!bundleProcessor.getBundleFinalizationCallbackRegistrations().isEmpty()) {
        finalizeBundleHandler.registerCallbacks(
            bundleProcessor.getInstructionId(),
            ImmutableList.copyOf(bundleProcessor.getBundleFinalizationCallbackRegistrations()));
        response.setRequiresFinalization(true);
      }

      bundleProcessorCache.release(
          request.getProcessBundle().getProcessBundleDescriptorId(), bundleProcessor);
    }
    return BeamFnApi.InstructionResponse.newBuilder().setProcessBundle(response);
  }

  public BeamFnApi.InstructionResponse.Builder progress(BeamFnApi.InstructionRequest request)
      throws Exception {
    BundleProcessor bundleProcessor =
        bundleProcessorCache.find(request.getProcessBundleProgress().getInstructionId());
    BeamFnApi.ProcessBundleProgressResponse.Builder response =
        BeamFnApi.ProcessBundleProgressResponse.newBuilder();

    if (bundleProcessor == null) {
      // We might be unable to find an active bundle if ProcessBundleProgressRequest is received by
      // the SDK before the ProcessBundleRequest. In this case, we send an empty response instead of
      // failing so that the runner does not fail/timeout.
      return BeamFnApi.InstructionResponse.newBuilder()
          .setProcessBundleProgress(BeamFnApi.ProcessBundleProgressResponse.getDefaultInstance());
    }

    // TODO(BEAM-6597): This should really only be reporting monitoring infos where the data
    // changed
    // and we should be using the short id system.

    // Get start bundle Execution Time Metrics.
    response.addAllMonitoringInfos(
        bundleProcessor.getStartFunctionRegistry().getExecutionTimeMonitoringInfos());
    // Get process bundle Execution Time Metrics.
    response.addAllMonitoringInfos(
        bundleProcessor.getpCollectionConsumerRegistry().getExecutionTimeMonitoringInfos());
    // Get finish bundle Execution Time Metrics.
    response.addAllMonitoringInfos(
        bundleProcessor.getFinishFunctionRegistry().getExecutionTimeMonitoringInfos());
    // Extract all other MonitoringInfos other than the execution time monitoring infos.
    response.addAllMonitoringInfos(
        bundleProcessor.getMetricsContainerRegistry().getMonitoringInfos());
    // Add any additional monitoring infos that the "runners" report explicitly.
    for (ProgressRequestCallback progressRequestCallback :
        bundleProcessor.getProgressRequestCallbacks()) {
      response.addAllMonitoringInfos(progressRequestCallback.getMonitoringInfos());
    }

    return BeamFnApi.InstructionResponse.newBuilder().setProcessBundleProgress(response);
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

  private BundleProcessor createBundleProcessor(
      String bundleId, BeamFnApi.ProcessBundleRequest processBundleRequest) throws IOException {
    // Note: We must create one instance of the QueueingBeamFnDataClient as it is designed to
    // handle the life of a bundle. It will insert elements onto a queue and drain them off so all
    // process() calls will execute on this thread when queueingClient.drainAndBlock() is called.
    QueueingBeamFnDataClient queueingClient = new QueueingBeamFnDataClient(this.beamFnDataClient);

    BeamFnApi.ProcessBundleDescriptor bundleDescriptor =
        (BeamFnApi.ProcessBundleDescriptor) fnApiRegistry.apply(bundleId);

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
    for (Map.Entry<String, RunnerApi.PTransform> entry :
        bundleDescriptor.getTransformsMap().entrySet()) {
      for (String pCollectionId : entry.getValue().getInputsMap().values()) {
        pCollectionIdsToConsumingPTransforms.put(pCollectionId, entry.getKey());
      }
    }

    // Instantiate a State API call handler depending on whether a State ApiServiceDescriptor
    // was specified.
    HandleStateCallsForBundle beamFnStateClient =
        bundleDescriptor.hasStateApiServiceDescriptor()
            ? new BlockTillStateCallsFinish(
                beamFnStateGrpcClientCache.forApiServiceDescriptor(
                    bundleDescriptor.getStateApiServiceDescriptor()))
            : new FailAllStateCallsForBundle(processBundleRequest);

    // Instantiate a Timer client registration handler depending on whether a Timer
    // ApiServiceDescriptor was specified.
    BeamFnTimerClient beamFnTimerClient =
        bundleDescriptor.hasTimerApiServiceDescriptor()
            ? new BeamFnTimerGrpcClient(
                queueingClient, bundleDescriptor.getTimerApiServiceDescriptor())
            : new FailAllTimerRegistrations(processBundleRequest);

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
            queueingClient,
            bundleFinalizationCallbackRegistrations);

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
          beamFnTimerClient,
          queueingClient,
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
          splitListener,
          bundleFinalizer,
          bundleProcessor.getChannelRoots());
    }
    return bundleProcessor;
  }

  /** A cache for {@link BundleProcessor}s. */
  public static class BundleProcessorCache {

    private final Map<String, ConcurrentLinkedQueue<BundleProcessor>> cachedBundleProcessors;
    private final Map<String, BundleProcessor> activeBundleProcessors;

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    BundleProcessorCache() {
      this.cachedBundleProcessors = Maps.newConcurrentMap();
      // We specifically use a weak hash map so that references will automatically go out of scope
      // and not need to be freed explicitly from the cache.
      this.activeBundleProcessors = Collections.synchronizedMap(new WeakHashMap<>());
    }

    Map<String, ConcurrentLinkedQueue<BundleProcessor>> getCachedBundleProcessors() {
      return cachedBundleProcessors;
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
        String bundleDescriptorId,
        String instructionId,
        Supplier<BundleProcessor> bundleProcessorSupplier) {
      ConcurrentLinkedQueue<BundleProcessor> bundleProcessors =
          cachedBundleProcessors.computeIfAbsent(
              bundleDescriptorId, descriptorId -> new ConcurrentLinkedQueue<>());
      BundleProcessor bundleProcessor = bundleProcessors.poll();
      if (bundleProcessor == null) {
        bundleProcessor = bundleProcessorSupplier.get();
      }

      bundleProcessor.setInstructionId(instructionId);
      activeBundleProcessors.put(instructionId, bundleProcessor);
      return bundleProcessor;
    }

    /**
     * Finds an active bundle processor for the specified {@code instructionId} or null if one could
     * not be found.
     */
    BundleProcessor find(String instructionId) {
      return activeBundleProcessors.get(instructionId);
    }

    /**
     * Add a {@link BundleProcessor} to cache. The {@link BundleProcessor} will be reset before
     * being added to the cache and will be marked as inactive.
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

    /** Shutdown all the cached {@link BundleProcessor}s, running the tearDown() functions. */
    void shutdown() throws Exception {
      for (ConcurrentLinkedQueue<BundleProcessor> bundleProcessors :
          cachedBundleProcessors.values()) {
        for (BundleProcessor bundleProcessor : bundleProcessors) {
          for (ThrowingRunnable tearDownFunction : bundleProcessor.getTearDownFunctions()) {
            LOG.debug("Tearing down function {}", tearDownFunction);
            tearDownFunction.run();
          }
        }
      }
      cachedBundleProcessors.clear();
    }
  }

  /** A container for the reusable information used to process a bundle. */
  @AutoValue
  public abstract static class BundleProcessor {
    public static BundleProcessor create(
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
        QueueingBeamFnDataClient queueingClient,
        Collection<CallbackRegistration> bundleFinalizationCallbackRegistrations) {
      return new AutoValue_ProcessBundleHandler_BundleProcessor(
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
          queueingClient,
          bundleFinalizationCallbackRegistrations,
          new ArrayList<>());
    }

    private String instructionId;

    abstract PTransformFunctionRegistry getStartFunctionRegistry();

    abstract PTransformFunctionRegistry getFinishFunctionRegistry();

    abstract List<ThrowingRunnable> getResetFunctions();

    abstract List<ThrowingRunnable> getTearDownFunctions();

    abstract List<ProgressRequestCallback> getProgressRequestCallbacks();

    abstract BundleSplitListener.InMemory getSplitListener();

    abstract PCollectionConsumerRegistry getpCollectionConsumerRegistry();

    abstract MetricsContainerStepMap getMetricsContainerRegistry();

    abstract ExecutionStateTracker getStateTracker();

    abstract HandleStateCallsForBundle getBeamFnStateClient();

    abstract QueueingBeamFnDataClient getQueueingClient();

    abstract Collection<CallbackRegistration> getBundleFinalizationCallbackRegistrations();

    abstract Collection<BeamFnDataReadRunner> getChannelRoots();

    String getInstructionId() {
      return this.instructionId;
    }

    void setInstructionId(String instructionId) {
      this.instructionId = instructionId;
    }

    void reset() throws Exception {
      getStartFunctionRegistry().reset();
      getFinishFunctionRegistry().reset();
      getSplitListener().clear();
      getpCollectionConsumerRegistry().reset();
      getMetricsContainerRegistry().reset();
      getStateTracker().reset();
      ExecutionStateSampler.instance().reset();
      getBundleFinalizationCallbackRegistrations().clear();
      for (ThrowingRunnable resetFunction : getResetFunctions()) {
        resetFunction.run();
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
    public void handle(
        StateRequest.Builder requestBuilder, CompletableFuture<StateResponse> response) {
      // Register each request with the phaser and arrive and deregister each time a request
      // completes.
      phaser.register();
      response.whenComplete((stateResponse, throwable) -> phaser.arriveAndDeregister());
      beamFnStateClient.handle(requestBuilder, response);
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
    public void handle(Builder requestBuilder, CompletableFuture<StateResponse> response) {
      throw new IllegalStateException(
          String.format(
              "State API calls are unsupported because the "
                  + "ProcessBundleRequest %s does not support state.",
              request));
    }
  }

  /**
   * A {@link BeamFnTimerClient} which fails all registrations because the {@link
   * ProcessBundleRequest} does not contain a Timer {@link ApiServiceDescriptor}.
   */
  private static class FailAllTimerRegistrations implements BeamFnTimerClient {
    private final ProcessBundleRequest request;

    private FailAllTimerRegistrations(ProcessBundleRequest request) {
      this.request = request;
    }

    @Override
    public <T> TimerHandler<T> register(
        LogicalEndpoint timerEndpoint,
        org.apache.beam.sdk.coders.Coder<Timer<T>> coder,
        FnDataReceiver<Timer<T>> receiver) {
      throw new IllegalStateException(
          String.format(
              "Timers are unsupported because the "
                  + "ProcessBundleRequest %s does not provide a timer ApiServiceDescriptor.",
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
  }
}
