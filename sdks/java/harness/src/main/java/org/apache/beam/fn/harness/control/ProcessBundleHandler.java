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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.PTransformRunnerFactory;
import org.apache.beam.fn.harness.PTransformRunnerFactory.Registrar;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.fn.harness.state.BeamFnStateGrpcClientCache;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest.Builder;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes {@link BeamFnApi.ProcessBundleRequest}s by materializing
 * the set of required runners for each {@link RunnerApi.FunctionSpec},
 * wiring them together based upon the {@code input} and {@code output} map definitions.
 *
 * <p>Finally executes the DAG based graph by starting all runners in reverse topological order,
 * and finishing all runners in forward topological order.
 */
public class ProcessBundleHandler {

  // TODO: What should the initial set of URNs be?
  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";
  public static final String JAVA_SOURCE_URN = "urn:org.apache.beam:source:java:0.1";

  private static final Logger LOG = LoggerFactory.getLogger(ProcessBundleHandler.class);
  private static final Map<String, PTransformRunnerFactory> REGISTERED_RUNNER_FACTORIES;

  static {
    Set<Registrar> pipelineRunnerRegistrars =
        Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
    pipelineRunnerRegistrars.addAll(
        Lists.newArrayList(ServiceLoader.load(Registrar.class,
            ReflectHelpers.findClassLoader())));

    // Load all registered PTransform runner factories.
    ImmutableMap.Builder<String, PTransformRunnerFactory> builder =
        ImmutableMap.builder();
    for (Registrar registrar : pipelineRunnerRegistrars) {
      builder.putAll(registrar.getPTransformRunnerFactories());
    }
    REGISTERED_RUNNER_FACTORIES = builder.build();
  }

  private final PipelineOptions options;
  private final Function<String, Message> fnApiRegistry;
  private final BeamFnDataClient beamFnDataClient;
  private final BeamFnStateGrpcClientCache beamFnStateGrpcClientCache;
  private final Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap;
  private final PTransformRunnerFactory defaultPTransformRunnerFactory;


  public ProcessBundleHandler(
      PipelineOptions options,
      Function<String, Message> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache) {
    this(options,
        fnApiRegistry,
        beamFnDataClient,
        beamFnStateGrpcClientCache,
        REGISTERED_RUNNER_FACTORIES);
  }

  @VisibleForTesting
  ProcessBundleHandler(
      PipelineOptions options,
      Function<String, Message> fnApiRegistry,
      BeamFnDataClient beamFnDataClient,
      BeamFnStateGrpcClientCache beamFnStateGrpcClientCache,
      Map<String, PTransformRunnerFactory> urnToPTransformRunnerFactoryMap) {
    this.options = options;
    this.fnApiRegistry = fnApiRegistry;
    this.beamFnDataClient = beamFnDataClient;
    this.beamFnStateGrpcClientCache = beamFnStateGrpcClientCache;
    this.urnToPTransformRunnerFactoryMap = urnToPTransformRunnerFactoryMap;
    this.defaultPTransformRunnerFactory =
        new UnknownPTransformRunnerFactory(urnToPTransformRunnerFactoryMap.keySet());
  }

  private void createRunnerAndConsumersForPTransformRecursively(
      BeamFnStateClient beamFnStateClient,
      String pTransformId,
      RunnerApi.PTransform pTransform,
      Supplier<String> processBundleInstructionId,
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      Multimap<String, String> pCollectionIdsToConsumingPTransforms,
      Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
      Consumer<ThrowingRunnable> addStartFunction,
      Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

    // Recursively ensure that all consumers of the output PCollection have been created.
    // Since we are creating the consumers first, we know that the we are building the DAG
    // in reverse topological order.
    for (String pCollectionId : pTransform.getOutputsMap().values()) {
      // If we have created the consumers for this PCollection we can skip it.
      if (pCollectionIdsToConsumers.containsKey(pCollectionId)) {
        continue;
      }

      for (String consumingPTransformId : pCollectionIdsToConsumingPTransforms.get(pCollectionId)) {
        createRunnerAndConsumersForPTransformRecursively(
            beamFnStateClient,
            consumingPTransformId,
            processBundleDescriptor.getTransformsMap().get(consumingPTransformId),
            processBundleInstructionId,
            processBundleDescriptor,
            pCollectionIdsToConsumingPTransforms,
            pCollectionIdsToConsumers,
            addStartFunction,
            addFinishFunction);
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

    urnToPTransformRunnerFactoryMap.getOrDefault(
        pTransform.getSpec().getUrn(), defaultPTransformRunnerFactory)
        .createRunnerForPTransform(
            options,
            beamFnDataClient,
            beamFnStateClient,
            pTransformId,
            pTransform,
            processBundleInstructionId,
            processBundleDescriptor.getPcollectionsMap(),
            processBundleDescriptor.getCodersMap(),
            processBundleDescriptor.getWindowingStrategiesMap(),
            pCollectionIdsToConsumers,
            addStartFunction,
            addFinishFunction);
  }

  public BeamFnApi.InstructionResponse.Builder processBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    BeamFnApi.InstructionResponse.Builder response =
        BeamFnApi.InstructionResponse.newBuilder()
            .setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance());

    String bundleId = request.getProcessBundle().getProcessBundleDescriptorReference();
    BeamFnApi.ProcessBundleDescriptor bundleDescriptor =
        (BeamFnApi.ProcessBundleDescriptor) fnApiRegistry.apply(bundleId);

    Multimap<String, String> pCollectionIdsToConsumingPTransforms = HashMultimap.create();
    Multimap<String,
        FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers =
        HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();

    // Build a multimap of PCollection ids to PTransform ids which consume said PCollections
    for (Map.Entry<String, RunnerApi.PTransform> entry
        : bundleDescriptor.getTransformsMap().entrySet()) {
      for (String pCollectionId : entry.getValue().getInputsMap().values()) {
        pCollectionIdsToConsumingPTransforms.put(pCollectionId, entry.getKey());
      }
    }

    // Instantiate a State API call handler depending on whether a State Api service descriptor
    // was specified.
    try (HandleStateCallsForBundle beamFnStateClient =
        bundleDescriptor.hasStateApiServiceDescriptor()
        ? new BlockTillStateCallsFinish(beamFnStateGrpcClientCache.forApiServiceDescriptor(
            bundleDescriptor.getStateApiServiceDescriptor()))
        : new FailAllStateCallsForBundle(request.getProcessBundle())) {
      // Create a BeamFnStateClient
      for (Map.Entry<String, RunnerApi.PTransform> entry
          : bundleDescriptor.getTransformsMap().entrySet()) {
        // Skip anything which isn't a root
        // TODO: Remove source as a root and have it be triggered by the Runner.
        if (!DATA_INPUT_URN.equals(entry.getValue().getSpec().getUrn())
            && !JAVA_SOURCE_URN.equals(entry.getValue().getSpec().getUrn())
            && !PTransformTranslation.READ_TRANSFORM_URN.equals(
                entry.getValue().getSpec().getUrn())) {
          continue;
        }

        createRunnerAndConsumersForPTransformRecursively(
            beamFnStateClient,
            entry.getKey(),
            entry.getValue(),
            request::getInstructionId,
            bundleDescriptor,
            pCollectionIdsToConsumingPTransforms,
            pCollectionIdsToConsumers,
            startFunctions::add,
            finishFunctions::add);
      }

      // Already in reverse topological order so we don't need to do anything.
      for (ThrowingRunnable startFunction : startFunctions) {
        LOG.debug("Starting function {}", startFunction);
        startFunction.run();
      }

      // Need to reverse this since we want to call finish in topological order.
      for (ThrowingRunnable finishFunction : Lists.reverse(finishFunctions)) {
        LOG.debug("Finishing function {}", finishFunction);
        finishFunction.run();
      }
    }

    return response;
  }

  /**
   * A {@link BeamFnStateClient} which counts the number of outstanding {@link StateRequest}s and
   * blocks till they are all finished.
   */
  private class BlockTillStateCallsFinish extends HandleStateCallsForBundle {
    private final BeamFnStateClient beamFnStateClient;
    private final Phaser phaser;
    private int currentPhase;

    private BlockTillStateCallsFinish(BeamFnStateClient beamFnStateClient) {
      this.beamFnStateClient = beamFnStateClient;
      this.phaser  = new Phaser(1 /* initial party is the process bundle handler */);
      this.currentPhase = phaser.getPhase();
    }

    @Override
    public void close() throws Exception {
      int unarrivedParties = phaser.getUnarrivedParties();
      if (unarrivedParties > 0) {
        LOG.debug("Waiting for {} parties to arrive before closing, current phase {}.",
            unarrivedParties, currentPhase);
      }
      currentPhase = phaser.arriveAndAwaitAdvance();
    }

    @Override
    public void handle(StateRequest.Builder requestBuilder,
        CompletableFuture<StateResponse> response) {
      // Register each request with the phaser and arrive and deregister each time a request
      // completes.
      phaser.register();
      response.whenComplete((stateResponse, throwable) -> phaser.arriveAndDeregister());
      beamFnStateClient.handle(requestBuilder, response);
    }
  }

  /**
   * A {@link BeamFnStateClient} which fails all requests because the {@link ProcessBundleRequest}
   * does not contain a State API {@link ApiServiceDescriptor}.
   */
  private class FailAllStateCallsForBundle extends HandleStateCallsForBundle {
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
      throw new IllegalStateException(String.format("State API calls are unsupported because the "
          + "ProcessBundleRequest %s does not support state.", request));
    }
  }

  private abstract class HandleStateCallsForBundle implements AutoCloseable, BeamFnStateClient {
  }

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
        String pTransformId,
        RunnerApi.PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) {
      String message =
          String.format(
              "No factory registered for %s, known factories %s",
              pTransform.getSpec().getUrn(), knownUrns);
      LOG.error(message);
      throw new IllegalStateException(message);
    }
  }
}
