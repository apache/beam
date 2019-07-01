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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateDelegator.Registration;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A high-level client for an SDK harness.
 *
 * <p>This provides a Java-friendly wrapper around {@link InstructionRequestHandler} and {@link
 * CloseableFnDataReceiver}, which handle lower-level gRPC message wrangling.
 */
public class SdkHarnessClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SdkHarnessClient.class);

  /**
   * A processor capable of creating bundles for some registered {@link ProcessBundleDescriptor}.
   */
  public class BundleProcessor {
    private final ProcessBundleDescriptor processBundleDescriptor;
    private final CompletionStage<RegisterResponse> registrationFuture;
    private final Map<String, RemoteInputDestination<WindowedValue<?>>> remoteInputs;
    private final StateDelegator stateDelegator;

    private BundleProcessor(
        ProcessBundleDescriptor processBundleDescriptor,
        CompletionStage<RegisterResponse> registrationFuture,
        Map<String, RemoteInputDestination<WindowedValue<?>>> remoteInputs,
        StateDelegator stateDelegator) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.registrationFuture = registrationFuture;
      this.remoteInputs = remoteInputs;
      this.stateDelegator = stateDelegator;
    }

    public CompletionStage<RegisterResponse> getRegistrationFuture() {
      return registrationFuture;
    }

    /**
     * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
     *
     * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
     * in the {@link BeamFnApi.ProcessBundleDescriptor}.
     *
     * <p>NOTE: It is important to {@link #close()} each bundle after all elements are emitted.
     *
     * <pre>{@code
     * try (ActiveBundle bundle = SdkHarnessClient.newBundle(...)) {
     *   FnDataReceiver<InputT> inputReceiver =
     *       (FnDataReceiver) bundle.getInputReceivers().get(mainPCollectionId);
     *   // send all main input elements ...
     * }
     * }</pre>
     */
    public ActiveBundle newBundle(
        Map<String, RemoteOutputReceiver<?>> outputReceivers,
        BundleProgressHandler progressHandler) {
      return newBundle(
          outputReceivers,
          request -> {
            throw new UnsupportedOperationException(
                String.format(
                    "The %s does not have a registered state handler.",
                    ActiveBundle.class.getSimpleName()));
          },
          progressHandler);
    }

    /**
     * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
     *
     * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
     * in the {@link BeamFnApi.ProcessBundleDescriptor}.
     *
     * <p>NOTE: It is important to {@link #close()} each bundle after all elements are emitted.
     *
     * <pre>{@code
     * try (ActiveBundle bundle = SdkHarnessClient.newBundle(...)) {
     *   FnDataReceiver<InputT> inputReceiver =
     *       (FnDataReceiver) bundle.getInputReceivers().get(mainPCollectionId);
     *   // send all elements ...
     * }
     * }</pre>
     */
    public ActiveBundle newBundle(
        Map<String, RemoteOutputReceiver<?>> outputReceivers,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler) {
      String bundleId = idGenerator.getId();

      final CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
          fnApiControlClient.handle(
              BeamFnApi.InstructionRequest.newBuilder()
                  .setInstructionId(bundleId)
                  .setProcessBundle(
                      BeamFnApi.ProcessBundleRequest.newBuilder()
                          .setProcessBundleDescriptorReference(processBundleDescriptor.getId()))
                  .build());
      LOG.debug(
          "Sent {} with ID {} for {} with ID {}",
          ProcessBundleRequest.class.getSimpleName(),
          bundleId,
          ProcessBundleDescriptor.class.getSimpleName(),
          processBundleDescriptor.getId());

      CompletionStage<BeamFnApi.ProcessBundleResponse> specificResponse =
          genericResponse.thenApply(InstructionResponse::getProcessBundle);
      Map<String, InboundDataClient> outputClients = new HashMap<>();
      for (Map.Entry<String, RemoteOutputReceiver<?>> receiver : outputReceivers.entrySet()) {
        InboundDataClient outputClient =
            attachReceiver(bundleId, receiver.getKey(), (RemoteOutputReceiver) receiver.getValue());
        outputClients.put(receiver.getKey(), outputClient);
      }

      ImmutableMap.Builder<String, CloseableFnDataReceiver<WindowedValue<?>>> dataReceiversBuilder =
          ImmutableMap.builder();
      for (Map.Entry<String, RemoteInputDestination<WindowedValue<?>>> remoteInput :
          remoteInputs.entrySet()) {
        dataReceiversBuilder.put(
            remoteInput.getKey(),
            fnApiDataService.send(
                LogicalEndpoint.of(bundleId, remoteInput.getValue().getPTransformId()),
                (Coder) remoteInput.getValue().getCoder()));
      }

      return new ActiveBundle(
          bundleId,
          specificResponse,
          dataReceiversBuilder.build(),
          outputClients,
          stateDelegator.registerForProcessBundleInstructionId(bundleId, stateRequestHandler),
          progressHandler);
    }

    private <OutputT> InboundDataClient attachReceiver(
        String bundleId,
        String ptransformId,
        RemoteOutputReceiver<WindowedValue<OutputT>> receiver) {
      return fnApiDataService.receive(
          LogicalEndpoint.of(bundleId, ptransformId), receiver.getCoder(), receiver.getReceiver());
    }
  }

  /** An active bundle for a particular {@link BeamFnApi.ProcessBundleDescriptor}. */
  public static class ActiveBundle implements RemoteBundle {
    private final String bundleId;
    private final CompletionStage<BeamFnApi.ProcessBundleResponse> response;
    private final Map<String, CloseableFnDataReceiver<WindowedValue<?>>> inputReceivers;
    private final Map<String, InboundDataClient> outputClients;
    private final StateDelegator.Registration stateRegistration;
    private final BundleProgressHandler progressHandler;

    private ActiveBundle(
        String bundleId,
        CompletionStage<ProcessBundleResponse> response,
        Map<String, CloseableFnDataReceiver<WindowedValue<?>>> inputReceivers,
        Map<String, InboundDataClient> outputClients,
        StateDelegator.Registration stateRegistration,
        BundleProgressHandler progressHandler) {
      this.bundleId = bundleId;
      this.response = response;
      this.inputReceivers = inputReceivers;
      this.outputClients = outputClients;
      this.stateRegistration = stateRegistration;
      this.progressHandler = progressHandler;
    }

    /** Returns an id used to represent this bundle. */
    @Override
    public String getId() {
      return bundleId;
    }

    /**
     * Get a map of PCollection ids to {@link FnDataReceiver receiver}s which consume input
     * elements, forwarding them to the remote environment.
     */
    @Override
    public Map<String, FnDataReceiver<WindowedValue<?>>> getInputReceivers() {
      return (Map) inputReceivers;
    }

    /**
     * Blocks until bundle processing is finished. This is comprised of:
     *
     * <ul>
     *   <li>closing each {@link #getInputReceivers() input receiver}.
     *   <li>waiting for the SDK to say that processing the bundle is finished.
     *   <li>waiting for all inbound data clients to complete
     * </ul>
     *
     * <p>This method will throw an exception if bundle processing has failed. {@link
     * Throwable#getSuppressed()} will return all the reasons as to why processing has failed.
     */
    @Override
    public void close() throws Exception {
      Exception exception = null;
      for (CloseableFnDataReceiver<?> inputReceiver : inputReceivers.values()) {
        try {
          inputReceiver.close();
        } catch (Exception e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
      try {
        // We don't have to worry about the completion stage.
        if (exception == null) {
          BeamFnApi.ProcessBundleResponse completedResponse = MoreFutures.get(response);
          progressHandler.onCompleted(completedResponse);
          if (completedResponse.getResidualRootsCount() > 0) {
            throw new IllegalStateException(
                "TODO: [BEAM-2939] residual roots in process bundle response not yet supported.");
          }
        } else {
          // TODO: [BEAM-3962] Handle aborting the bundle being processed.
          throw new IllegalStateException(
              "Processing bundle failed, " + "TODO: [BEAM-3962] abort bundle.");
        }
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
      try {
        if (exception == null) {
          stateRegistration.deregister();
        } else {
          stateRegistration.abort();
        }
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
      for (InboundDataClient outputClient : outputClients.values()) {
        try {
          // If we failed processing this bundle, we should cancel all inbound data.
          if (exception == null) {
            outputClient.awaitCompletion();
          } else {
            outputClient.cancel();
          }
        } catch (Exception e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
      }
      if (exception != null) {
        throw exception;
      }
    }
  }

  private final IdGenerator idGenerator;
  private final InstructionRequestHandler fnApiControlClient;
  private final FnDataService fnApiDataService;

  private final ConcurrentHashMap<String, BundleProcessor> clientProcessors;

  private SdkHarnessClient(
      InstructionRequestHandler fnApiControlClient,
      FnDataService fnApiDataService,
      IdGenerator idGenerator) {
    this.fnApiDataService = fnApiDataService;
    this.idGenerator = idGenerator;
    this.fnApiControlClient = fnApiControlClient;
    this.clientProcessors = new ConcurrentHashMap<>();
  }

  /**
   * Creates a client for a particular SDK harness. It is the responsibility of the caller to ensure
   * that these correspond to the same SDK harness, so control plane and data plane messages can be
   * correctly associated.
   */
  public static SdkHarnessClient usingFnApiClient(
      InstructionRequestHandler fnApiControlClient, FnDataService fnApiDataService) {
    return new SdkHarnessClient(
        fnApiControlClient, fnApiDataService, IdGenerators.incrementingLongs());
  }

  public SdkHarnessClient withIdGenerator(IdGenerator idGenerator) {
    return new SdkHarnessClient(fnApiControlClient, fnApiDataService, idGenerator);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles not containing any state
   * accesses such as:
   *
   * <ul>
   *   <li>Side inputs
   *   <li>User state
   *   <li>Remote references
   * </ul>
   *
   * <p>Note that bundle processors are cached based upon the the {@link
   * ProcessBundleDescriptor#getId() process bundle descriptor id}. A previously created instance
   * may be returned.
   */
  public BundleProcessor getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      Map<String, RemoteInputDestination<WindowedValue<?>>> remoteInputDesinations) {
    checkState(
        !descriptor.hasStateApiServiceDescriptor(),
        "The %s cannot support a %s containing a state %s.",
        BundleProcessor.class.getSimpleName(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        Endpoints.ApiServiceDescriptor.class.getSimpleName());
    return getProcessor(descriptor, remoteInputDesinations, NoOpStateDelegator.INSTANCE);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles containing state
   * accesses such as:
   *
   * <ul>
   *   <li>Side inputs
   *   <li>User state
   *   <li>Remote references
   * </ul>
   *
   * <p>Note that bundle processors are cached based upon the the {@link
   * ProcessBundleDescriptor#getId() process bundle descriptor id}. A previously created instance
   * may be returned.
   */
  public BundleProcessor getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      Map<String, RemoteInputDestination<WindowedValue<?>>> remoteInputDestinations,
      StateDelegator stateDelegator) {
    @SuppressWarnings("unchecked")
    BundleProcessor bundleProcessor =
        clientProcessors.computeIfAbsent(
            descriptor.getId(), s -> create(descriptor, remoteInputDestinations, stateDelegator));
    checkArgument(
        bundleProcessor.processBundleDescriptor.equals(descriptor),
        "The provided %s with id %s collides with an existing %s with the same id but "
            + "containing different contents.",
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        descriptor.getId(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName());
    return bundleProcessor;
  }

  /**
   * A {@link StateDelegator} that issues zero state requests to any provided {@link
   * StateRequestHandler state handlers}.
   */
  private static class NoOpStateDelegator implements StateDelegator {
    private static final NoOpStateDelegator INSTANCE = new NoOpStateDelegator();

    @Override
    public Registration registerForProcessBundleInstructionId(
        String processBundleInstructionId, StateRequestHandler handler) {
      return Registration.INSTANCE;
    }

    /** The corresponding registration for a {@link NoOpStateDelegator} that does nothing. */
    private static class Registration implements StateDelegator.Registration {
      private static final Registration INSTANCE = new Registration();

      @Override
      public void deregister() {}

      @Override
      public void abort() {}
    }
  }

  /** Registers a {@link BeamFnApi.ProcessBundleDescriptor} for future processing. */
  private BundleProcessor create(
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      Map<String, RemoteInputDestination<WindowedValue<?>>> remoteInputDestinations,
      StateDelegator stateDelegator) {

    LOG.debug("Registering {}", processBundleDescriptor);
    // TODO: validate that all the necessary data endpoints are known
    CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setRegister(
                    BeamFnApi.RegisterRequest.newBuilder()
                        .addProcessBundleDescriptor(processBundleDescriptor)
                        .build())
                .build());

    CompletionStage<RegisterResponse> registerResponseFuture =
        genericResponse.thenApply(InstructionResponse::getRegister);

    BundleProcessor bundleProcessor =
        new BundleProcessor(
            processBundleDescriptor,
            registerResponseFuture,
            remoteInputDestinations,
            stateDelegator);

    return bundleProcessor;
  }

  @Override
  public void close() {}
}
