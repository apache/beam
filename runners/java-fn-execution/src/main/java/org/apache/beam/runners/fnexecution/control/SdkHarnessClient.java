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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.util.WindowedValue;
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
   * A supply of unique identifiers, used internally. These must be unique across all Fn API
   * clients.
   */
  public interface IdGenerator {
    String getId();
  }

  /** A supply of unique identifiers that are simply incrementing longs. */
  private static class CountingIdGenerator implements IdGenerator {
    private final AtomicLong nextId = new AtomicLong(0L);

    @Override
    public String getId() {
      return String.valueOf(nextId.incrementAndGet());
    }
  }

  /**
   * A processor capable of creating bundles for some registered {@link ProcessBundleDescriptor}.
   */
  public class BundleProcessor<T> {
    private final ProcessBundleDescriptor processBundleDescriptor;
    private final CompletionStage<RegisterResponse> registrationFuture;
    private final RemoteInputDestination<WindowedValue<T>> remoteInput;
    private final StateDelegator stateDelegator;

    private BundleProcessor(
        ProcessBundleDescriptor processBundleDescriptor,
        CompletionStage<RegisterResponse> registrationFuture,
        RemoteInputDestination<WindowedValue<T>> remoteInput,
        StateDelegator stateDelegator) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.registrationFuture = registrationFuture;
      this.remoteInput = remoteInput;
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
     * <pre>{@code
     * try (ActiveBundle<InputT> bundle = SdkHarnessClient.newBundle(...)) {
     *   FnDataReceiver<InputT> inputReceiver = bundle.getInputReceiver();
     *   // send all elements ...
     * }
     * }</pre>
     */
    public ActiveBundle<T> newBundle(
        Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers) {
      return newBundle(outputReceivers, request -> {
        throw new UnsupportedOperationException(String.format(
            "The %s does not have a registered state handler.",
            ActiveBundle.class.getSimpleName()));
      });
    }

    /**
     * Start a new bundle for the given {@link BeamFnApi.ProcessBundleDescriptor} identifier.
     *
     * <p>The input channels for the returned {@link ActiveBundle} are derived from the instructions
     * in the {@link BeamFnApi.ProcessBundleDescriptor}.
     *
     * <p>NOTE: It is important to {@link #close()} each bundle after all elements are emitted.
     * <pre>{@code
     * try (ActiveBundle<InputT> bundle = SdkHarnessClient.newBundle(...)) {
     *   FnDataReceiver<InputT> inputReceiver = bundle.getInputReceiver();
     *   // send all elements ...
     * }
     * }</pre>
     */
    public ActiveBundle<T> newBundle(
        Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers,
        StateRequestHandler stateRequestHandler) {
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
      Map<BeamFnApi.Target, InboundDataClient> outputClients = new HashMap<>();
      for (Map.Entry<BeamFnApi.Target, RemoteOutputReceiver<?>> targetReceiver
          : outputReceivers.entrySet()) {
        InboundDataClient outputClient =
            attachReceiver(
                bundleId,
                targetReceiver.getKey(),
                (RemoteOutputReceiver) targetReceiver.getValue());
        outputClients.put(targetReceiver.getKey(), outputClient);
      }

      CloseableFnDataReceiver<WindowedValue<T>> dataReceiver =
          fnApiDataService.send(
              LogicalEndpoint.of(bundleId, remoteInput.getTarget()), remoteInput.getCoder());

      return new ActiveBundle<>(
          bundleId,
          specificResponse,
          dataReceiver,
          outputClients,
          stateDelegator.registerForProcessBundleInstructionId(bundleId, stateRequestHandler));
    }

    private <OutputT> InboundDataClient attachReceiver(
        String bundleId,
        BeamFnApi.Target target,
        RemoteOutputReceiver<WindowedValue<OutputT>> receiver) {
      return fnApiDataService.receive(
          LogicalEndpoint.of(bundleId, target), receiver.getCoder(), receiver.getReceiver());
    }
  }

  /** An active bundle for a particular {@link BeamFnApi.ProcessBundleDescriptor}. */
  public static class ActiveBundle<InputT> implements RemoteBundle<InputT> {
    private final String bundleId;
    private final CompletionStage<BeamFnApi.ProcessBundleResponse> response;
    private final CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver;
    private final Map<BeamFnApi.Target, InboundDataClient> outputClients;
    private final StateDelegator.Registration stateRegistration;

    private ActiveBundle(
        String bundleId,
        CompletionStage<BeamFnApi.ProcessBundleResponse> response,
        CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver,
        Map<BeamFnApi.Target, InboundDataClient> outputClients,
        StateDelegator.Registration stateRegistration) {
      this.bundleId = bundleId;
      this.response = response;
      this.inputReceiver = inputReceiver;
      this.outputClients = outputClients;
      this.stateRegistration = stateRegistration;
    }

    /**
     * Returns an id used to represent this bundle.
     */
    public String getId() {
      return bundleId;
    }

    /**
     * Returns a {@link FnDataReceiver receiver} which consumes input elements forwarding them
     * to the SDK.
     */
    public FnDataReceiver<WindowedValue<InputT>> getInputReceiver() {
      return inputReceiver;
    }

    /**
     * Blocks till bundle processing is finished. This is comprised of:
     * <ul>
     *   <li>closing the {@link #getInputReceiver() input receiver}.</li>
     *   <li>waiting for the SDK to say that processing the bundle is finished.</li>
     *   <li>waiting for all inbound data clients to complete</li>
     * </ul>
     *
     * <p>This method will throw an exception if bundle processing has failed.
     * {@link Throwable#getSuppressed()} will return all the reasons as to why processing has
     * failed.
     */
    @Override
    public void close() throws Exception {
      Exception exception = null;
      try {
        inputReceiver.close();
      } catch (Exception e) {
        exception = e;
      }
      try {
        // We don't have to worry about the completion stage.
        if (exception == null) {
          MoreFutures.get(response);
        } else {
          // TODO: [BEAM-3962] Handle aborting the bundle being processed.
          throw new IllegalStateException("Processing bundle failed, "
              + "TODO: [BEAM-3962] abort bundle.");
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

  @SuppressWarnings("unchecked") /* SdkHarnessClient does not need to know the type information of
  BundleProcessor. */
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
    return new SdkHarnessClient(fnApiControlClient, fnApiDataService, new CountingIdGenerator());
  }

  public SdkHarnessClient withIdGenerator(IdGenerator idGenerator) {
    return new SdkHarnessClient(fnApiControlClient, fnApiDataService, idGenerator);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles not
   * containing any state accesses such as:
   * <ul>
   *   <li>Side inputs</li>
   *   <li>User state</li>
   *   <li>Remote references</li>
   * </ul>
   *
   * <p>Note that bundle processors are cached based upon the the
   * {@link ProcessBundleDescriptor#getId() process bundle descriptor id}.
   * A previously created instance may be returned.
   */
  public <T> BundleProcessor<T> getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      RemoteInputDestination<WindowedValue<T>> remoteInputDesination) {
    checkState(
        !descriptor.hasStateApiServiceDescriptor(),
        "The %s cannot support a %s containing a state %s.",
        BundleProcessor.class.getSimpleName(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        Endpoints.ApiServiceDescriptor.class.getSimpleName());
    return getProcessor(descriptor, remoteInputDesination, NoOpStateDelegator.INSTANCE);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles containing
   * state accesses such as:
   * <ul>
   *   <li>Side inputs</li>
   *   <li>User state</li>
   *   <li>Remote references</li>
   * </ul>
   *
   * <p>Note that bundle processors are cached based upon the the
   * {@link ProcessBundleDescriptor#getId() process bundle descriptor id}.
   * A previously created instance may be returned.
   */
  @SuppressWarnings("unchecked")
  public <T> BundleProcessor<T> getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      RemoteInputDestination<WindowedValue<T>> remoteInputDesination,
      StateDelegator stateDelegator) {
    BundleProcessor<T> bundleProcessor =
        clientProcessors.computeIfAbsent(
            descriptor.getId(),
            s -> create(
                descriptor,
                remoteInputDesination,
                stateDelegator));
    checkArgument(bundleProcessor.processBundleDescriptor.equals(descriptor),
        "The provided %s with id %s collides with an existing %s with the same id but "
            + "containing different contents.",
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        descriptor.getId(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName());
    return bundleProcessor;
  }

  /**
   * A {@link StateDelegator} that issues zero state requests to any provided
   * {@link StateRequestHandler state handlers}.
   */
  private static class NoOpStateDelegator implements StateDelegator {
    private static final NoOpStateDelegator INSTANCE = new NoOpStateDelegator();
    @Override
    public Registration registerForProcessBundleInstructionId(String processBundleInstructionId,
        StateRequestHandler handler) {
      return Registration.INSTANCE;
    }

    /**
     * The corresponding registration for a {@link NoOpStateDelegator} that does nothing.
     */
    private static class Registration implements StateDelegator.Registration {
      private static final Registration INSTANCE = new Registration();

      @Override
      public void deregister() {
      }

      @Override
      public void abort() {
      }
    }
  }

  /**
   * Registers a {@link BeamFnApi.ProcessBundleDescriptor} for future processing.
   */
  private <T> BundleProcessor<T> create(
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      RemoteInputDestination<WindowedValue<T>> remoteInputDestination,
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

    BundleProcessor<T> bundleProcessor = new BundleProcessor<>(
        processBundleDescriptor,
        registerResponseFuture,
        remoteInputDestination,
        stateDelegator);

    return bundleProcessor;
  }

  @Override
  public void close() {}

}
