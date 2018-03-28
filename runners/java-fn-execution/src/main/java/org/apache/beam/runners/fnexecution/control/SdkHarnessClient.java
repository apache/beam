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

import com.google.auto.value.AutoValue;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.sdk.coders.Coder;
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
    private final String processBundleDescriptorId;
    private final CompletionStage<RegisterResponse> registrationFuture;

    private final RemoteInputDestination<WindowedValue<T>> remoteInput;

    private BundleProcessor(
        String processBundleDescriptorId,
        CompletionStage<RegisterResponse> registrationFuture,
        RemoteInputDestination<WindowedValue<T>> remoteInput) {
      this.processBundleDescriptorId = processBundleDescriptorId;
      this.registrationFuture = registrationFuture;
      this.remoteInput = remoteInput;
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
     *   // send all elements
     * }
     * }</pre>
     */
    public ActiveBundle<T> newBundle(
        Map<BeamFnApi.Target, RemoteOutputReceiver<?>> outputReceivers) {
      String bundleId = idGenerator.getId();

      final CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
          fnApiControlClient.handle(
              BeamFnApi.InstructionRequest.newBuilder()
                  .setInstructionId(bundleId)
                  .setProcessBundle(
                      BeamFnApi.ProcessBundleRequest.newBuilder()
                          .setProcessBundleDescriptorReference(processBundleDescriptorId))
                  .build());
      LOG.debug(
          "Sent {} with ID {} for {} with ID {}",
          ProcessBundleRequest.class.getSimpleName(),
          bundleId,
          ProcessBundleDescriptor.class.getSimpleName(),
          processBundleDescriptorId);

      CompletionStage<BeamFnApi.ProcessBundleResponse> specificResponse =
          genericResponse.thenApply(InstructionResponse::getProcessBundle);
      Map<BeamFnApi.Target, InboundDataClient> outputClients = new HashMap<>();
      for (Map.Entry<BeamFnApi.Target, RemoteOutputReceiver<?>> targetReceiver :
          outputReceivers.entrySet()) {
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

      return new ActiveBundle(bundleId, specificResponse, dataReceiver, outputClients);
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
  public static class ActiveBundle<InputT> implements AutoCloseable {
    private final String bundleId;
    private final CompletionStage<BeamFnApi.ProcessBundleResponse> response;
    private final CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver;
    private final Map<BeamFnApi.Target, InboundDataClient> outputClients;

    private ActiveBundle(
        String bundleId,
        CompletionStage<BeamFnApi.ProcessBundleResponse> response,
        CloseableFnDataReceiver<WindowedValue<InputT>> inputReceiver,
        Map<BeamFnApi.Target, InboundDataClient> outputClients) {
      this.bundleId = bundleId;
      this.response = response;
      this.inputReceiver = inputReceiver;
      this.outputClients = outputClients;
    }

    /**
     * Returns an id used to represent this bundle.
     */
    public String getBundleId() {
      return bundleId;
    }

    /**
     * Returns a {@link FnDataReceiver receiver} which consumes input elements forwarding them
     * to the SDK. When
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
          // TODO: Handle aborting the bundle being processed.
          throw new IllegalStateException("Processing bundle failed, TODO: abort bundle.");
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

  private final Cache<String, BundleProcessor> clientProcessors =
      CacheBuilder.newBuilder().build();

  private SdkHarnessClient(
      InstructionRequestHandler fnApiControlClient,
      FnDataService fnApiDataService,
      IdGenerator idGenerator) {
    this.fnApiDataService = fnApiDataService;
    this.idGenerator = idGenerator;
    this.fnApiControlClient = fnApiControlClient;
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

  public <T> BundleProcessor<T> getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      RemoteInputDestination<WindowedValue<T>> remoteInputDesination) {
    try {
      return clientProcessors.get(
          descriptor.getId(),
          () ->
              (BundleProcessor)
                  register(
                          Collections.singletonMap(
                              descriptor, (RemoteInputDestination) remoteInputDesination))
                      .get(descriptor.getId()));
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Registers a {@link BeamFnApi.ProcessBundleDescriptor} for future processing.
   *
   * <p>A client may block on the result future, but may also proceed without blocking.
   */
  public Map<String, BundleProcessor> register(
      Map<BeamFnApi.ProcessBundleDescriptor, RemoteInputDestination<WindowedValue<?>>>
          processBundleDescriptors) {

    LOG.debug("Registering {}", processBundleDescriptors.keySet());
    // TODO: validate that all the necessary data endpoints are known
    CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
        fnApiControlClient.handle(
            BeamFnApi.InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setRegister(
                    BeamFnApi.RegisterRequest.newBuilder()
                        .addAllProcessBundleDescriptor(processBundleDescriptors.keySet())
                        .build())
                .build());

    CompletionStage<RegisterResponse> registerResponseFuture =
        genericResponse.thenApply(InstructionResponse::getRegister);

    for (Map.Entry<ProcessBundleDescriptor, RemoteInputDestination<WindowedValue<?>>>
        descriptorInputEntry : processBundleDescriptors.entrySet()) {
      clientProcessors.put(
          descriptorInputEntry.getKey().getId(),
          new BundleProcessor<Object>(
              descriptorInputEntry.getKey().getId(),
              registerResponseFuture,
              (RemoteInputDestination) descriptorInputEntry.getValue()));
    }

    return clientProcessors.asMap();
  }

  @Override
  public void close() {}

  /**
   * A pair of {@link Coder} and {@link FnDataReceiver} which can be registered to receive elements
   * for a {@link LogicalEndpoint}.
   */
  @AutoValue
  public abstract static class RemoteOutputReceiver<T> {
    public static <T> RemoteOutputReceiver of (Coder<T> coder, FnDataReceiver<T> receiver) {
      return new AutoValue_SdkHarnessClient_RemoteOutputReceiver<>(coder, receiver);
    }

    public abstract Coder<T> getCoder();
    public abstract FnDataReceiver<T> getReceiver();
  }
}
