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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RegisterResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
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
   * A {@link StateDelegator} that issues zero state requests to any provided
   * {@link StateRequestHandler state handlers}.
   */
  private static class NoOpStateDelegator implements StateDelegator {
    private static final NoOpStateDelegator INSTANCE = new NoOpStateDelegator();
    @Override
    public Registration registerForProcessBundleInstructionId(
        String processBundleInstructionId,
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
  public <T> BundleProcessor<T> getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      RemoteInputDestination<WindowedValue<T>> remoteInputDestination,
      StateDelegator stateDelegator) {
    BundleProcessor<T> bundleProcessor =
        clientProcessors.computeIfAbsent(
            descriptor.getId(),
            s -> create(
                descriptor,
                (RemoteInputDestination) remoteInputDestination,
                stateDelegator));
    checkArgument(bundleProcessor.getProcessBundleDescriptor().equals(descriptor),
        "The provided %s with id %s collides with an existing %s with the same id but "
            + "containing different contents.",
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        descriptor.getId(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName());
    return bundleProcessor;
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
        fnApiControlClient, fnApiDataService, idGenerator, processBundleDescriptor,
        registerResponseFuture,
        remoteInputDestination,
        stateDelegator);

    return bundleProcessor;
  }

  @Override
  public void close() {}

}
