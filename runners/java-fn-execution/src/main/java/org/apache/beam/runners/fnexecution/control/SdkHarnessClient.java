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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleProgressResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitRequest.DesiredSplit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleSplitResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.fnexecution.control.ProcessBundleDescriptors.TimerSpec;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.RemoteInputDestination;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
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
    private final List<RemoteInputDestination> remoteInputs;
    private final Map<String, Map<String, TimerSpec>> timerSpecs;
    private final StateDelegator stateDelegator;

    private BundleProcessor(
        ProcessBundleDescriptor processBundleDescriptor,
        List<RemoteInputDestination> remoteInputs,
        Map<String, Map<String, TimerSpec>> timerSpecs,
        StateDelegator stateDelegator) {
      this.processBundleDescriptor = processBundleDescriptor;
      this.remoteInputs = remoteInputs;
      this.timerSpecs = timerSpecs;
      this.stateDelegator = stateDelegator;
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
     *
     * <p>An exception during {@link #close()} will be thrown if the bundle requests finalization or
     * attempts to checkpoint by returning a {@link BeamFnApi.DelayedBundleApplication}.
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
     *   // send all main input elements ...
     * }
     * }</pre>
     *
     * <p>An exception during {@link #close()} will be thrown if the bundle requests finalization or
     * attempts to checkpoint by returning a {@link BeamFnApi.DelayedBundleApplication}.
     */
    public ActiveBundle newBundle(
        Map<String, RemoteOutputReceiver<?>> outputReceivers,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler) {
      return newBundle(
          outputReceivers,
          Collections.emptyMap(),
          stateRequestHandler,
          progressHandler,
          BundleSplitHandler.unsupported(),
          request -> {
            throw new UnsupportedOperationException(
                String.format(
                    "The %s does not have a registered bundle checkpoint handler.",
                    ActiveBundle.class.getSimpleName()));
          },
          bundleId -> {
            throw new UnsupportedOperationException(
                String.format(
                    "The %s does not have a registered bundle finalization handler.",
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
     *
     * <pre>{@code
     * try (ActiveBundle bundle = SdkHarnessClient.newBundle(...)) {
     *   FnDataReceiver<InputT> inputReceiver =
     *       (FnDataReceiver) bundle.getInputReceivers().get(mainPCollectionId);
     *   // send all main input elements ...
     * }
     * }</pre>
     *
     * <p>An exception during {@link #close()} will be thrown if the bundle requests finalization or
     * attempts to checkpoint by returning a {@link BeamFnApi.DelayedBundleApplication}.
     */
    public ActiveBundle newBundle(
        Map<String, RemoteOutputReceiver<?>> outputReceivers,
        Map<KV<String, String>, RemoteOutputReceiver<Timer<?>>> timerReceivers,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler) {
      return newBundle(
          outputReceivers,
          timerReceivers,
          stateRequestHandler,
          progressHandler,
          BundleSplitHandler.unsupported(),
          request -> {
            throw new UnsupportedOperationException(
                String.format(
                    "The %s does not have a registered bundle checkpoint handler.",
                    ActiveBundle.class.getSimpleName()));
          },
          bundleId -> {
            throw new UnsupportedOperationException(
                String.format(
                    "The %s does not have a registered bundle finalization handler.",
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
        Map<KV<String, String>, RemoteOutputReceiver<Timer<?>>> timerReceivers,
        StateRequestHandler stateRequestHandler,
        BundleProgressHandler progressHandler,
        BundleSplitHandler splitHandler,
        BundleCheckpointHandler checkpointHandler,
        BundleFinalizationHandler finalizationHandler) {
      String bundleId = idGenerator.getId();

      final CompletionStage<BeamFnApi.InstructionResponse> genericResponse =
          fnApiControlClient.handle(
              BeamFnApi.InstructionRequest.newBuilder()
                  .setInstructionId(bundleId)
                  .setProcessBundle(
                      BeamFnApi.ProcessBundleRequest.newBuilder()
                          .setProcessBundleDescriptorId(processBundleDescriptor.getId())
                          .addAllCacheTokens(stateRequestHandler.getCacheTokens()))
                  .build());
      LOG.debug(
          "Sent {} with ID {} for {} with ID {}",
          ProcessBundleRequest.class.getSimpleName(),
          bundleId,
          ProcessBundleDescriptor.class.getSimpleName(),
          processBundleDescriptor.getId());

      CompletionStage<BeamFnApi.ProcessBundleResponse> specificResponse =
          genericResponse.thenApply(InstructionResponse::getProcessBundle);
      Map<LogicalEndpoint, InboundDataClient> outputClients = new HashMap<>();
      for (Map.Entry<String, RemoteOutputReceiver<?>> receiver : outputReceivers.entrySet()) {
        LogicalEndpoint endpoint = LogicalEndpoint.data(bundleId, receiver.getKey());
        InboundDataClient outputClient =
            attachReceiver(endpoint, (RemoteOutputReceiver) receiver.getValue());
        outputClients.put(endpoint, outputClient);
      }
      for (Map.Entry<KV<String, String>, RemoteOutputReceiver<Timer<?>>> timerReceiver :
          timerReceivers.entrySet()) {
        LogicalEndpoint endpoint =
            LogicalEndpoint.timer(
                bundleId, timerReceiver.getKey().getKey(), timerReceiver.getKey().getValue());
        InboundDataClient outputClient = attachReceiver(endpoint, timerReceiver.getValue());
        outputClients.put(endpoint, outputClient);
      }

      ImmutableMap.Builder<LogicalEndpoint, CloseableFnDataReceiver> receiverBuilder =
          ImmutableMap.builder();
      for (RemoteInputDestination remoteInput : remoteInputs) {
        LogicalEndpoint endpoint = LogicalEndpoint.data(bundleId, remoteInput.getPTransformId());
        receiverBuilder.put(
            endpoint,
            new CountingFnDataReceiver(fnApiDataService.send(endpoint, remoteInput.getCoder())));
      }

      for (Map.Entry<String, Map<String, TimerSpec>> entry : timerSpecs.entrySet()) {
        for (TimerSpec timerSpec : entry.getValue().values()) {
          LogicalEndpoint endpoint =
              LogicalEndpoint.timer(bundleId, timerSpec.transformId(), timerSpec.timerId());
          receiverBuilder.put(endpoint, fnApiDataService.send(endpoint, timerSpec.coder()));
        }
      }

      return new ActiveBundle(
          bundleId,
          specificResponse,
          receiverBuilder.build(),
          outputClients,
          stateDelegator.registerForProcessBundleInstructionId(bundleId, stateRequestHandler),
          progressHandler,
          splitHandler,
          checkpointHandler,
          finalizationHandler);
    }

    private <OutputT> InboundDataClient attachReceiver(
        LogicalEndpoint endpoint, RemoteOutputReceiver<OutputT> receiver) {
      return fnApiDataService.receive(endpoint, receiver.getCoder(), receiver.getReceiver());
    }

    /** An active bundle for a particular {@link BeamFnApi.ProcessBundleDescriptor}. */
    public class ActiveBundle implements RemoteBundle {
      private final String bundleId;
      private final CompletionStage<BeamFnApi.ProcessBundleResponse> response;
      private final Map<LogicalEndpoint, CloseableFnDataReceiver> inputReceivers;
      private final Map<LogicalEndpoint, InboundDataClient> outputClients;
      private final StateDelegator.Registration stateRegistration;
      private final BundleProgressHandler progressHandler;
      private final BundleSplitHandler splitHandler;
      private final BundleCheckpointHandler checkpointHandler;
      private final BundleFinalizationHandler finalizationHandler;
      private final Phaser outstandingRequests;
      private final AtomicBoolean isClosed;
      private boolean bundleIsCompleted;

      private ActiveBundle(
          String bundleId,
          CompletionStage<ProcessBundleResponse> response,
          Map<LogicalEndpoint, CloseableFnDataReceiver> inputReceivers,
          Map<LogicalEndpoint, InboundDataClient> outputClients,
          StateDelegator.Registration stateRegistration,
          BundleProgressHandler progressHandler,
          BundleSplitHandler splitHandler,
          BundleCheckpointHandler checkpointHandler,
          BundleFinalizationHandler finalizationHandler) {
        this.bundleId = bundleId;
        this.response = response;
        this.inputReceivers = inputReceivers;
        this.outputClients = outputClients;
        this.stateRegistration = stateRegistration;
        this.progressHandler = progressHandler;
        this.splitHandler = splitHandler;
        this.checkpointHandler = checkpointHandler;
        this.finalizationHandler = finalizationHandler;
        this.outstandingRequests = new Phaser(1);
        this.isClosed = new AtomicBoolean(false);

        // Ensure that we mark when the bundle is completed
        this.response.whenComplete(
            (processBundleResponse, throwable) -> {
              synchronized (ActiveBundle.this) {
                this.bundleIsCompleted = true;
              }
            });
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
      public Map<String, FnDataReceiver> getInputReceivers() {
        ImmutableMap.Builder<String, FnDataReceiver> rval = ImmutableMap.builder();
        for (Map.Entry<LogicalEndpoint, CloseableFnDataReceiver> entry :
            inputReceivers.entrySet()) {
          if (!entry.getKey().isTimer()) {
            rval.put(entry.getKey().getTransformId(), entry.getValue());
          }
        }
        return rval.build();
      }

      @Override
      public Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers() {
        ImmutableMap.Builder<KV<String, String>, FnDataReceiver<Timer>> rval =
            ImmutableMap.builder();
        for (Map.Entry<LogicalEndpoint, CloseableFnDataReceiver> entry :
            inputReceivers.entrySet()) {
          if (entry.getKey().isTimer()) {
            rval.put(
                KV.of(entry.getKey().getTransformId(), entry.getKey().getTimerFamilyId()),
                entry.getValue());
          }
        }
        return rval.build();
      }

      @Override
      public void requestProgress() {
        synchronized (this) {
          if (bundleIsCompleted) {
            return;
          }
          outstandingRequests.register();
        }
        InstructionRequest request =
            InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setProcessBundleProgress(
                    ProcessBundleProgressRequest.newBuilder().setInstructionId(bundleId).build())
                .build();
        CompletionStage<InstructionResponse> response = fnApiControlClient.handle(request);
        response
            .whenComplete(
                (instructionResponse, throwable) -> {
                  // Don't forward empty responses.
                  if (ProcessBundleProgressResponse.getDefaultInstance()
                      .equals(instructionResponse.getProcessBundleProgress())) {
                    return;
                  }
                  progressHandler.onProgress(instructionResponse.getProcessBundleProgress());
                })
            .whenComplete((instructionResponse, throwable) -> outstandingRequests.arrive());
      }

      @Override
      public void split(double fractionOfRemainder) {
        synchronized (this) {
          if (bundleIsCompleted) {
            return;
          }
          outstandingRequests.register();
        }
        Map<String, DesiredSplit> splits = new HashMap<>();
        for (Map.Entry<LogicalEndpoint, CloseableFnDataReceiver> ptransformToInput :
            inputReceivers.entrySet()) {
          if (!ptransformToInput.getKey().isTimer()) {
            splits.put(
                ptransformToInput.getKey().getTransformId(),
                DesiredSplit.newBuilder()
                    .setFractionOfRemainder(fractionOfRemainder)
                    .setEstimatedInputElements(
                        ((CountingFnDataReceiver) ptransformToInput.getValue()).getCount())
                    .build());
          }
        }
        InstructionRequest request =
            InstructionRequest.newBuilder()
                .setInstructionId(idGenerator.getId())
                .setProcessBundleSplit(
                    ProcessBundleSplitRequest.newBuilder()
                        .setInstructionId(bundleId)
                        .putAllDesiredSplits(splits)
                        .build())
                .build();
        CompletionStage<InstructionResponse> response = fnApiControlClient.handle(request);
        response
            .whenComplete(
                (instructionResponse, throwable) -> {
                  // Don't forward empty responses representing the failure to split.
                  if (ProcessBundleSplitResponse.getDefaultInstance()
                      .equals(instructionResponse.getProcessBundleSplit())) {
                    return;
                  }
                  splitHandler.split(instructionResponse.getProcessBundleSplit());
                })
            .whenComplete((instructionResponse, throwable) -> outstandingRequests.arrive());
      }

      /**
       * Blocks until bundle processing is finished. This is comprised of:
       *
       * <ul>
       *   <li>closing each {@link #getInputReceivers() input receiver}.
       *   <li>closing each {@link #getTimerReceivers() timer receiver}.
       *   <li>waiting for the SDK to say that processing the bundle is finished.
       *   <li>waiting for all inbound data clients to complete
       * </ul>
       *
       * <p>This method will throw an exception if bundle processing has failed. {@link
       * Throwable#getSuppressed()} will return all the reasons as to why processing has failed.
       */
      @Override
      public void close() throws Exception {
        if (isClosed.getAndSet(true)) {
          return;
        }

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
            outstandingRequests.arriveAndAwaitAdvance();

            progressHandler.onCompleted(completedResponse);
            if (completedResponse.getResidualRootsCount() > 0) {
              checkpointHandler.onCheckpoint(completedResponse);
            }
            if (completedResponse.getRequiresFinalization()) {
              finalizationHandler.requestsFinalization(bundleId);
            }
          } else {
            // TODO: [BEAM-3962] Handle aborting the bundle being processed.
            throw new IllegalStateException(
                "Processing bundle failed, TODO: [BEAM-3962] abort bundle.");
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
   * Provides {@link BundleProcessor} that is capable of processing bundles not containing timers or
   * state accesses such as:
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
      List<RemoteInputDestination> remoteInputDesinations) {
    checkState(
        !descriptor.hasStateApiServiceDescriptor(),
        "The %s cannot support a %s containing a state %s.",
        BundleProcessor.class.getSimpleName(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        Endpoints.ApiServiceDescriptor.class.getSimpleName());
    return getProcessor(descriptor, remoteInputDesinations, NoOpStateDelegator.INSTANCE);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles not containing timers.
   *
   * <p>Note that bundle processors are cached based upon the the {@link
   * ProcessBundleDescriptor#getId() process bundle descriptor id}. A previously created instance
   * may be returned.
   */
  public BundleProcessor getProcessor(
      BeamFnApi.ProcessBundleDescriptor descriptor,
      List<RemoteInputDestination> remoteInputDesinations,
      StateDelegator stateDelegator) {
    checkState(
        !descriptor.hasTimerApiServiceDescriptor(),
        "The %s cannot support a %s containing a timer %s.",
        BundleProcessor.class.getSimpleName(),
        BeamFnApi.ProcessBundleDescriptor.class.getSimpleName(),
        Endpoints.ApiServiceDescriptor.class.getSimpleName());
    return getProcessor(descriptor, remoteInputDesinations, stateDelegator, Collections.EMPTY_MAP);
  }

  /**
   * Provides {@link BundleProcessor} that is capable of processing bundles containing timers and
   * state accesses such as:
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
      List<RemoteInputDestination> remoteInputDestinations,
      StateDelegator stateDelegator,
      Map<String, Map<String, TimerSpec>> timerSpecs) {
    @SuppressWarnings("unchecked")
    BundleProcessor bundleProcessor =
        clientProcessors.computeIfAbsent(
            descriptor.getId(),
            s -> create(descriptor, remoteInputDestinations, timerSpecs, stateDelegator));
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

  /**
   * A {@link CloseableFnDataReceiver} which counts the number of elements that have been accepted.
   */
  private static class CountingFnDataReceiver<T> implements CloseableFnDataReceiver<T> {
    private final CloseableFnDataReceiver delegate;
    private long count;

    private CountingFnDataReceiver(CloseableFnDataReceiver delegate) {
      this.delegate = delegate;
    }

    public long getCount() {
      return count;
    }

    @Override
    public void accept(T input) throws Exception {
      delegate.accept(input);
      count += 1;
    }

    @Override
    public void flush() throws Exception {
      delegate.flush();
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }
  }

  /** Registers a {@link BeamFnApi.ProcessBundleDescriptor} for future processing. */
  private BundleProcessor create(
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor,
      List<RemoteInputDestination> remoteInputDestinations,
      Map<String, Map<String, TimerSpec>> timerSpecs,
      StateDelegator stateDelegator) {

    LOG.debug("Registering {}", processBundleDescriptor);
    // TODO: validate that all the necessary data endpoints are known
    fnApiControlClient.registerProcessBundleDescriptor(processBundleDescriptor);
    BundleProcessor bundleProcessor =
        new BundleProcessor(
            processBundleDescriptor, remoteInputDestinations, timerSpecs, stateDelegator);

    return bundleProcessor;
  }

  @Override
  public void close() {}
}
