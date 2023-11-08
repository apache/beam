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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.errorprone.annotations.CheckReturnValue;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.ProcessWorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for StreamingEngine. Given a {@link GetWorkBudget}, divides the budget and starts the
 * {@link WindmillStream.GetWorkStream}(s).
 */
@Internal
@CheckReturnValue
@ThreadSafe
public class StreamingEngineClient {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEngineClient.class);
  private static final String CONSUMER_WORKER_METADATA_THREAD = "ConsumeWorkerMetadataThread";

  private final AtomicBoolean started;
  private final JobHeader jobHeader;
  private final GetWorkBudget totalGetWorkBudget;
  private final GrpcWindmillStreamFactory streamFactory;
  private final ProcessWorkItem processWorkItem;
  private final WindmillStubFactory stubFactory;
  private final GetWorkBudgetDistributor getWorkBudgetDistributor;
  private final GrpcDispatcherClient dispatcherClient;
  private final AtomicBoolean isBudgetRefreshPaused;
  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamEngineConnectionState> connections;

  private final GetWorkBudgetRefresher getWorkBudgetRefresher;
  private final AtomicReference<Instant> lastBudgetRefresh;
  private final ThrottleTimer getWorkerMetadataThrottleTimer;
  private final CountDownLatch getWorkerMetadataReady;
  private final ExecutorService consumeWorkerMetadataExecutor;
  private final long clientId;
  /**
   * Reference to {@link GetWorkerMetadataStream} that is lazily initialized via double check
   * locking, with its initial value being null.
   */
  private volatile @Nullable GetWorkerMetadataStream getWorkerMetadataStream;

  private StreamingEngineClient(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      ProcessWorkItem processWorkItem,
      WindmillStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId) {
    this.jobHeader = jobHeader;
    this.totalGetWorkBudget = totalGetWorkBudget;
    this.started = new AtomicBoolean();
    this.streamFactory = streamFactory;
    this.processWorkItem = processWorkItem;
    this.connections = connections;
    this.stubFactory = stubFactory;
    this.getWorkBudgetDistributor = getWorkBudgetDistributor;
    this.dispatcherClient = dispatcherClient;
    this.isBudgetRefreshPaused = new AtomicBoolean(false);
    this.getWorkerMetadataThrottleTimer = new ThrottleTimer();
    this.consumeWorkerMetadataExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(StreamingEngineClient.CONSUMER_WORKER_METADATA_THREAD)
                // JVM will be responsible for shutdown and garbage collect these threads.
                .setDaemon(true)
                .setUncaughtExceptionHandler(
                    (t, e) ->
                        LOG.error(
                            "{} failed due to uncaught exception during execution. ",
                            t.getName(),
                            e))
                .build());
    this.getWorkerMetadataStream = null;
    this.getWorkerMetadataReady = new CountDownLatch(1);
    this.clientId = clientId;
    this.lastBudgetRefresh = new AtomicReference<>(Instant.EPOCH);
    this.getWorkBudgetRefresher =
        new GetWorkBudgetRefresher(
            isBudgetRefreshPaused::get,
            () -> {
              getWorkBudgetDistributor.distributeBudget(
                  connections.get().windmillStreams().values(), totalGetWorkBudget);
              lastBudgetRefresh.set(Instant.now());
            });
  }

  /**
   * Creates an instance of {@link StreamingEngineClient} and starts the {@link
   * GetWorkerMetadataStream} with an RPC to the StreamingEngine backend. {@link
   * GetWorkerMetadataStream} will populate {@link #connections} when a response is received. Calls
   * to {@link #startAndCacheStreams()} will block until {@link #connections} are populated.
   */
  public static StreamingEngineClient create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      ProcessWorkItem processWorkItem,
      WindmillStubFactory windmillGrpcStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient) {
    StreamingEngineClient streamingEngineClient =
        new StreamingEngineClient(
            jobHeader,
            totalGetWorkBudget,
            new AtomicReference<>(StreamEngineConnectionState.EMPTY),
            streamingEngineStreamFactory,
            processWorkItem,
            windmillGrpcStubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            new Random().nextLong());
    streamingEngineClient.startGetWorkerMetadataStream();
    return streamingEngineClient;
  }

  @VisibleForTesting
  static StreamingEngineClient forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      ProcessWorkItem processWorkItem,
      WindmillStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId) {
    StreamingEngineClient streamingEngineClient =
        new StreamingEngineClient(
            jobHeader,
            totalGetWorkBudget,
            connections,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            clientId);
    streamingEngineClient.startGetWorkerMetadataStream();
    return streamingEngineClient;
  }

  /**
   * Starts the streams with the {@link #connections} values. Does nothing if this has already been
   * called.
   *
   * @throws IllegalArgumentException if trying to start before {@link #connections} are set with
   *     {@link GetWorkerMetadataStream}.
   */
  public void startAndCacheStreams() {
    // Do nothing if we have already initialized the initial streams.
    if (!started.compareAndSet(false, true)) {
      return;
    }

    // This will block if we have not received the first response from GetWorkerMetadata.
    waitForFirstStreamingEngineEndpoints();
    StreamEngineConnectionState currentConnectionsState = connections.get();
    Preconditions.checkState(
        !StreamEngineConnectionState.EMPTY.equals(currentConnectionsState),
        "Cannot start streams without connections.");

    LOG.info("Starting initial GetWorkStreams with connections={}", currentConnectionsState);
    ImmutableCollection<WindmillStreamSender> windmillStreamSenders =
        currentConnectionsState.windmillStreams().values();
    getWorkBudgetDistributor.distributeBudget(
        currentConnectionsState.windmillStreams().values(), totalGetWorkBudget);
    lastBudgetRefresh.compareAndSet(Instant.EPOCH, Instant.now());
    windmillStreamSenders.forEach(WindmillStreamSender::startStreams);
    getWorkBudgetRefresher.start();
  }

  @VisibleForTesting
  void finish() {
    if (started.compareAndSet(true, false)) {
      return;
    }

    Optional.ofNullable(getWorkerMetadataStream).ifPresent(GetWorkerMetadataStream::close);
    getWorkBudgetRefresher.stop();
    consumeWorkerMetadataExecutor.shutdownNow();
  }

  private void waitForFirstStreamingEngineEndpoints() {
    try {
      getWorkerMetadataReady.await();
    } catch (InterruptedException e) {
      throw new StreamingEngineClientException(
          "Error occurred waiting for StreamingEngine backend endpoints.", e);
    }
  }

  /**
   * {@link java.util.function.Consumer< WindmillEndpoints >} used to update {@link #connections} on
   * new backend worker metadata.
   */
  private synchronized void consumeWindmillWorkerEndpoints(WindmillEndpoints newWindmillEndpoints) {
    isBudgetRefreshPaused.set(true);
    LOG.info("Consuming new windmill endpoints: {}", newWindmillEndpoints);
    ImmutableMap<Endpoint, WindmillConnection> newWindmillConnections =
        createNewWindmillConnections(ImmutableSet.copyOf(newWindmillEndpoints.windmillEndpoints()));
    ImmutableMap<WindmillConnection, WindmillStreamSender> newWindmillStreams =
        closeStaleStreamsAndCreateNewStreams(ImmutableSet.copyOf(newWindmillConnections.values()));
    ImmutableMap<Endpoint, Supplier<GetDataStream>> newGlobalDataStreams =
        createNewGlobalDataStreams(
            ImmutableSet.copyOf(newWindmillEndpoints.globalDataEndpoints().values()));

    StreamEngineConnectionState newConnectionsState =
        StreamEngineConnectionState.builder()
            .setWindmillConnections(newWindmillConnections)
            .setWindmillStreams(newWindmillStreams)
            .setGlobalDataEndpoints(newWindmillEndpoints.globalDataEndpoints())
            .setGlobalDataStreams(newGlobalDataStreams)
            .build();

    LOG.info(
        "Setting new connections: {}. Previous connections: {}.",
        newConnectionsState,
        connections.get());
    connections.set(newConnectionsState);
    isBudgetRefreshPaused.set(false);

    // Trigger on first worker metadata. Since startAndCacheStreams will block until first worker
    // metadata is ready. Afterwards, just trigger a budget refresh.
    if (getWorkerMetadataReady.getCount() > 0) {
      getWorkerMetadataReady.countDown();
    } else {
      getWorkBudgetRefresher.requestBudgetRefresh();
    }
  }

  public final ImmutableList<Long> getAndResetThrottleTimes() {
    StreamEngineConnectionState currentConnections = connections.get();

    ImmutableList<Long> keyedWorkStreamThrottleTimes =
        currentConnections.windmillStreams().values().stream()
            .map(WindmillStreamSender::getAndResetThrottleTime)
            .collect(toImmutableList());

    return ImmutableList.<Long>builder()
        .add(getWorkerMetadataThrottleTimer.getAndResetThrottleTime())
        .addAll(keyedWorkStreamThrottleTimes)
        .build();
  }

  /** Starts {@link GetWorkerMetadataStream}. */
  @SuppressWarnings({
    "FutureReturnValueIgnored", // ignoring Future returned from Executor.submit()
    "nullness" // Uninitialized value of getWorkerMetadataStream is null.
  })
  private void startGetWorkerMetadataStream() {
    // We only want to set and start this value once.
    if (getWorkerMetadataStream == null) {
      synchronized (this) {
        if (getWorkerMetadataStream == null) {
          getWorkerMetadataStream =
              streamFactory.createGetWorkerMetadataStream(
                  dispatcherClient.getDispatcherStub(),
                  getWorkerMetadataThrottleTimer,
                  endpoints ->
                      // Run this on a separate thread than the grpc stream thread.
                      consumeWorkerMetadataExecutor.submit(
                          () -> consumeWindmillWorkerEndpoints(endpoints)));
        }
      }
    }
  }

  private synchronized ImmutableMap<Endpoint, WindmillConnection> createNewWindmillConnections(
      ImmutableSet<Endpoint> newWindmillEndpoints) {
    ImmutableMap<Endpoint, WindmillConnection> currentWindmillConnections =
        connections.get().windmillConnections();
    Map<Endpoint, WindmillConnection> newWindmillConnections =
        currentWindmillConnections.entrySet().stream()
            .filter(entry -> newWindmillEndpoints.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // Reuse existing stubs if they exist.
    newWindmillEndpoints.forEach(
        newEndpoint ->
            newWindmillConnections.putIfAbsent(
                newEndpoint, WindmillConnection.from(newEndpoint, this::createWindmillStub)));

    return ImmutableMap.copyOf(newWindmillConnections);
  }

  private synchronized ImmutableMap<WindmillConnection, WindmillStreamSender>
      closeStaleStreamsAndCreateNewStreams(
          ImmutableSet<WindmillConnection> newWindmillConnections) {
    ImmutableMap<WindmillConnection, WindmillStreamSender> currentStreams =
        connections.get().windmillStreams();

    // Close the streams that are no longer valid.
    currentStreams.entrySet().stream()
        .filter(
            connectionAndStream -> !newWindmillConnections.contains(connectionAndStream.getKey()))
        .map(Map.Entry::getValue)
        .forEach(WindmillStreamSender::closeAllStreams);

    return newWindmillConnections.stream()
        .collect(
            toImmutableMap(
                Function.identity(),
                newConnection ->
                    Optional.ofNullable(currentStreams.get(newConnection))
                        .orElseGet(() -> createWindmillStreamSenderWithNoBudget(newConnection))));
  }

  private ImmutableMap<Endpoint, Supplier<GetDataStream>> createNewGlobalDataStreams(
      ImmutableSet<Endpoint> newGlobalDataEndpoints) {
    ImmutableMap<Endpoint, Supplier<GetDataStream>> currentGlobalDataStreams =
        connections.get().globalDataStreams();

    return newGlobalDataEndpoints.stream()
        .map(endpoint -> existingOrNewGetDataStreamFor(endpoint, currentGlobalDataStreams))
        .collect(toImmutableMap(Pair::getKey, Pair::getValue));
  }

  private Pair<Endpoint, Supplier<GetDataStream>> existingOrNewGetDataStreamFor(
      Endpoint endpoint, ImmutableMap<Endpoint, Supplier<GetDataStream>> currentGlobalDataStreams) {
    return Pair.of(
        endpoint,
        Preconditions.checkNotNull(
            currentGlobalDataStreams.getOrDefault(
                endpoint,
                () ->
                    streamFactory.createGetDataStream(
                        WindmillConnection.from(endpoint, this::createWindmillStub).stub(),
                        new ThrottleTimer()))));
  }

  private WindmillStreamSender createWindmillStreamSenderWithNoBudget(
      WindmillConnection connection) {
    // Initially create each stream with no budget, it will be assigned by the
    // GetWorkBudgetDistributor before the stream is started.
    return WindmillStreamSender.create(
        connection.stub(),
        GetWorkRequest.newBuilder()
            .setClientId(clientId)
            .setJobId(jobHeader.getJobId())
            .setProjectId(jobHeader.getProjectId())
            .setWorkerId(jobHeader.getWorkerId())
            .build(),
        GetWorkBudget.noBudget(),
        streamFactory,
        processWorkItem);
  }

  private CloudWindmillServiceV1Alpha1Stub createWindmillStub(Endpoint endpoint) {
    switch (stubFactory.getKind()) {
        // This is only used in tests.
      case IN_PROCESS:
        return stubFactory.inProcess().get();
        // Create stub for direct_endpoint or just default to Dispatcher stub.
      case REMOTE:
        return endpoint
            .directEndpoint()
            .map(stubFactory.remote())
            .orElseGet(dispatcherClient::getDispatcherStub);
        // Should never be called, this switch statement is exhaustive.
      default:
        throw new UnsupportedOperationException(
            "Only remote or in-process stub factories are available.");
    }
  }

  private static class StreamingEngineClientException extends IllegalStateException {
    private StreamingEngineClientException(String message, Throwable exception) {
      super(message, exception);
    }
  }
}
