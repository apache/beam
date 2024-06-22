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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Queues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
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
public final class StreamingEngineClient {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingEngineClient.class);
  private static final String PUBLISH_NEW_WORKER_METADATA_THREAD = "PublishNewWorkerMetadataThread";
  private static final String CONSUME_NEW_WORKER_METADATA_THREAD = "ConsumeNewWorkerMetadataThread";
  private static final String STREAM_MANAGER_THREAD = "WindmillStreamManager";

  private final JobHeader jobHeader;
  private final GrpcWindmillStreamFactory streamFactory;
  private final WorkItemScheduler workItemScheduler;
  private final ChannelCachingStubFactory channelCachingStubFactory;
  private final GrpcDispatcherClient dispatcherClient;
  private final AtomicBoolean isBudgetRefreshPaused;
  private final GetWorkBudgetRefresher getWorkBudgetRefresher;
  private final AtomicReference<Instant> lastBudgetRefresh;
  private final ThrottleTimer getWorkerMetadataThrottleTimer;
  private final ExecutorService newWorkerMetadataPublisher;
  private final ExecutorService newWorkerMetadataConsumer;
  private final long clientId;
  private final Supplier<GetWorkerMetadataStream> getWorkerMetadataStream;
  private final Queue<WindmillEndpoints> newWindmillEndpoints;
  private final Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory;
  private final Function<
          GetDataStream, BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse>>
      keyedGetDataFn;
  private final ExecutorService windmillStreamManager;

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineConnectionState> connections;

  private volatile boolean started;

  @SuppressWarnings("FutureReturnValueIgnored")
  private StreamingEngineClient(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler workItemScheduler,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Function<GetDataStream, BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse>>
          keyedGetDataFn) {
    this.jobHeader = jobHeader;
    this.keyedGetDataFn = keyedGetDataFn;
    this.started = false;
    this.streamFactory = streamFactory;
    this.workItemScheduler = workItemScheduler;
    this.connections = new AtomicReference<>(StreamingEngineConnectionState.EMPTY);
    this.channelCachingStubFactory = channelCachingStubFactory;
    this.dispatcherClient = dispatcherClient;
    this.isBudgetRefreshPaused = new AtomicBoolean(false);
    this.getWorkerMetadataThrottleTimer = new ThrottleTimer();
    this.newWorkerMetadataPublisher =
        singleThreadedExecutorServiceOf(PUBLISH_NEW_WORKER_METADATA_THREAD);
    this.newWorkerMetadataConsumer =
        singleThreadedExecutorServiceOf(CONSUME_NEW_WORKER_METADATA_THREAD);
    this.clientId = clientId;
    this.lastBudgetRefresh = new AtomicReference<>(Instant.EPOCH);
    this.newWindmillEndpoints = Queues.synchronizedQueue(EvictingQueue.create(1));
    this.getWorkBudgetRefresher =
        new GetWorkBudgetRefresher(
            isBudgetRefreshPaused::get,
            () -> {
              getWorkBudgetDistributor.distributeBudget(
                  connections.get().windmillStreams().values(), totalGetWorkBudget);
              lastBudgetRefresh.set(Instant.now());
            });
    this.getWorkerMetadataStream =
        Suppliers.memoize(
            () ->
                streamFactory.createGetWorkerMetadataStream(
                    dispatcherClient.getWindmillMetadataServiceStubBlocking(),
                    getWorkerMetadataThrottleTimer,
                    endpoints ->
                        // Run this on a separate thread than the grpc stream thread.
                        newWorkerMetadataPublisher.submit(
                            () -> newWindmillEndpoints.add(endpoints))));
    this.workCommitterFactory = workCommitterFactory;
    this.windmillStreamManager =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(STREAM_MANAGER_THREAD).build());
  }

  private static ExecutorService singleThreadedExecutorServiceOf(String threadName) {
    return Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat(threadName)
            .setUncaughtExceptionHandler(
                (t, e) -> {
                  LOG.error(
                      "{} failed due to uncaught exception during execution. ", t.getName(), e);
                  throw new StreamingEngineClientException(e);
                })
            .build());
  }

  /**
   * Creates an instance of {@link StreamingEngineClient} in a non-started state.
   *
   * @implNote Does not block the calling thread. Callers must explicitly call {@link #start()}.
   */
  public static StreamingEngineClient create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Function<GetDataStream, BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse>>
          keyedGetDataFn) {
    return new StreamingEngineClient(
        jobHeader,
        totalGetWorkBudget,
        streamingEngineStreamFactory,
        processWorkItem,
        channelCachingStubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        /* clientId= */ new Random().nextLong(),
        workCommitterFactory,
        keyedGetDataFn);
  }

  @VisibleForTesting
  static StreamingEngineClient forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Function<GetDataStream, BiFunction<String, KeyedGetDataRequest, KeyedGetDataResponse>>
          keyedGetDataFn) {
    StreamingEngineClient streamingEngineClient =
        new StreamingEngineClient(
            jobHeader,
            totalGetWorkBudget,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            clientId,
            workCommitterFactory,
            keyedGetDataFn);
    streamingEngineClient.start();
    return streamingEngineClient;
  }

  @SuppressWarnings("ReturnValueIgnored")
  public synchronized void start() {
    Preconditions.checkState(!started, "StreamingEngineClient cannot start twice.");
    // Starts the stream, this value is memoized.
    getWorkerMetadataStream.get();
    startWorkerMetadataConsumer();
    getWorkBudgetRefresher.start();
    started = true;
  }

  public ImmutableSet<HostAndPort> currentWindmillEndpoints() {
    return connections.get().windmillConnections().keySet().stream()
        .map(Endpoint::directEndpoint)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(
            windmillServiceAddress ->
                windmillServiceAddress.getKind() == WindmillServiceAddress.Kind.GCP_SERVICE_ADDRESS
                    ? windmillServiceAddress.gcpServiceAddress()
                    : windmillServiceAddress.authenticatedGcpServiceAddress().gcpServiceAddress())
        .collect(toImmutableSet());
  }

  /**
   * Fetches {@link GetDataStream} mapped to globalDataKey if one exists, or defaults to {@link
   * GetDataStream} pointing to dispatcher.
   */
  public GetDataStream getGlobalDataStream(String globalDataKey) {
    return Optional.ofNullable(connections.get().globalDataStreams().get(globalDataKey))
        .map(Supplier::get)
        .orElseGet(
            () ->
                streamFactory.createGetDataStream(
                    dispatcherClient.getWindmillServiceStub(), new ThrottleTimer()));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void startWorkerMetadataConsumer() {
    newWorkerMetadataConsumer.submit(
        () -> {
          while (true) {
            Optional.ofNullable(newWindmillEndpoints.poll())
                .ifPresent(this::consumeWindmillWorkerEndpoints);
          }
        });
  }

  @VisibleForTesting
  public synchronized void finish() {
    Preconditions.checkState(started, "StreamingEngineClient never started.");
    getWorkerMetadataStream.get().close();
    getWorkBudgetRefresher.stop();
    newWorkerMetadataPublisher.shutdownNow();
    newWorkerMetadataConsumer.shutdownNow();
    channelCachingStubFactory.shutdown();
  }

  /**
   * {@link java.util.function.Consumer<WindmillEndpoints>} used to update {@link #connections} on
   * new backend worker metadata.
   */
  private synchronized void consumeWindmillWorkerEndpoints(WindmillEndpoints newWindmillEndpoints) {
    isBudgetRefreshPaused.set(true);
    LOG.info("Consuming new windmill endpoints: {}", newWindmillEndpoints);
    ImmutableMap<Endpoint, WindmillConnection> newWindmillConnections =
        createNewWindmillConnections(newWindmillEndpoints.windmillEndpoints());

    CompletableFuture<Void> closeStaleStreams = closeStaleStreams(newWindmillConnections.values());

    StreamingEngineConnectionState newConnectionsState =
        StreamingEngineConnectionState.builder()
            .setWindmillConnections(newWindmillConnections)
            .setWindmillStreams(createAndStartNewStreams(newWindmillConnections.values()).join())
            .setGlobalDataStreams(
                createNewGlobalDataStreams(newWindmillEndpoints.globalDataEndpoints()))
            .build();

    LOG.info(
        "Setting new connections: {}. Previous connections: {}.",
        newConnectionsState,
        connections.get());
    connections.set(newConnectionsState);
    isBudgetRefreshPaused.set(false);
    getWorkBudgetRefresher.requestBudgetRefresh();
    closeStaleStreams.join();
  }

  /** Close the streams that are no longer valid in parallel. */
  private synchronized CompletableFuture<Void> closeStaleStreams(
      Collection<WindmillConnection> newWindmillConnections) {
    ImmutableMap<WindmillConnection, WindmillStreamSender> currentStreams =
        connections.get().windmillStreams();
    return CompletableFuture.allOf(
        currentStreams.entrySet().stream()
            .filter(
                connectionAndStream ->
                    !newWindmillConnections.contains(connectionAndStream.getKey()))
            .map(
                entry ->
                    CompletableFuture.runAsync(
                        () -> {
                          entry.getValue().closeAllStreams();
                          entry
                              .getKey()
                              .directEndpoint()
                              .ifPresent(channelCachingStubFactory::remove);
                        },
                        windmillStreamManager))
            .toArray(CompletableFuture[]::new));
  }

  private synchronized CompletableFuture<ImmutableMap<WindmillConnection, WindmillStreamSender>>
      createAndStartNewStreams(Collection<WindmillConnection> newWindmillConnections) {
    ImmutableMap<WindmillConnection, WindmillStreamSender> currentStreams =
        connections.get().windmillStreams();
    CompletionStage<List<Pair<WindmillConnection, WindmillStreamSender>>>
        connectionAndSenderFuture =
            MoreFutures.allAsList(
                newWindmillConnections.stream()
                    .map(
                        connection ->
                            MoreFutures.supplyAsync(
                                () ->
                                    Pair.of(
                                        connection,
                                        Optional.ofNullable(currentStreams.get(connection))
                                            .orElseGet(
                                                () ->
                                                    createAndStartWindmillStreamSenderFor(
                                                        connection))),
                                windmillStreamManager))
                    .collect(Collectors.toList()));

    return connectionAndSenderFuture
        .thenApply(
            connectionsAndSenders ->
                connectionsAndSenders.stream()
                    .collect(toImmutableMap(Pair::getLeft, Pair::getRight)))
        .toCompletableFuture();
  }

  /** Add up all the throttle times of all streams including GetWorkerMetadataStream. */
  public long getAndResetThrottleTimes() {
    return connections.get().windmillStreams().values().stream()
            .map(WindmillStreamSender::getAndResetThrottleTime)
            .reduce(0L, Long::sum)
        + getWorkerMetadataThrottleTimer.getAndResetThrottleTime();
  }

  public long currentActiveCommitBytes() {
    return connections.get().windmillStreams().values().stream()
        .map(WindmillStreamSender::getCurrentActiveCommitBytes)
        .reduce(0L, Long::sum);
  }

  @VisibleForTesting
  StreamingEngineConnectionState getCurrentConnections() {
    return connections.get();
  }

  private synchronized ImmutableMap<Endpoint, WindmillConnection> createNewWindmillConnections(
      List<Endpoint> newWindmillEndpoints) {
    ImmutableMap<Endpoint, WindmillConnection> currentConnections =
        connections.get().windmillConnections();
    return newWindmillEndpoints.stream()
        .collect(
            toImmutableMap(
                Function.identity(),
                // Reuse existing stubs if they exist.
                endpoint ->
                    currentConnections.getOrDefault(
                        endpoint, WindmillConnection.from(endpoint, this::createWindmillStub))));
  }

  private ImmutableMap<String, Supplier<GetDataStream>> createNewGlobalDataStreams(
      ImmutableMap<String, Endpoint> newGlobalDataEndpoints) {
    ImmutableMap<String, Supplier<GetDataStream>> currentGlobalDataStreams =
        connections.get().globalDataStreams();
    return newGlobalDataEndpoints.entrySet().stream()
        .collect(
            toImmutableMap(
                Entry::getKey,
                keyedEndpoint ->
                    existingOrNewGetDataStreamFor(keyedEndpoint, currentGlobalDataStreams)));
  }

  private Supplier<GetDataStream> existingOrNewGetDataStreamFor(
      Entry<String, Endpoint> keyedEndpoint,
      ImmutableMap<String, Supplier<GetDataStream>> currentGlobalDataStreams) {
    return Preconditions.checkNotNull(
        currentGlobalDataStreams.getOrDefault(
            keyedEndpoint.getKey(),
            () ->
                streamFactory.createGetDataStream(
                    newOrExistingStubFor(keyedEndpoint.getValue()), new ThrottleTimer())));
  }

  private CloudWindmillServiceV1Alpha1Stub newOrExistingStubFor(Endpoint endpoint) {
    return Optional.ofNullable(connections.get().windmillConnections().get(endpoint))
        .map(WindmillConnection::stub)
        .orElseGet(() -> createWindmillStub(endpoint));
  }

  private WindmillStreamSender createAndStartWindmillStreamSenderFor(
      WindmillConnection connection) {
    // Initially create each stream with no budget. The budget will be eventually assigned by the
    // GetWorkBudgetDistributor.
    WindmillStreamSender windmillStreamSender =
        WindmillStreamSender.create(
            connection.backendWorkerToken().orElseGet(() -> ""),
            connection.stub(),
            GetWorkRequest.newBuilder()
                .setClientId(clientId)
                .setJobId(jobHeader.getJobId())
                .setProjectId(jobHeader.getProjectId())
                .setWorkerId(jobHeader.getWorkerId())
                .build(),
            GetWorkBudget.noBudget(),
            streamFactory,
            workItemScheduler,
            workCommitterFactory,
            keyedGetDataFn);
    windmillStreamSender.startStreams();
    return windmillStreamSender;
  }

  private CloudWindmillServiceV1Alpha1Stub createWindmillStub(Endpoint endpoint) {
    return endpoint
        .directEndpoint()
        .map(channelCachingStubFactory::createWindmillServiceStub)
        .orElseGet(dispatcherClient::getWindmillServiceStub);
  }

  private static class StreamingEngineClientException extends IllegalStateException {

    private StreamingEngineClientException(Throwable exception) {
      super(exception);
    }
  }
}
