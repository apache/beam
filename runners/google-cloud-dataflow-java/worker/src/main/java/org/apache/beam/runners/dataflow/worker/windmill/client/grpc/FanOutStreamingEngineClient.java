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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.CheckReturnValue;
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
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Queues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for StreamingEngine that manages fan out. Given a {@link GetWorkBudget}, divides the
 * budget and starts the {@link WindmillStream.GetWorkStream}(s).
 */
@Internal
@CheckReturnValue
@ThreadSafe
public final class FanOutStreamingEngineClient {
  private static final Logger LOG = LoggerFactory.getLogger(FanOutStreamingEngineClient.class);
  private static final String PUBLISH_NEW_WORKER_METADATA_THREAD = "PublishNewWorkerMetadataThread";
  private static final String CONSUME_NEW_WORKER_METADATA_THREAD = "ConsumeNewWorkerMetadataThread";
  private static final String STREAM_MANAGER_THREAD = "WindmillStreamManager";

  private final AtomicBoolean started;
  private final JobHeader jobHeader;
  private final GrpcWindmillStreamFactory streamFactory;
  private final WorkItemProcessor workItemProcessor;
  private final ChannelCachingStubFactory channelCachingStubFactory;
  private final GrpcDispatcherClient dispatcherClient;
  private final AtomicBoolean isBudgetRefreshPaused;
  private final GetWorkBudgetRefresher getWorkBudgetRefresher;
  private final AtomicReference<Instant> lastBudgetRefresh;
  private final ThrottleTimer getWorkerMetadataThrottleTimer;
  private final ExecutorService newWorkerMetadataPublisher;
  private final ExecutorService newWorkerMetadataConsumer;
  private final ExecutorService windmillStreamManager;
  private final long clientId;
  private final Supplier<GetWorkerMetadataStream> getWorkerMetadataStream;
  private final Queue<WindmillEndpoints> newWindmillEndpoints;

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineConnectionState> connections;

  @SuppressWarnings("FutureReturnValueIgnored")
  private FanOutStreamingEngineClient(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamingEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemProcessor workItemProcessor,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId) {
    this.jobHeader = jobHeader;
    this.started = new AtomicBoolean();
    this.streamFactory = streamFactory;
    this.workItemProcessor = workItemProcessor;
    this.connections = connections;
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
                    dispatcherClient.getWindmillMetadataServiceStub(),
                    getWorkerMetadataThrottleTimer,
                    endpoints ->
                        // Run this on a separate thread than the grpc stream thread.
                        newWorkerMetadataPublisher.submit(
                            () -> newWindmillEndpoints.add(endpoints))));
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
   * Creates an instance of {@link FanOutStreamingEngineClient} and starts the {@link
   * GetWorkerMetadataStream} with an RPC to the StreamingEngine backend. {@link
   * GetWorkerMetadataStream} will populate {@link #connections} when a response is received.
   *
   * @implNote Does not block the calling thread.
   */
  public static FanOutStreamingEngineClient create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemProcessor processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient) {
    FanOutStreamingEngineClient fanOutStreamingEngineClient =
        new FanOutStreamingEngineClient(
            jobHeader,
            totalGetWorkBudget,
            new AtomicReference<>(StreamingEngineConnectionState.EMPTY),
            streamingEngineStreamFactory,
            processWorkItem,
            channelCachingStubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            new Random().nextLong());
    fanOutStreamingEngineClient.start();
    return fanOutStreamingEngineClient;
  }

  @VisibleForTesting
  static FanOutStreamingEngineClient forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamingEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemProcessor processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId) {
    FanOutStreamingEngineClient fanOutStreamingEngineClient =
        new FanOutStreamingEngineClient(
            jobHeader,
            totalGetWorkBudget,
            connections,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            clientId);
    fanOutStreamingEngineClient.start();
    return fanOutStreamingEngineClient;
  }

  private void start() {
    startGetWorkerMetadataStream();
    startWorkerMetadataConsumer();
    getWorkBudgetRefresher.start();
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
  void finish() {
    if (!started.compareAndSet(true, false)) {
      return;
    }
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

    closeStaleStreams.join();
    LOG.info(
        "Setting new connections: {}. Previous connections: {}.",
        newConnectionsState,
        connections.get());
    connections.set(newConnectionsState);
    isBudgetRefreshPaused.set(false);
    getWorkBudgetRefresher.requestBudgetRefresh();
  }

  public ImmutableList<Long> getAndResetThrottleTimes() {
    StreamingEngineConnectionState currentConnections = connections.get();

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
    "ReturnValueIgnored", // starts the stream, this value is memoized.
  })
  private void startGetWorkerMetadataStream() {
    started.set(true);
    getWorkerMetadataStream.get();
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
            connection.stub(),
            GetWorkRequest.newBuilder()
                .setClientId(clientId)
                .setJobId(jobHeader.getJobId())
                .setProjectId(jobHeader.getProjectId())
                .setWorkerId(jobHeader.getWorkerId())
                .build(),
            GetWorkBudget.noBudget(),
            streamFactory,
            workItemProcessor);
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
