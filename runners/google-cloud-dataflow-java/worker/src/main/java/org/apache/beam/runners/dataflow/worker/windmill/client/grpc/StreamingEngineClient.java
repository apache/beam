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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
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
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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
  private final long clientId;
  private final Supplier<GetWorkerMetadataStream> getWorkerMetadataStream;
  private final Queue<WindmillEndpoints> newWindmillEndpoints;
  private final Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory;
  private final Consumer<List<Windmill.ComputationHeartbeatResponse>> heartbeatResponseProcessor;

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineConnectionState> connections;

  @SuppressWarnings("FutureReturnValueIgnored")
  private StreamingEngineClient(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamingEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemProcessor workItemProcessor,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> heartbeatResponseProcessor) {
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
                    dispatcherClient.getWindmillMetadataServiceStubBlocking(),
                    getWorkerMetadataThrottleTimer,
                    endpoints ->
                        // Run this on a separate thread than the grpc stream thread.
                        newWorkerMetadataPublisher.submit(
                            () -> newWindmillEndpoints.add(endpoints))));
    this.workCommitterFactory = workCommitterFactory;
    this.heartbeatResponseProcessor = heartbeatResponseProcessor;
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
   * Creates an instance of {@link StreamingEngineClient} and starts the {@link
   * GetWorkerMetadataStream} with an RPC to the StreamingEngine backend. {@link
   * GetWorkerMetadataStream} will populate {@link #connections} when a response is received.
   *
   * @implNote Does not block the calling thread.
   */
  public static StreamingEngineClient create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemProcessor processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> heartbeatProcessor,
      AtomicReference<StreamingEngineConnectionState> streamingEngineConnectionsState) {
    // Set the connectionState to a valid state if it is null.
    streamingEngineConnectionsState.compareAndSet(null, StreamingEngineConnectionState.EMPTY);
    return new StreamingEngineClient(
        jobHeader,
        totalGetWorkBudget,
        streamingEngineConnectionsState,
        streamingEngineStreamFactory,
        processWorkItem,
        channelCachingStubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        new Random().nextLong(),
        workCommitterFactory,
        heartbeatProcessor);
  }

  @VisibleForTesting
  static StreamingEngineClient forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      AtomicReference<StreamingEngineConnectionState> connections,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemProcessor processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      Consumer<List<Windmill.ComputationHeartbeatResponse>> heartbeatResponseProcessor) {
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
            clientId,
            workCommitterFactory,
            heartbeatResponseProcessor);
    streamingEngineClient.start();
    return streamingEngineClient;
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      startGetWorkerMetadataStream();
      startWorkerMetadataConsumer();
      getWorkBudgetRefresher.start();
    }
  }

  public ImmutableSet<HostAndPort> currentWindmillEndpoints() {
    return connections.get().windmillConnections().keySet().stream()
        .map(Endpoint::directEndpoint)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(
            windmillServiceAddress ->
                windmillServiceAddress.getKind() != WindmillServiceAddress.Kind.IPV6)
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

  public void finish() {
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

    StreamingEngineConnectionState newConnectionsState =
        StreamingEngineConnectionState.builder()
            .setWindmillConnections(newWindmillConnections)
            .setWindmillStreams(
                closeStaleStreamsAndCreateNewStreams(newWindmillConnections.values()))
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
  }

  public long getAndResetThrottleTimes() {
    StreamingEngineConnectionState currentConnections = connections.get();

    ImmutableList<Long> keyedWorkStreamThrottleTimes =
        currentConnections.windmillStreams().values().stream()
            .map(WindmillStreamSender::getAndResetThrottleTime)
            .collect(toImmutableList());

    return ImmutableList.<Long>builder()
        .add(getWorkerMetadataThrottleTimer.getAndResetThrottleTime())
        .addAll(keyedWorkStreamThrottleTimes).build().stream()
        .reduce(0L, Long::sum);
  }

  public long currentActiveCommitBytes() {
    return connections.get().windmillStreams().values().stream()
        .map(WindmillStreamSender::getCurrentActiveCommitBytes)
        .reduce(0L, Long::sum);
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

  private synchronized ImmutableMap<WindmillConnection, WindmillStreamSender>
      closeStaleStreamsAndCreateNewStreams(Collection<WindmillConnection> newWindmillConnections) {
    ImmutableMap<WindmillConnection, WindmillStreamSender> currentStreams =
        connections.get().windmillStreams();

    // Close the streams that are no longer valid.
    currentStreams.entrySet().stream()
        .filter(
            connectionAndStream -> !newWindmillConnections.contains(connectionAndStream.getKey()))
        .forEach(
            entry -> {
              entry.getValue().closeAllStreams();
              entry.getKey().directEndpoint().ifPresent(channelCachingStubFactory::remove);
            });

    return newWindmillConnections.stream()
        .collect(
            toImmutableMap(
                Function.identity(),
                newConnection ->
                    Optional.ofNullable(currentStreams.get(newConnection))
                        .orElseGet(() -> createAndStartWindmillStreamSenderFor(newConnection))));
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
            workItemProcessor,
            workCommitterFactory,
            heartbeatResponseProcessor);
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
