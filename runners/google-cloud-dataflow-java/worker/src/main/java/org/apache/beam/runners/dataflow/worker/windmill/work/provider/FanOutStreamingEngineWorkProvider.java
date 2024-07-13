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
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
package org.apache.beam.runners.dataflow.worker.streaming.harness;
========
package org.apache.beam.runners.dataflow.worker.windmill.work.provider;
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java

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
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
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
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.StreamGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.sdk.annotations.Internal;
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
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
 * {@link StreamingWorkerHarness} implementation that manages fan out to multiple backend
 * destinations. Given a {@link GetWorkBudget}, divides the budget and starts the {@link
========
 * {@link WorkProvider} implementation that manages fan out to multiple backend destinations. Given
 * a {@link GetWorkBudget}, divides the budget and starts the {@link
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
 * WindmillStream.GetWorkStream}(s).
 */
@Internal
@CheckReturnValue
@ThreadSafe
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
public final class FanOutStreamingEngineWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutStreamingEngineWorkerHarness.class);
========
public final class FanOutStreamingEngineWorkProvider implements WorkProvider {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutStreamingEngineWorkProvider.class);
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
  private static final String PUBLISH_NEW_WORKER_METADATA_THREAD = "PublishNewWorkerMetadataThread";
  private static final String CONSUME_NEW_WORKER_METADATA_THREAD = "ConsumeNewWorkerMetadataThread";

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
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineConnectionState> connections;

  private volatile boolean started;

  @SuppressWarnings("FutureReturnValueIgnored")
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
  private FanOutStreamingEngineWorkerHarness(
========
  private FanOutStreamingEngineWorkProvider(
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler workItemScheduler,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.jobHeader = jobHeader;
    this.getDataMetricTracker = getDataMetricTracker;
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
  }

  private static ExecutorService singleThreadedExecutorServiceOf(String threadName) {
    return Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat(threadName).build());
  }

  /**
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
   * Creates an instance of {@link FanOutStreamingEngineWorkerHarness} in a non-started state.
   *
   * @implNote Does not block the calling thread. Callers must explicitly call {@link #start()}.
   */
  public static FanOutStreamingEngineWorkerHarness create(
========
   * Creates an instance of {@link FanOutStreamingEngineWorkProvider} in a non-started state.
   *
   * @implNote Does not block the calling thread. Callers must explicitly call {@link #start()}.
   */
  public static FanOutStreamingEngineWorkProvider create(
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
    return new FanOutStreamingEngineWorkerHarness(
========
    return new FanOutStreamingEngineWorkProvider(
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
        jobHeader,
        totalGetWorkBudget,
        streamingEngineStreamFactory,
        processWorkItem,
        channelCachingStubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        /* clientId= */ new Random().nextLong(),
        workCommitterFactory,
        getDataMetricTracker);
  }

  @VisibleForTesting
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
  static FanOutStreamingEngineWorkerHarness forTesting(
========
  static FanOutStreamingEngineWorkProvider forTesting(
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      long clientId,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
<<<<<<<< HEAD:runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/streaming/harness/FanOutStreamingEngineWorkerHarness.java
    FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkProvider =
        new FanOutStreamingEngineWorkerHarness(
========
    FanOutStreamingEngineWorkProvider fanOutStreamingEngineWorkProvider =
        new FanOutStreamingEngineWorkProvider(
>>>>>>>> 1479775a19e (change StreamingEngineClient to FanOutStreamingEngineWorkProvider and have it implement WorkProvider interface. Move class to windmill/work/provider directory, update visibility for dependent classes and move tests, add GetWorkBudgetOwnerInterface):runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/windmill/work/provider/FanOutStreamingEngineWorkProvider.java
            jobHeader,
            totalGetWorkBudget,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            clientId,
            workCommitterFactory,
            getDataMetricTracker);
    fanOutStreamingEngineWorkProvider.start();
    return fanOutStreamingEngineWorkProvider;
  }

  @SuppressWarnings("ReturnValueIgnored")
  @Override
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
  private GetDataStream getGlobalDataStream(String globalDataKey) {
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
  @Override
  public synchronized void shutdown() {
    Preconditions.checkState(started, "StreamingEngineClient never started.");
    getWorkerMetadataStream.get().halfClose();
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
                endpoint ->
                    // Reuse existing stubs if they exist. Optional.orElseGet only calls the
                    // supplier if the value is not present, preventing constructing expensive
                    // objects.
                    Optional.ofNullable(currentConnections.get(endpoint))
                        .orElseGet(
                            () -> WindmillConnection.from(endpoint, this::createWindmillStub))));
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
            connection,
            GetWorkRequest.newBuilder()
                .setClientId(clientId)
                .setJobId(jobHeader.getJobId())
                .setProjectId(jobHeader.getProjectId())
                .setWorkerId(jobHeader.getWorkerId())
                .build(),
            GetWorkBudget.noBudget(),
            streamFactory,
            workItemScheduler,
            getDataStream ->
                StreamGetDataClient.create(
                    getDataStream, this::getGlobalDataStream, getDataMetricTracker),
            workCommitterFactory);
    windmillStreamSender.startStreams();
    return windmillStreamSender;
  }

  private CloudWindmillServiceV1Alpha1Stub createWindmillStub(Endpoint endpoint) {
    return endpoint
        .directEndpoint()
        .map(channelCachingStubFactory::createWindmillServiceStub)
        .orElseGet(dispatcherClient::getWindmillServiceStub);
  }
}
