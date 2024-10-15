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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet.toImmutableSet;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;
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
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamingWorkerHarness} implementation that manages fan out to multiple backend
 * destinations. Given a {@link GetWorkBudget}, divides the budget and starts the {@link
 * WindmillStream.GetWorkStream}(s).
 */
@Internal
@CheckReturnValue
@ThreadSafe
public final class FanOutStreamingEngineWorkerHarness implements StreamingWorkerHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(FanOutStreamingEngineWorkerHarness.class);
  private static final String WORKER_METADATA_CONSUMER_THREAD_NAME =
      "WindmillWorkerMetadataConsumerThread";
  private static final String STREAM_MANAGER_THREAD_NAME = "WindmillStreamManager-%d";

  private final JobHeader jobHeader;
  private final GrpcWindmillStreamFactory streamFactory;
  private final WorkItemScheduler workItemScheduler;
  private final ChannelCachingStubFactory channelCachingStubFactory;
  private final GrpcDispatcherClient dispatcherClient;
  private final GetWorkBudgetDistributor getWorkBudgetDistributor;
  private final GetWorkBudget totalGetWorkBudget;
  private final ThrottleTimer getWorkerMetadataThrottleTimer;
  private final Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory;
  private final ThrottlingGetDataMetricTracker getDataMetricTracker;
  private final ExecutorService windmillStreamManager;
  private final ExecutorService workerMetadataConsumer;
  private final Object metadataLock = new Object();

  /** Writes are guarded by synchronization, reads are lock free. */
  private final AtomicReference<StreamingEngineBackends> backends;

  private final GetWorkerMetadataStream getWorkerMetadataStream;

  @GuardedBy("this")
  private long activeMetadataVersion;

  @GuardedBy("metadataLock")
  private long pendingMetadataVersion;

  @GuardedBy("this")
  private boolean started;

  private FanOutStreamingEngineWorkerHarness(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler workItemScheduler,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    this.jobHeader = jobHeader;
    this.getDataMetricTracker = getDataMetricTracker;
    this.started = false;
    this.streamFactory = streamFactory;
    this.workItemScheduler = workItemScheduler;
    this.backends = new AtomicReference<>(StreamingEngineBackends.EMPTY);
    this.channelCachingStubFactory = channelCachingStubFactory;
    this.dispatcherClient = dispatcherClient;
    this.getWorkerMetadataThrottleTimer = new ThrottleTimer();
    this.windmillStreamManager =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat(STREAM_MANAGER_THREAD_NAME).build());
    this.workerMetadataConsumer =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(WORKER_METADATA_CONSUMER_THREAD_NAME).build());
    this.getWorkBudgetDistributor = getWorkBudgetDistributor;
    this.totalGetWorkBudget = totalGetWorkBudget;
    this.activeMetadataVersion = Long.MIN_VALUE;
    this.workCommitterFactory = workCommitterFactory;
    // To satisfy CheckerFramework complaining about reference to "this" in constructor.
    @SuppressWarnings("methodref.receiver.bound")
    Consumer<WindmillEndpoints> newEndpointsConsumer = this::consumeWorkerMetadata;
    this.getWorkerMetadataStream =
        streamFactory.createGetWorkerMetadataStream(
            dispatcherClient::getWindmillMetadataServiceStubBlocking,
            getWorkerMetadataThrottleTimer,
            newEndpointsConsumer);
  }

  /**
   * Creates an instance of {@link FanOutStreamingEngineWorkerHarness} in a non-started state.
   *
   * @implNote Does not block the calling thread. Callers must explicitly call {@link #start()}.
   */
  public static FanOutStreamingEngineWorkerHarness create(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamingEngineStreamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory channelCachingStubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    return new FanOutStreamingEngineWorkerHarness(
        jobHeader,
        totalGetWorkBudget,
        streamingEngineStreamFactory,
        processWorkItem,
        channelCachingStubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        workCommitterFactory,
        getDataMetricTracker);
  }

  @VisibleForTesting
  static FanOutStreamingEngineWorkerHarness forTesting(
      JobHeader jobHeader,
      GetWorkBudget totalGetWorkBudget,
      GrpcWindmillStreamFactory streamFactory,
      WorkItemScheduler processWorkItem,
      ChannelCachingStubFactory stubFactory,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      GrpcDispatcherClient dispatcherClient,
      Function<WindmillStream.CommitWorkStream, WorkCommitter> workCommitterFactory,
      ThrottlingGetDataMetricTracker getDataMetricTracker) {
    FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkProvider =
        new FanOutStreamingEngineWorkerHarness(
            jobHeader,
            totalGetWorkBudget,
            streamFactory,
            processWorkItem,
            stubFactory,
            getWorkBudgetDistributor,
            dispatcherClient,
            workCommitterFactory,
            getDataMetricTracker);
    fanOutStreamingEngineWorkProvider.start();
    return fanOutStreamingEngineWorkProvider;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(!started, "FanOutStreamingEngineWorkerHarness cannot start twice.");
    getWorkerMetadataStream.start();
    started = true;
  }

  public ImmutableSet<HostAndPort> currentWindmillEndpoints() {
    return backends.get().windmillStreams().keySet().stream()
        .map(Endpoint::directEndpoint)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(WindmillServiceAddress::getServiceAddress)
        .collect(toImmutableSet());
  }

  /**
   * Fetches {@link GetDataStream} mapped to globalDataKey if or throws {@link
   * NoSuchElementException} if one is not found.
   */
  private GetDataStream getGlobalDataStream(String globalDataKey) {
    return Optional.ofNullable(backends.get().globalDataStreams().get(globalDataKey))
        .map(GlobalDataStreamSender::get)
        .orElseThrow(
            () -> new NoSuchElementException("No endpoint for global data tag: " + globalDataKey));
  }

  @VisibleForTesting
  @Override
  public synchronized void shutdown() {
    Preconditions.checkState(started, "FanOutStreamingEngineWorkerHarness never started.");
    Preconditions.checkNotNull(getWorkerMetadataStream).shutdown();
    workerMetadataConsumer.shutdownNow();
    closeStreamsNotIn(WindmillEndpoints.none());
    channelCachingStubFactory.shutdown();

    try {
      Preconditions.checkNotNull(getWorkerMetadataStream).awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted waiting for GetWorkerMetadataStream to shutdown.", e);
    }

    windmillStreamManager.shutdown();
    boolean isStreamManagerShutdown = false;
    try {
      isStreamManagerShutdown = windmillStreamManager.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted waiting for windmillStreamManager to shutdown.", e);
    }
    if (!isStreamManagerShutdown) {
      windmillStreamManager.shutdownNow();
    }
  }

  private void consumeWorkerMetadata(WindmillEndpoints windmillEndpoints) {
    synchronized (metadataLock) {
      // Only process versions greater than what we currently have to prevent double processing of
      // metadata. workerMetadataConsumer is single-threaded so we maintain ordering.
      if (windmillEndpoints.version() > pendingMetadataVersion) {
        pendingMetadataVersion = windmillEndpoints.version();
        workerMetadataConsumer.execute(() -> consumeWindmillWorkerEndpoints(windmillEndpoints));
      }
    }
  }

  private synchronized void consumeWindmillWorkerEndpoints(WindmillEndpoints newWindmillEndpoints) {
    // Since this is run on a single threaded executor, multiple versions of the metadata maybe
    // queued up while a previous version of the windmillEndpoints were being consumed. Only consume
    // the endpoints if they are the most current version.
    synchronized (metadataLock) {
      if (newWindmillEndpoints.version() < pendingMetadataVersion) {
        return;
      }
    }

    LOG.debug(
        "Consuming new endpoints: {}. previous metadata version: {}, current metadata version: {}",
        newWindmillEndpoints,
        activeMetadataVersion,
        newWindmillEndpoints.version());
    closeStreamsNotIn(newWindmillEndpoints);
    ImmutableMap<Endpoint, WindmillStreamSender> newStreams =
        createAndStartNewStreams(newWindmillEndpoints.windmillEndpoints()).join();
    StreamingEngineBackends newBackends =
        StreamingEngineBackends.builder()
            .setWindmillStreams(newStreams)
            .setGlobalDataStreams(
                createNewGlobalDataStreams(newWindmillEndpoints.globalDataEndpoints()))
            .build();
    backends.set(newBackends);
    getWorkBudgetDistributor.distributeBudget(newStreams.values(), totalGetWorkBudget);
    activeMetadataVersion = newWindmillEndpoints.version();
  }

  /** Close the streams that are no longer valid asynchronously. */
  private void closeStreamsNotIn(WindmillEndpoints newWindmillEndpoints) {
    StreamingEngineBackends currentBackends = backends.get();
    currentBackends.windmillStreams().entrySet().stream()
        .filter(
            connectionAndStream ->
                !newWindmillEndpoints.windmillEndpoints().contains(connectionAndStream.getKey()))
        .forEach(
            entry ->
                windmillStreamManager.execute(
                    () -> closeStreamSender(entry.getKey(), entry.getValue())));

    Set<Endpoint> newGlobalDataEndpoints =
        new HashSet<>(newWindmillEndpoints.globalDataEndpoints().values());
    currentBackends.globalDataStreams().values().stream()
        .filter(sender -> !newGlobalDataEndpoints.contains(sender.endpoint()))
        .forEach(
            sender ->
                windmillStreamManager.execute(() -> closeStreamSender(sender.endpoint(), sender)));
  }

  private void closeStreamSender(Endpoint endpoint, Closeable sender) {
    LOG.debug("Closing streams to endpoint={}, sender={}", endpoint, sender);
    try {
      sender.close();
      endpoint.directEndpoint().ifPresent(channelCachingStubFactory::remove);
      LOG.debug("Successfully closed streams to {}", endpoint);
    } catch (Exception e) {
      LOG.error("Error closing streams to endpoint={}, sender={}", endpoint, sender);
    }
  }

  private synchronized CompletableFuture<ImmutableMap<Endpoint, WindmillStreamSender>>
      createAndStartNewStreams(ImmutableSet<Endpoint> newWindmillEndpoints) {
    ImmutableMap<Endpoint, WindmillStreamSender> currentStreams = backends.get().windmillStreams();
    return MoreFutures.allAsList(
            newWindmillEndpoints.stream()
                .map(endpoint -> getOrCreateWindmillStreamSenderFuture(endpoint, currentStreams))
                .collect(Collectors.toList()))
        .thenApply(
            backends -> backends.stream().collect(toImmutableMap(Pair::getLeft, Pair::getRight)))
        .toCompletableFuture();
  }

  private CompletionStage<Pair<Endpoint, WindmillStreamSender>>
      getOrCreateWindmillStreamSenderFuture(
          Endpoint endpoint, ImmutableMap<Endpoint, WindmillStreamSender> currentStreams) {
    return MoreFutures.supplyAsync(
        () ->
            Pair.of(
                endpoint,
                Optional.ofNullable(currentStreams.get(endpoint))
                    .orElseGet(() -> createAndStartWindmillStreamSender(endpoint))),
        windmillStreamManager);
  }

  /** Add up all the throttle times of all streams including GetWorkerMetadataStream. */
  public long getAndResetThrottleTime() {
    return backends.get().windmillStreams().values().stream()
            .map(WindmillStreamSender::getAndResetThrottleTime)
            .reduce(0L, Long::sum)
        + getWorkerMetadataThrottleTimer.getAndResetThrottleTime();
  }

  public long currentActiveCommitBytes() {
    return backends.get().windmillStreams().values().stream()
        .map(WindmillStreamSender::getCurrentActiveCommitBytes)
        .reduce(0L, Long::sum);
  }

  @VisibleForTesting
  StreamingEngineBackends currentBackends() {
    return backends.get();
  }

  private ImmutableMap<String, GlobalDataStreamSender> createNewGlobalDataStreams(
      ImmutableMap<String, Endpoint> newGlobalDataEndpoints) {
    ImmutableMap<String, GlobalDataStreamSender> currentGlobalDataStreams =
        backends.get().globalDataStreams();
    return newGlobalDataEndpoints.entrySet().stream()
        .collect(
            toImmutableMap(
                Entry::getKey,
                keyedEndpoint ->
                    getOrCreateGlobalDataSteam(keyedEndpoint, currentGlobalDataStreams)));
  }

  private GlobalDataStreamSender getOrCreateGlobalDataSteam(
      Entry<String, Endpoint> keyedEndpoint,
      ImmutableMap<String, GlobalDataStreamSender> currentGlobalDataStreams) {
    return Optional.ofNullable(currentGlobalDataStreams.get(keyedEndpoint.getKey()))
        .orElseGet(
            () ->
                new GlobalDataStreamSender(
                    streamFactory.createGetDataStream(
                        createWindmillStub(keyedEndpoint.getValue()), new ThrottleTimer()),
                    keyedEndpoint.getValue()));
  }

  private WindmillStreamSender createAndStartWindmillStreamSender(Endpoint endpoint) {
    WindmillStreamSender windmillStreamSender =
        WindmillStreamSender.create(
            WindmillConnection.from(endpoint, this::createWindmillStub),
            GetWorkRequest.newBuilder()
                .setClientId(jobHeader.getClientId())
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
    windmillStreamSender.start();
    return windmillStreamSender;
  }

  private CloudWindmillServiceV1Alpha1Stub createWindmillStub(Endpoint endpoint) {
    return endpoint
        .directEndpoint()
        .map(channelCachingStubFactory::createWindmillServiceStub)
        .orElseGet(dispatcherClient::getWindmillServiceStub);
  }
}
