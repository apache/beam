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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.LOCALHOST;
import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.localhostChannel;
import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.remoteChannel;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.auto.value.AutoValue;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillApplianceGrpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
final class WindmillConnectionsCache implements ReadWriteWindmillConnectionsCache {
  private static final Logger LOG = LoggerFactory.getLogger(WindmillConnectionsCache.class);

  private final Object dispatcherConnectionLock;

  @GuardedBy("dispatcherConnectionLock")
  private final List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs;

  @GuardedBy("dispatcherConnectionLock")
  private final Set<HostAndPort> dispatcherEndpoints;

  @GuardedBy("dispatcherConnectionLock")
  private final Random rand;

  /**
   * Writes are guarded by synchronization on "this". Reads are done grabbing the {@link
   * AtomicReference#get()} before reading the state. Writes are all or nothing since this will get
   * set only after the new state is computed. Reads may be stale, and in that case the work item
   * may be retried later with a fresh connection since Windmill operations on the work item will
   * fail.
   */
  private final AtomicReference<WindmillWorkerConnections> windmillWorkerConnectionsState;

  private final WindmillGrpcStubFactory windmillGrpcStubFactory;
  private final @Nullable WindmillApplianceGrpc.WindmillApplianceBlockingStub syncApplianceStub;
  private final int windmillServiceRpcChannelTimeoutSec;

  WindmillConnectionsCache(
      List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs,
      Set<HostAndPort> dispatcherEndpoints,
      AtomicReference<WindmillWorkerConnections> windmillWorkerConnectionsState,
      @Nullable WindmillApplianceGrpc.WindmillApplianceBlockingStub syncApplianceStub,
      int windmillServiceRpcChannelTimeoutSec,
      WindmillGrpcStubFactory windmillGrpcStubFactory) {
    this.dispatcherConnectionLock = new Object();
    this.dispatcherStubs = dispatcherStubs;
    this.dispatcherEndpoints = dispatcherEndpoints;
    this.syncApplianceStub = syncApplianceStub;
    this.rand = new Random();
    this.windmillServiceRpcChannelTimeoutSec = windmillServiceRpcChannelTimeoutSec;
    this.windmillGrpcStubFactory = windmillGrpcStubFactory;
    this.windmillWorkerConnectionsState = windmillWorkerConnectionsState;
  }

  /** Returns a stub for Windmill dispatcher. */
  @Override
  public CloudWindmillServiceV1Alpha1Stub getDispatcherStub() {
    synchronized (dispatcherConnectionLock) {
      if (dispatcherStubs.isEmpty()) {
        throw new IllegalStateException("windmillServiceEndpoint has not been set");
      }

      return (dispatcherStubs.size() == 1
          ? dispatcherStubs.get(0)
          : dispatcherStubs.get(rand.nextInt(dispatcherStubs.size())));
    }
  }

  @Override
  public void consumeWindmillDispatcherEndpoints(ImmutableSet<HostAndPort> dispatcherEndpoints) {
    Preconditions.checkArgument(
        dispatcherEndpoints != null && !dispatcherEndpoints.isEmpty(),
        "Cannot set dispatcher endpoints to nothing.");
    synchronized (dispatcherConnectionLock) {
      if (this.dispatcherEndpoints.equals(dispatcherEndpoints)) {
        // The endpoints are equal don't recreate the stubs.
        return;
      }

      LOG.info("Creating a new windmill stub, endpoints: {}", dispatcherEndpoints);
      if (!this.dispatcherEndpoints.isEmpty()) {
        LOG.info("Previous windmill stub endpoints: {}", this.dispatcherEndpoints);
      }

      resetDispatcherEndpoints(dispatcherEndpoints);
    }
  }

  private void resetDispatcherEndpoints(ImmutableSet<HostAndPort> newDispatcherEndpoints) {
    synchronized (dispatcherConnectionLock) {
      LOG.info(
          "Initializing Streaming Engine GRPC client for endpoints: {}", newDispatcherEndpoints);
      this.dispatcherStubs.clear();
      this.dispatcherEndpoints.clear();
      this.dispatcherEndpoints.addAll(newDispatcherEndpoints);

      dispatcherEndpoints.stream()
          .map(this::createDispatcherStubForWindmillService)
          .forEach(dispatcherStubs::add);
    }
  }

  /**
   * Used by GetDataStream and CommitWorkStream calls inorder to make sure Windmill API calls get
   * routed to the same workers that GetWork was initiated on. If no {@link WindmillConnection} is
   * found, it means that the windmill worker working on the key range has moved on (ranges have
   * moved to other windmill workers, windmill worker crash, etc.).
   *
   * @see <a
   *     href=https://medium.com/google-cloud/streaming-engine-execution-model-1eb2eef69a8e>Windmill
   *     API</a>
   */
  @Override
  public Optional<WindmillConnection> getWindmillWorkerConnection(
      WindmillConnectionCacheToken key) {
    WindmillWorkerConnections currentWindmillWorkerConnections =
        Optional.ofNullable(windmillWorkerConnectionsState.get())
            .orElseThrow(UninitializedWindmillConnectionsException::new);

    Optional<WindmillConnection> windmillWorkerConnection =
        Optional.ofNullable(currentWindmillWorkerConnections.windmillWorkerStubs().get(key));

    if (!windmillWorkerConnection.isPresent()) {
      LOG.warn("Attempted to fetch WindmillWorkerConnection for {}, but none exist.", key);
    }

    return windmillWorkerConnection;
  }

  /**
   * Used by GetWorkStream to initiate the Windmill API calls and fetch Work items.
   *
   * @see <a
   *     href=https://medium.com/google-cloud/streaming-engine-execution-model-1eb2eef69a8e>Windmill
   *     API</a>
   */
  @Override
  public WindmillConnection getNewWindmillWorkerConnection() {
    WindmillWorkerConnections currentWindmillWorkerConnections =
        Optional.ofNullable(windmillWorkerConnectionsState.get())
            .orElseThrow(UninitializedWindmillConnectionsException::new);

    return currentWindmillWorkerConnections
        .roundRobinGetWorkStubSelector()
        .pickNextGetWorkServer()
        .flatMap(this::getWindmillWorkerConnection)
        .orElseThrow(
            () -> new IllegalStateException("No windmill connections have been initiated."));
  }

  /**
   * Acquires a lock and creates a new {@link WindmillWorkerConnections} view derived from the new
   * {@link WindmillEndpoints} and the current {@link WindmillWorkerConnections} view in {@link
   * #windmillWorkerConnectionsState}.
   *
   * <p>Read-only {@link ReadOnlyWindmillConnectionsCache} and Read-write {@link
   * ReadWriteWindmillConnectionsCache} interfaces are separated to allow for different read/write
   * paths.
   */
  @Override
  public synchronized void consumeWindmillWorkerEndpoints(WindmillEndpoints windmillEndpoints) {
    Preconditions.checkNotNull(windmillEndpoints, "Cannot consume null endpoints.");
    Preconditions.checkArgument(
        !windmillEndpoints.globalDataEndpoints().isEmpty(),
        "Cannot consume empty global_data_endpoints.");
    Preconditions.checkArgument(
        !windmillEndpoints.windmillEndpoints().isEmpty(),
        "Cannot consume empty windmill_endpoints.");

    LOG.info("Consuming new windmill endpoints: {}", windmillEndpoints);
    // This is null when it is the 1st time the windmillEndpoints are being consumed.
    Optional<WindmillWorkerConnections> currentWindmillWorkerConnections =
        Optional.ofNullable(windmillWorkerConnectionsState.get());
    LOG.info("Current windmillWorkerConnectionsState: {}", currentWindmillWorkerConnections);
    ImmutableMap<Endpoint, WindmillConnectionCacheToken> newEndpointToCacheTokenMap =
        createEndpointToCacheTokenMap(
            windmillEndpoints.windmillEndpoints(),
            currentWindmillWorkerConnections
                .map(WindmillWorkerConnections::endpointToCacheTokenMap)
                .orElseGet(ImmutableMap::of));
    LOG.info("Mapped endpoints to tokens. {}", newEndpointToCacheTokenMap);
    ImmutableList<WindmillConnectionCacheToken> cacheTokensForRandomAccess =
        ImmutableList.copyOf(newEndpointToCacheTokenMap.values());

    WindmillWorkerConnections newWindmillWorkerConnections =
        WindmillWorkerConnections.builder()
            .setEndpointToCacheTokenMap(newEndpointToCacheTokenMap)
            .setWindmillWorkerStubs(
                currentWindmillWorkerConnections
                    .map(WindmillWorkerConnections::windmillWorkerStubs)
                    .map(
                        existingStubs ->
                            createNewWindmillWorkerStubs(newEndpointToCacheTokenMap, existingStubs))
                    // There are no existing stubs since this is the first time metadata is being
                    // consumed.
                    .orElseGet(
                        () ->
                            createNewWindmillWorkerStubs(
                                newEndpointToCacheTokenMap, ImmutableMap.of())))
            .setGlobalDataStubs(createGlobalDataStubs(windmillEndpoints.globalDataEndpoints()))
            .setRoundRobinGetWorkStubSelector(
                currentWindmillWorkerConnections
                    .map(WindmillWorkerConnections::roundRobinGetWorkStubSelector)
                    .map(
                        stubSelector ->
                            RoundRobinGetWorkStubSelector.next(
                                stubSelector, cacheTokensForRandomAccess))
                    .orElseGet(
                        () -> RoundRobinGetWorkStubSelector.create(cacheTokensForRandomAccess)))
            .build();

    LOG.info("New windmillWorkerConnectionsState: {}", newWindmillWorkerConnections);
    windmillWorkerConnectionsState.set(newWindmillWorkerConnections);
  }

  private ImmutableMap<Endpoint, WindmillConnectionCacheToken> createEndpointToCacheTokenMap(
      ImmutableList<Endpoint> newEndpoints,
      ImmutableMap<Endpoint, WindmillConnectionCacheToken> currentEndpointsToCacheTokens) {
    return newEndpoints.stream()
        .collect(
            toImmutableMap(
                Function.identity(),
                endpoint ->
                    Optional.ofNullable(currentEndpointsToCacheTokens.get(endpoint))
                        .orElseGet(WindmillConnectionCacheToken::generateRandomToken)));
  }

  private ImmutableMap<WindmillConnectionCacheToken, WindmillConnection>
      createNewWindmillWorkerStubs(
          ImmutableMap<Endpoint, WindmillConnectionCacheToken> newEndpointToCacheTokenMap,
          ImmutableMap<WindmillConnectionCacheToken, WindmillConnection>
              currentWindmillWorkerConnections) {
    return newEndpointToCacheTokenMap.entrySet().stream()
        .collect(
            toImmutableMap(
                Map.Entry::getValue,
                entry ->
                    // If a connection for the endpoint already exists, just reuse the connection,
                    // else make a new one.
                    Optional.ofNullable(currentWindmillWorkerConnections.get(entry.getValue()))
                        .orElseGet(() -> createNewWindmillConnection(entry))));
  }

  private WindmillConnection createNewWindmillConnection(
      Map.Entry<Endpoint, WindmillConnectionCacheToken> endpointAndCacheToken) {
    return WindmillConnection.from(endpointAndCacheToken.getKey(), this::newWindmillStub)
        .setCacheToken(endpointAndCacheToken.getValue())
        .build();
  }

  private ImmutableMap<String, CloudWindmillServiceV1Alpha1Stub> createGlobalDataStubs(
      ImmutableMap<String, Endpoint> newGlobalDataEndpoints) {
    return newGlobalDataEndpoints.entrySet().stream()
        .collect(
            toImmutableMap(
                Map.Entry::getKey, // global data tag
                entry -> newWindmillStub(entry.getValue())));
  }

  @Override
  public Optional<CloudWindmillServiceV1Alpha1Stub> getGlobalDataStub(
      Windmill.GlobalDataId globalDataId) {
    WindmillWorkerConnections currentWindmillWorkerConnections =
        Optional.ofNullable(windmillWorkerConnectionsState.get())
            .orElseThrow(UninitializedWindmillConnectionsException::new);

    return Optional.ofNullable(
        currentWindmillWorkerConnections.globalDataStubs().get(globalDataId.getTag()));
  }

  private CloudWindmillServiceV1Alpha1Stub newWindmillStub(Endpoint endpoint) {
    switch (windmillGrpcStubFactory.getKind()) {
        // This is only used in tests.
      case INPROCESS:
        return windmillGrpcStubFactory.inProcess().get();
        // Create stub for direct_endpoint or just default to Dispatcher stub.
      case REMOTE:
        return endpoint
            .directEndpoint()
            .map(windmillGrpcStubFactory.remote())
            .orElseGet(this::getDispatcherStub);
        // Should never be called, this switch statement is exhaustive.
      default:
        throw new UnsupportedOperationException(
            "Only remote or in-process stub factories are available.");
    }
  }

  private CloudWindmillServiceV1Alpha1Stub createDispatcherStubForWindmillService(
      HostAndPort endpoint) {
    if (LOCALHOST.equals(endpoint.getHost())) {
      return CloudWindmillServiceV1Alpha1Grpc.newStub(localhostChannel(endpoint.getPort()));
    }

    // Use an in-process stub if testing.
    return windmillGrpcStubFactory.getKind() == WindmillGrpcStubFactory.Kind.INPROCESS
        ? windmillGrpcStubFactory.inProcess().get()
        : CloudWindmillServiceV1Alpha1Grpc.newStub(
            remoteChannel(endpoint, windmillServiceRpcChannelTimeoutSec));
  }

  @Override
  public Optional<WindmillApplianceGrpc.WindmillApplianceBlockingStub> getWindmillApplianceStub() {
    return Optional.ofNullable(syncApplianceStub);
  }

  @AutoValue
  abstract static class WindmillWorkerConnections {
    private static WindmillWorkerConnections.Builder builder() {
      return new AutoValue_WindmillConnectionsCache_WindmillWorkerConnections.Builder();
    }

    abstract ImmutableMap<Endpoint, WindmillConnectionCacheToken> endpointToCacheTokenMap();

    abstract ImmutableMap<WindmillConnectionCacheToken, WindmillConnection> windmillWorkerStubs();

    abstract RoundRobinGetWorkStubSelector roundRobinGetWorkStubSelector();

    abstract ImmutableMap<String, CloudWindmillServiceV1Alpha1Stub> globalDataStubs();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEndpointToCacheTokenMap(
          Map<Endpoint, WindmillConnectionCacheToken> endpointToCacheTokenMap);

      abstract Builder setWindmillWorkerStubs(
          ImmutableMap<WindmillConnectionCacheToken, WindmillConnection> windmillWorkerStubs);

      abstract Builder setRoundRobinGetWorkStubSelector(
          RoundRobinGetWorkStubSelector roundRobinGetWorkStubSelector);

      abstract Builder setGlobalDataStubs(
          ImmutableMap<String, CloudWindmillServiceV1Alpha1Stub> globalDataStubs);

      abstract WindmillWorkerConnections build();
    }
  }

  /**
   * Naive round-robin {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetWorkStream} stub selector.
   * This is only used for {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetWorkStream} because {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetDataStream} and {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.CommitWorkStream} know which
   * Windmill workers to communicate with, and {@link
   * org.apache.beam.runners.dataflow.worker.windmill.WindmillStream.GetWorkerMetadataStream}
   * communicates with the Windmill Dispatcher.
   *
   * @implNote Package private for {@link AutoValue} visibility.
   */
  @ThreadSafe
  static class RoundRobinGetWorkStubSelector {
    private static final String GET_WORK_STUB_SELECTOR_LOCK = "RoundRobinGetWorkStubSelector.this";
    private static final int INITIAL_INDEX = -1;

    @GuardedBy(GET_WORK_STUB_SELECTOR_LOCK)
    private final ImmutableList<WindmillConnectionCacheToken> windmillConnectionCacheTokens;

    @GuardedBy(GET_WORK_STUB_SELECTOR_LOCK)
    private int lastSelectedStubIndex;

    private RoundRobinGetWorkStubSelector(
        int lastSelectedStubIndex,
        ImmutableList<WindmillConnectionCacheToken> windmillConnectionCacheTokens) {
      this.lastSelectedStubIndex = lastSelectedStubIndex;
      this.windmillConnectionCacheTokens = windmillConnectionCacheTokens;
    }

    private static RoundRobinGetWorkStubSelector create(
        ImmutableList<WindmillConnectionCacheToken> cacheTokens) {
      return new RoundRobinGetWorkStubSelector(INITIAL_INDEX, cacheTokens);
    }

    private static RoundRobinGetWorkStubSelector next(
        RoundRobinGetWorkStubSelector prev,
        ImmutableList<WindmillConnectionCacheToken> newCacheTokens) {
      // If it getNextValidIndex returns 0, it means that we have either reached the bounds, or
      // pickNextGetWorkServer() was never called on the previous stubSelector. In that case just
      // create a fresh stubSelector with the new cacheTokens, else keep the lastSelectedStubIndex
      // incrementing if valid.
      int nextValidStubSelectorIndex = prev.getNextValidIndex(newCacheTokens.size());
      return nextValidStubSelectorIndex > 0
          // Keep the lastSelectedStubIndex moving
          ? new RoundRobinGetWorkStubSelector(nextValidStubSelectorIndex, newCacheTokens)
          : create(newCacheTokens);
    }

    private synchronized Optional<WindmillConnectionCacheToken> pickNextGetWorkServer() {
      if (windmillConnectionCacheTokens.isEmpty()) {
        return Optional.empty();
      }

      int nextSelectedStubIndex = getNextValidIndex(windmillConnectionCacheTokens.size());
      lastSelectedStubIndex = nextSelectedStubIndex;
      return Optional.of(windmillConnectionCacheTokens.get(nextSelectedStubIndex));
    }

    private synchronized int getNextValidIndex(int bounds) {
      return lastSelectedStubIndex == INITIAL_INDEX || lastSelectedStubIndex == bounds
          ? 0
          : lastSelectedStubIndex + 1;
    }
  }

  @VisibleForTesting
  static class UninitializedWindmillConnectionsException extends IllegalStateException {
    private UninitializedWindmillConnectionsException() {
      super(
          "Attempted to fetch a new windmill worker connection when no windmill connections"
              + " have been initiated.");
    }
  }
}
