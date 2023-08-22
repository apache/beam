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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillConnectionsCache.WindmillWorkerConnections;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.util.MutableHandlerRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillConnectionsCacheTest {

  private static final String FAKE_SERVER_NAME = "Fake server for GrpcGetWorkerMetadataStreamTest";
  private static final WindmillServiceAddress DEFAULT_WINDMILL_SERVICE_ADDRESS =
      WindmillServiceAddress.create(HostAndPort.fromParts(WindmillChannelFactory.LOCALHOST, 443));
  private static final Endpoint DEFAULT_ENDPOINT =
      Endpoint.builder().setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS).build();
  private static final ImmutableMap<String, Endpoint> DEFAULT_GLOBAL_DATA_ENDPOINTS =
      ImmutableMap.of("global_data", DEFAULT_ENDPOINT);

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs = new ArrayList<>();
  private final Set<HostAndPort> dispatcherEndpoints = new HashSet<>();
  private final AtomicReference<WindmillWorkerConnections> windmillWorkerConnectionsState =
      new AtomicReference<>();
  private Server server;

  @Before
  public void setUp() throws IOException {
    server =
        InProcessServerBuilder.forName(FAKE_SERVER_NAME)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    grpcCleanup.register(server);
    dispatcherStubs.clear();
    dispatcherEndpoints.clear();
    windmillWorkerConnectionsState.set(null);
  }

  @After
  public void tearDown() {
    server.shutdownNow();
  }

  private ReadWriteWindmillConnectionsCache newWindmillConnectionsCache() {
    return new WindmillConnectionsCache(
        dispatcherStubs,
        dispatcherEndpoints,
        windmillWorkerConnectionsState,
        null,
        /* windmillServiceRpcChannelTimeoutSec= */ -1,
        WindmillGrpcStubFactory.inProcessStubFactory(FAKE_SERVER_NAME));
  }

  @Test
  public void testGetDispatcherStub() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    HostAndPort dispatcherEndpoint = DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress();
    windmillConnectionsCache.consumeWindmillDispatcherEndpoints(
        ImmutableSet.of(dispatcherEndpoint));
    assertNotNull(windmillConnectionsCache.getDispatcherStub());
    assertEquals(1, dispatcherStubs.size());
    assertEquals(1, dispatcherEndpoints.size());
    HostAndPort cachedDispatcherEndpoint = new ArrayList<>(dispatcherEndpoints).get(0);
    assertEquals(dispatcherEndpoint.getHost(), cachedDispatcherEndpoint.getHost());
    assertEquals(dispatcherEndpoint.getPort(), cachedDispatcherEndpoint.getPort());
  }

  @Test
  public void testGetDispatcherStub_throwsIllegalStateExceptionIfDispatcherEndpointsNotSet() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    assertThrows(IllegalStateException.class, windmillConnectionsCache::getDispatcherStub);
  }

  @Test
  public void testConsumeWindmillDispatcherEndpoints_correctlySetsDispatcherStubs() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    HostAndPort hostAndPort = DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress();
    windmillConnectionsCache.consumeWindmillDispatcherEndpoints(ImmutableSet.of(hostAndPort));
    assertTrue(dispatcherEndpoints.contains(hostAndPort));
    assertEquals(1, dispatcherEndpoints.size());
    assertEquals(1, dispatcherStubs.size());
  }

  @Test
  public void testConsumeWindmillDispatcherEndpoints_doesNothingIfNewEndpointsIdentical() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    HostAndPort hostAndPort = DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress();
    windmillConnectionsCache.consumeWindmillDispatcherEndpoints(ImmutableSet.of(hostAndPort));
    assertTrue(dispatcherEndpoints.contains(hostAndPort));
    assertEquals(1, dispatcherEndpoints.size());
    assertEquals(1, dispatcherStubs.size());
  }

  @Test
  public void testConsumeWindmillDispatcherEndpoints_errorIfNewEndpointsEmptyOrNull() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    assertThrows(
        IllegalArgumentException.class,
        () -> windmillConnectionsCache.consumeWindmillDispatcherEndpoints(ImmutableSet.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> windmillConnectionsCache.consumeWindmillDispatcherEndpoints(null));
  }

  @Test
  public void testGetNewWindmillWorkerConnection() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String workerToken = "workerToken";
    ImmutableList<Endpoint> windmillEndpoints =
        ImmutableList.of(Endpoint.builder().setWorkerToken(workerToken).build());
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .setWindmillEndpoints(windmillEndpoints)
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());
    WindmillConnection windmillWorkerConnection =
        windmillConnectionsCache.getNewWindmillWorkerConnection();

    assertTrue(windmillWorkerConnection.backendWorkerToken().isPresent());
    assertEquals(workerToken, windmillWorkerConnection.backendWorkerToken().get());
    assertTrue(windmillWorkerConnection.directStub().isPresent());
    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsKey(windmillWorkerConnection.cacheToken()));
    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsValue(windmillWorkerConnection));
  }

  @Test
  public void testGetNewWindmillWorkerConnectionConcurrentReads() throws InterruptedException {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken).build())
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken2).build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    AtomicReference<WindmillConnection> windmillWorkerConnection = new AtomicReference<>();
    AtomicReference<WindmillConnection> windmillWorkerConnection2 = new AtomicReference<>();

    Thread getWindmillWorkerEndpoint1 =
        new Thread(
            () ->
                windmillWorkerConnection.set(
                    windmillConnectionsCache.getNewWindmillWorkerConnection()));

    Thread getWindmillWorkerEndpoint2 =
        new Thread(
            () ->
                windmillWorkerConnection2.set(
                    windmillConnectionsCache.getNewWindmillWorkerConnection()));

    getWindmillWorkerEndpoint1.start();
    getWindmillWorkerEndpoint2.start();

    // Wait for threads to complete executing before assertions.
    getWindmillWorkerEndpoint1.join();
    getWindmillWorkerEndpoint2.join();

    // Assert all expected values are present since the order of execution is non-deterministic.
    assertTrue(windmillWorkerConnection.get().backendWorkerToken().isPresent());
    assertTrue(windmillWorkerConnection2.get().backendWorkerToken().isPresent());

    assertTrue(windmillWorkerConnection.get().directStub().isPresent());
    assertTrue(windmillWorkerConnection2.get().directStub().isPresent());

    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsKey(windmillWorkerConnection.get().cacheToken()));
    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsKey(windmillWorkerConnection2.get().cacheToken()));

    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsValue(windmillWorkerConnection.get()));
    assertTrue(
        windmillWorkerConnectionsState
            .get()
            .windmillWorkerStubs()
            .containsValue(windmillWorkerConnection2.get()));
  }

  @Test
  public void
      testGetNewWindmillWorkerConnection_throwsUninitializedWindmillConnectionsException_ifConnectionsNeverConsumed() {
    assertThrows(
        WindmillConnectionsCache.UninitializedWindmillConnectionsException.class,
        newWindmillConnectionsCache()::getNewWindmillWorkerConnection);
  }

  @Test
  public void testGetWindmillWorkerConnection_returnsEmptyOptionalIfConnectionNotPresent() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken("workerToken").build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    windmillConnectionsCache.getNewWindmillWorkerConnection();

    assertFalse(
        newWindmillConnectionsCache()
            .getWindmillWorkerConnection(WindmillConnectionCacheToken.create("thisDoesNotExist"))
            .isPresent());
  }

  @Test
  public void testGetWindmillWorkerConnection() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken("workerToken").build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    WindmillConnection connection = windmillConnectionsCache.getNewWindmillWorkerConnection();
    Optional<WindmillConnection> cachedConnection =
        windmillConnectionsCache.getWindmillWorkerConnection(connection.cacheToken());

    assertTrue(cachedConnection.isPresent());
    assertEquals(connection, cachedConnection.get());
  }

  @Test
  public void
      testGetWindmillWorkerConnection_throwsUninitializedWindmillConnectionsException_ifConnectionsNeverConsumed() {
    assertThrows(
        WindmillConnectionsCache.UninitializedWindmillConnectionsException.class,
        () ->
            newWindmillConnectionsCache()
                .getWindmillWorkerConnection(WindmillConnectionCacheToken.create("doesn't exist")));
  }

  @Test
  public void testConsumeWindmillWorkerEndpoints_correctlyRemovesStaleWindmillServers() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken).build())
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken2).build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken3).build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    assertEquals(1, windmillWorkerConnectionsState.get().windmillWorkerStubs().size());
    Set<String> workerTokens =
        windmillWorkerConnectionsState.get().windmillWorkerStubs().values().stream()
            .map(WindmillConnection::backendWorkerToken)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    assertFalse(workerTokens.contains(workerToken));
    assertFalse(workerTokens.contains(workerToken2));
  }

  @Test
  public void testConsumeWindmillWorkerEndpoints_correctlyAddsNewWindmillServers() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken).build())
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken2).build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken(workerToken3).build())
            .setGlobalDataEndpoints(DEFAULT_GLOBAL_DATA_ENDPOINTS)
            .build());

    assertEquals(1, windmillWorkerConnectionsState.get().windmillWorkerStubs().size());
    Set<String> workerTokens =
        windmillWorkerConnectionsState.get().windmillWorkerStubs().values().stream()
            .map(WindmillConnection::backendWorkerToken)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    assertTrue(workerTokens.contains(workerToken3));
  }

  @Test
  public void testConsumeWindmillWorkerEndpoints_correctlyConsumesGlobalDataServers() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String globalDataKey = "globalDataKey";
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(DEFAULT_ENDPOINT)
            .addGlobalDataEndpoint(globalDataKey, Endpoint.builder().build())
            .build());

    assertTrue(windmillWorkerConnectionsState.get().globalDataStubs().containsKey(globalDataKey));
  }

  @Test
  public void testGetGlobalDataStub_returnsGlobalDataStubIfPresent() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    String globalDataKey = "globalDataKey";
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken("workerToken").build())
            .addGlobalDataEndpoint(globalDataKey, Endpoint.builder().build())
            .build());

    assertTrue(
        windmillConnectionsCache
            .getGlobalDataStub(
                Windmill.GlobalDataId.newBuilder()
                    .setTag(globalDataKey)
                    .setVersion(ByteString.fromHex(""))
                    .build())
            .isPresent());
  }

  @Test
  public void testGetGlobalDataStub_returnsEmptyOptionalIfNoStubs() {
    ReadWriteWindmillConnectionsCache windmillConnectionsCache = newWindmillConnectionsCache();
    windmillConnectionsCache.consumeWindmillWorkerEndpoints(
        WindmillEndpoints.builder()
            .addWindmillEndpoint(Endpoint.builder().setWorkerToken("workerToken").build())
            .addGlobalDataEndpoint("globalDataKey", Endpoint.builder().build())
            .build());

    assertFalse(
        windmillConnectionsCache
            .getGlobalDataStub(
                Windmill.GlobalDataId.newBuilder()
                    .setTag("nonExistentKey")
                    .setVersion(ByteString.fromHex(""))
                    .build())
            .isPresent());
  }

  @Test
  public void
      testGetGlobalDataStub_throwsUninitializedWindmillConnectionsException_ifConnectionsNeverConsumed() {
    assertThrows(
        WindmillConnectionsCache.UninitializedWindmillConnectionsException.class,
        () ->
            newWindmillConnectionsCache()
                .getGlobalDataStub(
                    Windmill.GlobalDataId.newBuilder()
                        .setTag("nonExistentKey")
                        .setVersion(ByteString.fromHex(""))
                        .build()));
  }
}
