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
package org.apache.beam.runners.dataflow.worker.windmill.grpcclient;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.dataflow.worker.windmill.AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.util.MutableHandlerRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class GrpcGetWorkerMetadataStreamTest {
  private static final String IPV6_ADDRESS_1 = "2001:db8:0000:bac5:0000:0000:fed0:81a2";
  private static final String IPV6_ADDRESS_2 = "2001:db8:0000:bac5:0000:0000:fed0:82a3";
  private static final List<WorkerMetadataResponse.Endpoint> DIRECT_PATH_ENDPOINTS =
      Lists.newArrayList(
          WorkerMetadataResponse.Endpoint.newBuilder()
              .setDirectEndpoint(IPV6_ADDRESS_1)
              .setWorkerToken("worker_token")
              .build());
  private static final Map<String, WorkerMetadataResponse.Endpoint> GLOBAL_DATA_ENDPOINTS =
      Maps.newHashMap();
  private static final JobHeader TEST_JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId("test_job")
          .setWorkerId("test_worker")
          .setProjectId("test_project")
          .build();
  private static final String FAKE_SERVER_NAME = "Fake server for GrpcGetWorkerMetadataStreamTest";
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry = new HashSet<>();
  private ManagedChannel inProcessChannel;
  private GrpcGetWorkerMetadataStream stream;

  private GrpcGetWorkerMetadataStream getWorkerMetadataTestStream(
      GetWorkerMetadataTestStub getWorkerMetadataTestStub,
      int metadataVersion,
      Consumer<WindmillEndpoints> endpointsConsumer) {
    serviceRegistry.addService(getWorkerMetadataTestStub);
    return GrpcGetWorkerMetadataStream.create(
        responseObserver ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel)
                .getWorkerMetadataStream(responseObserver),
        FluentBackoff.DEFAULT.backoff(),
        StreamObserverFactory.direct(DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, 1),
        streamRegistry,
        1, // logEveryNStreamFailures
        TEST_JOB_HEADER,
        metadataVersion,
        new ThrottleTimer(),
        endpointsConsumer);
  }

  @Before
  public void setUp() throws IOException {
    Server server =
        InProcessServerBuilder.forName(FAKE_SERVER_NAME)
            .fallbackHandlerRegistry(serviceRegistry)
            .directExecutor()
            .build()
            .start();

    inProcessChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(FAKE_SERVER_NAME).directExecutor().build());
    grpcCleanup.register(server);
    grpcCleanup.register(inProcessChannel);
    GLOBAL_DATA_ENDPOINTS.put(
        "global_data",
        WorkerMetadataResponse.Endpoint.newBuilder()
            .setDirectEndpoint(IPV6_ADDRESS_1)
            .setWorkerToken("worker_token")
            .build());
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testGetWorkerMetadata() {
    WorkerMetadataResponse mockResponse =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addAllWorkEndpoints(DIRECT_PATH_ENDPOINTS)
            .putAllGlobalDataEndpoints(GLOBAL_DATA_ENDPOINTS)
            .build();
    TestWindmillEndpointsConsumer testWindmillEndpointsConsumer =
        new TestWindmillEndpointsConsumer();
    GetWorkerMetadataTestStub testStub =
        new GetWorkerMetadataTestStub(new TestGetWorkMetadataRequestObserver());
    int metadataVersion = -1;
    stream = getWorkerMetadataTestStream(testStub, metadataVersion, testWindmillEndpointsConsumer);
    testStub.injectWorkerMetadata(mockResponse);

    assertThat(testWindmillEndpointsConsumer.globalDataEndpoints.keySet())
        .containsExactlyElementsIn(GLOBAL_DATA_ENDPOINTS.keySet());
    assertThat(testWindmillEndpointsConsumer.windmillEndpoints)
        .containsExactlyElementsIn(
            DIRECT_PATH_ENDPOINTS.stream()
                .map(WindmillEndpoints.Endpoint::from)
                .collect(Collectors.toList()));
  }

  @Test
  public void testGetWorkerMetadata_consumesSubsequentResponseMetadata() {
    WorkerMetadataResponse initialResponse =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addAllWorkEndpoints(DIRECT_PATH_ENDPOINTS)
            .putAllGlobalDataEndpoints(GLOBAL_DATA_ENDPOINTS)
            .build();
    TestWindmillEndpointsConsumer testWindmillEndpointsConsumer =
        Mockito.spy(new TestWindmillEndpointsConsumer());

    GetWorkerMetadataTestStub testStub =
        new GetWorkerMetadataTestStub(new TestGetWorkMetadataRequestObserver());
    int metadataVersion = 0;
    stream = getWorkerMetadataTestStream(testStub, metadataVersion, testWindmillEndpointsConsumer);
    testStub.injectWorkerMetadata(initialResponse);

    List<WorkerMetadataResponse.Endpoint> newDirectPathEndpoints =
        Lists.newArrayList(
            WorkerMetadataResponse.Endpoint.newBuilder().setDirectEndpoint(IPV6_ADDRESS_2).build());
    Map<String, WorkerMetadataResponse.Endpoint> newGlobalDataEndpoints = new HashMap<>();
    newGlobalDataEndpoints.put(
        "new_global_data",
        WorkerMetadataResponse.Endpoint.newBuilder().setDirectEndpoint(IPV6_ADDRESS_2).build());

    WorkerMetadataResponse newWorkMetadataResponse =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(initialResponse.getMetadataVersion() + 1)
            .addAllWorkEndpoints(newDirectPathEndpoints)
            .putAllGlobalDataEndpoints(newGlobalDataEndpoints)
            .build();

    testStub.injectWorkerMetadata(newWorkMetadataResponse);

    assertThat(newGlobalDataEndpoints.keySet())
        .containsExactlyElementsIn(testWindmillEndpointsConsumer.globalDataEndpoints.keySet());
    assertThat(testWindmillEndpointsConsumer.windmillEndpoints)
        .containsExactlyElementsIn(
            newDirectPathEndpoints.stream()
                .map(WindmillEndpoints.Endpoint::from)
                .collect(Collectors.toList()));
  }

  @Test
  public void testGetWorkerMetadata_doesNotConsumeResponseIfMetadataStale() {
    WorkerMetadataResponse freshEndpoints =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addAllWorkEndpoints(DIRECT_PATH_ENDPOINTS)
            .putAllGlobalDataEndpoints(GLOBAL_DATA_ENDPOINTS)
            .build();

    TestWindmillEndpointsConsumer testWindmillEndpointsConsumer =
        Mockito.spy(new TestWindmillEndpointsConsumer());
    GetWorkerMetadataTestStub testStub =
        new GetWorkerMetadataTestStub(new TestGetWorkMetadataRequestObserver());
    int metadataVersion = 0;
    stream = getWorkerMetadataTestStream(testStub, metadataVersion, testWindmillEndpointsConsumer);
    testStub.injectWorkerMetadata(freshEndpoints);

    List<WorkerMetadataResponse.Endpoint> staleDirectPathEndpoints =
        Lists.newArrayList(
            WorkerMetadataResponse.Endpoint.newBuilder()
                .setDirectEndpoint("staleWindmillEndpoint")
                .build());
    Map<String, WorkerMetadataResponse.Endpoint> staleGlobalDataEndpoints = new HashMap<>();
    staleGlobalDataEndpoints.put(
        "stale_global_data",
        WorkerMetadataResponse.Endpoint.newBuilder().setDirectEndpoint("staleGlobalData").build());

    testStub.injectWorkerMetadata(
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addAllWorkEndpoints(staleDirectPathEndpoints)
            .putAllGlobalDataEndpoints(staleGlobalDataEndpoints)
            .build());

    // Should have ignored the stale update and only used initial.
    verify(testWindmillEndpointsConsumer).accept(WindmillEndpoints.from(freshEndpoints));
    verifyNoMoreInteractions(testWindmillEndpointsConsumer);
  }

  @Test
  public void testGetWorkerMetadata_correctlyAddsAndRemovesStreamFromRegistry() {
    GetWorkerMetadataTestStub testStub =
        new GetWorkerMetadataTestStub(new TestGetWorkMetadataRequestObserver());
    stream = getWorkerMetadataTestStream(testStub, 0, new TestWindmillEndpointsConsumer());
    testStub.injectWorkerMetadata(
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addAllWorkEndpoints(DIRECT_PATH_ENDPOINTS)
            .putAllGlobalDataEndpoints(GLOBAL_DATA_ENDPOINTS)
            .build());

    assertTrue(streamRegistry.contains(stream));
    stream.close();
    assertFalse(streamRegistry.contains(stream));
  }

  @Test
  public void testSendHealthCheck() {
    TestGetWorkMetadataRequestObserver requestObserver =
        Mockito.spy(new TestGetWorkMetadataRequestObserver());
    GetWorkerMetadataTestStub testStub = new GetWorkerMetadataTestStub(requestObserver);
    stream = getWorkerMetadataTestStream(testStub, 0, new TestWindmillEndpointsConsumer());
    stream.sendHealthCheck();

    verify(requestObserver).onNext(WorkerMetadataRequest.getDefaultInstance());
  }

  private static class GetWorkerMetadataTestStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    private final TestGetWorkMetadataRequestObserver requestObserver;
    private @Nullable StreamObserver<WorkerMetadataResponse> responseObserver;

    private GetWorkerMetadataTestStub(TestGetWorkMetadataRequestObserver requestObserver) {
      this.requestObserver = requestObserver;
    }

    @Override
    public StreamObserver<WorkerMetadataRequest> getWorkerMetadataStream(
        StreamObserver<WorkerMetadataResponse> responseObserver) {
      if (this.responseObserver == null) {
        this.responseObserver = responseObserver;
        requestObserver.responseObserver = this.responseObserver;
      }

      return requestObserver;
    }

    private void injectWorkerMetadata(WorkerMetadataResponse response) {
      if (responseObserver != null) {
        responseObserver.onNext(response);
      }
    }
  }

  @SuppressWarnings("UnusedVariable")
  private static class TestGetWorkMetadataRequestObserver
      implements StreamObserver<WorkerMetadataRequest> {
    private @Nullable StreamObserver<WorkerMetadataResponse> responseObserver;

    @Override
    public void onNext(WorkerMetadataRequest workerMetadataRequest) {}

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
    }
  }

  private static class TestWindmillEndpointsConsumer implements Consumer<WindmillEndpoints> {
    private final Map<String, WindmillEndpoints.Endpoint> globalDataEndpoints;
    private final Set<WindmillEndpoints.Endpoint> windmillEndpoints;

    private TestWindmillEndpointsConsumer() {
      this.globalDataEndpoints = new HashMap<>();
      this.windmillEndpoints = new HashSet<>();
    }

    @Override
    public void accept(WindmillEndpoints windmillEndpoints) {
      this.globalDataEndpoints.clear();
      this.windmillEndpoints.clear();
      this.globalDataEndpoints.putAll(windmillEndpoints.globalDataEndpoints());
      this.windmillEndpoints.addAll(windmillEndpoints.windmillEndpoints());
    }
  }
}
