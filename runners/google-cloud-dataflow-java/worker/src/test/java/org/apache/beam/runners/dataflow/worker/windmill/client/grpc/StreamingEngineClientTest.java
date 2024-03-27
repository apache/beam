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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessSocketAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.util.MutableHandlerRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineClientTest {
  private static final WindmillServiceAddress DEFAULT_WINDMILL_SERVICE_ADDRESS =
      WindmillServiceAddress.create(HostAndPort.fromParts(WindmillChannelFactory.LOCALHOST, 443));
  private static final ImmutableMap<String, WorkerMetadataResponse.Endpoint> DEFAULT =
      ImmutableMap.of(
          "global_data",
          WorkerMetadataResponse.Endpoint.newBuilder()
              .setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().toString())
              .build());
  private static final long CLIENT_ID = 1L;
  private static final String JOB_ID = "jobId";
  private static final String PROJECT_ID = "projectId";
  private static final String WORKER_ID = "workerId";
  private static final JobHeader JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId(JOB_ID)
          .setProjectId(PROJECT_ID)
          .setWorkerId(WORKER_ID)
          .build();

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final GrpcWindmillStreamFactory streamFactory =
      spy(GrpcWindmillStreamFactory.of(JOB_HEADER).build());
  private final ChannelCachingStubFactory stubFactory =
      new FakeWindmillStubFactory(
          () ->
              grpcCleanup.register(
                  WindmillChannelFactory.inProcessChannel("StreamingEngineClientTest")));
  private final GrpcDispatcherClient dispatcherClient =
      GrpcDispatcherClient.forTesting(
          stubFactory, new ArrayList<>(), new ArrayList<>(), new HashSet<>());
  private final AtomicReference<StreamingEngineConnectionState> connections =
      new AtomicReference<>(StreamingEngineConnectionState.EMPTY);
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private Server fakeStreamingEngineServer;
  private CountDownLatch getWorkerMetadataReady;
  private GetWorkerMetadataTestStub fakeGetWorkerMetadataStub;
  private StreamingEngineClient streamingEngineClient;

  private static WorkItemProcessor noOpProcessWorkItemFn() {
    return (computation,
        inputDataWatermark,
        synchronizedProcessingTime,
        workItem,
        ackQueuedWorkItem,
        getWorkStreamLatencies) -> {};
  }

  private static GetWorkRequest getWorkRequest(long items, long bytes) {
    return GetWorkRequest.newBuilder()
        .setJobId(JOB_ID)
        .setProjectId(PROJECT_ID)
        .setWorkerId(WORKER_ID)
        .setClientId(CLIENT_ID)
        .setMaxItems(items)
        .setMaxBytes(bytes)
        .build();
  }

  private static WorkerMetadataResponse.Endpoint metadataResponseEndpoint(String workerToken) {
    return WorkerMetadataResponse.Endpoint.newBuilder()
        .setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().getHost())
        .setBackendWorkerToken(workerToken)
        .build();
  }

  @Before
  public void setUp() throws IOException {
    stubFactory.shutdown();
    fakeStreamingEngineServer =
        grpcCleanup.register(
            InProcessServerBuilder.forName("StreamingEngineClientTest")
                .fallbackHandlerRegistry(serviceRegistry)
                .executor(Executors.newFixedThreadPool(1))
                .build());

    fakeStreamingEngineServer.start();
    dispatcherClient.consumeWindmillDispatcherEndpoints(
        ImmutableSet.of(
            HostAndPort.fromString(
                new InProcessSocketAddress("StreamingEngineClientTest").toString())));
    getWorkerMetadataReady = new CountDownLatch(1);
    fakeGetWorkerMetadataStub = new GetWorkerMetadataTestStub(getWorkerMetadataReady);
    serviceRegistry.addService(fakeGetWorkerMetadataStub);
  }

  @After
  public void cleanUp() {
    Preconditions.checkNotNull(streamingEngineClient).finish();
    fakeGetWorkerMetadataStub.close();
    fakeStreamingEngineServer.shutdownNow();
    stubFactory.shutdown();
  }

  private StreamingEngineClient newStreamingEngineClient(
      GetWorkBudget getWorkBudget,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      WorkItemProcessor workItemProcessor) {
    return StreamingEngineClient.forTesting(
        JOB_HEADER,
        getWorkBudget,
        connections,
        streamFactory,
        workItemProcessor,
        stubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        CLIENT_ID);
  }

  @Test
  public void testStreamsStartCorrectly() throws InterruptedException {
    long items = 10L;
    long bytes = 10L;
    int numBudgetDistributionsExpected = 1;

    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(numBudgetDistributionsExpected));

    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(items).setBytes(bytes).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(metadataResponseEndpoint(workerToken))
            .addWorkEndpoints(metadataResponseEndpoint(workerToken2))
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);

    StreamingEngineConnectionState currentConnections = connections.get();

    assertEquals(2, currentConnections.windmillConnections().size());
    assertEquals(2, currentConnections.windmillStreams().size());
    Set<String> workerTokens =
        currentConnections.windmillConnections().values().stream()
            .map(WindmillConnection::backendWorkerToken)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    assertTrue(workerTokens.contains(workerToken));
    assertTrue(workerTokens.contains(workerToken2));

    verify(getWorkBudgetDistributor, atLeast(1))
        .distributeBudget(
            any(), eq(GetWorkBudget.builder().setItems(items).setBytes(bytes).build()));

    verify(streamFactory, times(2))
        .createDirectGetWorkStream(
            any(), eq(getWorkRequest(0, 0)), any(), any(), any(), eq(noOpProcessWorkItemFn()));

    verify(streamFactory, times(2)).createGetDataStream(any(), any());
    verify(streamFactory, times(2)).createCommitWorkStream(any(), any());
  }

  @Test
  public void testScheduledBudgetRefresh() throws InterruptedException {
    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(2));
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1L).setBytes(1L).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(metadataResponseEndpoint("workerToken"))
            .putAllGlobalDataEndpoints(DEFAULT)
            .build());
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);
    verify(getWorkBudgetDistributor, atLeast(2)).distributeBudget(any(), any());
  }

  @Test
  public void testOnNewWorkerMetadata_correctlyRemovesStaleWindmillServers()
      throws InterruptedException {
    int metadataCount = 2;
    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(metadataCount));
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken)
                    .build())
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken2)
                    .build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken3)
                    .build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);
    StreamingEngineConnectionState currentConnections = connections.get();
    assertEquals(1, currentConnections.windmillConnections().size());
    assertEquals(1, currentConnections.windmillStreams().size());
    Set<String> workerTokens =
        connections.get().windmillConnections().values().stream()
            .map(WindmillConnection::backendWorkerToken)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    assertFalse(workerTokens.contains(workerToken));
    assertFalse(workerTokens.contains(workerToken2));
  }

  @Test
  public void testOnNewWorkerMetadata_redistributesBudget() throws InterruptedException {
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken)
                    .build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken2)
                    .build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse thirdWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(3)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder()
                    .setBackendWorkerToken(workerToken3)
                    .build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    List<WorkerMetadataResponse> workerMetadataResponses =
        Lists.newArrayList(firstWorkerMetadata, secondWorkerMetadata, thirdWorkerMetadata);

    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(workerMetadataResponses.size()));
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    getWorkerMetadataReady.await();

    // Make sure we are injecting the metadata from smallest to largest.
    workerMetadataResponses.stream()
        .sorted(Comparator.comparingLong(WorkerMetadataResponse::getMetadataVersion))
        .forEach(fakeGetWorkerMetadataStub::injectWorkerMetadata);

    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);
    verify(getWorkBudgetDistributor, atLeast(workerMetadataResponses.size()))
        .distributeBudget(any(), any());
  }

  private void waitForWorkerMetadataToBeConsumed(
      TestGetWorkBudgetDistributor getWorkBudgetDistributor) throws InterruptedException {
    getWorkBudgetDistributor.waitForBudgetDistribution();
  }

  private static class GetWorkerMetadataTestStub
      extends CloudWindmillMetadataServiceV1Alpha1Grpc
          .CloudWindmillMetadataServiceV1Alpha1ImplBase {
    private static final WorkerMetadataResponse CLOSE_ALL_STREAMS =
        WorkerMetadataResponse.newBuilder().setMetadataVersion(Long.MAX_VALUE).build();
    private final CountDownLatch ready;
    private @Nullable StreamObserver<WorkerMetadataResponse> responseObserver;

    private GetWorkerMetadataTestStub(CountDownLatch ready) {
      this.ready = ready;
    }

    @Override
    public StreamObserver<WorkerMetadataRequest> getWorkerMetadata(
        StreamObserver<WorkerMetadataResponse> responseObserver) {
      if (this.responseObserver == null) {
        ready.countDown();
        this.responseObserver = responseObserver;
      }

      return new StreamObserver<WorkerMetadataRequest>() {
        @Override
        public void onNext(WorkerMetadataRequest workerMetadataRequest) {}

        @Override
        public void onError(Throwable throwable) {
          if (responseObserver != null) {
            responseObserver.onError(throwable);
          }
        }

        @Override
        public void onCompleted() {}
      };
    }

    private void injectWorkerMetadata(WorkerMetadataResponse response) {
      if (responseObserver != null) {
        responseObserver.onNext(response);
      }
    }

    private void close() {
      if (responseObserver != null) {
        // Send an empty response to close out all the streams and channels currently open in
        // Streaming Engine Client.
        responseObserver.onNext(CLOSE_ALL_STREAMS);
      }
    }
  }

  private static class TestGetWorkBudgetDistributor implements GetWorkBudgetDistributor {
    private final CountDownLatch getWorkBudgetDistributorTriggered;

    private TestGetWorkBudgetDistributor(int numBudgetDistributionsExpected) {
      this.getWorkBudgetDistributorTriggered = new CountDownLatch(numBudgetDistributionsExpected);
    }

    @SuppressWarnings("ReturnValueIgnored")
    private void waitForBudgetDistribution() throws InterruptedException {
      getWorkBudgetDistributorTriggered.await(5, TimeUnit.SECONDS);
    }

    @Override
    public void distributeBudget(
        ImmutableCollection<WindmillStreamSender> streams, GetWorkBudget getWorkBudget) {
      streams.forEach(stream -> stream.adjustBudget(getWorkBudget.items(), getWorkBudget.bytes()));
      getWorkBudgetDistributorTriggered.countDown();
    }
  }
}
