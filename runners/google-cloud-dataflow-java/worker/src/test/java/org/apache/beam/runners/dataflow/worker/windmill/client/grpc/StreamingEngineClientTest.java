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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetRefresher;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineClientTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
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
  private final Set<ManagedChannel> channels = new HashSet<>();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

  private final GrpcWindmillStreamFactory streamFactory =
      spy(GrpcWindmillStreamFactory.of(JOB_HEADER).build());
  private final WindmillStubFactory stubFactory =
      WindmillStubFactory.inProcessStubFactory(
          "StreamingEngineClientTest",
          name -> {
            ManagedChannel channel =
                grpcCleanup.register(WindmillChannelFactory.inProcessChannel(name));
            channels.add(channel);
            return channel;
          });
  private final GrpcDispatcherClient dispatcherClient =
      GrpcDispatcherClient.forTesting(stubFactory, new ArrayList<>(), new HashSet<>());
  private final GetWorkBudgetDistributor getWorkBudgetDistributor =
      spy(new TestGetWorkBudgetDistributor());
  private final AtomicReference<StreamingEngineConnectionState> connections =
      new AtomicReference<>(StreamingEngineConnectionState.EMPTY);
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
    return WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken).build();
  }

  @Before
  public void setUp() throws IOException {
    channels.forEach(ManagedChannel::shutdownNow);
    channels.clear();
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
    fakeGetWorkerMetadataStub.close();
    fakeStreamingEngineServer.shutdownNow();
    channels.forEach(ManagedChannel::shutdownNow);
    Preconditions.checkNotNull(streamingEngineClient).finish();
  }

  private StreamingEngineClient newStreamingEngineClient(
      GetWorkBudget getWorkBudget, WorkItemProcessor workItemProcessor) {
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

    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(items).setBytes(bytes).build(),
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
    StreamingEngineConnectionState currentConnections = waitForWorkerMetadataToBeConsumed(1);

    assertEquals(2, currentConnections.windmillConnections().size());
    assertEquals(2, currentConnections.windmillStreams().size());
    Set<String> workerTokens =
        connections.get().windmillConnections().values().stream()
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
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1L).setBytes(1L).build(), noOpProcessWorkItemFn());

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(metadataResponseEndpoint("workerToken"))
            .putAllGlobalDataEndpoints(DEFAULT)
            .build());
    waitForWorkerMetadataToBeConsumed(1);
    Thread.sleep(GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS);
    verify(getWorkBudgetDistributor, atLeast(2)).distributeBudget(any(), any());
  }

  @Test
  @Ignore("https://github.com/apache/beam/issues/28957") // stuck test
  public void testOnNewWorkerMetadata_correctlyRemovesStaleWindmillServers()
      throws InterruptedException {
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(), noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken).build())
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken2).build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken3).build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);

    StreamingEngineConnectionState currentConnections = waitForWorkerMetadataToBeConsumed(2);

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
    streamingEngineClient =
        newStreamingEngineClient(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(), noOpProcessWorkItemFn());

    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";
    String workerToken3 = "workerToken3";

    WorkerMetadataResponse firstWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(1)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken).build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse secondWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(2)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken2).build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();
    WorkerMetadataResponse thirdWorkerMetadata =
        WorkerMetadataResponse.newBuilder()
            .setMetadataVersion(3)
            .addWorkEndpoints(
                WorkerMetadataResponse.Endpoint.newBuilder().setWorkerToken(workerToken3).build())
            .putAllGlobalDataEndpoints(DEFAULT)
            .build();

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    Thread.sleep(50);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    Thread.sleep(50);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(thirdWorkerMetadata);
    Thread.sleep(50);
    verify(getWorkBudgetDistributor, atLeast(3)).distributeBudget(any(), any());
  }

  private StreamingEngineConnectionState waitForWorkerMetadataToBeConsumed(
      int expectedMetadataConsumed) throws InterruptedException {
    int currentMetadataConsumed = 0;
    StreamingEngineConnectionState currentConsumedMetadata = StreamingEngineConnectionState.EMPTY;
    while (true) {
      if (!connections.get().equals(currentConsumedMetadata)) {
        ++currentMetadataConsumed;
        if (currentMetadataConsumed == expectedMetadataConsumed) {
          break;
        }
        currentConsumedMetadata = connections.get();
      }
    }
    // Wait for metadata to be consumed and budgets to be redistributed.
    Thread.sleep(GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS);
    return connections.get();
  }

  private static class GetWorkerMetadataTestStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    private static final WorkerMetadataResponse CLOSE_ALL_STREAMS =
        WorkerMetadataResponse.newBuilder().setMetadataVersion(100).build();
    private final CountDownLatch ready;
    private @Nullable StreamObserver<WorkerMetadataResponse> responseObserver;

    private GetWorkerMetadataTestStub(CountDownLatch ready) {
      this.ready = ready;
    }

    @Override
    public StreamObserver<WorkerMetadataRequest> getWorkerMetadataStream(
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
    @Override
    public void distributeBudget(
        ImmutableCollection<WindmillStreamSender> streams, GetWorkBudget getWorkBudget) {
      streams.forEach(stream -> stream.adjustBudget(getWorkBudget.items(), getWorkBudget.bytes()));
    }
  }
}
