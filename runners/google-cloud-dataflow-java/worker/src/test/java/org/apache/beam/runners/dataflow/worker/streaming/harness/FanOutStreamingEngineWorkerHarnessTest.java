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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkerMetadataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactoryFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetSpender;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FanOutStreamingEngineWorkerHarnessTest {
  private static final WindmillServiceAddress DEFAULT_WINDMILL_SERVICE_ADDRESS =
      WindmillServiceAddress.create(HostAndPort.fromParts(WindmillChannelFactory.LOCALHOST, 443));
  private static final ImmutableMap<String, WorkerMetadataResponse.Endpoint> DEFAULT =
      ImmutableMap.of(
          "global_data",
          WorkerMetadataResponse.Endpoint.newBuilder()
              .setDirectEndpoint(DEFAULT_WINDMILL_SERVICE_ADDRESS.gcpServiceAddress().toString())
              .build());

  private static final String JOB_ID = "jobId";
  private static final String PROJECT_ID = "projectId";
  private static final String WORKER_ID = "workerId";
  private static final JobHeader JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId(JOB_ID)
          .setProjectId(PROJECT_ID)
          .setWorkerId(WORKER_ID)
          .setClientId(1L)
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
          PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class),
          new FakeWindmillStubFactoryFactory(stubFactory),
          new ArrayList<>(),
          new ArrayList<>(),
          new HashSet<>());
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private Server fakeStreamingEngineServer;
  private CountDownLatch getWorkerMetadataReady;
  private GetWorkerMetadataTestStub fakeGetWorkerMetadataStub;
  private FanOutStreamingEngineWorkerHarness fanOutStreamingEngineWorkProvider;

  private static WorkItemScheduler noOpProcessWorkItemFn() {
    return (workItem, watermarks, processingContext, getWorkStreamLatencies) -> {};
  }

  private static GetWorkRequest getWorkRequest(long items, long bytes) {
    return GetWorkRequest.newBuilder()
        .setJobId(JOB_ID)
        .setProjectId(PROJECT_ID)
        .setWorkerId(WORKER_ID)
        .setClientId(JOB_HEADER.getClientId())
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
    Preconditions.checkNotNull(fanOutStreamingEngineWorkProvider).shutdown();
    fakeStreamingEngineServer.shutdownNow();
    stubFactory.shutdown();
  }

  private FanOutStreamingEngineWorkerHarness newFanOutStreamingEngineWorkerHarness(
      GetWorkBudget getWorkBudget,
      GetWorkBudgetDistributor getWorkBudgetDistributor,
      WorkItemScheduler workItemScheduler) {
    return FanOutStreamingEngineWorkerHarness.forTesting(
        JOB_HEADER,
        getWorkBudget,
        streamFactory,
        workItemScheduler,
        stubFactory,
        getWorkBudgetDistributor,
        dispatcherClient,
        ignored -> mock(WorkCommitter.class),
        new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)));
  }

  @Test
  public void testStreamsStartCorrectly() throws InterruptedException {
    long items = 10L;
    long bytes = 10L;
    int numBudgetDistributionsExpected = 1;

    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(numBudgetDistributionsExpected));

    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
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

    StreamingEngineBackends currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();

    assertEquals(2, currentBackends.windmillStreams().size());
    Set<String> workerTokens =
        currentBackends.windmillStreams().keySet().stream()
            .map(endpoint -> endpoint.workerToken().orElseThrow(IllegalStateException::new))
            .collect(Collectors.toSet());

    assertTrue(workerTokens.contains(workerToken));
    assertTrue(workerTokens.contains(workerToken2));

    verify(getWorkBudgetDistributor, atLeast(1))
        .distributeBudget(
            any(), eq(GetWorkBudget.builder().setItems(items).setBytes(bytes).build()));

    verify(streamFactory, times(2))
        .createDirectGetWorkStream(
            any(),
            eq(getWorkRequest(0, 0)),
            any(),
            any(),
            any(),
            any(),
            eq(noOpProcessWorkItemFn()));

    verify(streamFactory, times(2)).createGetDataStream(any(), any());
    verify(streamFactory, times(2)).createCommitWorkStream(any(), any());
  }

  @Test
  public void testOnNewWorkerMetadata_correctlyRemovesStaleWindmillServers()
      throws InterruptedException {
    int metadataCount = 2;
    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(metadataCount));
    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
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
            .build();

    getWorkerMetadataReady.await();
    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);
    StreamingEngineBackends currentBackends = fanOutStreamingEngineWorkProvider.currentBackends();
    assertEquals(1, currentBackends.windmillStreams().size());
    Set<String> workerTokens =
        fanOutStreamingEngineWorkProvider.currentBackends().windmillStreams().keySet().stream()
            .map(endpoint -> endpoint.workerToken().orElseThrow(IllegalStateException::new))
            .collect(Collectors.toSet());

    assertFalse(workerTokens.contains(workerToken));
    assertFalse(workerTokens.contains(workerToken2));
    assertTrue(currentBackends.globalDataStreams().isEmpty());
  }

  @Test
  public void testOnNewWorkerMetadata_redistributesBudget() throws InterruptedException {
    String workerToken = "workerToken1";
    String workerToken2 = "workerToken2";

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

    TestGetWorkBudgetDistributor getWorkBudgetDistributor =
        spy(new TestGetWorkBudgetDistributor(1));
    fanOutStreamingEngineWorkProvider =
        newFanOutStreamingEngineWorkerHarness(
            GetWorkBudget.builder().setItems(1).setBytes(1).build(),
            getWorkBudgetDistributor,
            noOpProcessWorkItemFn());

    getWorkerMetadataReady.await();

    fakeGetWorkerMetadataStub.injectWorkerMetadata(firstWorkerMetadata);
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);
    getWorkBudgetDistributor.expectNumDistributions(1);
    fakeGetWorkerMetadataStub.injectWorkerMetadata(secondWorkerMetadata);
    waitForWorkerMetadataToBeConsumed(getWorkBudgetDistributor);

    verify(getWorkBudgetDistributor, times(2)).distributeBudget(any(), any());
  }

  private void waitForWorkerMetadataToBeConsumed(
      TestGetWorkBudgetDistributor getWorkBudgetDistributor) throws InterruptedException {
    getWorkBudgetDistributor.waitForBudgetDistribution();
  }

  private static class GetWorkerMetadataTestStub
      extends CloudWindmillMetadataServiceV1Alpha1Grpc
          .CloudWindmillMetadataServiceV1Alpha1ImplBase {
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
  }

  private static class TestGetWorkBudgetDistributor implements GetWorkBudgetDistributor {
    private CountDownLatch getWorkBudgetDistributorTriggered;

    private TestGetWorkBudgetDistributor(int numBudgetDistributionsExpected) {
      this.getWorkBudgetDistributorTriggered = new CountDownLatch(numBudgetDistributionsExpected);
    }

    @SuppressWarnings("ReturnValueIgnored")
    private void waitForBudgetDistribution() throws InterruptedException {
      getWorkBudgetDistributorTriggered.await(5, TimeUnit.SECONDS);
    }

    private void expectNumDistributions(int numBudgetDistributionsExpected) {
      this.getWorkBudgetDistributorTriggered = new CountDownLatch(numBudgetDistributionsExpected);
    }

    @Override
    public <T extends GetWorkBudgetSpender> void distributeBudget(
        ImmutableCollection<T> streams, GetWorkBudget getWorkBudget) {
      streams.forEach(stream -> stream.setBudget(getWorkBudget.items(), getWorkBudget.bytes()));
      getWorkBudgetDistributorTriggered.countDown();
    }
  }
}
