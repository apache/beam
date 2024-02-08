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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream.DEFAULT_STREAM_RPC_DEADLINE_SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.streaming.Commit;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.CompleteCommit;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.util.MutableHandlerRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class GrpcDirectCommitWorkStreamTest {
  private static final ByteString DEFAULT_KEY = ByteString.copyFromUtf8("testKey");
  private static final JobHeader TEST_JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId("test_job")
          .setWorkerId("test_worker")
          .setProjectId("test_project")
          .build();
  private static final String FAKE_SERVER_NAME = "Fake server for DirectCommitWorkStreamTest";
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry = new HashSet<>();
  private final AtomicLong idGenerator = new AtomicLong();
  private ManagedChannel inProcessChannel;

  private static WorkItemCommitRequest workItemCommitRequest(WorkItem workItem) {
    return Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(DEFAULT_KEY)
        .setShardingKey(1L)
        .setWorkToken(workItem.getWorkToken())
        .setCacheToken(workItem.getCacheToken())
        .build();
  }

  private static WorkItem workItem(long workToken, long cacheToken) {
    return WorkItem.newBuilder()
        .setKey(DEFAULT_KEY)
        .setWorkToken(workToken)
        .setCacheToken(cacheToken)
        .build();
  }

  private static Work work(WorkItem workItem) {
    return Work.create(workItem, Instant::now, new ArrayList<>(), unused -> {});
  }

  private static Work work(WorkItem workItem, Consumer<Work> workProcessor) {
    return Work.create(workItem, Instant::now, new ArrayList<>(), workProcessor);
  }

  private static ComputationState computationState(String computationId) {
    return new ComputationState(
        computationId,
        new MapTask().setStageName("test").setSystemName("test"),
        new BoundedQueueExecutor(
            1,
            10,
            TimeUnit.SECONDS,
            1,
            10000000,
            new ThreadFactoryBuilder()
                .setNameFormat("TestComputationState-%d")
                .setDaemon(true)
                .build()),
        new HashMap<>(),
        null);
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
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testQueueCommits() throws InterruptedException {
    TestGrpcDirectCommitWorkStreamStub commitWorkStreamStub =
        new TestGrpcDirectCommitWorkStreamStub(new TestCommitWorkStreamRequestObserver());
    GrpcDirectCommitWorkStream commitWorkStream =
        directCommitWorkTestStream(commitWorkStreamStub, (unused) -> {}, (unused) -> {});
    WorkItem workItem = workItem(1, 1);
    Work work = work(workItem);
    work.setState(Work.State.PROCESSING);
    ComputationState computationState = computationState("test");
    computationState.activateWork(ShardedKey.create(DEFAULT_KEY, 1), work);
    commitWorkStream.queueCommit(
        Commit.create(workItemCommitRequest(workItem), computationState, work));

    assertThat(work.getState()).isEqualTo(Work.State.QUEUED);
    // Wait for commit to get queued.
    Thread.sleep(100);
    assertThat(commitWorkStream.currentActiveCommitBytes())
        .isEqualTo(
            Commit.create(workItemCommitRequest(workItem), computationState, work).getSize());
    commitWorkStreamStub.injectCommitResponse(
        StreamingCommitResponse.newBuilder()
            .addRequestId(idGenerator.get())
            .addStatus(Windmill.CommitStatus.OK)
            .build());
    // Wait for commit response to get processed.
    Thread.sleep(100);
    assertThat(work.getState()).isEqualTo(Work.State.COMMITTING);
  }

  @Test
  public void testCommitForFailedWork() throws InterruptedException, TimeoutException {
    TestGrpcDirectCommitWorkStreamStub commitWorkStreamStub =
        new TestGrpcDirectCommitWorkStreamStub(new TestCommitWorkStreamRequestObserver());
    Set<Commit> failedCommitsResult = new HashSet<>();
    GrpcDirectCommitWorkStream commitWorkStream =
        Mockito.spy(
            directCommitWorkTestStream(
                commitWorkStreamStub, failedCommitsResult::add, (unused) -> {}));
    ShardedKey shardedKey = ShardedKey.create(DEFAULT_KEY, 1);
    WorkItem failedWorkItem = workItem(1, 1);
    WorkItemCommitRequest failedWorkItemCommitRequest = workItemCommitRequest(failedWorkItem);
    Work failedWork = work(failedWorkItem);
    failedWork.setState(Work.State.PROCESSING);
    failedWork.setFailed();

    Phaser ackWorkProcessed = new AdvancingPhaser(1);
    Set<Work> workProcessed = new HashSet<>();
    WorkItem nextWorkItem = workItem(2, 2);
    Work nextWork =
        work(
            nextWorkItem,
            work -> {
              workProcessed.add(work);
              ackWorkProcessed.arriveAndDeregister();
            });

    ComputationState computationState = computationState("test");
    computationState.activateWork(shardedKey, failedWork);
    computationState.activateWork(shardedKey, nextWork);

    commitWorkStream.queueCommit(
        Commit.create(workItemCommitRequest(failedWorkItem), computationState, failedWork));
    try {
      ackWorkProcessed.awaitAdvanceInterruptibly(0, 100, TimeUnit.MILLISECONDS);
    } catch (TimeoutException unused) {
    }

    assertThat(failedWork.getState()).isEqualTo(Work.State.QUEUED);
    Mockito.verify(commitWorkStream, times(0)).commitWorkItem(any(), any(), any());
    assertThat(failedCommitsResult)
        .contains(Commit.create(failedWorkItemCommitRequest, computationState, failedWork));
    assertThat(workProcessed).contains(nextWork);
  }

  @Test
  public void testCommitFailedInStreamingEngine() throws InterruptedException {
    TestGrpcDirectCommitWorkStreamStub commitWorkStreamStub =
        new TestGrpcDirectCommitWorkStreamStub(new TestCommitWorkStreamRequestObserver());
    HashSet<CompleteCommit> completeCommits = new HashSet<>();
    GrpcDirectCommitWorkStream commitWorkStream =
        directCommitWorkTestStream(commitWorkStreamStub, (unused) -> {}, completeCommits::add);
    WorkItem workItem = workItem(1, 1);
    Work work = work(workItem);
    work.setState(Work.State.PROCESSING);
    ComputationState computationState = computationState("test");
    computationState.activateWork(ShardedKey.create(DEFAULT_KEY, 1), work);
    commitWorkStream.queueCommit(
        Commit.create(workItemCommitRequest(workItem), computationState, work));
    assertThat(work.getState()).isEqualTo(Work.State.QUEUED);
    Commit expectedFailedCommit =
        Commit.create(workItemCommitRequest(workItem), computationState, work);
    CompleteCommit expectedFailedCommitAndStatus =
        CompleteCommit.create(expectedFailedCommit, Windmill.CommitStatus.ABORTED);
    assertThat(commitWorkStream.currentActiveCommitBytes())
        .isEqualTo(expectedFailedCommit.getSize());
    Thread.sleep(100);
    commitWorkStreamStub.injectCommitResponse(
        StreamingCommitResponse.newBuilder()
            .addRequestId(idGenerator.get())
            .addStatus(Windmill.CommitStatus.ABORTED)
            .build());

    assertThat(work.getState()).isEqualTo(Work.State.COMMITTING);
    assertThat(completeCommits).contains(expectedFailedCommitAndStatus);
  }

  private GrpcDirectCommitWorkStream directCommitWorkTestStream(
      TestGrpcDirectCommitWorkStreamStub directCommitWorkStreamTestStub,
      Consumer<Commit> onFailedCommits,
      Consumer<CompleteCommit> onCompleteCommit) {
    serviceRegistry.addService(directCommitWorkStreamTestStub);
    return GrpcDirectCommitWorkStream.create(
        responseObserver ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel)
                .commitWorkStream(responseObserver),
        FluentBackoff.DEFAULT.backoff(),
        StreamObserverFactory.direct(DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, 1),
        streamRegistry,
        1, // logEveryNStreamFailures
        new ThrottleTimer(),
        TEST_JOB_HEADER,
        idGenerator,
        1, // streamingRpcBatchLimit
        onFailedCommits,
        onCompleteCommit);
  }

  private static class TestGrpcDirectCommitWorkStreamStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    private final TestCommitWorkStreamRequestObserver requestObserver;
    private @Nullable StreamObserver<StreamingCommitResponse> responseObserver;

    private TestGrpcDirectCommitWorkStreamStub(
        TestCommitWorkStreamRequestObserver requestObserver) {
      this.requestObserver = requestObserver;
    }

    @Override
    public StreamObserver<StreamingCommitWorkRequest> commitWorkStream(
        StreamObserver<StreamingCommitResponse> responseObserver) {
      if (this.responseObserver == null) {
        this.responseObserver = responseObserver;
        requestObserver.responseObserver = this.responseObserver;
      }

      return requestObserver;
    }

    private void injectCommitResponse(StreamingCommitResponse response) {
      if (responseObserver != null) {
        responseObserver.onNext(response);
      }
    }
  }

  @SuppressWarnings("UnusedVariable")
  private static class TestCommitWorkStreamRequestObserver
      implements StreamObserver<StreamingCommitWorkRequest> {
    private @Nullable StreamObserver<StreamingCommitResponse> responseObserver;

    @Override
    public void onNext(StreamingCommitWorkRequest workerMetadataRequest) {}

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
    }
  }
}
