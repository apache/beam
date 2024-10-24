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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.ServerCallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.util.MutableHandlerRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;

@RunWith(JUnit4.class)
public class GrpcCommitWorkStreamTest {
  private static final String FAKE_SERVER_NAME = "Fake server for GrpcCommitWorkStreamTest";
  private static final Windmill.JobHeader TEST_JOB_HEADER =
      Windmill.JobHeader.newBuilder()
          .setJobId("test_job")
          .setWorkerId("test_worker")
          .setProjectId("test_project")
          .build();
  private static final String COMPUTATION_ID = "computationId";

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private ManagedChannel inProcessChannel;

  private static Windmill.WorkItemCommitRequest workItemCommitRequest(long value) {
    return Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(ByteString.EMPTY)
        .setShardingKey(value)
        .setWorkToken(value)
        .setCacheToken(value)
        .build();
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
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  private GrpcCommitWorkStream createCommitWorkStream(CommitWorkStreamTestStub testStub) {
    serviceRegistry.addService(testStub);
    GrpcCommitWorkStream commitWorkStream =
        (GrpcCommitWorkStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .build()
                .createCommitWorkStream(
                    CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel),
                    new ThrottleTimer());
    commitWorkStream.start();
    return commitWorkStream;
  }

  @Test
  public void testShutdown_abortsQueuedCommits() throws InterruptedException {
    int numCommits = 5;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);
    Set<Windmill.CommitStatus> onDone = new HashSet<>();

    TestCommitWorkStreamRequestObserver requestObserver =
        spy(new TestCommitWorkStreamRequestObserver());
    CommitWorkStreamTestStub testStub = new CommitWorkStreamTestStub(requestObserver);
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream(testStub);
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        batcher.commitWorkItem(
            COMPUTATION_ID,
            workItemCommitRequest(i),
            commitStatus -> {
              onDone.add(commitStatus);
              commitProcessed.countDown();
            });
      }
    }

    // Verify that we sent the commits above in a request + the initial header.
    verify(requestObserver, times(2)).onNext(any(Windmill.StreamingCommitWorkRequest.class));
    // We won't get responses so we will have some pending requests.
    assertTrue(commitWorkStream.hasPendingRequests());

    commitWorkStream.shutdown();
    commitProcessed.await();

    assertThat(onDone).containsExactly(Windmill.CommitStatus.ABORTED);
  }

  @Test
  public void testCommitWorkItem_afterShutdownFalse() {
    int numCommits = 5;

    CommitWorkStreamTestStub testStub =
        new CommitWorkStreamTestStub(new TestCommitWorkStreamRequestObserver());
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream(testStub);

    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        assertTrue(batcher.commitWorkItem(COMPUTATION_ID, workItemCommitRequest(i), ignored -> {}));
      }
    }
    commitWorkStream.shutdown();

    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        Set<Windmill.CommitStatus> commitStatuses = new HashSet<>();
        assertFalse(
            batcher.commitWorkItem(COMPUTATION_ID, workItemCommitRequest(i), commitStatuses::add));
        assertThat(commitStatuses).containsExactly(Windmill.CommitStatus.ABORTED);
      }
    }
  }

  @Test
  public void testSend_notCalledAfterShutdown() {
    int numCommits = 5;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);

    TestCommitWorkStreamRequestObserver requestObserver =
        spy(new TestCommitWorkStreamRequestObserver());
    InOrder requestObserverVerifier = inOrder(requestObserver);

    CommitWorkStreamTestStub testStub = new CommitWorkStreamTestStub(requestObserver);
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream(testStub);
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        assertTrue(
            batcher.commitWorkItem(
                COMPUTATION_ID,
                workItemCommitRequest(i),
                commitStatus -> commitProcessed.countDown()));
      }
      // Shutdown the stream before we exit the try-with-resources block which will try to send()
      // the batched request.
      commitWorkStream.shutdown();
    }

    // send() uses the requestObserver to send requests. We expect 1 send since startStream() sends
    // the header, which happens before we shutdown.
    requestObserverVerifier
        .verify(requestObserver)
        .onNext(any(Windmill.StreamingCommitWorkRequest.class));
    requestObserverVerifier.verify(requestObserver).onError(any());
    requestObserverVerifier.verifyNoMoreInteractions();
  }

  private static class TestCommitWorkStreamRequestObserver
      implements StreamObserver<Windmill.StreamingCommitWorkRequest> {
    private @Nullable StreamObserver<Windmill.StreamingCommitResponse> responseObserver;

    @Override
    public void onNext(Windmill.StreamingCommitWorkRequest streamingCommitWorkRequest) {}

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {
      if (responseObserver != null) {
        responseObserver.onCompleted();
      }
    }
  }

  private static class CommitWorkStreamTestStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    private final TestCommitWorkStreamRequestObserver requestObserver;
    private @Nullable StreamObserver<Windmill.StreamingCommitResponse> responseObserver;

    private CommitWorkStreamTestStub(TestCommitWorkStreamRequestObserver requestObserver) {
      this.requestObserver = requestObserver;
    }

    @Override
    public StreamObserver<Windmill.StreamingCommitWorkRequest> commitWorkStream(
        StreamObserver<Windmill.StreamingCommitResponse> responseObserver) {
      if (this.responseObserver == null) {
        ((ServerCallStreamObserver<Windmill.StreamingCommitResponse>) responseObserver)
            .setOnCancelHandler(() -> {});
        this.responseObserver = responseObserver;
        requestObserver.responseObserver = this.responseObserver;
      }

      return requestObserver;
    }
  }
}
