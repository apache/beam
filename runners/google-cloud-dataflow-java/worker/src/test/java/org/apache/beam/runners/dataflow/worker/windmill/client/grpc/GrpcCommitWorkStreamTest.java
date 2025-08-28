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
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.TriggeredScheduledExecutorService;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverCancelledException;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  @SuppressWarnings("InlineMeInliner") // inline `Strings.repeat()` - Java 11+ API only
  private static final ByteString LARGE_BYTE_STRING =
      ByteString.copyFromUtf8(Strings.repeat("a", 2 * 1024 * 1024));

  @Rule public final ErrorCollector errorCollector = new ErrorCollector();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Rule
  public transient Timeout globalTimeout =
      Timeout.builder().withTimeout(10, TimeUnit.MINUTES).withLookingForStuckThread(true).build();

  private final FakeWindmillGrpcService fakeService = new FakeWindmillGrpcService(errorCollector);
  private ManagedChannel inProcessChannel;
  private Server inProcessServer;

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
    inProcessServer =
        grpcCleanup.register(
            InProcessServerBuilder.forName(FAKE_SERVER_NAME)
                .addService(fakeService)
                .directExecutor()
                .build()
                .start());
    inProcessChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName(FAKE_SERVER_NAME).directExecutor().build());
  }

  @After
  public void cleanUp() {
    inProcessServer.shutdownNow();
    inProcessChannel.shutdownNow();
  }

  private GrpcCommitWorkStream createCommitWorkStream() {
    GrpcCommitWorkStream commitWorkStream =
        (GrpcCommitWorkStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .build()
                .createCommitWorkStream(CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel));
    commitWorkStream.start();
    return commitWorkStream;
  }

  private GrpcCommitWorkStream createCommitWorkStreamWithPhysicalStreamHandover(
      ScheduledExecutorService executor) {
    GrpcCommitWorkStream commitWorkStream =
        (GrpcCommitWorkStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .setDirectStreamingRpcPhysicalStreamHalfCloseAfter(Duration.ofMinutes(1))
                .setScheduledExecutorServiceSupplier(
                    new Supplier<ScheduledExecutorService>() {
                      private final AtomicBoolean vended = new AtomicBoolean();

                      @Override
                      public ScheduledExecutorService get() {
                        assertFalse(vended.getAndSet(true));
                        return executor;
                      }
                    })
                .build()
                .createDirectCommitWorkStream(
                    WindmillConnection.builder()
                        .setStubSupplier(
                            () -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel))
                        .build());
    commitWorkStream.start();
    return commitWorkStream;
  }

  @Test
  public void testShutdown_abortsActiveCommits() throws InterruptedException, ExecutionException {
    int numCommits = 5;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);
    Set<Windmill.CommitStatus> onDone = new HashSet<>();

    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
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
    } catch (StreamObserverCancelledException ignored) {
    }

    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();
    // The next request should have some chunks.
    assertThat(streamInfo.requests.take().getCommitChunkList()).isNotEmpty();

    // We won't get responses so we will have some pending requests.
    assertThat(commitProcessed.getCount()).isGreaterThan(0);
    commitWorkStream.shutdown();
    streamInfo.onDone.get();

    commitProcessed.await();

    assertThat(onDone).containsExactly(Windmill.CommitStatus.ABORTED);
  }

  @Test
  public void testCommitWorkItem_abortsCommitsSentAfterShutdown()
      throws InterruptedException, ExecutionException {
    int numCommits = 5;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);

    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();
    commitWorkStream.shutdown();
    assertNotNull(streamInfo.onDone.get());

    AtomicBoolean allAborted = new AtomicBoolean(true);
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        assertTrue(
            batcher.commitWorkItem(
                COMPUTATION_ID,
                workItemCommitRequest(i),
                (status) -> {
                  if (status != Windmill.CommitStatus.ABORTED) {
                    allAborted.set(false);
                  }
                  commitProcessed.countDown();
                }));
      }
    }
    commitProcessed.await();
    assertTrue(allAborted.get());
  }

  @Test
  public void testCommitWorkItem_retryOnNewStream() throws Exception {
    int numCommits = 5;
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    final AtomicBoolean allOk = new AtomicBoolean(true);
    final CountDownLatch firstResponsesDone = new CountDownLatch(2);
    final CountDownLatch secondResponsesDone = new CountDownLatch(3);
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        int finalI = i;
        assertTrue(
            batcher.commitWorkItem(
                COMPUTATION_ID,
                workItemCommitRequest(i),
                (status) -> {
                  if (status != Windmill.CommitStatus.OK) {
                    allOk.set(false);
                  }
                  if (finalI == 0 || finalI == 4) {
                    firstResponsesDone.countDown();
                  } else {
                    secondResponsesDone.countDown();
                  }
                }));
      }
    }
    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertEquals(5, request.getCommitChunkCount());
    {
      // Check if request ids and work tokens match.
      Map<Long, Long> requestIdWorkTokenMap = new HashMap<>();
      Map<Long, Long> expectedRequestIdWorkTokenMap = new HashMap<>();
      for (int i = 0; i < 5; ++i) {
        Windmill.WorkItemCommitRequest parsedRequest =
            Windmill.WorkItemCommitRequest.parseFrom(
                request.getCommitChunk(i).getSerializedWorkItemCommit());
        requestIdWorkTokenMap.put(
            request.getCommitChunk(i).getRequestId(), parsedRequest.getWorkToken());
      }
      for (int i = 1; i <= 5; ++i) {
        expectedRequestIdWorkTokenMap.put((long) i, (long) (i - 1));
      }
      assertThat(requestIdWorkTokenMap).containsExactlyEntriesIn(expectedRequestIdWorkTokenMap);
    }
    // Send back that 1 and 5 finished.
    streamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).addRequestId(5).build());
    firstResponsesDone.await();

    // Simulate that the server breaks.
    streamInfo.responseObserver.onError(new IOException("test error"));

    // The stream should reconnect and retry the requests.
    FakeWindmillGrpcService.CommitStreamInfo reconnectStreamInfo =
        waitForConnectionAndConsumeHeader();
    Windmill.StreamingCommitWorkRequest reconnectRequest = reconnectStreamInfo.requests.take();
    assertEquals(3, reconnectRequest.getCommitChunkCount());
    {
      // Check if request ids and work tokens match.
      Map<Long, Long> requestIdWorkTokenMap = new HashMap<>();
      Map<Long, Long> expectedRequestIdWorkTokenMap = new HashMap<>();
      for (int i = 0; i < 3; ++i) {
        Windmill.WorkItemCommitRequest parsedRequest =
            Windmill.WorkItemCommitRequest.parseFrom(
                reconnectRequest.getCommitChunk(i).getSerializedWorkItemCommit());
        requestIdWorkTokenMap.put(
            reconnectRequest.getCommitChunk(i).getRequestId(), parsedRequest.getWorkToken());
      }
      for (int i = 2; i <= 4; ++i) {
        expectedRequestIdWorkTokenMap.put((long) i, (long) (i - 1));
      }
      assertThat(requestIdWorkTokenMap).containsExactlyEntriesIn(expectedRequestIdWorkTokenMap);
    }
    // Send back that 2 and 3 finished.
    reconnectStreamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).addRequestId(3).build());
    reconnectStreamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(4).build());
    secondResponsesDone.await();

    assertThat(reconnectStreamInfo.requests).isEmpty();
    assertThat(streamInfo.requests).isEmpty();
    assertTrue(allOk.get());
  }

  @Test
  public void testCommitWorkItem_retryOnNewStreamHalfClose() throws Exception {
    int numCommits = 5;
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    final AtomicBoolean allOk = new AtomicBoolean(true);
    final CountDownLatch firstResponsesDone = new CountDownLatch(2);
    final CountDownLatch secondResponsesDone = new CountDownLatch(3);
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        int finalI = i;
        assertTrue(
            batcher.commitWorkItem(
                COMPUTATION_ID,
                workItemCommitRequest(i),
                (status) -> {
                  if (status != Windmill.CommitStatus.OK) {
                    allOk.set(false);
                  }
                  if (finalI == 0 || finalI == 4) {
                    firstResponsesDone.countDown();
                  } else {
                    secondResponsesDone.countDown();
                  }
                }));
      }
    }
    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertEquals(5, request.getCommitChunkCount());
    {
      // Check if request ids and work tokens match.
      Map<Long, Long> requestIdWorkTokenMap = new HashMap<>();
      Map<Long, Long> expectedRequestIdWorkTokenMap = new HashMap<>();
      for (int i = 0; i < 5; ++i) {
        Windmill.WorkItemCommitRequest parsedRequest =
            Windmill.WorkItemCommitRequest.parseFrom(
                request.getCommitChunk(i).getSerializedWorkItemCommit());
        requestIdWorkTokenMap.put(
            request.getCommitChunk(i).getRequestId(), parsedRequest.getWorkToken());
      }
      for (int i = 1; i <= 5; ++i) {
        expectedRequestIdWorkTokenMap.put((long) i, (long) (i - 1));
      }
      assertThat(requestIdWorkTokenMap).containsExactlyEntriesIn(expectedRequestIdWorkTokenMap);
    }
    // Half-close the logical stream. This shouldn't prevent reconnection of the physical stream
    // from succeeding.
    commitWorkStream.halfClose();
    assertNull(streamInfo.onDone.get());

    // Send back that 1 and 5 finished.
    streamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).addRequestId(5).build());
    firstResponsesDone.await();

    // Simulate that the server breaks.
    streamInfo.responseObserver.onError(new IOException("test error"));

    // The stream should reconnect and retry the requests.
    FakeWindmillGrpcService.CommitStreamInfo reconnectStreamInfo =
        waitForConnectionAndConsumeHeader();

    // We don't expect any more streams since we finish successfully below.
    fakeService.expectNoMoreStreams();

    Windmill.StreamingCommitWorkRequest reconnectRequest = reconnectStreamInfo.requests.take();
    assertEquals(3, reconnectRequest.getCommitChunkCount());
    {
      // Check if request ids and work tokens match.
      Map<Long, Long> requestIdWorkTokenMap = new HashMap<>();
      Map<Long, Long> expectedRequestIdWorkTokenMap = new HashMap<>();
      for (int i = 0; i < 3; ++i) {
        Windmill.WorkItemCommitRequest parsedRequest =
            Windmill.WorkItemCommitRequest.parseFrom(
                reconnectRequest.getCommitChunk(i).getSerializedWorkItemCommit());
        requestIdWorkTokenMap.put(
            reconnectRequest.getCommitChunk(i).getRequestId(), parsedRequest.getWorkToken());
      }
      for (int i = 2; i <= 4; ++i) {
        expectedRequestIdWorkTokenMap.put((long) i, (long) (i - 1));
      }
      assertThat(requestIdWorkTokenMap).containsExactlyEntriesIn(expectedRequestIdWorkTokenMap);
    }
    assertNull(streamInfo.onDone.get());

    // Send back that 2 and 3 finished and then 4 finishes.
    reconnectStreamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).addRequestId(3).build());
    reconnectStreamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(4).build());
    // The half-close completes
    reconnectStreamInfo.responseObserver.onCompleted();
    secondResponsesDone.await();

    assertThat(reconnectStreamInfo.requests).isEmpty();
    assertThat(streamInfo.requests).isEmpty();
    assertTrue(allOk.get());
  }

  @Test
  public void testSend_notCalledAfterShutdown_Single()
      throws ExecutionException, InterruptedException {
    int numCommits = 1;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID,
              workItemCommitRequest(0),
              commitStatus -> {
                errorCollector.checkThat(commitStatus, equalTo(Windmill.CommitStatus.ABORTED));
                errorCollector.checkThat(commitProcessed.getCount(), greaterThan(0L));
                commitProcessed.countDown();
              }));
      // Shutdown the stream before we exit the try-with-resources block which will try to send()
      // the batched request.
      commitWorkStream.shutdown();
    }
    commitProcessed.await();

    assertNotNull(streamInfo.onDone.get());
    assertThat(streamInfo.requests).isEmpty();
  }

  @Test
  public void testSend_notCalledAfterShutdown_Batch()
      throws ExecutionException, InterruptedException {
    int numCommits = 2;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      for (int i = 0; i < numCommits; i++) {
        assertTrue(
            batcher.commitWorkItem(
                COMPUTATION_ID,
                workItemCommitRequest(i),
                commitStatus -> {
                  errorCollector.checkThat(commitStatus, equalTo(Windmill.CommitStatus.ABORTED));
                  errorCollector.checkThat(commitProcessed.getCount(), greaterThan(0L));
                  commitProcessed.countDown();
                }));
      }
      // Shutdown the stream before we exit the try-with-resources block which will try to send()
      // the batched request.
      commitWorkStream.shutdown();
    }
    commitProcessed.await();

    assertNotNull(streamInfo.onDone.get());
    assertThat(streamInfo.requests).isEmpty();
  }

  @Test
  public void testSend_notCalledAfterShutdown_Multichunk()
      throws ExecutionException, InterruptedException {
    int numCommits = 1;
    CountDownLatch commitProcessed = new CountDownLatch(numCommits);
    GrpcCommitWorkStream commitWorkStream = createCommitWorkStream();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID,
              workItemCommitRequest(0)
                  .toBuilder()
                  .addBagUpdates(Windmill.TagBag.newBuilder().setTag(LARGE_BYTE_STRING).build())
                  .build(),
              commitStatus -> {
                errorCollector.checkThat(commitStatus, equalTo(Windmill.CommitStatus.ABORTED));
                errorCollector.checkThat(commitProcessed.getCount(), greaterThan(0L));
                commitProcessed.countDown();
              }));
      // Shutdown the stream before we exit the try-with-resources block which will try to send()
      // the batched request.
      commitWorkStream.shutdown();
    }
    commitProcessed.await();
    assertNotNull(streamInfo.onDone.get());
    assertThat(streamInfo.requests).isEmpty();
  }

  private Windmill.WorkItemCommitRequest createTestCommit(int id) {
    return Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(ByteString.EMPTY)
        .setShardingKey(id)
        .setWorkToken(id * 100L)
        .setCacheToken(id * 1000L)
        .build();
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams() throws Exception {
    // A special executor that allows triggering scheduled futures (of which the handover is the
    // only such future).
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // Send a request where the response is captured in a future.
    Windmill.WorkItemCommitRequest workItemCommitRequest = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest, commitStatusFuture::complete));
    }

    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertThat(request.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest =
        Windmill.WorkItemCommitRequest.parseFrom(
            request.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest).isEqualTo(workItemCommitRequest);

    // Trigger a new stream to be created by forcing the scheduled halfCloseFuture scheduled within
    // AbstractWindmillStream to run.
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    // Previous stream client should be half-closed.
    assertNull(streamInfo.onDone.get());

    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }
    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).build());

    streamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    assertThat(commitStatusFuture.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Complete server-side half-close of first stream. No new
    // stream should be created since the current stream is active.
    streamInfo.responseObserver.onCompleted();

    // Close the stream, the open stream should be client half-closed
    // but logical remains not terminated.
    commitWorkStream.halfClose();
    assertNull(streamInfo2.onDone.get());
    assertFalse(commitWorkStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    // Complete half-closing from the server and verify shutdown completes.
    streamInfo2.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_oldStreamFails() throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    Windmill.WorkItemCommitRequest workItemCommitRequest = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest, commitStatusFuture::complete));
    }

    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertThat(request.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest =
        Windmill.WorkItemCommitRequest.parseFrom(
            request.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest).isEqualTo(workItemCommitRequest);

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    // Previous stream client should be half-closed.
    assertNull(streamInfo.onDone.get());

    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }
    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).build());
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Complete first stream with an error. No new
    // stream should be created since the current stream is active. The request should have an
    // error and the request should be retried on the new stream.
    streamInfo.responseObserver.onError(new RuntimeException("test error"));
    Windmill.StreamingCommitWorkRequest request3 = streamInfo2.requests.take();
    assertThat(request3.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest3 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request3.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest3).isEqualTo(workItemCommitRequest);

    // Close the stream, the open stream should be client half-closed
    // but logical remains not terminated.
    commitWorkStream.halfClose();
    assertNull(streamInfo2.onDone.get());
    assertFalse(commitWorkStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    assertThat(commitStatusFuture.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Complete half-closing from the server and verify shutdown completes.
    streamInfo2.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_newStreamFailsWhileEmpty()
      throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    Windmill.WorkItemCommitRequest workItemCommitRequest = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest, commitStatusFuture::complete));
    }

    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertThat(request.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest =
        Windmill.WorkItemCommitRequest.parseFrom(
            request.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest).isEqualTo(workItemCommitRequest);

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());

    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    // Before stream 1 is finished simulate stream 2 failing.
    streamInfo2.responseObserver.onError(new IOException("stream 2 failed"));
    // A new stream should be created and handle new requests.
    FakeWindmillGrpcService.CommitStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }
    Windmill.StreamingCommitWorkRequest request2 = streamInfo3.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    streamInfo3.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).build());

    streamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    assertThat(commitStatusFuture.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Close the stream.
    commitWorkStream.halfClose();
    assertNull(streamInfo.onDone.get());
    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_newStreamFailsWithRequests()
      throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    Windmill.WorkItemCommitRequest workItemCommitRequest = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest, commitStatusFuture::complete));
    }

    Windmill.StreamingCommitWorkRequest request = streamInfo.requests.take();
    assertThat(request.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest =
        Windmill.WorkItemCommitRequest.parseFrom(
            request.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest).isEqualTo(workItemCommitRequest);

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());

    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }
    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    // Before stream 1 is finished simulate stream 2 failing.
    streamInfo2.responseObserver.onError(new IOException("stream 2 failed"));
    // A new stream should be created and receive the pending requests from stream2 but not the
    // request from stream1.
    FakeWindmillGrpcService.CommitStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());
    Windmill.StreamingCommitWorkRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest3 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request3.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest3).isEqualTo(workItemCommitRequest2);

    streamInfo3.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).build());

    streamInfo.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    assertThat(commitStatusFuture.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Close the stream.
    commitWorkStream.halfClose();
    assertNull(streamInfo.onDone.get());
    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_multipleHandovers() throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo1 = waitForConnectionAndConsumeHeader();

    // Commit request 1 on stream 1
    Windmill.WorkItemCommitRequest workItemCommitRequest1 = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture1 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest1, commitStatusFuture1::complete));
    }

    Windmill.StreamingCommitWorkRequest request1 = streamInfo1.requests.take();
    assertThat(request1.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest1 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request1.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest1).isEqualTo(workItemCommitRequest1);

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo1.onDone.get());

    // Commit request 2 on stream 2
    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }

    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Commit request 3 on stream 3
    Windmill.WorkItemCommitRequest workItemCommitRequest3 = createTestCommit(3);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture3 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest3, commitStatusFuture3::complete));
    }

    Windmill.StreamingCommitWorkRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest3 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request3.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest3).isEqualTo(workItemCommitRequest3);

    // Respond to all requests
    streamInfo1.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(2).build());
    streamInfo3.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(3).build());

    assertThat(commitStatusFuture1.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertThat(commitStatusFuture3.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Close the stream
    commitWorkStream.halfClose();
    assertNull(streamInfo3.onDone.get());

    // Verify no more streams
    fakeService.expectNoMoreStreams();
    streamInfo1.responseObserver.onCompleted();
    streamInfo2.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_oldStreamFailsWhileNewStreamInBackoff()
      throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo1 = waitForConnectionAndConsumeHeader();

    // Commit request 1 on stream 1
    Windmill.WorkItemCommitRequest workItemCommitRequest1 = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture1 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest1, commitStatusFuture1::complete));
    }

    Windmill.StreamingCommitWorkRequest request1 = streamInfo1.requests.take();
    assertThat(request1.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest1 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request1.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest1).isEqualTo(workItemCommitRequest1);

    // Trigger handover but fail new connections
    assertTrue(triggeredExecutor.unblockNextFuture());
    fakeService.setFailedStreamConnectsRemaining(1);
    fakeService.waitForFailedConnectAttempts();
    assertNull(streamInfo1.onDone.get());

    // Fail first stream
    streamInfo1.responseObserver.onError(new RuntimeException("test error"));

    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest1);

    // Respond to the request
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(1).build());
    assertThat(commitStatusFuture1.get()).isEqualTo(Windmill.CommitStatus.OK);

    // Close the stream
    commitWorkStream.halfClose();
    assertNull(streamInfo2.onDone.get());

    streamInfo2.responseObserver.onCompleted();

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_multipleHandovers_shutdown()
      throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo1 = waitForConnectionAndConsumeHeader();

    // Commit request 1 on stream 1
    Windmill.WorkItemCommitRequest workItemCommitRequest1 = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture1 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest1, commitStatusFuture1::complete));
    }

    Windmill.StreamingCommitWorkRequest request1 = streamInfo1.requests.take();
    assertThat(request1.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest1 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request1.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest1).isEqualTo(workItemCommitRequest1);

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo1.onDone.get());

    // Commit request 2 on stream 2
    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }

    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Commit request 3 on stream 3
    Windmill.WorkItemCommitRequest workItemCommitRequest3 = createTestCommit(3);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture3 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest3, commitStatusFuture3::complete));
    }

    Windmill.StreamingCommitWorkRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest3 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request3.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest3).isEqualTo(workItemCommitRequest3);

    // Shutdown while there are active streams and verify it isn't completed until all the streams
    // are done.
    fakeService.expectNoMoreStreams();
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.SECONDS));
    commitWorkStream.shutdown();
    assertThat(commitStatusFuture1.isDone()).isTrue();
    assertThat(commitStatusFuture2.isDone()).isTrue();
    assertThat(commitStatusFuture3.isDone()).isTrue();
    assertFalse(commitWorkStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo3.responseObserver.onCompleted();
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo1.responseObserver.onCompleted();
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo2.responseObserver.onError(new RuntimeException("test"));
    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testCommitWorkItem_multiplePhysicalStreams_multipleHandovers_halfClose()
      throws Exception {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcCommitWorkStream commitWorkStream =
        createCommitWorkStreamWithPhysicalStreamHandover(triggeredExecutor);
    commitWorkStream.start();
    FakeWindmillGrpcService.CommitStreamInfo streamInfo1 = waitForConnectionAndConsumeHeader();

    // Commit request 1 on stream 1
    Windmill.WorkItemCommitRequest workItemCommitRequest1 = createTestCommit(1);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture1 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest1, commitStatusFuture1::complete));
    }

    Windmill.StreamingCommitWorkRequest request1 = streamInfo1.requests.take();
    assertThat(request1.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest1 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request1.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest1).isEqualTo(workItemCommitRequest1);

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo1.onDone.get());

    // Commit request 2 on stream 2
    Windmill.WorkItemCommitRequest workItemCommitRequest2 = createTestCommit(2);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture2 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest2, commitStatusFuture2::complete));
    }

    Windmill.StreamingCommitWorkRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest2 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request2.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest2).isEqualTo(workItemCommitRequest2);

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.CommitStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Commit request 3 on stream 3
    Windmill.WorkItemCommitRequest workItemCommitRequest3 = createTestCommit(3);
    CompletableFuture<Windmill.CommitStatus> commitStatusFuture3 = new CompletableFuture<>();
    try (WindmillStream.CommitWorkStream.RequestBatcher batcher = commitWorkStream.batcher()) {
      assertTrue(
          batcher.commitWorkItem(
              COMPUTATION_ID, workItemCommitRequest3, commitStatusFuture3::complete));
    }

    Windmill.StreamingCommitWorkRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getCommitChunkList()).hasSize(1);
    Windmill.WorkItemCommitRequest parsedRequest3 =
        Windmill.WorkItemCommitRequest.parseFrom(
            request3.getCommitChunk(0).getSerializedWorkItemCommit());
    assertThat(parsedRequest3).isEqualTo(workItemCommitRequest3);

    // Shutdown while there are active streams and verify it isn't completed until all the streams
    // are done.
    fakeService.expectNoMoreStreams();
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.SECONDS));
    commitWorkStream.halfClose();

    assertFalse(commitWorkStream.awaitTermination(10, TimeUnit.MILLISECONDS));
    assertThat(streamInfo3.onDone.get()).isNull();

    assertThat(commitStatusFuture1.isDone()).isFalse();
    assertThat(commitStatusFuture2.isDone()).isFalse();
    assertThat(commitStatusFuture3.isDone()).isFalse();

    streamInfo3.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder().addRequestId(3).build());
    streamInfo3.responseObserver.onCompleted();
    assertThat(commitStatusFuture3.get()).isEqualTo(Windmill.CommitStatus.OK);
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.MILLISECONDS));

    streamInfo1.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder()
            .addRequestId(1)
            .addStatus(Windmill.CommitStatus.ABORTED)
            .build());
    streamInfo1.responseObserver.onCompleted();
    assertThat(commitStatusFuture1.get()).isEqualTo(Windmill.CommitStatus.ABORTED);
    assertFalse(commitWorkStream.awaitTermination(0, TimeUnit.MILLISECONDS));

    streamInfo2.responseObserver.onNext(
        Windmill.StreamingCommitResponse.newBuilder()
            .addRequestId(2)
            .addStatus(Windmill.CommitStatus.ALREADY_IN_COMMIT)
            .build());
    streamInfo2.responseObserver.onCompleted();
    assertThat(commitStatusFuture2.get()).isEqualTo(Windmill.CommitStatus.ALREADY_IN_COMMIT);

    assertTrue(commitWorkStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  private FakeWindmillGrpcService.CommitStreamInfo waitForConnectionAndConsumeHeader() {
    try {
      FakeWindmillGrpcService.CommitStreamInfo info = fakeService.waitForConnectedCommitStream();
      Windmill.StreamingCommitWorkRequest request = info.requests.take();
      errorCollector.checkThat(request.getHeader(), is(TEST_JOB_HEADER));
      assertEquals(0, request.getCommitChunkCount());
      return info;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
