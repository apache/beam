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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
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

  // XXX add handover tests needed such as:
  // - simple physical half close and new stream starting
  // - when half-closed background stream fails and retries need to occur on new stream
  // - when active stream fails with a background stream, new stream needs to be created and should
  // just get
  //   requests from failed stream
  // - creation of current stream is in backoff due to start failure and background stream fails,
  //   make sure requests eventually retried
  // - logical halfclose with background streams
  // - shutdown with background streams

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
