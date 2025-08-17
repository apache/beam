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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItemMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.util.MutableHandlerRegistry;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcDirectGetWorkStreamTest {

  private static final WorkItemScheduler NO_OP_WORK_ITEM_SCHEDULER =
      (workItem,
          serializedWorkItemSize,
          watermarks,
          processingContext,
          getWorkStreamLatencies) -> {};
  private static final Windmill.JobHeader TEST_JOB_HEADER =
      Windmill.JobHeader.newBuilder()
          .setClientId(1L)
          .setJobId("test_job")
          .setWorkerId("test_worker")
          .setProjectId("test_project")
          .build();
  private static final String FAKE_SERVER_NAME = "Fake server for GrpcDirectGetWorkStreamTest";
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();

  @Rule
  public transient Timeout globalTimeout =
      Timeout.builder().withTimeout(10, TimeUnit.MINUTES).withLookingForStuckThread(true).build();

  private ManagedChannel inProcessChannel;
  private GrpcDirectGetWorkStream stream;

  private static Windmill.StreamingGetWorkRequestExtension extension(GetWorkBudget budget) {
    return Windmill.StreamingGetWorkRequestExtension.newBuilder()
        .setMaxItems(budget.items())
        .setMaxBytes(budget.bytes())
        .build();
  }

  private static void assertHeader(
      Windmill.StreamingGetWorkRequest getWorkRequest, GetWorkBudget expectedInitialBudget) {
    assertTrue(getWorkRequest.hasRequest());
    assertFalse(getWorkRequest.hasRequestExtension());
    assertThat(getWorkRequest.getRequest())
        .isEqualTo(
            Windmill.GetWorkRequest.newBuilder()
                .setClientId(TEST_JOB_HEADER.getClientId())
                .setJobId(TEST_JOB_HEADER.getJobId())
                .setProjectId(TEST_JOB_HEADER.getProjectId())
                .setWorkerId(TEST_JOB_HEADER.getWorkerId())
                .setMaxItems(expectedInitialBudget.items())
                .setMaxBytes(expectedInitialBudget.bytes())
                .build());
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
    checkNotNull(stream).shutdown();
    inProcessChannel.shutdownNow();
  }

  private GrpcDirectGetWorkStream createGetWorkStream(
      GetWorkStreamTestStub testStub,
      GetWorkBudget initialGetWorkBudget,
      WorkItemScheduler workItemScheduler) {
    serviceRegistry.addService(testStub);
    GrpcDirectGetWorkStream getWorkStream =
        (GrpcDirectGetWorkStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .build()
                .createDirectGetWorkStream(
                    WindmillConnection.builder()
                        .setStubSupplier(
                            () -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel))
                        .build(),
                    Windmill.GetWorkRequest.newBuilder()
                        .setClientId(TEST_JOB_HEADER.getClientId())
                        .setJobId(TEST_JOB_HEADER.getJobId())
                        .setProjectId(TEST_JOB_HEADER.getProjectId())
                        .setWorkerId(TEST_JOB_HEADER.getWorkerId())
                        .setMaxItems(initialGetWorkBudget.items())
                        .setMaxBytes(initialGetWorkBudget.bytes())
                        .build(),
                    mock(HeartbeatSender.class),
                    mock(GetDataClient.class),
                    mock(WorkCommitter.class),
                    workItemScheduler);
    getWorkStream.start();
    return getWorkStream;
  }

  private Windmill.StreamingGetWorkResponseChunk createResponse(Windmill.WorkItem... workItems) {
    return createResponse(
        Arrays.stream(workItems).map(WorkItem::toByteString).collect(Collectors.toList()), 0);
  }

  private Windmill.StreamingGetWorkResponseChunk createResponse(
      List<ByteString> workItemChunks, int remainingBytes) {
    StreamingGetWorkResponseChunk.Builder builder =
        StreamingGetWorkResponseChunk.newBuilder()
            .setStreamId(1L)
            .setComputationMetadata(
                ComputationWorkItemMetadata.newBuilder()
                    .setComputationId("compId")
                    .setInputDataWatermark(1L)
                    .setDependentRealtimeInputWatermark(1L)
                    .build());
    for (ByteString workItemChunk : workItemChunks) {
      builder.addSerializedWorkItem(workItemChunk);
    }
    return builder.setRemainingBytesForWorkItem(remainingBytes).build();
  }

  @Test
  public void testSetBudget_computesAndSendsCorrectExtension_noExistingBudget()
      throws InterruptedException {
    int expectedRequests = 2;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    stream = createGetWorkStream(testStub, GetWorkBudget.noBudget(), NO_OP_WORK_ITEM_SCHEDULER);
    GetWorkBudget newBudget = GetWorkBudget.builder().setItems(10).setBytes(10).build();
    stream.setBudget(newBudget);

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    // Header and extension.
    assertThat(requestObserver.sent()).hasSize(expectedRequests);
    assertHeader(requestObserver.sent().get(0), GetWorkBudget.noBudget());
    assertThat(Iterables.getLast(requestObserver.sent()).getRequestExtension())
        .isEqualTo(extension(newBudget));
  }

  @Test
  public void testSetBudget_computesAndSendsCorrectExtension_existingBudget()
      throws InterruptedException {
    int expectedRequests = 2;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    GetWorkBudget initialBudget = GetWorkBudget.builder().setItems(10).setBytes(10).build();
    stream = createGetWorkStream(testStub, initialBudget, NO_OP_WORK_ITEM_SCHEDULER);
    GetWorkBudget newBudget = GetWorkBudget.builder().setItems(100).setBytes(100).build();
    stream.setBudget(newBudget);
    GetWorkBudget diff = newBudget.subtract(initialBudget);

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    List<Windmill.StreamingGetWorkRequest> requests = requestObserver.sent();
    // Header and extension.
    assertThat(requests).hasSize(expectedRequests);
    assertHeader(requests.get(0), initialBudget);
    assertThat(Iterables.getLast(requests).getRequestExtension()).isEqualTo(extension(diff));
  }

  @Test
  public void testSetBudget_doesNotSendExtensionIfOutstandingBudgetHigh()
      throws InterruptedException {
    int expectedRequests = 1;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    GetWorkBudget initialBudget =
        GetWorkBudget.builder().setItems(Long.MAX_VALUE).setBytes(Long.MAX_VALUE).build();
    stream = createGetWorkStream(testStub, initialBudget, NO_OP_WORK_ITEM_SCHEDULER);
    stream.setBudget(GetWorkBudget.builder().setItems(10).setBytes(10).build());

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    List<Windmill.StreamingGetWorkRequest> requests = requestObserver.sent();
    // Assert that the extension was never sent, only the header.
    assertThat(requests).hasSize(expectedRequests);
    assertHeader(Iterables.getOnlyElement(requests), initialBudget);
  }

  @Test
  public void testSetBudget_doesNothingIfStreamShutdown() throws InterruptedException {
    int expectedRequests = 1;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    stream = createGetWorkStream(testStub, GetWorkBudget.noBudget(), NO_OP_WORK_ITEM_SCHEDULER);
    stream.shutdown();
    stream.setBudget(
        GetWorkBudget.builder().setItems(Long.MAX_VALUE).setBytes(Long.MAX_VALUE).build());

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    List<Windmill.StreamingGetWorkRequest> requests = requestObserver.sent();
    // Assert that the extension was never sent, only the header.
    assertThat(requests).hasSize(1);
    assertHeader(Iterables.getOnlyElement(requests), GetWorkBudget.noBudget());
  }

  @Test
  public void testConsumedWorkItem_computesAndSendsCorrectExtension() throws InterruptedException {
    int expectedRequests = 2;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    GetWorkBudget initialBudget = GetWorkBudget.builder().setItems(1).setBytes(100).build();
    Set<Windmill.WorkItem> scheduledWorkItems = new HashSet<>();
    stream =
        createGetWorkStream(
            testStub,
            initialBudget,
            (work,
                serializedWorkItemSize,
                watermarks,
                processingContext,
                getWorkStreamLatencies) -> {
              scheduledWorkItems.add(work);
            });
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key"))
            .setWorkToken(1L)
            .setShardingKey(1L)
            .setCacheToken(1L)
            .build();

    testStub.injectResponse(createResponse(workItem));

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    assertThat(scheduledWorkItems).containsExactly(workItem);
    List<Windmill.StreamingGetWorkRequest> requests = requestObserver.sent();
    long inFlightBytes = initialBudget.bytes() - workItem.getSerializedSize();

    assertThat(requests).hasSize(expectedRequests);
    assertHeader(requests.get(0), initialBudget);
    assertThat(Iterables.getLast(requests).getRequestExtension())
        .isEqualTo(
            extension(
                GetWorkBudget.builder()
                    .setItems(1)
                    .setBytes(initialBudget.bytes() - inFlightBytes)
                    .build()));
  }

  @Test
  public void testConsumedWorkItem_doesNotSendExtensionIfOutstandingBudgetHigh()
      throws InterruptedException {
    int expectedRequests = 1;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    Set<Windmill.WorkItem> scheduledWorkItems = new HashSet<>();
    GetWorkBudget initialBudget =
        GetWorkBudget.builder().setItems(Long.MAX_VALUE).setBytes(Long.MAX_VALUE).build();
    stream =
        createGetWorkStream(
            testStub,
            initialBudget,
            (work, serializedWorkItemSize, watermarks, processingContext, getWorkStreamLatencies) ->
                scheduledWorkItems.add(work));
    Windmill.WorkItem workItem =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key"))
            .setWorkToken(1L)
            .setShardingKey(1L)
            .setCacheToken(1L)
            .build();

    testStub.injectResponse(createResponse(workItem));

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    assertThat(scheduledWorkItems).containsExactly(workItem);
    List<Windmill.StreamingGetWorkRequest> requests = requestObserver.sent();

    // Assert that the extension was never sent, only the header.
    assertThat(requests).hasSize(expectedRequests);
    assertHeader(Iterables.getOnlyElement(requests), initialBudget);
  }

  @Test
  public void testConsumedWorkItems() throws InterruptedException {
    int expectedRequests = 2;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    GetWorkBudget initialBudget = GetWorkBudget.builder().setItems(2).setBytes(100).build();
    Set<Windmill.WorkItem> scheduledWorkItems = new HashSet<>();
    stream =
        createGetWorkStream(
            testStub,
            initialBudget,
            (work,
                serializedWorkItemSize,
                watermarks,
                processingContext,
                getWorkStreamLatencies) -> {
              scheduledWorkItems.add(work);
            });
    Windmill.WorkItem workItem1 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key1"))
            .setWorkToken(1L)
            .setShardingKey(1L)
            .setCacheToken(1L)
            .build();
    Windmill.WorkItem workItem2 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key2"))
            .setWorkToken(2L)
            .setShardingKey(2L)
            .setCacheToken(2L)
            .build();

    testStub.injectResponse(createResponse(workItem1, workItem2));

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    assertThat(scheduledWorkItems).containsExactly(workItem1, workItem2);
  }

  @Test
  public void testConsumedWorkItems_itemsSplitAcrossResponses() throws InterruptedException {
    int expectedRequests = 3;
    CountDownLatch waitForRequests = new CountDownLatch(expectedRequests);
    TestGetWorkRequestObserver requestObserver = new TestGetWorkRequestObserver(waitForRequests);
    GetWorkStreamTestStub testStub = new GetWorkStreamTestStub(requestObserver);
    GetWorkBudget initialBudget = GetWorkBudget.builder().setItems(3).setBytes(100).build();
    Set<Windmill.WorkItem> scheduledWorkItems = new HashSet<>();
    stream =
        createGetWorkStream(
            testStub,
            initialBudget,
            (work,
                serializedWorkItemSize,
                watermarks,
                processingContext,
                getWorkStreamLatencies) -> {
              scheduledWorkItems.add(work);
            });
    Windmill.WorkItem workItem1 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key1"))
            .setWorkToken(1L)
            .setShardingKey(1L)
            .setCacheToken(1L)
            .build();
    Windmill.WorkItem workItem2 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key2"))
            .setWorkToken(2L)
            .setShardingKey(2L)
            .setCacheToken(2L)
            .build();
    Windmill.WorkItem workItem3 =
        Windmill.WorkItem.newBuilder()
            .setKey(ByteString.copyFromUtf8("somewhat_long_key3"))
            .setWorkToken(2L)
            .setShardingKey(2L)
            .setCacheToken(2L)
            .build();

    List<ByteString> chunks1 = new ArrayList<>();
    List<ByteString> chunks2 = new ArrayList<>();
    List<ByteString> chunks3 = new ArrayList<>();
    chunks1.add(workItem1.toByteString());
    ByteString bytes = workItem2.toByteString();
    int third = bytes.size() / 3;
    chunks1.add(bytes.substring(0, third));

    chunks2.add(bytes.substring(third, 2 * third));
    chunks3.add(bytes.substring(2 * third));

    chunks3.add(workItem3.toByteString());

    testStub.injectResponse(createResponse(chunks1, bytes.size() - third));
    testStub.injectResponse(createResponse(chunks2, bytes.size() - 2 * third));
    testStub.injectResponse(createResponse(chunks3, 0));

    assertTrue(waitForRequests.await(5, TimeUnit.SECONDS));

    assertThat(scheduledWorkItems).containsExactly(workItem1, workItem2, workItem3);
  }

  private static class GetWorkStreamTestStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {

    private final TestGetWorkRequestObserver requestObserver;
    private @Nullable StreamObserver<Windmill.StreamingGetWorkResponseChunk> responseObserver;

    private GetWorkStreamTestStub(TestGetWorkRequestObserver requestObserver) {
      this.requestObserver = requestObserver;
    }

    @Override
    public StreamObserver<Windmill.StreamingGetWorkRequest> getWorkStream(
        StreamObserver<Windmill.StreamingGetWorkResponseChunk> responseObserver) {
      if (this.responseObserver == null) {
        this.responseObserver = responseObserver;
        requestObserver.responseObserver = this.responseObserver;
      }

      return requestObserver;
    }

    private void injectResponse(Windmill.StreamingGetWorkResponseChunk responseChunk) {
      checkNotNull(responseObserver).onNext(responseChunk);
    }
  }

  private static class TestGetWorkRequestObserver
      implements StreamObserver<Windmill.StreamingGetWorkRequest> {

    private final List<Windmill.StreamingGetWorkRequest> requests =
        Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch waitForRequests;
    private @Nullable volatile StreamObserver<Windmill.StreamingGetWorkResponseChunk>
        responseObserver;

    public TestGetWorkRequestObserver(CountDownLatch waitForRequests) {
      this.waitForRequests = waitForRequests;
    }

    @Override
    public void onNext(Windmill.StreamingGetWorkRequest request) {
      requests.add(request);
      waitForRequests.countDown();
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
    }

    List<Windmill.StreamingGetWorkRequest> sent() {
      return requests;
    }
  }
}
