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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.TriggeredScheduledExecutorService;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GrpcGetDataStreamTest {
  private static final String FAKE_SERVER_NAME = "Fake server for GrpcGetDataStreamTest";
  private static final Windmill.JobHeader TEST_JOB_HEADER =
      Windmill.JobHeader.newBuilder()
          .setJobId("test_job")
          .setWorkerId("test_worker")
          .setProjectId("test_project")
          .build();

  @Rule public final ErrorCollector errorCollector = new ErrorCollector();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Rule
  public transient Timeout globalTimeout =
      Timeout.builder().withTimeout(10, TimeUnit.MINUTES).withLookingForStuckThread(true).build();

  private final FakeWindmillGrpcService fakeService = new FakeWindmillGrpcService(errorCollector);
  private ManagedChannel inProcessChannel;
  private Server inProcessServer;

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
    Logger.getLogger(GrpcGetDataStream.class.getName()).setLevel(Level.ALL);
    Logger.getLogger(AbstractMethodError.class.getName()).setLevel(Level.ALL);
  }

  @After
  public void cleanUp() {
    inProcessServer.shutdownNow();
    inProcessChannel.shutdownNow();
  }

  private GrpcGetDataStream createGetDataStream() {
    GrpcGetDataStream getDataStream =
        (GrpcGetDataStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .setSendKeyedGetDataRequests(false)
                .build()
                .createGetDataStream(CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel));
    getDataStream.start();
    return getDataStream;
  }

  private GrpcGetDataStream createGetDataStreamWithPhysicalStreamHandover(
      Duration handover, @Nullable ScheduledExecutorService executor) {
    GrpcGetDataStream getDataStream =
        (GrpcGetDataStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .setDirectStreamingRpcPhysicalStreamHalfCloseAfter(handover)
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
                .createDirectGetDataStream(
                    WindmillConnection.builder()
                        .setStubSupplier(
                            () -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel))
                        .build());
    getDataStream.start();
    return getDataStream;
  }

  @Test
  public void testRequestKeyedData() throws InterruptedException {
    GrpcGetDataStream getDataStream = createGetDataStream();
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());

    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);
  }

  @Test
  public void testRequestKeyedData_sendOnShutdownStreamThrowsWindmillStreamShutdownException() {
    GrpcGetDataStream getDataStream = createGetDataStream();
    int numSendThreads = 5;
    ExecutorService getDataStreamSenders = Executors.newFixedThreadPool(numSendThreads);
    CountDownLatch waitForSendAttempt = new CountDownLatch(1);
    // These will block until they are successfully sent.
    List<CompletableFuture<Void>> sendFutures =
        IntStream.range(0, 5)
            .sequential()
            .mapToObj(
                i ->
                    (Runnable)
                        () -> {
                          // Prevent some threads from sending until we close the stream.
                          if (i % 2 == 0) {
                            try {
                              waitForSendAttempt.await();
                            } catch (InterruptedException e) {
                              throw new RuntimeException(e);
                            }
                          }
                          try {
                            getDataStream.requestKeyedData("computationId", createTestRequest(i));
                          } catch (WindmillStreamShutdownException e) {
                            throw new RuntimeException(e);
                          }
                        })
            // Run the code above on multiple threads.
            .map(runnable -> CompletableFuture.runAsync(runnable, getDataStreamSenders))
            .collect(Collectors.toList());

    getDataStream.shutdown();

    // Free up waiting threads so that they can try to send on a closed stream.
    waitForSendAttempt.countDown();

    for (int i = 0; i < numSendThreads; i++) {
      CompletableFuture<Void> sendFuture = sendFutures.get(i);
      try {
        // Wait for future to complete.
        sendFuture.join();
      } catch (Exception ignored) {
      }
      if (i % 2 == 0) {
        assertTrue(sendFuture.isCompletedExceptionally());
        ExecutionException e = assertThrows(ExecutionException.class, sendFuture::get);
        assertThat(e)
            .hasCauseThat()
            .hasCauseThat()
            .isInstanceOf(WindmillStreamShutdownException.class);
      }
    }
  }

  @Test
  public void testRequestKeyedData_reconnectOnStreamError() throws InterruptedException {
    GrpcGetDataStream getDataStream = createGetDataStream();
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setCacheToken(1)
            .setWorkToken(1)
            .build();

    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // Simulate an error on the grpc stream, this should trigger a retry of the request internal to
    // the stream.
    streamInfo.responseObserver.onError(new IOException("test error"));

    streamInfo = waitForConnectionAndConsumeHeader();
    while (true) {
      request = streamInfo.requests.poll(5, TimeUnit.SECONDS);
      if (request != null) break;
      if (sendFuture.isDone()) {
        fail("Unexpected send completion " + sendFuture);
      }
    }
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    getDataStream.shutdown();
    assertThrows(RuntimeException.class, sendFuture::join);
  }

  @Test
  public void testRequestKeyedData_reconnectOnStreamErrorAfterHalfClose()
      throws InterruptedException, ExecutionException {
    GrpcGetDataStream getDataStream = createGetDataStream();
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest =
        Windmill.KeyedGetDataRequest.newBuilder()
            .setKey(ByteString.EMPTY)
            .setShardingKey(1)
            .setCacheToken(1)
            .setWorkToken(1)
            .build();

    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // Close the stream.
    getDataStream.halfClose();
    assertNull(streamInfo.onDone.get());

    // Simulate an error on the grpc stream, this should trigger retrying the requests on a new
    // stream
    // which is half-closed.
    streamInfo.responseObserver.onError(new IOException("test error"));

    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request2.getStateRequest(0).getRequests(0));
    assertNull(streamInfo2.onDone.get());
    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));

    // Sending an error this time shouldn't result in a new stream since there were no requests.
    fakeService.expectNoMoreStreams();
    streamInfo2.responseObserver.onError(new IOException("test error"));

    getDataStream.awaitTermination(60, TimeUnit.MINUTES);
  }

  private Windmill.KeyedGetDataRequest createTestRequest(long id) {
    return Windmill.KeyedGetDataRequest.newBuilder()
        .setKey(ByteString.EMPTY)
        .setShardingKey(id)
        .setCacheToken(id * 100)
        .setWorkToken(id * 1000)
        .build();
  }

  private Windmill.KeyedGetDataResponse createTestResponse(long id) {
    return Windmill.KeyedGetDataResponse.newBuilder()
        .setShardingKey(id)
        .setKey(ByteString.EMPTY)
        .build();
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);

    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    // Previous stream client should be half-closed.
    assertNull(streamInfo.onDone.get());

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);

    // Complete server-side half-close of first stream. No new
    // stream should be created since the current stream is active.
    streamInfo.responseObserver.onCompleted();

    // Close the stream, the open stream should be client half-closed
    // but logical remains not terminated.
    getDataStream.halfClose();
    assertNull(streamInfo2.onDone.get());
    assertFalse(getDataStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    // Complete half-closing from the server and verify shutdown completes.
    streamInfo2.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_oldStreamFails()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    // Previous stream client should be half-closed.
    assertNull(streamInfo.onDone.get());

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);

    // Complete first stream with an error. No new
    // stream should be created since the current stream is active. The request should have an
    // error and the request should be retried on the new stream.
    streamInfo.responseObserver.onError(new RuntimeException("test error"));
    Windmill.StreamingGetDataRequest request3 = streamInfo2.requests.take();
    assertThat(request3.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request3.getStateRequest(0).getRequests(0));

    // Close the stream, the open stream should be client half-closed
    // but logical remains not terminated.
    getDataStream.halfClose();
    assertNull(streamInfo2.onDone.get());
    assertFalse(getDataStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);

    // Complete half-closing from the server and verify shutdown completes.
    streamInfo2.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_newStreamFailsWhileEmpty()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());

    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    // Before stream 1 is finished simulate stream 2 failing.
    streamInfo2.responseObserver.onError(new IOException("stream 2 failed"));
    // A new stream should be created and handle new requests.
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo3.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo3.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);

    // Close the stream.
    getDataStream.halfClose();
    assertNull(streamInfo.onDone.get());
    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_newStreamFailsWithRequests()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // These will block until they are successfully sent.
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // A new stream should be created due to handover.
    assertTrue(triggeredExecutor.unblockNextFuture());

    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    // Before stream 1 is finished simulate stream 2 failing.
    streamInfo2.responseObserver.onError(new IOException("stream 2 failed"));
    // A new stream should be created and receive the pending requests from stream2 but not the
    // request from stream1.
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());
    Windmill.StreamingGetDataRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request3.getStateRequest(0).getRequests(0));

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo3.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);

    // Close the stream.
    getDataStream.halfClose();
    assertNull(streamInfo.onDone.get());
    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_multipleHandovers_allResponsesReceived()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // Request 1, Stream 1
    Windmill.KeyedGetDataRequest keyedGetDataRequest1 = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture1 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest1);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request1 = streamInfo.requests.take();
    assertThat(request1.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest1, request1.getStateRequest(0).getRequests(0));

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    // Request 2, Stream 2
    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Request 3, Stream 3
    Windmill.KeyedGetDataRequest keyedGetDataRequest3 = createTestRequest(3);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture3 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest3);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getRequestIdList()).containsExactly(3L);
    assertEquals(keyedGetDataRequest3, request3.getStateRequest(0).getRequests(0));

    // Respond to all requests
    Windmill.KeyedGetDataResponse keyedGetDataResponse1 = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse1.toByteString())
            .build());

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());
    streamInfo2.responseObserver.onCompleted();

    Windmill.KeyedGetDataResponse keyedGetDataResponse3 = createTestResponse(3);
    streamInfo3.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(3)
            .addSerializedResponse(keyedGetDataResponse3.toByteString())
            .build());

    assertThat(sendFuture1.join()).isEqualTo(keyedGetDataResponse1);
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);
    assertThat(sendFuture3.join()).isEqualTo(keyedGetDataResponse3);

    // Close the stream.
    getDataStream.halfClose();
    assertNull(streamInfo3.onDone.get());

    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_oldStreamFailsWhileNewStreamInBackoff()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    Windmill.StreamingGetDataRequest request = streamInfo.requests.take();
    assertThat(request.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request.getStateRequest(0).getRequests(0));

    // A new stream should be created due to handover. However we configure the server to have
    // errors.
    assertTrue(triggeredExecutor.unblockNextFuture());
    fakeService.setFailedStreamConnectsRemaining(1);
    fakeService.waitForFailedConnectAttempts();
    // Previous stream client should be half-closed.
    assertNull(streamInfo.onDone.get());
    // Complete first stream with an error. No new
    // stream should be created since the current stream is being created or created. The request
    // should have an
    // error and the request should be retried on the new stream.
    streamInfo.responseObserver.onError(new RuntimeException("test error"));

    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    fakeService.expectNoMoreStreams();

    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest, request2.getStateRequest(0).getRequests(0));

    // Close the stream, the open stream should be client half-closed
    // but logical remains not terminated.
    getDataStream.halfClose();
    assertNull(streamInfo2.onDone.get());
    assertFalse(getDataStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture.join()).isEqualTo(keyedGetDataResponse);

    // Complete half-closing from the server and verify shutdown completes.
    streamInfo2.responseObserver.onCompleted();

    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_multipleHandovers_shutdown()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // Request 1, Stream 1
    Windmill.KeyedGetDataRequest keyedGetDataRequest1 = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture1 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest1);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request1 = streamInfo.requests.take();
    assertThat(request1.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest1, request1.getStateRequest(0).getRequests(0));

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    // Request 2, Stream 2
    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Request 3, Stream 3
    Windmill.KeyedGetDataRequest keyedGetDataRequest3 = createTestRequest(3);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture3 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest3);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getRequestIdList()).containsExactly(3L);
    assertEquals(keyedGetDataRequest3, request3.getStateRequest(0).getRequests(0));

    // Shutdown while there are active streams and verify it isn't completed until all the streams
    // are done.
    fakeService.expectNoMoreStreams();
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.SECONDS));
    getDataStream.shutdown();
    assertThrows("WindmillStreamShutdownException", CompletionException.class, sendFuture1::join);
    assertThrows("WindmillStreamShutdownException", CompletionException.class, sendFuture2::join);
    assertThrows("WindmillStreamShutdownException", CompletionException.class, sendFuture3::join);
    assertFalse(getDataStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo3.responseObserver.onCompleted();
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo.responseObserver.onCompleted();
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    streamInfo2.responseObserver.onError(new RuntimeException("test"));
    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_multiplePhysicalStreams_multipleHandovers_halfClose()
      throws InterruptedException, ExecutionException {
    TriggeredScheduledExecutorService triggeredExecutor = new TriggeredScheduledExecutorService();
    GrpcGetDataStream getDataStream =
        createGetDataStreamWithPhysicalStreamHandover(Duration.ofSeconds(60), triggeredExecutor);
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();

    // Request 1, Stream 1
    Windmill.KeyedGetDataRequest keyedGetDataRequest1 = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture1 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest1);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request1 = streamInfo.requests.take();
    assertThat(request1.getRequestIdList()).containsExactly(1L);
    assertEquals(keyedGetDataRequest1, request1.getStateRequest(0).getRequests(0));

    // Trigger handover 1
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo2 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo.onDone.get());

    // Request 2, Stream 2
    Windmill.KeyedGetDataRequest keyedGetDataRequest2 = createTestRequest(2);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture2 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest2);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request2 = streamInfo2.requests.take();
    assertThat(request2.getRequestIdList()).containsExactly(2L);
    assertEquals(keyedGetDataRequest2, request2.getStateRequest(0).getRequests(0));

    // Trigger handover 2 before streamInfo2 completes
    assertTrue(triggeredExecutor.unblockNextFuture());
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo3 = waitForConnectionAndConsumeHeader();
    assertNull(streamInfo2.onDone.get());

    // Request 3, Stream 3
    Windmill.KeyedGetDataRequest keyedGetDataRequest3 = createTestRequest(3);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture3 =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest3);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });
    Windmill.StreamingGetDataRequest request3 = streamInfo3.requests.take();
    assertThat(request3.getRequestIdList()).containsExactly(3L);
    assertEquals(keyedGetDataRequest3, request3.getStateRequest(0).getRequests(0));

    // Half-close while there are active streams and verify it isn't completed until all the streams
    // are done. Streams with requests should have requests resent.
    fakeService.expectNoMoreStreams();
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.SECONDS));
    getDataStream.halfClose();
    assertNull(streamInfo.onDone.get());
    assertFalse(getDataStream.awaitTermination(10, TimeUnit.MILLISECONDS));

    Windmill.KeyedGetDataResponse keyedGetDataResponse3 = createTestResponse(3);
    streamInfo3.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(3)
            .addSerializedResponse(keyedGetDataResponse3.toByteString())
            .build());
    assertThat(sendFuture3.join()).isEqualTo(keyedGetDataResponse3);

    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));
    Windmill.KeyedGetDataResponse keyedGetDataResponse = createTestResponse(1);
    streamInfo.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(keyedGetDataResponse.toByteString())
            .build());
    assertThat(sendFuture1.join()).isEqualTo(keyedGetDataResponse);

    streamInfo.responseObserver.onCompleted();
    assertFalse(getDataStream.awaitTermination(0, TimeUnit.MILLISECONDS));

    Windmill.KeyedGetDataResponse keyedGetDataResponse2 = createTestResponse(2);
    streamInfo2.responseObserver.onNext(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(2)
            .addSerializedResponse(keyedGetDataResponse2.toByteString())
            .build());
    assertThat(sendFuture2.join()).isEqualTo(keyedGetDataResponse2);
    streamInfo2.responseObserver.onCompleted();
    streamInfo3.responseObserver.onCompleted();
    assertTrue(getDataStream.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testRequestKeyedData_raceShutdownDuringTrySendBatch() throws Exception {
    AtomicBoolean connectedOnce = new AtomicBoolean(false);
    CountDownLatch failedConnects = new CountDownLatch(2);
    GrpcGetDataStream getDataStream =
        (GrpcGetDataStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .setSendKeyedGetDataRequests(false)
                .build()
                .createGetDataStream(
                    CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel)
                        .withInterceptors(
                            new ClientInterceptor() {
                              @Override
                              public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                  MethodDescriptor<ReqT, RespT> methodDescriptor,
                                  CallOptions callOptions,
                                  Channel channel) {
                                if (connectedOnce.getAndSet(true)) {
                                  failedConnects.countDown();
                                  throw new RuntimeException("test error");
                                }
                                return channel.newCall(methodDescriptor, callOptions);
                              }
                            }));
    getDataStream.start();
    // Wait for the first stream to succeed and cause it to fail, the rest should fail.
    FakeWindmillGrpcService.GetDataStreamInfo streamInfo = waitForConnectionAndConsumeHeader();
    streamInfo.responseObserver.onError(new RuntimeException("fake error"));

    failedConnects.await();

    // Send while we're in this state.
    // Create a request
    Windmill.KeyedGetDataRequest keyedGetDataRequest = createTestRequest(1);
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData("computationId", keyedGetDataRequest);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    // The shutdown should work if it occurs either before or after the above request is sent.
    Thread.sleep(100);
    getDataStream.shutdown();

    // The request should complete with an exception, it may or may not get there.
    assertThrows(CompletionException.class, sendFuture::join);
    assertTrue(sendFuture.isCompletedExceptionally());
  }

  private FakeWindmillGrpcService.GetDataStreamInfo waitForConnectionAndConsumeHeader() {
    try {
      FakeWindmillGrpcService.GetDataStreamInfo info = fakeService.waitForConnectedGetDataStream();
      Windmill.StreamingGetDataRequest request = info.requests.take();
      errorCollector.checkThat(request.getHeader(), Matchers.is(TEST_JOB_HEADER));
      assertEquals(0, request.getRequestIdCount());
      assertEquals(0, request.getComputationHeartbeatRequestCount());
      return info;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
