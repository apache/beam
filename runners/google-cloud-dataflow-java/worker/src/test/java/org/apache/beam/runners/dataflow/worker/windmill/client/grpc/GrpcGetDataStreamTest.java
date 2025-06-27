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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
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

  @Test
  public void testRequestKeyedData() throws InterruptedException {
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

    Windmill.KeyedGetDataResponse keyedGetDataResponse =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setShardingKey(1)
            .setKey(ByteString.EMPTY)
            .build();

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
                            getDataStream.requestKeyedData(
                                "computationId",
                                Windmill.KeyedGetDataRequest.newBuilder()
                                    .setKey(ByteString.EMPTY)
                                    .setShardingKey(i)
                                    .setCacheToken(i)
                                    .setWorkToken(i)
                                    .build());
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

    // Simulate an error on the grpc stream, this should trigger an error on all
    // existing requests but no new connection since we half-closed and nothing left after
    // responding with errors.
    fakeService.expectNoMoreStreams();
    streamInfo.responseObserver.onError(new IOException("test error"));
    assertThrows(RuntimeException.class, sendFuture::join);

    getDataStream.shutdown();
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
