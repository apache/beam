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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

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
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private ManagedChannel inProcessChannel;

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

  private GrpcGetDataStream createGetDataStream(GetDataStreamTestStub testStub) {
    serviceRegistry.addService(testStub);
    GrpcGetDataStream getDataStream =
        (GrpcGetDataStream)
            GrpcWindmillStreamFactory.of(TEST_JOB_HEADER)
                .setSendKeyedGetDataRequests(false)
                .build()
                .createGetDataStream(
                    CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel),
                    new ThrottleTimer());
    getDataStream.start();
    return getDataStream;
  }

  @Test
  public void testRequestKeyedData() {
    GetDataStreamTestStub testStub =
        new GetDataStreamTestStub(new TestGetDataStreamRequestObserver());
    GrpcGetDataStream getDataStream = createGetDataStream(testStub);
    // These will block until they are successfully sent.
    CompletableFuture<Windmill.KeyedGetDataResponse> sendFuture =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return getDataStream.requestKeyedData(
                    "computationId",
                    Windmill.KeyedGetDataRequest.newBuilder()
                        .setKey(ByteString.EMPTY)
                        .setShardingKey(1)
                        .setCacheToken(1)
                        .setWorkToken(1)
                        .build());
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }
            });

    // Sleep a bit to allow future to run.
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    Windmill.KeyedGetDataResponse response =
        Windmill.KeyedGetDataResponse.newBuilder()
            .setShardingKey(1)
            .setKey(ByteString.EMPTY)
            .build();

    testStub.injectResponse(
        Windmill.StreamingGetDataResponse.newBuilder()
            .addRequestId(1)
            .addSerializedResponse(response.toByteString())
            .setRemainingBytesForResponse(0)
            .build());

    assertThat(sendFuture.join()).isEqualTo(response);
  }

  @Test
  public void testRequestKeyedData_sendOnShutdownStreamThrowsWindmillStreamShutdownException() {
    GetDataStreamTestStub testStub =
        new GetDataStreamTestStub(new TestGetDataStreamRequestObserver());
    GrpcGetDataStream getDataStream = createGetDataStream(testStub);
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

  private static class TestGetDataStreamRequestObserver
      implements StreamObserver<Windmill.StreamingGetDataRequest> {
    private @Nullable StreamObserver<Windmill.StreamingGetDataResponse> responseObserver;

    @Override
    public void onNext(Windmill.StreamingGetDataRequest streamingGetDataRequest) {}

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {
      if (responseObserver != null) {
        responseObserver.onCompleted();
      }
    }
  }

  private static class GetDataStreamTestStub
      extends CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase {
    private final TestGetDataStreamRequestObserver requestObserver;
    private @Nullable StreamObserver<Windmill.StreamingGetDataResponse> responseObserver;

    private GetDataStreamTestStub(TestGetDataStreamRequestObserver requestObserver) {
      this.requestObserver = requestObserver;
    }

    @Override
    public StreamObserver<Windmill.StreamingGetDataRequest> getDataStream(
        StreamObserver<Windmill.StreamingGetDataResponse> responseObserver) {
      if (this.responseObserver == null) {
        ((ServerCallStreamObserver<Windmill.StreamingGetDataResponse>) responseObserver)
            .setOnCancelHandler(() -> {});
        this.responseObserver = responseObserver;
        requestObserver.responseObserver = this.responseObserver;
      }

      return requestObserver;
    }

    private void injectResponse(Windmill.StreamingGetDataResponse getDataResponse) {
      checkNotNull(responseObserver).onNext(getDataResponse);
    }
  }
}
