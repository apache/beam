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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.util.FluentBackoff;
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
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry = new HashSet<>();
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
    return GrpcGetDataStream.create(
        "streamId",
        responseObserver ->
            CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel)
                .getDataStream(responseObserver),
        FluentBackoff.DEFAULT.backoff(),
        StreamObserverFactory.direct(DEFAULT_STREAM_RPC_DEADLINE_SECONDS * 2, 1),
        streamRegistry,
        1,
        new ThrottleTimer(),
        TEST_JOB_HEADER,
        new AtomicLong(),
        Integer.MAX_VALUE,
        false,
        ignored -> {});
  }

  @Test
  public void testRequestKeyedData_sendOnShutdownStreamThrowsWorkItemCancelledException() {
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
                          getDataStream.requestKeyedData(
                              "computationId",
                              Windmill.KeyedGetDataRequest.newBuilder()
                                  .setKey(ByteString.EMPTY)
                                  .setShardingKey(i)
                                  .setCacheToken(i)
                                  .setWorkToken(i)
                                  .build());
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
        assertThat(e).hasCauseThat().isInstanceOf(WorkItemCancelledException.class);
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
  }
}
