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

import static org.junit.Assert.*;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationHeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItemMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkStreamTimingInfo;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkStreamTimingInfo.Event;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataId;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.LatencyAttribution.State;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitRequestChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetDataResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingGetWorkResponseChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.TagValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.Value;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillApplianceGrpc;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamShutdownException;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.testing.FakeWindmillStubFactoryFactory;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.CallOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientCall;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptors;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Deadline;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.MethodDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.util.MutableHandlerRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link GrpcWindmillServer}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class GrpcWindmillServerTest {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcWindmillServerTest.class);
  private static final int STREAM_CHUNK_SIZE = 2 << 20;
  private final long clientId = 10L;
  private final Set<ManagedChannel> openedChannels = new HashSet<>();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Rule public GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  private Server server;
  private GrpcWindmillServer client;
  private int remainingErrors = 20;

  @Before
  public void setUp() throws Exception {
    startServerAndClient(new ArrayList<>());
  }

  @After
  public void tearDown() throws Exception {
    server.shutdownNow();
    openedChannels.forEach(ManagedChannel::shutdownNow);
  }

  private void startServerAndClient(List<String> experiments) throws Exception {
    String name = "Fake server for " + getClass();
    this.server =
        grpcCleanup.register(
            InProcessServerBuilder.forName(name)
                .fallbackHandlerRegistry(serviceRegistry)
                .executor(Executors.newFixedThreadPool(1))
                .build()
                .start());

    this.client =
        GrpcWindmillServer.newTestInstance(
            name,
            experiments,
            clientId,
            new FakeWindmillStubFactoryFactory(
                new FakeWindmillStubFactory(
                    () -> {
                      ManagedChannel channel =
                          grpcCleanup.register(WindmillChannelFactory.inProcessChannel(name));
                      openedChannels.add(channel);
                      return channel;
                    })));
  }

  private <Stream extends StreamObserver> void maybeInjectError(Stream stream) {
    if (remainingErrors > 0 && ThreadLocalRandom.current().nextInt(20) == 0) {
      try {
        stream.onError(new RuntimeException("oops"));
        remainingErrors--;
      } catch (IllegalStateException e) {
        // The stream is already closed.
      }
    }
  }

  @Test
  public void testWindmillApplianceGrpcDeadlineInterceptor() {
    Windmill.GetWorkResponse getWorkResponse =
        Windmill.GetWorkResponse.newBuilder()
            .addWork(
                Windmill.ComputationWorkItems.newBuilder()
                    .setComputationId("someComputation1")
                    .build())
            .build();

    serviceRegistry.addService(
        new WindmillApplianceGrpc.WindmillApplianceImplBase() {
          @Override
          public void getWork(
              Windmill.GetWorkRequest request,
              StreamObserver<Windmill.GetWorkResponse> responseObserver) {
            responseObserver.onNext(getWorkResponse);
            responseObserver.onCompleted();
          }
        });

    ClientInterceptor testInterceptor =
        new ClientInterceptor() {
          Deadline deadline;

          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> methodDescriptor,
              CallOptions callOptions,
              Channel channel) {
            assertNotNull(callOptions.getDeadline());
            if (deadline != null) {
              // We want to assert that a new deadline is being created on every call since
              // deadlines are absolute.
              assertNotEquals(deadline, callOptions.getDeadline());
            }

            deadline = callOptions.getDeadline();
            // Just forward the call once we record the deadline
            return channel.newCall(methodDescriptor, callOptions);
          }
        };

    Channel inprocessChannel =
        ClientInterceptors.intercept(
            InProcessChannelBuilder.forName("Fake server for " + getClass())
                .directExecutor()
                .build(),
            testInterceptor);

    this.client =
        GrpcWindmillServer.newApplianceTestInstance(
            inprocessChannel,
            new FakeWindmillStubFactoryFactory(
                new FakeWindmillStubFactory(() -> (ManagedChannel) inprocessChannel)));

    Windmill.GetWorkResponse response1 = client.getWork(GetWorkRequest.getDefaultInstance());
    Windmill.GetWorkResponse response2 = client.getWork(GetWorkRequest.getDefaultInstance());

    // Assert that the request/responses are being forwarded correctly.
    assertEquals(response1, getWorkResponse);
    assertEquals(response2, getWorkResponse);
  }

  @Test
  public void testStreamingGetWork() throws Exception {
    // This fake server returns an infinite stream of identical WorkItems, obeying the request size
    // limits set by the client.
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingGetWorkRequest> getWorkStream(
              StreamObserver<StreamingGetWorkResponseChunk> responseObserver) {
            return new StreamObserver<StreamingGetWorkRequest>() {
              final ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);
              boolean sawHeader = false;

              @Override
              public void onNext(StreamingGetWorkRequest request) {
                maybeInjectError(responseObserver);

                try {
                  long maxItems;
                  if (!sawHeader) {
                    errorCollector.checkThat(
                        request.getRequest(),
                        Matchers.equalTo(
                            GetWorkRequest.newBuilder()
                                .setClientId(10)
                                .setJobId("job")
                                .setProjectId("project")
                                .setWorkerId("worker")
                                .setMaxItems(3)
                                .setMaxBytes(10000)
                                .build()));
                    sawHeader = true;
                    maxItems = request.getRequest().getMaxItems();
                  } else {
                    maxItems = request.getRequestExtension().getMaxItems();
                  }

                  for (int item = 0; item < maxItems; item++) {
                    long id = ThreadLocalRandom.current().nextLong();
                    ByteString serializedResponse =
                        WorkItem.newBuilder()
                            .setKey(ByteString.copyFromUtf8("somewhat_long_key"))
                            .setWorkToken(id)
                            .setShardingKey(id)
                            .build()
                            .toByteString();

                    // Break the WorkItem into smaller chunks to test chunking code.
                    for (int i = 0; i < serializedResponse.size(); i += 10) {
                      int end = Math.min(serializedResponse.size(), i + 10);
                      StreamingGetWorkResponseChunk.Builder builder =
                          StreamingGetWorkResponseChunk.newBuilder()
                              .setStreamId(id)
                              .setSerializedWorkItem(serializedResponse.substring(i, end))
                              .setRemainingBytesForWorkItem(serializedResponse.size() - end);

                      if (i == 0) {
                        builder.setComputationMetadata(
                            ComputationWorkItemMetadata.newBuilder()
                                .setComputationId("comp")
                                .setDependentRealtimeInputWatermark(17000)
                                .setInputDataWatermark(18000));
                      }

                      try {
                        responseObserver.onNext(builder.build());
                      } catch (IllegalStateException e) {
                        // Client closed stream, we're done.
                        return;
                      }
                    }
                  }
                } catch (Exception e) {
                  errorCollector.addError(e);
                }
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                injector.cancel();
                responseObserver.onCompleted();
              }
            };
          }
        });

    // Read the stream of WorkItems until 100 of them are received.
    CountDownLatch latch = new CountDownLatch(100);
    GetWorkStream stream =
        client.getWorkStream(
            GetWorkRequest.newBuilder().setClientId(10).setMaxItems(3).setMaxBytes(10000).build(),
            (String computation,
                @Nullable Instant inputDataWatermark,
                Instant synchronizedProcessingTime,
                WorkItem workItem,
                Collection<LatencyAttribution> getWorkStreamLatencies) -> {
              latch.countDown();
              assertEquals(inputDataWatermark, new Instant(18));
              assertEquals(synchronizedProcessingTime, new Instant(17));
              assertEquals(workItem.getKey(), ByteString.copyFromUtf8("somewhat_long_key"));
            });
    assertTrue(latch.await(30, TimeUnit.SECONDS));

    stream.halfClose();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testStreamingGetData() throws Exception {
    // This server responds to GetDataRequests with responses that mirror the requests.
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingGetDataRequest> getDataStream(
              StreamObserver<StreamingGetDataResponse> responseObserver) {
            return new StreamObserver<StreamingGetDataRequest>() {
              final HashSet<Long> seenIds = new HashSet<>();
              final ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);
              final StreamingGetDataResponse.Builder responseBuilder =
                  StreamingGetDataResponse.newBuilder();
              boolean sawHeader = false;

              @Override
              public void onNext(StreamingGetDataRequest chunk) {
                maybeInjectError(responseObserver);
                try {
                  if (!sawHeader) {
                    LOG.info("Received header");
                    errorCollector.checkThat(
                        chunk.getHeader(),
                        Matchers.equalTo(
                            JobHeader.newBuilder()
                                .setJobId("job")
                                .setProjectId("project")
                                .setWorkerId("worker")
                                .setClientId(clientId)
                                .build()));
                    sawHeader = true;
                  } else {
                    LOG.info(
                        "Received get data of {} global data, {} data requests",
                        chunk.getGlobalDataRequestCount(),
                        chunk.getStateRequestCount());
                    errorCollector.checkThat(
                        chunk.getSerializedSize(), Matchers.lessThanOrEqualTo(STREAM_CHUNK_SIZE));

                    int i = 0;
                    for (GlobalDataRequest request : chunk.getGlobalDataRequestList()) {
                      long requestId = chunk.getRequestId(i++);
                      errorCollector.checkThat(seenIds.add(requestId), Matchers.is(true));
                      sendResponse(requestId, processGlobalDataRequest(request));
                    }

                    for (ComputationGetDataRequest request : chunk.getStateRequestList()) {
                      long requestId = chunk.getRequestId(i++);
                      errorCollector.checkThat(seenIds.add(requestId), Matchers.is(true));
                      sendResponse(requestId, processStateRequest(request));
                    }
                    flushResponse();
                  }
                } catch (Exception e) {
                  errorCollector.addError(e);
                }
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                injector.cancel();
                responseObserver.onCompleted();
              }

              private ByteString processGlobalDataRequest(GlobalDataRequest request) {
                errorCollector.checkThat(request.getStateFamily(), Matchers.is("family"));

                return GlobalData.newBuilder()
                    .setDataId(request.getDataId())
                    .setStateFamily("family")
                    .setData(ByteString.copyFromUtf8(request.getDataId().getTag()))
                    .build()
                    .toByteString();
              }

              private ByteString processStateRequest(ComputationGetDataRequest compRequest) {
                errorCollector.checkThat(compRequest.getRequestsCount(), Matchers.is(1));
                errorCollector.checkThat(
                    compRequest.getComputationId(), Matchers.is("computation"));
                KeyedGetDataRequest request = compRequest.getRequests(0);
                KeyedGetDataResponse response =
                    makeGetDataResponse(request.getValuesToFetch(0).getTag().toStringUtf8());
                return response.toByteString();
              }

              private void sendResponse(long id, ByteString serializedResponse) {
                if (ThreadLocalRandom.current().nextInt(4) == 0) {
                  sendChunkedResponse(id, serializedResponse);
                } else {
                  responseBuilder.addRequestId(id).addSerializedResponse(serializedResponse);
                  if (responseBuilder.getRequestIdCount() > 10) {
                    flushResponse();
                  }
                }
              }

              private void sendChunkedResponse(long id, ByteString serializedResponse) {
                LOG.info("Sending response with {} chunks", (serializedResponse.size() / 10) + 1);
                for (int i = 0; i < serializedResponse.size(); i += 10) {
                  int end = Math.min(serializedResponse.size(), i + 10);
                  try {
                    responseObserver.onNext(
                        StreamingGetDataResponse.newBuilder()
                            .addRequestId(id)
                            .addSerializedResponse(serializedResponse.substring(i, end))
                            .setRemainingBytesForResponse(serializedResponse.size() - end)
                            .build());
                  } catch (IllegalStateException e) {
                    // Stream is already closed.
                  }
                }
              }

              private void flushResponse() {
                if (responseBuilder.getRequestIdCount() > 0) {
                  LOG.info(
                      "Sending batched response of {} ids", responseBuilder.getRequestIdCount());
                  try {
                    responseObserver.onNext(responseBuilder.build());
                  } catch (Exception e) {
                    // Stream is already closed.
                    System.out.println("trieu: " + e);
                  }
                  responseBuilder.clear();
                }
              }
            };
          }
        });

    GetDataStream stream = client.getDataStream();

    // Make requests of varying sizes to test chunking, and verify the responses.
    ExecutorService executor = Executors.newFixedThreadPool(50);
    final CountDownLatch done = new CountDownLatch(200);
    for (int i = 0; i < 100; ++i) {
      final String key = "key" + i;
      final String s = i % 5 == 0 ? largeString(i) : "tag";
      executor.submit(
          () -> {
            try {
              errorCollector.checkThat(
                  stream.requestKeyedData("computation", makeGetDataRequest(key, s)),
                  Matchers.equalTo(makeGetDataResponse(s)));
            } catch (WindmillStreamShutdownException e) {
              throw new RuntimeException(e);
            }
            done.countDown();
          });
      executor.execute(
          () -> {
            try {
              errorCollector.checkThat(
                  stream.requestGlobalData(makeGlobalDataRequest(key)),
                  Matchers.equalTo(makeGlobalDataResponse(key)));
            } catch (WindmillStreamShutdownException e) {
              throw new RuntimeException(e);
            }
            done.countDown();
          });
    }
    done.await();
    stream.halfClose();
    assertTrue(stream.awaitTermination(60, TimeUnit.SECONDS));
    executor.shutdown();
  }

  private String largeString(int length) {
    return String.join("", Collections.nCopies(length, "."));
  }

  private KeyedGetDataRequest makeGetDataRequest(String key, String tag) {
    return KeyedGetDataRequest.newBuilder()
        .setKey(ByteString.copyFromUtf8(key))
        .setWorkToken(17)
        .addValuesToFetch(TagValue.newBuilder().setTag(ByteString.copyFromUtf8(tag)))
        .build();
  }

  private KeyedGetDataResponse makeGetDataResponse(String tag) {
    return KeyedGetDataResponse.newBuilder()
        .setKey(ByteString.copyFromUtf8("key"))
        .addValues(
            TagValue.newBuilder()
                .setTag(ByteString.copyFromUtf8("tag"))
                .setValue(
                    Value.newBuilder()
                        .setTimestamp(0)
                        .setData(ByteString.copyFromUtf8(tag + "-value"))))
        .build();
  }

  private GlobalDataRequest makeGlobalDataRequest(String key) {
    return GlobalDataRequest.newBuilder()
        .setStateFamily("family")
        .setDataId(GlobalDataId.newBuilder().setTag(key).setVersion(ByteString.EMPTY).build())
        .build();
  }

  private GlobalData makeGlobalDataResponse(String key) {
    return GlobalData.newBuilder()
        .setStateFamily("family")
        .setDataId(GlobalDataId.newBuilder().setTag(key).setVersion(ByteString.EMPTY))
        .setData(ByteString.copyFromUtf8(key))
        .build();
  }

  private WorkItemCommitRequest makeCommitRequest(int i, int size) {
    return WorkItemCommitRequest.newBuilder()
        .setKey(ByteString.copyFromUtf8("key" + i))
        .setWorkToken(i)
        .addValueUpdates(
            TagValue.newBuilder()
                .setTag(ByteString.copyFromUtf8("tag" + i))
                .setValue(
                    Value.newBuilder()
                        .setData(ByteString.copyFromUtf8(largeString(size)))
                        .setTimestamp(i)))
        .build();
  }

  // This server receives WorkItemCommitRequests, and verifies they are equal to the provided
  // commitRequest.
  private StreamObserver<StreamingCommitWorkRequest> getTestCommitStreamObserver(
      StreamObserver<StreamingCommitResponse> responseObserver,
      Map<Long, WorkItemCommitRequest> commitRequests) {
    return new StreamObserver<StreamingCommitWorkRequest>() {
      final ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);
      boolean sawHeader = false;
      InputStream buffer = null;
      long remainingBytes = 0;

      @Override
      public void onNext(StreamingCommitWorkRequest request) {
        maybeInjectError(responseObserver);

        if (!sawHeader) {
          errorCollector.checkThat(
              request.getHeader(),
              Matchers.equalTo(
                  JobHeader.newBuilder()
                      .setJobId("job")
                      .setProjectId("project")
                      .setWorkerId("worker")
                      .setClientId(clientId)
                      .build()));
          sawHeader = true;
          LOG.info("Received header");
        } else {
          boolean first = true;
          LOG.info("Received request with {} chunks", request.getCommitChunkCount());
          for (StreamingCommitRequestChunk chunk : request.getCommitChunkList()) {
            assertTrue(chunk.getSerializedWorkItemCommit().size() <= STREAM_CHUNK_SIZE);
            if (first || chunk.hasComputationId()) {
              errorCollector.checkThat(chunk.getComputationId(), Matchers.equalTo("computation"));
            }

            if (remainingBytes != 0) {
              errorCollector.checkThat(buffer, Matchers.notNullValue());
              errorCollector.checkThat(
                  remainingBytes,
                  Matchers.is(
                      chunk.getSerializedWorkItemCommit().size()
                          + chunk.getRemainingBytesForWorkItem()));
              buffer =
                  new SequenceInputStream(buffer, chunk.getSerializedWorkItemCommit().newInput());
            } else {
              errorCollector.checkThat(buffer, Matchers.nullValue());
              buffer = chunk.getSerializedWorkItemCommit().newInput();
            }
            remainingBytes = chunk.getRemainingBytesForWorkItem();
            if (remainingBytes == 0) {
              try {
                WorkItemCommitRequest received = WorkItemCommitRequest.parseFrom(buffer);
                errorCollector.checkThat(
                    received, Matchers.equalTo(commitRequests.get(received.getWorkToken())));
                try {
                  responseObserver.onNext(
                      StreamingCommitResponse.newBuilder()
                          .addRequestId(chunk.getRequestId())
                          .build());
                } catch (IllegalStateException e) {
                  // Stream is closed.
                }
              } catch (Exception e) {
                errorCollector.addError(e);
              }
              buffer = null;
            } else {
              errorCollector.checkThat(first, Matchers.is(true));
            }
            first = false;
          }
        }
      }

      @Override
      public void onError(Throwable throwable) {}

      @Override
      public void onCompleted() {
        injector.cancel();
        responseObserver.onCompleted();
      }
    };
  }

  private void commitWorkTestHelper(
      CommitWorkStream stream,
      ConcurrentHashMap<Long, WorkItemCommitRequest> requestsForService,
      int requestIdStart,
      int numRequests)
      throws InterruptedException {
    List<WorkItemCommitRequest> commitRequestList = new ArrayList<>();
    List<CountDownLatch> latches = new ArrayList<>();
    for (int i = 0; i < numRequests; ++i) {
      // Build some requests of varying size with a few big ones.
      WorkItemCommitRequest request =
          makeCommitRequest(i + requestIdStart, i * (i < numRequests * .9 ? 8 : 128));
      commitRequestList.add(request);
      requestsForService.put((long) i + requestIdStart, request);
      latches.add(new CountDownLatch(1));
    }
    Collections.shuffle(commitRequestList);
    try (CommitWorkStream.RequestBatcher batcher = stream.batcher()) {
      for (int i = 0; i < commitRequestList.size(); ) {
        final CountDownLatch latch = latches.get(i);
        if (batcher.commitWorkItem(
            "computation",
            commitRequestList.get(i),
            (CommitStatus status) -> {
              assertEquals(status, CommitStatus.OK);
              latch.countDown();
            })) {
          i++;
        } else {
          batcher.flush();
        }
      }
    }
    for (CountDownLatch latch : latches) {
      assertTrue(latch.await(1, TimeUnit.MINUTES));
    }
  }

  @Test
  public void testStreamingCommit() throws Exception {
    ConcurrentHashMap<Long, WorkItemCommitRequest> commitRequests = new ConcurrentHashMap<>();
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingCommitWorkRequest> commitWorkStream(
              StreamObserver<StreamingCommitResponse> responseObserver) {
            return getTestCommitStreamObserver(responseObserver, commitRequests);
          }
        });

    // Make the commit requests, waiting for each of them to be verified and acknowledged.
    CommitWorkStream stream = client.commitWorkStream();
    commitWorkTestHelper(stream, commitRequests, 0, 500);
    stream.halfClose();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  public void testStreamingCommitManyThreads() throws Exception {
    ConcurrentHashMap<Long, WorkItemCommitRequest> commitRequests = new ConcurrentHashMap<>();
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingCommitWorkRequest> commitWorkStream(
              StreamObserver<StreamingCommitResponse> responseObserver) {
            return getTestCommitStreamObserver(responseObserver, commitRequests);
          }
        });
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    // Make the commit requests, waiting for each of them to be verified and acknowledged.
    CommitWorkStream stream = client.commitWorkStream();
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      final int startRequestId = i * 50;
      futures.add(
          executor.submit(
              () -> {
                try {
                  commitWorkTestHelper(stream, commitRequests, startRequestId, 50);
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }));
    }
    // Surface any exceptions that might be thrown by submitting by blocking on the future.
    for (Future<?> f : futures) {
      f.get();
    }
    stream.halfClose();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
    executor.shutdown();
  }

  @Test
  // Tests stream retries on server errors before and after `close()`
  public void testStreamingCommitClosedStream() throws Exception {
    List<WorkItemCommitRequest> commitRequestList = new ArrayList<>();
    List<CountDownLatch> latches = new ArrayList<>();
    Map<Long, WorkItemCommitRequest> commitRequests = new ConcurrentHashMap<>();
    AtomicBoolean shouldServerReturnError = new AtomicBoolean(true);
    AtomicBoolean isClientClosed = new AtomicBoolean(false);
    AtomicInteger errorsBeforeClose = new AtomicInteger();
    AtomicInteger errorsAfterClose = new AtomicInteger();
    for (int i = 0; i < 500; ++i) {
      // Build some requests of varying size with a few big ones.
      WorkItemCommitRequest request = makeCommitRequest(i, i * (i < 480 ? 8 : 128));
      commitRequestList.add(request);
      commitRequests.put((long) i, request);
      latches.add(new CountDownLatch(1));
    }
    Collections.shuffle(commitRequestList);

    // This server returns errors if shouldServerReturnError is true, else returns valid responses.
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingCommitWorkRequest> commitWorkStream(
              StreamObserver<StreamingCommitResponse> responseObserver) {
            StreamObserver<StreamingCommitWorkRequest> testCommitStreamObserver =
                getTestCommitStreamObserver(responseObserver, commitRequests);
            return new StreamObserver<StreamingCommitWorkRequest>() {
              @Override
              public void onNext(StreamingCommitWorkRequest request) {
                if (shouldServerReturnError.get()) {
                  try {
                    responseObserver.onError(
                        new RuntimeException("shouldServerReturnError = true"));
                    if (isClientClosed.get()) {
                      errorsAfterClose.incrementAndGet();
                    } else {
                      errorsBeforeClose.incrementAndGet();
                    }
                  } catch (IllegalStateException e) {
                    // The stream is already closed.
                  }
                } else {
                  testCommitStreamObserver.onNext(request);
                }
              }

              @Override
              public void onError(Throwable throwable) {
                testCommitStreamObserver.onError(throwable);
              }

              @Override
              public void onCompleted() {
                testCommitStreamObserver.onCompleted();
              }
            };
          }
        });

    // Make the commit requests, waiting for each of them to be verified and acknowledged.
    CommitWorkStream stream = client.commitWorkStream();
    try (CommitWorkStream.RequestBatcher batcher = stream.batcher()) {
      for (int i = 0; i < commitRequestList.size(); ) {
        final CountDownLatch latch = latches.get(i);
        if (batcher.commitWorkItem(
            "computation",
            commitRequestList.get(i),
            (CommitStatus status) -> {
              assertEquals(status, CommitStatus.OK);
              latch.countDown();
            })) {
          i++;
        } else {
          batcher.flush();
        }
      }
    }

    long deadline = System.currentTimeMillis() + 60_000; // 1 min
    while (true) {
      Thread.sleep(100);
      int tmpErrorsBeforeClose = errorsBeforeClose.get();
      // wait for at least 1 error before close
      if (tmpErrorsBeforeClose > 0) {
        break;
      }
      if (System.currentTimeMillis() > deadline) {
        // Control should not reach here if the test is working as expected
        fail(
            String.format(
                "Expected errors not sent by server errorsBeforeClose: %s"
                    + " \n Should not reach here if the test is working as expected.",
                tmpErrorsBeforeClose));
      }
    }

    stream.halfClose();
    isClientClosed.set(true);

    deadline = System.currentTimeMillis() + 60_000; // 1 min
    while (true) {
      Thread.sleep(100);
      int tmpErrorsAfterClose = errorsAfterClose.get();
      // wait for at least 1 error after close
      if (tmpErrorsAfterClose > 0) {
        break;
      }
      if (System.currentTimeMillis() > deadline) {
        // Control should not reach here if the test is working as expected
        fail(
            String.format(
                "Expected errors not sent by server errorsAfterClose: %s"
                    + " \n Should not reach here if the test is working as expected.",
                tmpErrorsAfterClose));
      }
    }

    shouldServerReturnError.set(false);
    for (CountDownLatch latch : latches) {
      assertTrue(latch.await(1, TimeUnit.MINUTES));
    }
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }

  private List<KeyedGetDataRequest> makeGetDataHeartbeatRequest(List<String> keys) {
    List<KeyedGetDataRequest> result = new ArrayList<>();
    for (String key : keys) {
      result.add(
          Windmill.KeyedGetDataRequest.newBuilder()
              .setShardingKey(key.hashCode())
              .setWorkToken(0)
              .setCacheToken(0)
              .build());
    }
    return result;
  }

  private List<HeartbeatRequest> makeHeartbeatRequest(List<String> keys) {
    List<HeartbeatRequest> result = new ArrayList<>();
    for (String key : keys) {
      result.add(
          Windmill.HeartbeatRequest.newBuilder()
              .setShardingKey(key.hashCode())
              .setWorkToken(0)
              .setCacheToken(0)
              .build());
    }
    return result;
  }

  @Test
  public void testStreamingGetDataHeartbeatsAsKeyedGetDataRequests() throws Exception {
    // This server records the heartbeats observed but doesn't respond.
    final Map<String, List<KeyedGetDataRequest>> getDataHeartbeats = new HashMap<>();
    // Create a client and server different from the one in SetUp so we can add an experiment to the
    // options passed in. This requires teardown and re-constructing the client and server
    tearDown();
    startServerAndClient(
        Collections.singletonList("streaming_engine_disable_new_heartbeat_requests"));

    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingGetDataRequest> getDataStream(
              StreamObserver<StreamingGetDataResponse> responseObserver) {
            return new StreamObserver<StreamingGetDataRequest>() {
              boolean sawHeader = false;

              @Override
              public void onNext(StreamingGetDataRequest chunk) {
                try {
                  if (!sawHeader) {
                    LOG.info("Received header");
                    errorCollector.checkThat(
                        chunk.getHeader(),
                        Matchers.equalTo(
                            JobHeader.newBuilder()
                                .setJobId("job")
                                .setProjectId("project")
                                .setWorkerId("worker")
                                .setClientId(clientId)
                                .build()));
                    sawHeader = true;
                  } else {
                    LOG.info("Received {} getDataHeartbeats", chunk.getStateRequestCount());
                    errorCollector.checkThat(
                        chunk.getSerializedSize(), Matchers.lessThanOrEqualTo(STREAM_CHUNK_SIZE));
                    errorCollector.checkThat(chunk.getRequestIdCount(), Matchers.is(0));

                    synchronized (getDataHeartbeats) {
                      for (ComputationGetDataRequest request : chunk.getStateRequestList()) {
                        errorCollector.checkThat(request.getRequestsCount(), Matchers.is(1));
                        getDataHeartbeats.putIfAbsent(
                            request.getComputationId(), new ArrayList<>());
                        getDataHeartbeats
                            .get(request.getComputationId())
                            .add(request.getRequestsList().get(0));
                      }
                    }
                  }
                } catch (Exception e) {
                  errorCollector.addError(e);
                }
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        });

    List<String> computation1Keys = new ArrayList<>();
    List<String> computation2Keys = new ArrayList<>();

    for (int i = 0; i < 100; ++i) {
      computation1Keys.add("Computation1Key" + i);
      computation2Keys.add("Computation2Key" + largeString(i * 20));
    }
    // We're adding HeartbeatRequests to refreshActiveWork, but expecting to get back
    // KeyedGetDataRequests, so make a Map of both types.
    Map<String, List<KeyedGetDataRequest>> expectedKeyedGetDataRequests = new HashMap<>();
    expectedKeyedGetDataRequests.put("Computation1", makeGetDataHeartbeatRequest(computation1Keys));
    expectedKeyedGetDataRequests.put("Computation2", makeGetDataHeartbeatRequest(computation2Keys));
    Map<String, Collection<HeartbeatRequest>> heartbeatsToRefresh = new HashMap<>();
    heartbeatsToRefresh.put("Computation1", makeHeartbeatRequest(computation1Keys));
    heartbeatsToRefresh.put("Computation2", makeHeartbeatRequest(computation2Keys));

    GetDataStream stream = client.getDataStream();
    stream.refreshActiveWork(heartbeatsToRefresh);
    stream.halfClose();
    assertTrue(stream.awaitTermination(60, TimeUnit.SECONDS));

    boolean receivedAllGetDataHeartbeats = false;
    while (!receivedAllGetDataHeartbeats) {
      Thread.sleep(100);
      synchronized (getDataHeartbeats) {
        if (getDataHeartbeats.size() != expectedKeyedGetDataRequests.size()) {
          continue;
        }
        assertEquals(expectedKeyedGetDataRequests, getDataHeartbeats);
        receivedAllGetDataHeartbeats = true;
      }
    }
  }

  @Test
  public void testStreamingGetDataHeartbeatsAsHeartbeatRequests() throws Exception {
    // This server records the heartbeats observed but doesn't respond.
    final List<ComputationHeartbeatRequest> receivedHeartbeats = new ArrayList<>();

    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingGetDataRequest> getDataStream(
              StreamObserver<StreamingGetDataResponse> responseObserver) {
            return new StreamObserver<StreamingGetDataRequest>() {
              boolean sawHeader = false;

              @Override
              public void onNext(StreamingGetDataRequest chunk) {
                try {
                  if (!sawHeader) {
                    LOG.info("Received header");
                    errorCollector.checkThat(
                        chunk.getHeader(),
                        Matchers.equalTo(
                            JobHeader.newBuilder()
                                .setJobId("job")
                                .setProjectId("project")
                                .setWorkerId("worker")
                                .setClientId(clientId)
                                .build()));
                    sawHeader = true;
                  } else {
                    LOG.info(
                        "Received {} computationHeartbeatRequests",
                        chunk.getComputationHeartbeatRequestCount());
                    errorCollector.checkThat(
                        chunk.getSerializedSize(), Matchers.lessThanOrEqualTo(STREAM_CHUNK_SIZE));
                    errorCollector.checkThat(chunk.getRequestIdCount(), Matchers.is(0));

                    synchronized (receivedHeartbeats) {
                      receivedHeartbeats.addAll(chunk.getComputationHeartbeatRequestList());
                    }
                  }
                } catch (Exception e) {
                  errorCollector.addError(e);
                }
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        });

    List<String> computation1Keys = new ArrayList<>();
    List<String> computation2Keys = new ArrayList<>();

    // When sending heartbeats as HeartbeatRequest protos, all keys for the same computation should
    // be batched into the same ComputationHeartbeatRequest. Compare to the KeyedGetDataRequest
    // version in the test above, which only sends one key per ComputationGetDataRequest.
    List<ComputationHeartbeatRequest> expectedHeartbeats = new ArrayList<>();
    ComputationHeartbeatRequest.Builder comp1Builder =
        ComputationHeartbeatRequest.newBuilder().setComputationId("Computation1");
    ComputationHeartbeatRequest.Builder comp2Builder =
        ComputationHeartbeatRequest.newBuilder().setComputationId("Computation2");
    for (int i = 0; i < 100; ++i) {
      String computation1Key = "Computation1Key" + i;
      computation1Keys.add(computation1Key);
      comp1Builder.addHeartbeatRequests(
          makeHeartbeatRequest(Collections.singletonList(computation1Key)).get(0));
      String computation2Key = "Computation2Key" + largeString(i * 20);
      computation2Keys.add(computation2Key);
      comp2Builder.addHeartbeatRequests(
          makeHeartbeatRequest(Collections.singletonList(computation2Key)).get(0));
    }
    expectedHeartbeats.add(comp1Builder.build());
    expectedHeartbeats.add(comp2Builder.build());
    Map<String, Collection<HeartbeatRequest>> heartbeatRequestMap = new HashMap<>();
    heartbeatRequestMap.put("Computation1", makeHeartbeatRequest(computation1Keys));
    heartbeatRequestMap.put("Computation2", makeHeartbeatRequest(computation2Keys));

    GetDataStream stream = client.getDataStream();
    stream.refreshActiveWork(heartbeatRequestMap);
    stream.halfClose();
    assertTrue(stream.awaitTermination(60, TimeUnit.SECONDS));

    boolean receivedAllHeartbeatRequests = false;
    while (!receivedAllHeartbeatRequests) {
      Thread.sleep(100);
      synchronized (receivedHeartbeats) {
        if (receivedHeartbeats.size() != expectedHeartbeats.size()) {
          continue;
        }
        assertEquals(expectedHeartbeats, receivedHeartbeats);
        receivedAllHeartbeatRequests = true;
      }
    }
  }

  @Test
  public void testThrottleSignal() throws Exception {
    // This server responds with work items until the throttleMessage limit is hit at which point it
    // returns RESOURCE_EXHAUSTED errors for throttleTime msecs after which it resumes sending
    // work items.
    final int throttleTime = 2000;
    final int throttleMessage = 15;
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          long throttleStartTime = -1;
          int messageCount = 0;

          @Override
          public StreamObserver<StreamingGetWorkRequest> getWorkStream(
              StreamObserver<StreamingGetWorkResponseChunk> responseObserver) {
            return new StreamObserver<StreamingGetWorkRequest>() {
              boolean sawHeader = false;

              @Override
              public void onNext(StreamingGetWorkRequest request) {
                messageCount++;
                // If we are at the throttleMessage limit or we are currently throttling send an
                // error.
                if (messageCount == throttleMessage || throttleStartTime != -1) {
                  // If throttling has not started yet then start it.
                  if (throttleStartTime == -1) {
                    throttleStartTime = Instant.now().getMillis();
                  }
                  // If throttling has started and it has been throttleTime since we started
                  // throttling stop throttling.
                  if (throttleStartTime != -1
                      && ((Instant.now().getMillis() - throttleStartTime) > throttleTime)) {
                    throttleStartTime = -1;
                  }
                  StatusRuntimeException error =
                      new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
                  responseObserver.onError(error);
                  return;
                }
                // We are not throttling this message so respond as normal.
                try {
                  long maxItems;
                  if (!sawHeader) {
                    sawHeader = true;
                    maxItems = request.getRequest().getMaxItems();
                  } else {
                    maxItems = request.getRequestExtension().getMaxItems();
                  }

                  for (int item = 0; item < maxItems; item++) {
                    long id = ThreadLocalRandom.current().nextLong();
                    ByteString serializedResponse =
                        WorkItem.newBuilder()
                            .setKey(ByteString.copyFromUtf8("somewhat_long_key"))
                            .setWorkToken(id)
                            .setShardingKey(id)
                            .build()
                            .toByteString();

                    StreamingGetWorkResponseChunk.Builder builder =
                        StreamingGetWorkResponseChunk.newBuilder()
                            .setStreamId(id)
                            .setSerializedWorkItem(serializedResponse)
                            .setRemainingBytesForWorkItem(0)
                            .setComputationMetadata(
                                ComputationWorkItemMetadata.newBuilder()
                                    .setComputationId("computation")
                                    .setInputDataWatermark(1L)
                                    .setDependentRealtimeInputWatermark(1L)
                                    .build());
                    try {
                      responseObserver.onNext(builder.build());
                    } catch (IllegalStateException e) {
                      // Client closed stream, we're done.
                      return;
                    }
                  }
                } catch (Exception e) {
                  errorCollector.addError(e);
                }
              }

              @Override
              public void onError(Throwable throwable) {}

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            };
          }
        });

    // Read the stream of WorkItems until 100 of them are received.
    CountDownLatch latch = new CountDownLatch(100);
    GetWorkStream stream =
        client.getWorkStream(
            GetWorkRequest.newBuilder().setClientId(10).setMaxItems(3).setMaxBytes(10000).build(),
            (String computation,
                @Nullable Instant inputDataWatermark,
                Instant synchronizedProcessingTime,
                Windmill.WorkItem workItem,
                Collection<LatencyAttribution> getWorkStreamLatencies) -> latch.countDown());
    // Wait for 100 items or 30 seconds.
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    // Confirm that we report at least as much throttle time as our server sent errors for.  We will
    // actually report more due to backoff in restarting streams.
    assertTrue(this.client.getAndResetThrottleTime() > throttleTime);

    stream.halfClose();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }

  @Test
  public void testGetWorkTimingInfosTracker() throws Exception {
    GetWorkTimingInfosTracker tracker = new GetWorkTimingInfosTracker(() -> 50);
    List<GetWorkStreamTimingInfo> infos = new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_CREATION_START)
              .setTimestampUsec(0)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_CREATION_END)
              .setTimestampUsec(10000)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_RECEIVED_BY_DISPATCHER)
              .setTimestampUsec((i + 11) * 1000)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_FORWARDED_BY_DISPATCHER)
              .setTimestampUsec((i + 16) * 1000)
              .build());
      tracker.addTimingInfo(infos);
      infos.clear();
    }
    // durations for each chunk:
    // GET_WORK_IN_WINDMILL_WORKER: 10, 10, 10, 10
    // GET_WORK_IN_TRANSIT_TO_DISPATCHER: 1, 2, 3, 4 -> sum to 10
    // GET_WORK_IN_TRANSIT_TO_USER_WORKER: 34, 33, 32, 31 -> sum to 130
    Map<State, LatencyAttribution> latencies = new HashMap<>();
    List<LatencyAttribution> attributions = tracker.getLatencyAttributions();
    assertEquals(3, attributions.size());
    for (LatencyAttribution attribution : attributions) {
      latencies.put(attribution.getState(), attribution);
    }
    assertEquals(10L, latencies.get(State.GET_WORK_IN_WINDMILL_WORKER).getTotalDurationMillis());
    // elapsed time from 10 -> 50;
    long elapsedTime = 40;
    // sumDurations: 1 + 2 + 3 + 4 + 34 + 33 + 32 + 31;
    long sumDurations = 140;
    assertEquals(
        Math.min(4, (long) (elapsedTime * (10.0 / sumDurations))),
        latencies.get(State.GET_WORK_IN_TRANSIT_TO_DISPATCHER).getTotalDurationMillis());
    assertEquals(
        Math.min(34, (long) (elapsedTime * (130.0 / sumDurations))),
        latencies.get(State.GET_WORK_IN_TRANSIT_TO_USER_WORKER).getTotalDurationMillis());
  }

  @Test
  public void testGetWorkTimingInfosTracker_ClockSkew() throws Exception {
    int skewMicros = 50 * 1000;
    GetWorkTimingInfosTracker tracker = new GetWorkTimingInfosTracker(() -> 50);
    List<GetWorkStreamTimingInfo> infos = new ArrayList<>();
    for (int i = 0; i <= 3; i++) {
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_CREATION_START)
              .setTimestampUsec(skewMicros)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_CREATION_END)
              .setTimestampUsec(10000 + skewMicros)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_RECEIVED_BY_DISPATCHER)
              .setTimestampUsec((i + 11) * 1000 + skewMicros)
              .build());
      infos.add(
          GetWorkStreamTimingInfo.newBuilder()
              .setEvent(Event.GET_WORK_FORWARDED_BY_DISPATCHER)
              .setTimestampUsec((i + 16) * 1000 + skewMicros)
              .build());
      tracker.addTimingInfo(infos);
      infos.clear();
    }
    // durations for each chunk:
    // GET_WORK_IN_WINDMILL_WORKER: 10, 10, 10, 10
    // GET_WORK_IN_TRANSIT_TO_DISPATCHER: 1, 2, 3, 4 -> sum to 10
    // GET_WORK_IN_TRANSIT_TO_USER_WORKER: not observed due to skew
    Map<State, LatencyAttribution> latencies = new HashMap<>();
    List<LatencyAttribution> attributions = tracker.getLatencyAttributions();
    assertEquals(2, attributions.size());
    for (LatencyAttribution attribution : attributions) {
      latencies.put(attribution.getState(), attribution);
    }
    assertEquals(10L, latencies.get(State.GET_WORK_IN_WINDMILL_WORKER).getTotalDurationMillis());
    assertEquals(
        4L, latencies.get(State.GET_WORK_IN_TRANSIT_TO_DISPATCHER).getTotalDurationMillis());
    assertNull(latencies.get(State.GET_WORK_IN_TRANSIT_TO_USER_WORKER));
  }

  class ResponseErrorInjector<Stream extends StreamObserver> {

    private final Stream stream;
    private final Thread errorThread;
    private boolean cancelled = false;

    public ResponseErrorInjector(Stream stream) {
      this.stream = stream;
      errorThread = new Thread(this::errorThreadBody);
      errorThread.start();
    }

    private void errorThreadBody() {
      int i = 0;
      while (true) {
        try {
          Thread.sleep(ThreadLocalRandom.current().nextInt(++i * 10));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
        synchronized (this) {
          if (cancelled) {
            break;
          }
        }
        maybeInjectError(stream);
      }
    }

    public void cancel() {
      LOG.info("Starting cancel of error injector.");
      synchronized (this) {
        cancelled = true;
      }
      errorThread.interrupt();
      try {
        errorThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      LOG.info("Done cancelling.");
    }
  }
}
