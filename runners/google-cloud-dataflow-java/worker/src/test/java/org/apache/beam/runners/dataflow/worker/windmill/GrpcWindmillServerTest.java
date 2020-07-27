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
package org.apache.beam.runners.dataflow.worker.windmill;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1ImplBase;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationWorkItemMetadata;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalData;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataId;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataResponse;
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
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub.GetWorkStream;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.util.MutableHandlerRegistry;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link GrpcWindmillServer}. */
@RunWith(JUnit4.class)
public class GrpcWindmillServerTest {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcWindmillServer.class);

  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  private Server server;
  private GrpcWindmillServer client;
  private static final int STREAM_CHUNK_SIZE = 2 << 20;
  private int remainingErrors = 20;

  @Before
  public void setUp() throws Exception {
    String name = "Fake server for " + getClass();

    this.server =
        InProcessServerBuilder.forName(name)
            .fallbackHandlerRegistry(serviceRegistry)
            .executor(Executors.newFixedThreadPool(1))
            .build()
            .start();

    this.client = GrpcWindmillServer.newTestInstance(name, true);
  }

  @After
  public void tearDown() throws Exception {
    server.shutdownNow();
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

  class ResponseErrorInjector<Stream extends StreamObserver> {
    private Stream stream;
    private Thread errorThread;
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
          Thread.sleep(ThreadLocalRandom.current().nextInt((++i) * 10));
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
              boolean sawHeader = false;
              ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);

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
                Windmill.WorkItem workItem) -> {
              latch.countDown();
              assertEquals(inputDataWatermark, new Instant(18));
              assertEquals(synchronizedProcessingTime, new Instant(17));
              assertEquals(workItem.getKey(), ByteString.copyFromUtf8("somewhat_long_key"));
            });
    assertTrue(latch.await(30, TimeUnit.SECONDS));

    stream.close();
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
              boolean sawHeader = false;
              HashSet<Long> seenIds = new HashSet<>();
              ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);
              StreamingGetDataResponse.Builder responseBuilder =
                  StreamingGetDataResponse.newBuilder();

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
                    makeGetDataResponse(
                        request.getKey().toStringUtf8(),
                        request.getValuesToFetch(0).getTag().toStringUtf8());
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
                  } catch (IllegalStateException e) {
                    // Stream is already closed.
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
            errorCollector.checkThat(
                stream.requestKeyedData("computation", makeGetDataRequest(key, s)),
                Matchers.equalTo(makeGetDataResponse(key, s)));
            done.countDown();
          });
      executor.execute(
          () -> {
            errorCollector.checkThat(
                stream.requestGlobalData(makeGlobalDataRequest(key)),
                Matchers.equalTo(makeGlobalDataResponse(key)));
            done.countDown();
          });
    }
    done.await();
    stream.close();
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

  private KeyedGetDataResponse makeGetDataResponse(String key, String tag) {
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

  @Test
  public void testStreamingCommit() throws Exception {
    List<WorkItemCommitRequest> commitRequestList = new ArrayList<>();
    List<CountDownLatch> latches = new ArrayList<>();
    Map<Long, WorkItemCommitRequest> commitRequests = new HashMap<>();
    for (int i = 0; i < 500; ++i) {
      // Build some requests of varying size with a few big ones.
      WorkItemCommitRequest request = makeCommitRequest(i, i * (i < 480 ? 8 : 128));
      commitRequestList.add(request);
      commitRequests.put((long) i, request);
      latches.add(new CountDownLatch(1));
    }
    Collections.shuffle(commitRequestList);

    // This server receives WorkItemCommitRequests, and verifies they are equal to the above
    // commitRequest.
    serviceRegistry.addService(
        new CloudWindmillServiceV1Alpha1ImplBase() {
          @Override
          public StreamObserver<StreamingCommitWorkRequest> commitWorkStream(
              StreamObserver<StreamingCommitResponse> responseObserver) {
            return new StreamObserver<StreamingCommitWorkRequest>() {
              boolean sawHeader = false;
              InputStream buffer = null;
              long remainingBytes = 0;
              ResponseErrorInjector injector = new ResponseErrorInjector(responseObserver);

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
                              .build()));
                  sawHeader = true;
                  LOG.info("Received header");
                } else {
                  boolean first = true;
                  LOG.info("Received request with {} chunks", request.getCommitChunkCount());
                  for (StreamingCommitRequestChunk chunk : request.getCommitChunkList()) {
                    assertTrue(chunk.getSerializedWorkItemCommit().size() <= STREAM_CHUNK_SIZE);
                    if (first || chunk.hasComputationId()) {
                      errorCollector.checkThat(
                          chunk.getComputationId(), Matchers.equalTo("computation"));
                    }

                    if (remainingBytes != 0) {
                      errorCollector.checkThat(buffer, Matchers.notNullValue());
                      errorCollector.checkThat(
                          remainingBytes,
                          Matchers.is(
                              chunk.getSerializedWorkItemCommit().size()
                                  + chunk.getRemainingBytesForWorkItem()));
                      buffer =
                          new SequenceInputStream(
                              buffer, chunk.getSerializedWorkItemCommit().newInput());
                    } else {
                      errorCollector.checkThat(buffer, Matchers.nullValue());
                      buffer = chunk.getSerializedWorkItemCommit().newInput();
                    }
                    remainingBytes = chunk.getRemainingBytesForWorkItem();
                    if (remainingBytes == 0) {
                      try {
                        WorkItemCommitRequest received = WorkItemCommitRequest.parseFrom(buffer);
                        errorCollector.checkThat(
                            received,
                            Matchers.equalTo(commitRequests.get(received.getWorkToken())));
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
        });

    // Make the commit requests, waiting for each of them to be verified and acknowledged.
    CommitWorkStream stream = client.commitWorkStream();
    for (int i = 0; i < commitRequestList.size(); ) {
      final CountDownLatch latch = latches.get(i);
      if (stream.commitWorkItem(
          "computation",
          commitRequestList.get(i),
          (CommitStatus status) -> {
            assertEquals(status, CommitStatus.OK);
            latch.countDown();
          })) {
        i++;
      } else {
        stream.flush();
      }
    }
    stream.flush();
    for (CountDownLatch latch : latches) {
      assertTrue(latch.await(1, TimeUnit.MINUTES));
    }

    stream.close();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }

  private List<KeyedGetDataRequest> makeHeartbeatRequest(List<String> keys) {
    List<KeyedGetDataRequest> result = new ArrayList<>();
    for (String key : keys) {
      result.add(
          Windmill.KeyedGetDataRequest.newBuilder()
              .setKey(ByteString.copyFromUtf8(key))
              .setWorkToken(0)
              .build());
    }
    return result;
  }

  @Test
  public void testStreamingGetDataHeartbeats() throws Exception {
    // This server records the heartbeats observed but doesn't respond.
    final Map<String, List<KeyedGetDataRequest>> heartbeats = new HashMap<>();

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
                                .build()));
                    sawHeader = true;
                  } else {
                    LOG.info("Received {} heartbeats", chunk.getStateRequestCount());
                    errorCollector.checkThat(
                        chunk.getSerializedSize(), Matchers.lessThanOrEqualTo(STREAM_CHUNK_SIZE));
                    errorCollector.checkThat(chunk.getRequestIdCount(), Matchers.is(0));

                    synchronized (heartbeats) {
                      for (ComputationGetDataRequest request : chunk.getStateRequestList()) {
                        errorCollector.checkThat(request.getRequestsCount(), Matchers.is(1));
                        heartbeats.putIfAbsent(request.getComputationId(), new ArrayList<>());
                        heartbeats
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

    Map<String, List<KeyedGetDataRequest>> activeMap = new HashMap<>();
    List<String> computation1Keys = new ArrayList<>();
    List<String> computation2Keys = new ArrayList<>();

    for (int i = 0; i < 100; ++i) {
      computation1Keys.add("Computation1Key" + i);
      computation2Keys.add("Computation2Key" + largeString(i * 20));
    }
    activeMap.put("Computation1", makeHeartbeatRequest(computation1Keys));
    activeMap.put("Computation2", makeHeartbeatRequest(computation2Keys));

    GetDataStream stream = client.getDataStream();
    stream.refreshActiveWork(activeMap);
    stream.close();
    assertTrue(stream.awaitTermination(60, TimeUnit.SECONDS));

    while (true) {
      Thread.sleep(100);
      synchronized (heartbeats) {
        if (heartbeats.size() != activeMap.size()) {
          continue;
        }
        assertEquals(heartbeats, activeMap);
        break;
      }
    }
  }

  @Test
  public void testThrottleSignal() throws Exception {
    // This server responds with work items until the throttleMessage limit is hit at which point it
    // returns RESROUCE_EXHAUSTED errors for throttleTime msecs after which it resumes sending
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
                            .setRemainingBytesForWorkItem(0);
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
                Windmill.WorkItem workItem) -> {
              latch.countDown();
            });
    // Wait for 100 items or 30 seconds.
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    // Confirm that we report at least as much throttle time as our server sent errors for.  We will
    // actually report more due to backoff in restarting streams.
    assertTrue(this.client.getAndResetThrottleTime() > throttleTime);

    stream.close();
    assertTrue(stream.awaitTermination(30, TimeUnit.SECONDS));
  }
}
