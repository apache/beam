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

import com.google.auto.value.AutoOneOf;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStreamClosedException;

/** Utility data classes for {@link GrpcGetDataStream}. */
final class GrpcGetDataStreamRequests {
  private GrpcGetDataStreamRequests() {}

  static class QueuedRequest {
    private final long id;
    private final ComputationOrGlobalDataRequest dataRequest;
    private AppendableInputStream responseStream;

    private QueuedRequest(long id, ComputationOrGlobalDataRequest dataRequest) {
      this.id = id;
      this.dataRequest = dataRequest;
      responseStream = new AppendableInputStream();
    }

    static QueuedRequest forComputation(
        long id, String computation, KeyedGetDataRequest keyedGetDataRequest) {
      ComputationGetDataRequest computationGetDataRequest =
          ComputationGetDataRequest.newBuilder()
              .setComputationId(computation)
              .addRequests(keyedGetDataRequest)
              .build();
      return new QueuedRequest(
          id, ComputationOrGlobalDataRequest.computation(computationGetDataRequest));
    }

    static QueuedRequest global(long id, GlobalDataRequest globalDataRequest) {
      return new QueuedRequest(id, ComputationOrGlobalDataRequest.global(globalDataRequest));
    }

    static Comparator<QueuedRequest> globalRequestsFirst() {
      return (QueuedRequest r1, QueuedRequest r2) -> {
        boolean r1gd = r1.dataRequest.isGlobal();
        boolean r2gd = r2.dataRequest.isGlobal();
        return r1gd == r2gd ? 0 : (r1gd ? -1 : 1);
      };
    }

    long id() {
      return id;
    }

    long byteSize() {
      return dataRequest.serializedSize();
    }

    AppendableInputStream getResponseStream() {
      return responseStream;
    }

    void resetResponseStream() {
      this.responseStream = new AppendableInputStream();
    }

    public ComputationOrGlobalDataRequest getDataRequest() {
      return dataRequest;
    }

    void addToStreamingGetDataRequest(Windmill.StreamingGetDataRequest.Builder builder) {
      builder.addRequestId(id);
      if (dataRequest.isForComputation()) {
        builder.addStateRequest(dataRequest.computation());
      } else {
        builder.addGlobalDataRequest(dataRequest.global());
      }
    }
  }

  static class QueuedBatch {
    private final List<QueuedRequest> requests = new ArrayList<>();
    private final CountDownLatch sent = new CountDownLatch(1);
    private long byteSize = 0;
    private boolean finalized = false;
    private volatile boolean failed = false;

    CountDownLatch getLatch() {
      return sent;
    }

    List<QueuedRequest> requests() {
      return requests;
    }

    /**
     * Put all global data requests first because there is only a single repeated field for request
     * ids and the initial ids correspond to global data requests if they are present.
     */
    List<QueuedRequest> sortedRequests() {
      requests.sort(QueuedRequest.globalRequestsFirst());
      return requests;
    }

    void validateRequests(Consumer<QueuedRequest> requestValidator) {
      requests.forEach(requestValidator);
    }

    int requestCount() {
      return requests.size();
    }

    long byteSize() {
      return byteSize;
    }

    boolean isFinalized() {
      return finalized;
    }

    void markFinalized() {
      finalized = true;
    }

    void addRequest(QueuedRequest request) {
      requests.add(request);
      byteSize += request.byteSize();
    }

    /** Let waiting for threads know that the request has been successfully sent. */
    synchronized void notifySent() {
      sent.countDown();
    }

    /** Let waiting for threads know that a failure occurred. */
    synchronized void notifyFailed() {
      sent.countDown();
      failed = true;
    }

    /**
     * Block until notified of a successful send via {@link #notifySent()} or a non-retryable
     * failure via {@link #notifyFailed()}. On failure, throw an exception to on calling threads.
     */
    void waitForSendOrFailNotification() throws InterruptedException {
      sent.await();
      if (failed) {
        fail();
      }
    }

    void fail() {
      throw new WindmillStreamClosedException(
          "Requests failed for batch containing "
              + createStreamCancelledErrorMessage()
              + " requests. This is most likely due to the stream being explicitly closed"
              + " which happens when the work is marked as invalid on the streaming"
              + " backend when key ranges shuffle around. This is transient and corresponding"
              + " work will eventually be retried");
    }

    String createStreamCancelledErrorMessage() {
      return requests.stream()
          .map(
              request -> {
                switch (request.getDataRequest().getKind()) {
                  case GLOBAL:
                    return "GetSideInput=" + request.getDataRequest().global();
                  case COMPUTATION:
                    return request.getDataRequest().computation().getRequestsList().stream()
                        .map(
                            keyedRequest ->
                                "KeyedGetState=["
                                    + "key="
                                    + keyedRequest.getKey()
                                    + "shardingKey="
                                    + keyedRequest.getShardingKey()
                                    + "cacheToken="
                                    + keyedRequest.getCacheToken()
                                    + "workToken"
                                    + keyedRequest.getWorkToken()
                                    + "]")
                        .collect(Collectors.joining());
                  default:
                    // Will never happen switch is exhaustive.
                    throw new IllegalStateException();
                }
              })
          .collect(Collectors.joining(","));
    }
  }

  @AutoOneOf(ComputationOrGlobalDataRequest.Kind.class)
  abstract static class ComputationOrGlobalDataRequest {
    static ComputationOrGlobalDataRequest computation(
        ComputationGetDataRequest computationGetDataRequest) {
      return AutoOneOf_GrpcGetDataStreamRequests_ComputationOrGlobalDataRequest.computation(
          computationGetDataRequest);
    }

    static ComputationOrGlobalDataRequest global(GlobalDataRequest globalDataRequest) {
      return AutoOneOf_GrpcGetDataStreamRequests_ComputationOrGlobalDataRequest.global(
          globalDataRequest);
    }

    abstract Kind getKind();

    abstract ComputationGetDataRequest computation();

    abstract GlobalDataRequest global();

    boolean isGlobal() {
      return getKind() == Kind.GLOBAL;
    }

    boolean isForComputation() {
      return getKind() == Kind.COMPUTATION;
    }

    long serializedSize() {
      switch (getKind()) {
        case GLOBAL:
          return global().getSerializedSize();
        case COMPUTATION:
          return computation().getSerializedSize();
          // this will never happen since the switch is exhaustive.
        default:
          throw new UnsupportedOperationException("unknown dataRequest type.");
      }
    }

    enum Kind {
      COMPUTATION,
      GLOBAL
    }
  }
}
