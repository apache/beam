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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoOneOf;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility data classes for {@link GrpcGetDataStream}. */
final class GrpcGetDataStreamRequests {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcGetDataStreamRequests.class);
  private static final int STREAM_CANCELLED_ERROR_LOG_LIMIT = 3;

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
    private volatile boolean finalized = false;
    private volatile boolean failed = false;

    List<QueuedRequest> requests() {
      return requests;
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
      failed = true;
      sent.countDown();
    }

    /**
     * Block until notified of a successful send via {@link #notifySent()} or a non-retryable
     * failure via {@link #notifyFailed()}. On failure, throw an exception to on calling threads.
     */
    void waitForSendOrFailNotification() throws InterruptedException {
      sent.await();
      if (failed) {
        ImmutableList<String> cancelledRequests = createStreamCancelledErrorMessage();
        LOG.error("Requests failed for the following batches: {}", cancelledRequests);
        throw new WindmillStreamShutdownException(
            "Requests failed for batch containing "
                + String.join(", ", cancelledRequests)
                + " ... requests. This is most likely due to the stream being explicitly closed"
                + " which happens when the work is marked as invalid on the streaming"
                + " backend when key ranges shuffle around. This is transient and corresponding"
                + " work will eventually be retried.");
      }
    }

    ImmutableList<String> createStreamCancelledErrorMessage() {
      return requests.stream()
          .flatMap(
              request -> {
                switch (request.getDataRequest().getKind()) {
                  case GLOBAL:
                    return Stream.of("GetSideInput=" + request.getDataRequest().global());
                  case COMPUTATION:
                    return request.getDataRequest().computation().getRequestsList().stream()
                        .map(
                            keyedRequest ->
                                "KeyedGetState=["
                                    + "shardingKey="
                                    + keyedRequest.getShardingKey()
                                    + "cacheToken="
                                    + keyedRequest.getCacheToken()
                                    + "workToken"
                                    + keyedRequest.getWorkToken()
                                    + "]");
                  default:
                    // Will never happen switch is exhaustive.
                    throw new IllegalStateException();
                }
              })
          .limit(STREAM_CANCELLED_ERROR_LOG_LIMIT)
          .collect(toImmutableList());
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
