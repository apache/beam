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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.ComputationGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;

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

    CountDownLatch getLatch() {
      return sent;
    }

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

    void countDown() {
      sent.countDown();
    }

    void await() throws InterruptedException {
      sent.await();
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
