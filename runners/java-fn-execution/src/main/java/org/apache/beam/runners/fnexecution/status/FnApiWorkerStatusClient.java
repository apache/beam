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
package org.apache.beam.runners.fnexecution.status;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.sdk.fn.stream.SynchronizedStreamObserver;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for handling requests and responses over Fn Worker Status Api between runner and SDK
 * Harness.
 */
public class FnApiWorkerStatusClient implements Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(FnApiWorkerStatusClient.class);
  private final StreamObserver<WorkerStatusRequest> requestReceiver;
  private final Map<String, CompletableFuture<WorkerStatusResponse>> responseQueue =
      new ConcurrentHashMap<>();
  private final String workerId;
  private AtomicBoolean isClosed = new AtomicBoolean(false);

  private FnApiWorkerStatusClient(
      String workerId, StreamObserver<WorkerStatusRequest> requestReceiver) {
    this.requestReceiver = SynchronizedStreamObserver.wrapping(requestReceiver);
    this.workerId = workerId;
  }

  /**
   * Create new status api client with SDK Harness worker id and request observer.
   *
   * @param workerId SDK Harness worker id.
   * @param requestObserver The outbound request observer this client uses to send new status
   *     requests to its corresponding SDK Harness.
   * @return {@link FnApiWorkerStatusClient}
   */
  public static FnApiWorkerStatusClient forRequestObserver(
      String workerId, StreamObserver<WorkerStatusRequest> requestObserver) {
    return new FnApiWorkerStatusClient(workerId, requestObserver);
  }

  /**
   * Get the latest sdk worker status from the client's corresponding SDK Harness. A random UUID
   * will be used to specify the request_id field.
   *
   * @return {@link CompletableFuture} of the SDK Harness status response.
   */
  public CompletableFuture<WorkerStatusResponse> getWorkerStatus() {
    WorkerStatusRequest request =
        WorkerStatusRequest.newBuilder().setRequestId(UUID.randomUUID().toString()).build();
    return getWorkerStatus(request);
  }

  /**
   * Get the latest sdk worker status from the client's corresponding SDK Harness with request.
   *
   * @param request WorkerStatusRequest to be sent to SDK Harness.
   * @return {@link CompletableFuture} of the SDK Harness status response.
   */
  @VisibleForTesting
  public CompletableFuture<WorkerStatusResponse> getWorkerStatus(WorkerStatusRequest request) {
    CompletableFuture<WorkerStatusResponse> future = new CompletableFuture<>();
    this.requestReceiver.onNext(request);
    this.responseQueue.put(request.getRequestId(), future);
    return future;
  }

  @Override
  public void close() throws IOException {
    if (isClosed.getAndSet(true)) {
      return;
    }
    for (CompletableFuture<WorkerStatusResponse> pendingResponse : responseQueue.values()) {
      pendingResponse.completeExceptionally(
          new RuntimeException("Fn Status Api client shut down while waiting for the request"));
    }
    responseQueue.clear();
    requestReceiver.onCompleted();
  }

  /** Get the worker id for the client's corresponding SDK Harness. */
  public String getWorkerId() {
    return this.workerId;
  }

  /** Get the response observer of this client for retrieving inbound worker status responses. */
  public StreamObserver<WorkerStatusResponse> getResponseObserver() {
    return new ResponseStreamObserver();
  }

  /**
   * ResponseObserver for handling status responses. Each request will be cached with it's
   * request_id. Upon receiving response from SDK Harness with this StreamObserver, the future
   * mapped to same request_id will be finished accordingly.
   */
  private class ResponseStreamObserver implements StreamObserver<WorkerStatusResponse> {

    @Override
    public void onNext(WorkerStatusResponse response) {
      if (!responseQueue.containsKey(response.getRequestId())) {
        return;
      }
      CompletableFuture<WorkerStatusResponse> responseFuture =
          responseQueue.remove(response.getRequestId());
      responseFuture.complete(response);
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("{} received error {}", FnApiWorkerStatusClient.class.getSimpleName(), throwable);
    }

    @Override
    public void onCompleted() {
      try {
        close();
      } catch (IOException e) {
        LOG.warn("Error closing Fn status api client", e);
      }
    }
  }
}
