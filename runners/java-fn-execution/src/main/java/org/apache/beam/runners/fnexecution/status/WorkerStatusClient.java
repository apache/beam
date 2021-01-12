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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.WorkerStatusResponse;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.fn.stream.SynchronizedStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for handling requests and responses over Fn Worker Status Api between runner and SDK
 * harness.
 */
class WorkerStatusClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerStatusClient.class);
  private final IdGenerator idGenerator = IdGenerators.incrementingLongs();
  private final StreamObserver<WorkerStatusRequest> requestReceiver;
  private final Map<String, CompletableFuture<WorkerStatusResponse>> pendingResponses =
      Collections.synchronizedMap(new HashMap<>());
  private final String workerId;
  private AtomicBoolean isClosed = new AtomicBoolean(false);

  private WorkerStatusClient(String workerId, StreamObserver<WorkerStatusRequest> requestReceiver) {
    this.requestReceiver = SynchronizedStreamObserver.wrapping(requestReceiver);
    this.workerId = workerId;
  }

  /**
   * Create new status api client with SDK harness worker id and request observer.
   *
   * @param workerId SDK harness worker id.
   * @param requestObserver The outbound request observer this client uses to send new status
   *     requests to its corresponding SDK harness.
   * @return {@link WorkerStatusClient}
   */
  public static WorkerStatusClient forRequestObserver(
      String workerId, StreamObserver<WorkerStatusRequest> requestObserver) {
    return new WorkerStatusClient(workerId, requestObserver);
  }

  /**
   * Get the latest sdk worker status from the client's corresponding SDK harness. A random id will
   * be used to specify the request_id field.
   *
   * @return {@link CompletableFuture} of the SDK harness status response.
   */
  public CompletableFuture<WorkerStatusResponse> getWorkerStatus() {
    WorkerStatusRequest request =
        WorkerStatusRequest.newBuilder().setId(idGenerator.getId()).build();
    return getWorkerStatus(request);
  }

  /**
   * Get the latest sdk worker status from the client's corresponding SDK harness with request.
   *
   * @param request WorkerStatusRequest to be sent to SDK harness.
   * @return {@link CompletableFuture} of the SDK harness status response.
   */
  CompletableFuture<WorkerStatusResponse> getWorkerStatus(WorkerStatusRequest request) {
    CompletableFuture<WorkerStatusResponse> future = new CompletableFuture<>();
    if (isClosed.get()) {
      future.completeExceptionally(new RuntimeException("Worker status client already closed."));
      return future;
    }
    this.pendingResponses.put(request.getId(), future);
    this.requestReceiver.onNext(request);
    return future;
  }

  @Override
  public void close() throws IOException {
    if (isClosed.getAndSet(true)) {
      return;
    }
    synchronized (pendingResponses) {
      for (CompletableFuture<WorkerStatusResponse> pendingResponse : pendingResponses.values()) {
        pendingResponse.completeExceptionally(
            new RuntimeException("Fn Status Api client shut down while waiting for the request"));
      }
      pendingResponses.clear();
    }
    requestReceiver.onCompleted();
  }

  /** Check if the client connection has already been closed. */
  public boolean isClosed() {
    return isClosed.get();
  }

  /** Get the worker id for the client's corresponding SDK harness. */
  public String getWorkerId() {
    return this.workerId;
  }

  /** Get the response observer of this client for retrieving inbound worker status responses. */
  public StreamObserver<WorkerStatusResponse> getResponseObserver() {
    return new ResponseStreamObserver();
  }

  /**
   * ResponseObserver for handling status responses. Each request will be cached with it's
   * request_id. Upon receiving response from SDK harness with this StreamObserver, the future
   * mapped to same request_id will be finished accordingly.
   */
  private class ResponseStreamObserver implements StreamObserver<WorkerStatusResponse> {

    @Override
    public void onNext(WorkerStatusResponse response) {
      if (isClosed.get()) {
        return;
      }
      CompletableFuture<WorkerStatusResponse> future = pendingResponses.remove(response.getId());
      if (future != null) {
        future.complete(response);
      } else {
        LOG.warn(
            String.format(
                "Received response for status with unknown response id %s and status %s",
                response.getId(), response.getStatusInfo()));
      }
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("{} received error.", WorkerStatusClient.class.getSimpleName(), throwable);
      onCompleted();
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
