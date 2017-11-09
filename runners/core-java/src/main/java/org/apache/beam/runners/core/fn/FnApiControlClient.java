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
package org.apache.beam.runners.core.fn;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client for the control plane of an SDK harness, which can issue requests to it over the Fn API.
 *
 * <p>This class presents a low-level Java API de-inverting the Fn API's gRPC layer.
 *
 * <p>The Fn API is inverted so the runner is the server and the SDK harness is the client, for
 * firewalling reasons (the runner may execute in a more privileged environment forbidding outbound
 * connections).
 *
 * <p>This low-level client is responsible only for correlating requests with responses.
 *
 * @deprecated Runners should depend on the beam-runners-java-fn-execution module for this
 *     functionality.
 */
@Deprecated
class FnApiControlClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FnApiControlClient.class);

  // All writes to this StreamObserver need to be synchronized.
  private final StreamObserver<BeamFnApi.InstructionRequest> requestReceiver;
  private final ResponseStreamObserver responseObserver = new ResponseStreamObserver();
  private final Map<String, SettableFuture<BeamFnApi.InstructionResponse>> outstandingRequests;
  private volatile boolean isClosed;

  private FnApiControlClient(StreamObserver<BeamFnApi.InstructionRequest> requestReceiver) {
    this.requestReceiver = requestReceiver;
    this.outstandingRequests = new ConcurrentHashMap<>();
  }

  /**
   * Returns a {@link FnApiControlClient} which will submit its requests to the provided
   * observer.
   *
   * <p>It is the responsibility of the caller to register this object as an observer of incoming
   * responses (this will generally be done as part of fulfilling the contract of a gRPC service).
   */
  public static FnApiControlClient forRequestObserver(
      StreamObserver<BeamFnApi.InstructionRequest> requestObserver) {
    return new FnApiControlClient(requestObserver);
  }

  public synchronized ListenableFuture<BeamFnApi.InstructionResponse> handle(
      BeamFnApi.InstructionRequest request) {
    LOG.debug("Sending InstructionRequest {}", request);
    SettableFuture<BeamFnApi.InstructionResponse> resultFuture = SettableFuture.create();
    outstandingRequests.put(request.getInstructionId(), resultFuture);
    requestReceiver.onNext(request);
    return resultFuture;
  }

  StreamObserver<BeamFnApi.InstructionResponse> asResponseObserver() {
    return responseObserver;
  }

  @Override
  public void close() {
    closeAndTerminateOutstandingRequests(new IllegalStateException("Runner closed connection"));
  }

  /** Closes this client and terminates any outstanding requests exceptionally. */
  private synchronized void closeAndTerminateOutstandingRequests(Throwable cause) {
    if (isClosed) {
      return;
    }

    // Make a copy of the map to make the view of the outstanding requests consistent.
    Map<String, SettableFuture<BeamFnApi.InstructionResponse>> outstandingRequestsCopy =
        new ConcurrentHashMap<>(outstandingRequests);
    outstandingRequests.clear();
    isClosed = true;

    if (outstandingRequestsCopy.isEmpty()) {
      requestReceiver.onCompleted();
      return;
    }
    requestReceiver.onError(
        new StatusRuntimeException(Status.CANCELLED.withDescription(cause.getMessage())));

    LOG.error(
        "{} closed, clearing outstanding requests {}",
        FnApiControlClient.class.getSimpleName(),
        outstandingRequestsCopy);
    for (SettableFuture<BeamFnApi.InstructionResponse> outstandingRequest :
        outstandingRequestsCopy.values()) {
      outstandingRequest.setException(cause);
    }
  }

  /**
   * A private view of this class as a {@link StreamObserver} for connecting as a gRPC listener.
   */
  private class ResponseStreamObserver implements StreamObserver<BeamFnApi.InstructionResponse> {
    /**
     * Processes an incoming {@link BeamFnApi.InstructionResponse} by correlating it with the
     * corresponding {@link BeamFnApi.InstructionRequest} and completes the future that was returned
     * by {@link #handle}.
     */
    @Override
    public void onNext(BeamFnApi.InstructionResponse response) {
      LOG.debug("Received InstructionResponse {}", response);
      SettableFuture<BeamFnApi.InstructionResponse> completableFuture =
          outstandingRequests.remove(response.getInstructionId());
      if (completableFuture != null) {
        completableFuture.set(response);
      }
    }

    /** */
    @Override
    public void onCompleted() {
      closeAndTerminateOutstandingRequests(
          new IllegalStateException("SDK harness closed connection"));
    }

    @Override
    public void onError(Throwable cause) {
      LOG.error("{} received error {}", FnApiControlClient.class.getSimpleName(), cause);
      closeAndTerminateOutstandingRequests(cause);
    }
  }
}
