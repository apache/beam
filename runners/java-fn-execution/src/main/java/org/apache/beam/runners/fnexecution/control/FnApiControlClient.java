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
package org.apache.beam.runners.fnexecution.control;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.sdk.fn.stream.SynchronizedStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.StatusRuntimeException;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
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
 */
public class FnApiControlClient implements Closeable, InstructionRequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(FnApiControlClient.class);

  // All writes to this StreamObserver need to be synchronized.
  private final StreamObserver<BeamFnApi.InstructionRequest> requestReceiver;
  private final ResponseStreamObserver responseObserver = new ResponseStreamObserver();
  private final ConcurrentMap<String, CompletableFuture<BeamFnApi.InstructionResponse>>
      outstandingRequests;
  private final Set<Consumer<FnApiControlClient>> onCloseListeners = ConcurrentHashMap.newKeySet();
  private final String workerId;
  private AtomicBoolean isClosed = new AtomicBoolean(false);
  private final ConcurrentMap<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors;

  private FnApiControlClient(
      String workerId,
      StreamObserver<InstructionRequest> requestReceiver,
      ConcurrentMap<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors) {
    this.workerId = workerId;
    this.requestReceiver = SynchronizedStreamObserver.wrapping(requestReceiver);
    this.processBundleDescriptors = processBundleDescriptors;
    this.outstandingRequests = new ConcurrentHashMap<>();
  }

  /**
   * Returns a {@link FnApiControlClient} which will submit its requests to the provided observer.
   *
   * <p>It is the responsibility of the caller to register this object as an observer of incoming
   * responses (this will generally be done as part of fulfilling the contract of a gRPC service).
   */
  public static FnApiControlClient forRequestObserver(
      String workerId,
      StreamObserver<BeamFnApi.InstructionRequest> requestObserver,
      ConcurrentMap<String, BeamFnApi.ProcessBundleDescriptor> processBundleDescriptors) {
    return new FnApiControlClient(workerId, requestObserver, processBundleDescriptors);
  }

  @Override
  public CompletionStage<BeamFnApi.InstructionResponse> handle(
      BeamFnApi.InstructionRequest request) {
    LOG.debug("Sending InstructionRequest {}", request);
    CompletableFuture<BeamFnApi.InstructionResponse> resultFuture = new CompletableFuture<>();
    outstandingRequests.put(request.getInstructionId(), resultFuture);
    requestReceiver.onNext(request);
    return resultFuture;
  }

  public StreamObserver<BeamFnApi.InstructionResponse> asResponseObserver() {
    return responseObserver;
  }

  public BeamFnApi.ProcessBundleDescriptor getProcessBundleDescriptor(String id) {
    return processBundleDescriptors.get(id);
  }

  @Override
  public void registerProcessBundleDescriptor(
      BeamFnApi.ProcessBundleDescriptor processBundleDescriptor) {
    processBundleDescriptors.put(processBundleDescriptor.getId(), processBundleDescriptor);
  }

  @Override
  public void close() {
    closeAndTerminateOutstandingRequests(new IllegalStateException("Runner closed connection"));
  }

  public String getWorkerId() {
    return workerId;
  }

  /** Closes this client and terminates any outstanding requests exceptionally. */
  private void closeAndTerminateOutstandingRequests(Throwable cause) {
    if (isClosed.getAndSet(true)) {
      return;
    }

    try {
      // Make a copy of the map to make the view of the outstanding requests consistent.
      Map<String, CompletableFuture<BeamFnApi.InstructionResponse>> outstandingRequestsCopy =
          new ConcurrentHashMap<>(outstandingRequests);
      outstandingRequests.clear();

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
      for (CompletableFuture<BeamFnApi.InstructionResponse> outstandingRequest :
          outstandingRequestsCopy.values()) {
        outstandingRequest.completeExceptionally(cause);
      }
    } finally {
      for (Consumer<FnApiControlClient> onCloseListener : onCloseListeners) {
        onCloseListener.accept(this);
      }
    }
  }

  public void onClose(Consumer<FnApiControlClient> onCloseListener) {
    onCloseListeners.add(onCloseListener);
  }

  /** A private view of this class as a {@link StreamObserver} for connecting as a gRPC listener. */
  private class ResponseStreamObserver implements StreamObserver<BeamFnApi.InstructionResponse> {
    /**
     * Processes an incoming {@link BeamFnApi.InstructionResponse} by correlating it with the
     * corresponding {@link BeamFnApi.InstructionRequest} and completes the future that was returned
     * by {@link #handle}.
     */
    @Override
    public void onNext(BeamFnApi.InstructionResponse response) {
      LOG.debug("Received InstructionResponse {}", response);
      CompletableFuture<BeamFnApi.InstructionResponse> responseFuture =
          outstandingRequests.remove(response.getInstructionId());
      if (responseFuture == null) {
        LOG.warn("Dropped unknown InstructionResponse {}", response);
        return;
      }
      if (response.getError().isEmpty()) {
        responseFuture.complete(response);
      } else {
        responseFuture.completeExceptionally(
            new RuntimeException(
                String.format(
                    "Error received from SDK harness for instruction %s: %s",
                    response.getInstructionId(), response.getError())));
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
      LOG.error("{} received an error.", FnApiControlClient.class.getSimpleName(), cause);
      closeAndTerminateOutstandingRequests(cause);
    }
  }
}
