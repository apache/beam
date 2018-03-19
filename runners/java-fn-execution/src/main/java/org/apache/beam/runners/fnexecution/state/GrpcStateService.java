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
package org.apache.beam.runners.fnexecution.state;

import static com.google.common.base.Throwables.getStackTraceAsString;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnStateGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the Beam Fn State service. */
public class GrpcStateService extends BeamFnStateGrpc.BeamFnStateImplBase
    implements StateDelegator, FnService {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcStateService.class);
  private final ConcurrentHashMap<String, StateRequestHandler> requestHandlers;

  public GrpcStateService()
      throws Exception {
    this.requestHandlers = new ConcurrentHashMap<>();
  }

  @Override
  public void close() {
    // TODO: Track multiple clients and disconnect them cleanly instead of forcing termination
  }

  @Override
  public StreamObserver<StateRequest> state(StreamObserver<StateResponse> responseObserver) {
    return new Inbound(responseObserver);
  }

  @Override
  public AutoCloseable registerForProcessBundleInstructionId(
      String processBundleInstructionId, StateRequestHandler handler) {
    requestHandlers.put(processBundleInstructionId, handler);
    return () -> requestHandlers.remove(processBundleInstructionId);
  }

  /**
   * An inbound {@link StreamObserver} which delegates requests to registered handlers.
   *
   * <p>Is only threadsafe if the outbound observer is threadsafe.
   *
   * <p>TODO: Handle when the client indicates completion or an error on the inbound stream and
   * there are pending requests.
   */
  private class Inbound implements StreamObserver<StateRequest> {
    private final StreamObserver<StateResponse> outboundObserver;

    Inbound(StreamObserver<StateResponse> outboundObserver) {
      this.outboundObserver = outboundObserver;
    }

    @Override
    public void onNext(StateRequest request) {
      CompletionStage<StateResponse.Builder> responseStage = new CompletableFuture<>();
      responseStage.whenCompleteAsync(
          (StateResponse.Builder responseBuilder, Throwable t) ->
              // note that this is threadsafe if and only if outboundObserver is threadsafe.
              outboundObserver.onNext(
                  t == null
                      ? responseBuilder.setId(request.getId()).build()
                      : StateResponse.newBuilder()
                      .setId(request.getId())
                      .setError(getStackTraceAsString(t))
                      .build()));
      StateRequestHandler handler =
          requestHandlers.getOrDefault(request.getInstructionReference(), this::handlerNotFound);
      try {
        handler.accept(request, responseStage);
      } catch (Exception e) {
        responseStage.toCompletableFuture().completeExceptionally(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      outboundObserver.onCompleted();
    }

    @Override
    public void onCompleted() {
      outboundObserver.onCompleted();
    }

    private void handlerNotFound(
        StateRequest request, CompletionStage<StateResponse.Builder> responseFuture) {
      responseFuture.toCompletableFuture().complete(
          StateResponse.newBuilder()
              .setError(
                  String.format(
                      "Unknown process bundle instruction id '%s'",
                      request.getInstructionReference())));
    }
  }
}

