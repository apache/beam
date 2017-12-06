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
package org.apache.beam.fn.harness.state;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataGrpcClient;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnStateGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.stream.StreamObserverFactory.StreamObserverClientFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of {@link BeamFnStateClient}s which handle Beam Fn State requests using gRPC.
 *
 * <p>TODO: Add the ability to close which cancels any pending and stops any future requests.
 */
public class BeamFnStateGrpcClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcClient.class);

  private final ConcurrentMap<ApiServiceDescriptor, BeamFnStateClient> cache;
  private final Function<ApiServiceDescriptor, ManagedChannel> channelFactory;
  private final BiFunction<
          StreamObserverClientFactory<StateResponse, StateRequest>, StreamObserver<StateResponse>,
          StreamObserver<StateRequest>>
      streamObserverFactory;
  private final PipelineOptions options;
  private final Supplier<String> idGenerator;

  public BeamFnStateGrpcClientCache(
      PipelineOptions options,
      Supplier<String> idGenerator,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory,
      BiFunction<StreamObserverClientFactory<StateResponse, StateRequest>,
          StreamObserver<StateResponse>,
          StreamObserver<StateRequest>> streamObserverFactory) {
    this.options = options;
    this.idGenerator = idGenerator;
    this.channelFactory = channelFactory;
    this.streamObserverFactory = streamObserverFactory;
    this.cache = new ConcurrentHashMap<>();
  }

  /**(
   * Creates or returns an existing {@link BeamFnStateClient} depending on whether the passed in
   * {@link ApiServiceDescriptor} currently has a {@link BeamFnStateClient} bound to the same
   * channel.
   */
  public BeamFnStateClient forApiServiceDescriptor(ApiServiceDescriptor apiServiceDescriptor)
      throws IOException {
    return cache.computeIfAbsent(apiServiceDescriptor, this::createBeamFnStateClient);
  }

  private BeamFnStateClient createBeamFnStateClient(ApiServiceDescriptor apiServiceDescriptor) {
    return new GrpcStateClient(apiServiceDescriptor);
  }

  /**
   * A {@link BeamFnStateClient} for a given {@link ApiServiceDescriptor}.
   */
  private class GrpcStateClient implements BeamFnStateClient {
    private final ApiServiceDescriptor apiServiceDescriptor;
    private final ConcurrentMap<String, CompletableFuture<StateResponse>> outstandingRequests;
    private final StreamObserver<StateRequest> outboundObserver;
    private final ManagedChannel channel;
    private volatile RuntimeException closed;

    private GrpcStateClient(ApiServiceDescriptor apiServiceDescriptor) {
      this.apiServiceDescriptor = apiServiceDescriptor;
      this.outstandingRequests = new ConcurrentHashMap<>();
      this.channel = channelFactory.apply(apiServiceDescriptor);
      this.outboundObserver = streamObserverFactory.apply(
          BeamFnStateGrpc.newStub(channel)::state, new InboundObserver());
    }

    @Override
    public void handle(
        StateRequest.Builder requestBuilder, CompletableFuture<StateResponse> response) {
      requestBuilder.setId(idGenerator.get());
      StateRequest request = requestBuilder.build();
      outstandingRequests.put(request.getId(), response);

      // If the server closes, gRPC will throw an error if onNext is called.
      LOG.debug("Sending StateRequest {}", request);
      outboundObserver.onNext(request);
    }

    private synchronized void closeAndCleanUp(RuntimeException cause) {
      if (closed != null) {
        return;
      }
      cache.remove(apiServiceDescriptor);
      closed = cause;

      // Make a copy of the map to make the view of the outstanding requests consistent.
      Map<String, CompletableFuture<StateResponse>> outstandingRequestsCopy =
          new ConcurrentHashMap<>(outstandingRequests);

      if (outstandingRequestsCopy.isEmpty()) {
        outboundObserver.onCompleted();
        return;
      }

      outstandingRequests.clear();
      LOG.error("BeamFnState failed, clearing outstanding requests {}", outstandingRequestsCopy);

      for (CompletableFuture<StateResponse> entry : outstandingRequestsCopy.values()) {
        entry.completeExceptionally(cause);
      }
    }

    /**
     * A {@link StreamObserver} which propagates any server side state request responses by
     * completing the outstanding response future.
     *
     * <p>Also propagates server side failures and closes completing any outstanding requests
     * exceptionally.
     */
    private class InboundObserver implements StreamObserver<StateResponse> {
      @Override
      public void onNext(StateResponse value) {
        LOG.debug("Received StateResponse {}", value);
        CompletableFuture<StateResponse> responseFuture = outstandingRequests.remove(value.getId());
        if (responseFuture != null) {
          if (value.getError().isEmpty()) {
            responseFuture.complete(value);
          } else {
            responseFuture.completeExceptionally(new IllegalStateException(value.getError()));
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        closeAndCleanUp(t instanceof RuntimeException
            ? (RuntimeException) t
            : new RuntimeException(t));
      }

      @Override
      public void onCompleted() {
        closeAndCleanUp(new RuntimeException("Server hanged up."));
      }
    }
  }
}
