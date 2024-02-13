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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnStateGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache of {@link BeamFnStateClient}s which handle Beam Fn State requests using gRPC.
 *
 * <p>TODO: Add the ability to close which cancels any pending and stops any future requests.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamFnStateGrpcClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnStateGrpcClientCache.class);

  private final Map<ApiServiceDescriptor, BeamFnStateClient> cache;
  private final ManagedChannelFactory channelFactory;
  private final OutboundObserverFactory outboundObserverFactory;
  private final IdGenerator idGenerator;

  public BeamFnStateGrpcClientCache(
      IdGenerator idGenerator,
      ManagedChannelFactory channelFactory,
      OutboundObserverFactory outboundObserverFactory) {
    this.idGenerator = idGenerator;
    // We use the directExecutor because we just complete futures when handling responses.
    // This showed a 1-2% improvement in the ProcessBundleBenchmark#testState* benchmarks.
    this.channelFactory = channelFactory.withDirectExecutor();
    this.outboundObserverFactory = outboundObserverFactory;
    this.cache = new HashMap<>();
  }

  /**
   * Creates or returns an existing {@link BeamFnStateClient} depending on whether the passed in
   * {@link ApiServiceDescriptor} currently has a {@link BeamFnStateClient} bound to the same
   * channel.
   */
  public synchronized BeamFnStateClient forApiServiceDescriptor(
      ApiServiceDescriptor apiServiceDescriptor) throws IOException {
    // We specifically are synchronized so that we only create one GrpcStateClient at a time
    // preventing a race where multiple GrpcStateClient objects might be constructed at the same
    // for the same ApiServiceDescriptor.
    BeamFnStateClient rval;
    synchronized (cache) {
      rval = cache.get(apiServiceDescriptor);
    }
    if (rval == null) {
      // We can't be synchronized on cache while constructing the GrpcStateClient since if the
      // connection fails, onError may be invoked from the gRPC thread which will invoke
      // closeAndCleanUp that clears the cache.
      rval = new GrpcStateClient(apiServiceDescriptor);
      synchronized (cache) {
        cache.put(apiServiceDescriptor, rval);
      }
    }
    return rval;
  }

  /** A {@link BeamFnStateClient} for a given {@link ApiServiceDescriptor}. */
  private class GrpcStateClient implements BeamFnStateClient {
    private final Object lock = new Object();
    private final ApiServiceDescriptor apiServiceDescriptor;
    private final Map<String, CompletableFuture<StateResponse>> outstandingRequests;
    private final StreamObserver<StateRequest> outboundObserver;
    private final ManagedChannel channel;
    private RuntimeException closed;
    private boolean errorDuringConstruction;

    private GrpcStateClient(ApiServiceDescriptor apiServiceDescriptor) {
      this.apiServiceDescriptor = apiServiceDescriptor;
      this.outstandingRequests = new HashMap<>();
      this.channel = channelFactory.forDescriptor(apiServiceDescriptor);
      this.errorDuringConstruction = false;
      this.outboundObserver =
          outboundObserverFactory.outboundObserverFor(
              BeamFnStateGrpc.newStub(channel)::state, new InboundObserver());
      // Due to safe object publishing, the InboundObserver may invoke closeAndCleanUp before this
      // constructor completes. In that case there is a race where outboundObserver may have not
      // been initialized and hence we invoke onCompleted here.
      synchronized (lock) {
        if (errorDuringConstruction) {
          outboundObserver.onCompleted();
        }
      }
    }

    @Override
    public CompletableFuture<StateResponse> handle(StateRequest.Builder requestBuilder) {
      requestBuilder.setId(idGenerator.getId());
      StateRequest request = requestBuilder.build();
      CompletableFuture<StateResponse> response = new CompletableFuture<>();
      synchronized (lock) {
        if (closed != null) {
          response.completeExceptionally(closed);
          return response;
        }
        outstandingRequests.put(request.getId(), response);
      }

      // If the server closes, gRPC will throw an error if onNext is called.
      LOG.debug("Sending StateRequest {}", request);
      outboundObserver.onNext(request);
      return response;
    }

    private void closeAndCleanUp(RuntimeException cause) {
      synchronized (lock) {
        if (closed != null) {
          return;
        }
        closed = cause;

        synchronized (cache) {
          cache.remove(apiServiceDescriptor);
        }

        if (!outstandingRequests.isEmpty()) {
          LOG.error("BeamFnState failed, clearing outstanding requests {}", outstandingRequests);
          for (CompletableFuture<StateResponse> entry : outstandingRequests.values()) {
            entry.completeExceptionally(cause);
          }
          outstandingRequests.clear();
        }

        // Due to safe object publishing, outboundObserver may be null since InboundObserver may
        // call closeAndCleanUp before the GrpcStateClient finishes construction. In this case
        // we defer invoking onCompleted to the GrpcStateClient constructor.
        if (outboundObserver == null) {
          errorDuringConstruction = true;
        } else {
          outboundObserver.onCompleted();
        }
      }
    }

    /**
     * A {@link StreamObserver} which propagates any server side state request responses by
     * completing the outstanding response future.
     *
     * <p>Also propagates server side failures and closes completing any outstanding requests
     * exceptionally.
     *
     * <p>This implementation must never block since we use a direct executor.
     */
    private class InboundObserver implements StreamObserver<StateResponse> {
      @Override
      public void onNext(StateResponse value) {
        LOG.debug("Received StateResponse {}", value);
        CompletableFuture<StateResponse> responseFuture;
        synchronized (lock) {
          responseFuture = outstandingRequests.remove(value.getId());
        }
        if (responseFuture == null) {
          LOG.warn("Dropped unknown StateResponse {}", value);
          return;
        }
        if (value.getError().isEmpty()) {
          responseFuture.complete(value);
        } else {
          responseFuture.completeExceptionally(new IllegalStateException(value.getError()));
        }
      }

      @Override
      public void onError(Throwable t) {
        closeAndCleanUp(
            t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t));
      }

      @Override
      public void onCompleted() {
        closeAndCleanUp(new RuntimeException("Server hanged up."));
      }
    }
  }
}
