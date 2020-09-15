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
package org.apache.beam.runners.dataflow.worker.fn.data;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.worker.fn.grpc.BeamFnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataBufferingOutboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the Beam Fn Data service.
 *
 * <p>This service allows for multiple clients to transmit {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements} messages.
 *
 * <p>This service transmits all outgoing {@link
 * org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements} messages to the first client that
 * connects.
 */
public class BeamFnDataGrpcService extends BeamFnDataGrpc.BeamFnDataImplBase
    implements BeamFnService {

  private static final Logger LOG = LoggerFactory.getLogger(BeamFnDataGrpcService.class);
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final ConcurrentMap<String, CompletableFuture<BeamFnDataGrpcMultiplexer>>
      connectedClients;

  private final PipelineOptions options;
  private final Function<StreamObserver<BeamFnApi.Elements>, StreamObserver<BeamFnApi.Elements>>
      streamObserverFactory;
  private final HeaderAccessor headerAccessor;

  public BeamFnDataGrpcService(
      PipelineOptions options,
      Endpoints.ApiServiceDescriptor descriptor,
      Function<StreamObserver<Elements>, StreamObserver<Elements>> streamObserverFactory,
      HeaderAccessor headerAccessor) {
    this.options = options;
    this.streamObserverFactory = streamObserverFactory;
    this.headerAccessor = headerAccessor;
    this.connectedClients = new ConcurrentHashMap<>();
    this.apiServiceDescriptor = descriptor;
    LOG.info("Launched Beam Fn Data service {}", this.apiServiceDescriptor);
  }

  @Override
  public Endpoints.ApiServiceDescriptor getApiServiceDescriptor() {
    return apiServiceDescriptor;
  }

  @Override
  public void close() throws Exception {
    // TODO: Track multiple clients and disconnect them cleanly instead of forcing termination
  }

  // TODO: Remove this class once CompletableFutureInboundDataClient allows you access to the
  // future or once the FnDataService splits the InboundDataClient into two separate components,
  // one for reading data and one for waiting/cancellation.
  private class DeferredInboundDataClient<T> implements InboundDataClient {
    private final CompletableFuture<BeamFnDataInboundObserver> future;

    private DeferredInboundDataClient(
        String clientId,
        LogicalEndpoint inputLocation,
        Coder<T> coder,
        FnDataReceiver<T> consumer) {
      this.future =
          getClientFuture(clientId)
              .thenCompose(
                  beamFnDataGrpcMultiplexer -> {
                    BeamFnDataInboundObserver<T> inboundObserver =
                        BeamFnDataInboundObserver.forConsumer(inputLocation, coder, consumer);
                    beamFnDataGrpcMultiplexer.registerConsumer(inputLocation, inboundObserver);
                    return CompletableFuture.completedFuture(inboundObserver);
                  });
    }

    @Override
    public void awaitCompletion() throws Exception {
      future.get().awaitCompletion();
    }

    @Override
    public boolean isDone() {
      if (!future.isDone()) {
        return false;
      }
      try {
        return future.get().isDone();
      } catch (CancellationException | ExecutionException e) {
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    @Override
    public void cancel() {
      // Attempt to cancel the registration future. This prevents the construction
      // of the inbound data client and its registration.
      if (future.cancel(true)) {
        return;
      } else {
        try {
          future.get().cancel();
        } catch (CancellationException | ExecutionException e) {
          // Specifically ignore exceptions related to the future being cancelled/failed already
          // since it already is in a terminal state.
        } catch (InterruptedException e) {
          // This is unlikely to happen due to the fact that the future should have been
          // cancelled which would have transitioned it to a done state.
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public void complete() {
      try {
        future.get().complete();
      } catch (CancellationException | ExecutionException e) {
        // Specifically ignore exceptions related to the future being cancelled/failed already
        // since it already is in a terminal state.
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void fail(Throwable t) {
      // Attempt to complete exceptionally the registration future. This prevents the construction
      // of the inbound data client and its registration.
      if (future.completeExceptionally(t)) {
        return;
      } else {
        try {
          future.get().fail(t);
        } catch (CancellationException | ExecutionException e) {
          // Specifically ignore exceptions related to the future being cancelled/failed already
          // since it already is in a terminal state.
        } catch (InterruptedException e) {
          // This is unlikely to happen due to the fact that the future should have been
          // completed exceptionally which would have made sure that it became done.
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Get the anonymous subclass of GrpcDataService for the clientId */
  public GrpcDataService getDataService(final String clientId) {
    return new GrpcDataService() {
      @Override
      public <T> InboundDataClient receive(
          LogicalEndpoint inputLocation, Coder<T> coder, FnDataReceiver<T> consumer) {
        LOG.debug("Registering consumer for {}", inputLocation);

        return new DeferredInboundDataClient(clientId, inputLocation, coder, consumer);
      }

      @Override
      public <T> CloseableFnDataReceiver<T> send(LogicalEndpoint outputLocation, Coder<T> coder) {
        LOG.debug("Creating output consumer for {}", outputLocation);
        try {
          return BeamFnDataBufferingOutboundObserver.forLocation(
              options,
              outputLocation,
              coder,
              getClientFuture(clientId).get().getOutboundObserver());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      /** It is intended to do nothing in close. */
      @Override
      public void close() throws Exception {}
    };
  }

  @Override
  public StreamObserver<Elements> data(final StreamObserver<Elements> outboundObserver) {
    String sdkWorkerId = headerAccessor.getSdkWorkerId();
    LOG.info("Beam Fn Data client connected for clientId {}", sdkWorkerId);
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            apiServiceDescriptor,
            OutboundObserverFactory.trivial(),
            (StreamObserver<BeamFnApi.Elements> inboundObserver) ->
                streamObserverFactory.apply(outboundObserver));
    // First client that connects completes this future
    getClientFuture(sdkWorkerId).complete(multiplexer);
    try {
      // We specifically return the connected clients inbound observer so that all
      // incoming messages are sent to the single multiplexer instance.
      return getClientFuture(sdkWorkerId).get().getInboundObserver();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private CompletableFuture<BeamFnDataGrpcMultiplexer> getClientFuture(String sdkWorkerId) {
    Preconditions.checkNotNull(sdkWorkerId);
    return connectedClients.computeIfAbsent(sdkWorkerId, clientId -> new CompletableFuture<>());
  }
}
