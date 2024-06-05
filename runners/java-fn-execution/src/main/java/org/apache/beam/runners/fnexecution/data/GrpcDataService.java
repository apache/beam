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
package org.apache.beam.runners.fnexecution.data;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.BeamFnDataOutboundAggregator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.sdk.fn.stream.OutboundObserverFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FnDataService} implemented via gRPC.
 *
 * <p>This service allows for multiple clients to transmit {@link BeamFnApi.Elements} messages.
 *
 * <p>This service transmits all outgoing {@link BeamFnApi.Elements} messages to the first client
 * that connects.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class GrpcDataService extends BeamFnDataGrpc.BeamFnDataImplBase
    implements FnService, FnDataService {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataService.class);

  public static GrpcDataService create(
      PipelineOptions options,
      ExecutorService executor,
      OutboundObserverFactory outboundObserverFactory) {
    return new GrpcDataService(options, executor, outboundObserverFactory);
  }

  private final SettableFuture<BeamFnDataGrpcMultiplexer> connectedClient;
  /**
   * A collection of multiplexers which are not used to send data. A handle to these multiplexers is
   * maintained in order to perform an orderly shutdown.
   *
   * <p>TODO: (BEAM-3811) Replace with some cancellable collection, to ensure that new clients of a
   * closed {@link GrpcDataService} are closed with that {@link GrpcDataService}.
   */
  private final Queue<BeamFnDataGrpcMultiplexer> additionalMultiplexers;

  private final PipelineOptions options;
  private final ExecutorService executor;
  private final OutboundObserverFactory outboundObserverFactory;

  private GrpcDataService(
      PipelineOptions options,
      ExecutorService executor,
      OutboundObserverFactory outboundObserverFactory) {
    this.connectedClient = SettableFuture.create();
    this.additionalMultiplexers = new LinkedBlockingQueue<>();
    this.options = options;
    this.executor = executor;
    this.outboundObserverFactory = outboundObserverFactory;
  }

  /** @deprecated This constructor is for migrating Dataflow purpose only. */
  @Deprecated
  public GrpcDataService() {
    this.connectedClient = null;
    this.additionalMultiplexers = null;
    this.options = null;
    this.executor = null;
    this.outboundObserverFactory = null;
  }

  @Override
  public StreamObserver<BeamFnApi.Elements> data(
      final StreamObserver<BeamFnApi.Elements> outboundElementObserver) {
    LOG.info("Beam Fn Data client connected.");
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(
            null, outboundObserverFactory, inbound -> outboundElementObserver);
    // First client that connects completes this future.
    if (!connectedClient.set(multiplexer)) {
      additionalMultiplexers.offer(multiplexer);
    }
    try {
      // We specifically return the connected clients inbound observer so that all
      // incoming messages are sent to the single multiplexer instance.
      return connectedClient.get().getInboundObserver();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    // Cancel anything blocking on a client connecting to this service. This doesn't shut down the
    // Multiplexer, but if there isn't any multiplexer it prevents callers blocking forever.
    connectedClient.cancel(true);
    // Close any other open connections
    for (BeamFnDataGrpcMultiplexer additional : additionalMultiplexers) {
      try {
        additional.close();
      } catch (Exception ignored) {
        // Shutdown remaining clients
      }
    }
    if (!connectedClient.isCancelled()) {
      connectedClient.get().close();
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void registerReceiver(String instructionId, CloseableFnDataReceiver<Elements> observer) {
    LOG.debug("Registering observer for instruction {}", instructionId);
    if (connectedClient.isDone()) {
      try {
        connectedClient.get().registerConsumer(instructionId, observer);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e.getCause());
      }
    } else {
      executor.submit(
          () -> {
            try {
              connectedClient.get().registerConsumer(instructionId, observer);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throw new RuntimeException(e.getCause());
            }
          });
    }
  }

  @Override
  public void unregisterReceiver(String instructionId) {
    try {
      connectedClient.get().unregisterConsumer(instructionId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public BeamFnDataOutboundAggregator createOutboundAggregator(
      Supplier<String> processBundleRequestIdSupplier, boolean collectElementsIfNoFlushes) {
    try {
      return new BeamFnDataOutboundAggregator(
          options,
          processBundleRequestIdSupplier,
          connectedClient.get(3, TimeUnit.MINUTES).getOutboundObserver(),
          collectElementsIfNoFlushes);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException("No client connected within timeout", e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
