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

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.stub.StreamObserver;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnDataGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.BeamFnDataBufferingOutboundObserver;
import org.apache.beam.sdk.fn.data.BeamFnDataGrpcMultiplexer;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.fn.data.InboundDataClient;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
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
public class GrpcDataService extends BeamFnDataGrpc.BeamFnDataImplBase
    implements FnService, FnDataService {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataService.class);

  public static GrpcDataService create(ExecutorService executor) {
    return new GrpcDataService(executor);
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

  private final ExecutorService executor;

  private GrpcDataService(ExecutorService executor) {
    this.connectedClient = SettableFuture.create();
    this.additionalMultiplexers = new LinkedBlockingQueue<>();
    this.executor = executor;
  }

  @Override
  public StreamObserver<BeamFnApi.Elements> data(
      final StreamObserver<BeamFnApi.Elements> outboundElementObserver) {
    LOG.info("Beam Fn Data client connected.");
    BeamFnDataGrpcMultiplexer multiplexer =
        new BeamFnDataGrpcMultiplexer(null, inboundObserver -> outboundElementObserver);
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
  public <T> InboundDataClient receive(
      final LogicalEndpoint inputLocation,
      Coder<WindowedValue<T>> coder,
      FnDataReceiver<WindowedValue<T>> listener) {
    LOG.debug(
        "Registering receiver for instruction {} and target {}",
        inputLocation.getInstructionId(),
        inputLocation.getTarget());
    final BeamFnDataInboundObserver<T> observer =
        BeamFnDataInboundObserver.forConsumer(coder, listener);
    if (connectedClient.isDone()) {
      try {
        connectedClient.get().registerConsumer(inputLocation, observer);
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
              connectedClient.get().registerConsumer(inputLocation, observer);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            } catch (ExecutionException e) {
              throw new RuntimeException(e.getCause());
            }
          });
    }
    return observer;
  }

  @Override
  public <T> CloseableFnDataReceiver<WindowedValue<T>> send(
      LogicalEndpoint outputLocation, Coder<WindowedValue<T>> coder) {
    LOG.debug(
        "Creating sender for instruction {} and target {}",
        outputLocation.getInstructionId(),
        outputLocation.getTarget());
    try {
      return BeamFnDataBufferingOutboundObserver.forLocation(
          outputLocation, coder, connectedClient.get(3, TimeUnit.MINUTES).getOutboundObserver());
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
