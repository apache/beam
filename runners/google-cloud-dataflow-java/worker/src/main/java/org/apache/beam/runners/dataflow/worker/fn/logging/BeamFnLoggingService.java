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
package org.apache.beam.runners.dataflow.worker.fn.logging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.worker.fn.grpc.BeamFnService;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Server gRPC server} that fronts the Beam Fn Logging service.
 *
 * <p>This implementation handles multiples clients and forwards all received messages and emits
 * them to the provided {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry} {@link
 * Consumer}.
 */
public class BeamFnLoggingService extends BeamFnLoggingGrpc.BeamFnLoggingImplBase
    implements BeamFnService {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnLoggingService.class);
  private final Endpoints.ApiServiceDescriptor apiServiceDescriptor;
  private final Consumer<BeamFnApi.LogEntry> clientLogger;
  private final Function<StreamObserver<BeamFnApi.LogControl>, StreamObserver<BeamFnApi.LogControl>>
      streamObserverFactory;
  private final HeaderAccessor headerAccessor;
  private final ConcurrentMap<InboundObserver, StreamObserver<BeamFnApi.LogControl>>
      connectedClients;

  public BeamFnLoggingService(
      Endpoints.ApiServiceDescriptor apiServiceDescriptor,
      Consumer<BeamFnApi.LogEntry> clientLogger,
      Function<StreamObserver<BeamFnApi.LogControl>, StreamObserver<BeamFnApi.LogControl>>
          streamObserverFactory,
      HeaderAccessor headerAccessor)
      throws Exception {
    this.clientLogger = clientLogger;
    this.streamObserverFactory = streamObserverFactory;
    this.headerAccessor = headerAccessor;
    this.connectedClients = new ConcurrentHashMap<>();
    this.apiServiceDescriptor = apiServiceDescriptor;
    LOG.info("Launched Beam Fn Logging service {}", this.apiServiceDescriptor);
  }

  @Override
  public Endpoints.ApiServiceDescriptor getApiServiceDescriptor() {
    return apiServiceDescriptor;
  }

  @Override
  public void close() throws Exception {
    Set<InboundObserver> remainingClients = ImmutableSet.copyOf(connectedClients.keySet());
    if (!remainingClients.isEmpty()) {
      LOG.info(
          "{} Beam Fn Logging clients still connected during shutdown.", remainingClients.size());

      // Signal server shutting down to all remaining connected clients.
      for (InboundObserver client : remainingClients) {
        // We remove these from the connected clients map to prevent a race between
        // this close method and the InboundObserver calling a terminal method on the
        // StreamObserver. If we removed it, then we are responsible for the terminal call.
        completeIfNotNull(connectedClients.remove(client));
      }
    }
  }

  @Override
  public StreamObserver<BeamFnApi.LogEntry.List> logging(
      StreamObserver<BeamFnApi.LogControl> outboundObserver) {
    LOG.info("Beam Fn Logging client connected for client {}", headerAccessor.getSdkWorkerId());
    InboundObserver inboundObserver = new InboundObserver(headerAccessor.getSdkWorkerId());
    connectedClients.put(inboundObserver, streamObserverFactory.apply(outboundObserver));
    return inboundObserver;
  }

  private void completeIfNotNull(StreamObserver<BeamFnApi.LogControl> outboundObserver) {
    if (outboundObserver != null) {
      try {
        outboundObserver.onCompleted();
      } catch (RuntimeException ignored) {
        // Completing outbound observer failed, ignoring failure and continuing
        LOG.warn("Beam Fn Logging client failed to be complete.", ignored);
      }
    }
  }

  /**
   * An inbound {@link StreamObserver} that forwards incoming messages to the client logger.
   *
   * <p>Maintains the set of connected clients. Mutually hangs up on clients that have errored or
   * completed.
   */
  private class InboundObserver implements StreamObserver<BeamFnApi.LogEntry.List> {
    private final String sdkWorkerId;

    InboundObserver(String sdkWorkerId) {
      this.sdkWorkerId = sdkWorkerId;
    }

    @Override
    public void onNext(BeamFnApi.LogEntry.List value) {
      DataflowWorkerLoggingMDC.setSdkHarnessId(sdkWorkerId);
      for (BeamFnApi.LogEntry logEntry : value.getLogEntriesList()) {
        clientLogger.accept(logEntry);
      }
      DataflowWorkerLoggingMDC.setSdkHarnessId(null);
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Logging client failed unexpectedly. ClientId: {}", t, sdkWorkerId);
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));
    }

    @Override
    public void onCompleted() {
      LOG.info("Logging client hanged up. ClientId: {}", sdkWorkerId);
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));
    }
  }
}
