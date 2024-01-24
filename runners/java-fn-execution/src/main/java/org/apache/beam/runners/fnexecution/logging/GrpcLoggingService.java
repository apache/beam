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
package org.apache.beam.runners.fnexecution.logging;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the Beam Fn Logging Service over gRPC. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class GrpcLoggingService extends BeamFnLoggingGrpc.BeamFnLoggingImplBase
    implements FnService {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcLoggingService.class);

  public static GrpcLoggingService forWriter(LogWriter writer) {
    return new GrpcLoggingService(writer);
  }

  private final LogWriter logWriter;
  private final ConcurrentMap<InboundObserver, StreamObserver<LogControl>> connectedClients;

  private GrpcLoggingService(LogWriter logWriter) {
    this.logWriter = logWriter;
    connectedClients = new ConcurrentHashMap<>();
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
    LOG.info("Beam Fn Logging client connected.");
    InboundObserver inboundObserver = new InboundObserver();
    connectedClients.put(inboundObserver, outboundObserver);
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
   * <p>Mutually hangs up on clients that have errored or completed.
   */
  private class InboundObserver implements StreamObserver<BeamFnApi.LogEntry.List> {
    @Override
    public void onNext(BeamFnApi.LogEntry.List value) {
      for (BeamFnApi.LogEntry logEntry : value.getLogEntriesList()) {
        logWriter.log(logEntry);
      }
    }

    @Override
    public void onError(Throwable t) {
      LOG.warn("Logging client failed unexpectedly.", t);
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));
    }

    @Override
    public void onCompleted() {
      LOG.info("Logging client hanged up.");
      // We remove these from the connected clients map to prevent a race between
      // the close method and this InboundObserver calling a terminal method on the
      // StreamObserver. If we removed it, then we are responsible for the terminal call.
      completeIfNotNull(connectedClients.remove(this));
    }
  }
}
