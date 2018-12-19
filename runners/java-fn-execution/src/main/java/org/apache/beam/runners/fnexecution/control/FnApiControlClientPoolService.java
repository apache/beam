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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Fn API control service which adds incoming SDK harness connections to a sink. */
public class FnApiControlClientPoolService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements FnService {
  private static final Logger LOGGER = LoggerFactory.getLogger(FnApiControlClientPoolService.class);

  private final Object lock = new Object();
  private final ControlClientPool.Sink clientSink;
  private final HeaderAccessor headerAccessor;

  @GuardedBy("lock")
  private final Collection<FnApiControlClient> vendedClients = new ArrayList<>();

  @GuardedBy("lock")
  private boolean closed = false;

  private FnApiControlClientPoolService(
      ControlClientPool.Sink clientSink, HeaderAccessor headerAccessor) {
    this.clientSink = clientSink;
    this.headerAccessor = headerAccessor;
  }

  /**
   * Creates a new {@link FnApiControlClientPoolService} which will enqueue and vend new SDK harness
   * connections.
   *
   * <p>Clients placed into the {@code clientSink} are owned by whoever consumes them from the other
   * end of the pool. That consumer is responsible for closing the clients when they are no longer
   * needed.
   */
  public static FnApiControlClientPoolService offeringClientsToPool(
      ControlClientPool.Sink clientPool, HeaderAccessor headerAccessor) {
    return new FnApiControlClientPoolService(clientPool, headerAccessor);
  }

  /**
   * Called by gRPC for each incoming connection from an SDK harness, and enqueue an available SDK
   * harness client.
   *
   * <p>Note: currently does not distinguish what sort of SDK it is, so a separate instance is
   * required for each.
   */
  @Override
  public StreamObserver<BeamFnApi.InstructionResponse> control(
      StreamObserver<BeamFnApi.InstructionRequest> requestObserver) {
    final String workerId = headerAccessor.getSdkWorkerId();
    if (Strings.isNullOrEmpty(workerId)) {
      // TODO(BEAM-4149): Enforce proper worker id.
      LOGGER.warn("No worker_id header provided in control request");
    }

    LOGGER.info("Beam Fn Control client connected with id {}", workerId);
    FnApiControlClient newClient = FnApiControlClient.forRequestObserver(workerId, requestObserver);
    try {
      // Add the client to the pool of vended clients before making it available - we should close
      // the client when we close even if no one has picked it up yet. This can occur after the
      // service is closed, in which case the client will be discarded when the service is
      // discarded, which should be performed by a call to #shutdownNow. The remote caller must be
      // able to handle an unexpectedly terminated connection.
      synchronized (lock) {
        checkState(
            !closed, "%s already closed", FnApiControlClientPoolService.class.getSimpleName());
        // TODO: https://issues.apache.org/jira/browse/BEAM-4151: Prevent stale client references
        // from leaking.
        vendedClients.add(newClient);
      }
      // We do not attempt to transactionally add the client to our internal list and offer it to
      // the sink.
      clientSink.put(headerAccessor.getSdkWorkerId(), newClient);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return newClient.asResponseObserver();
  }

  @Override
  public void close() {
    synchronized (lock) {
      if (!closed) {
        closed = true;
        for (FnApiControlClient vended : vendedClients) {
          vended.close();
        }
      }
    }
  }
}
