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

import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.runners.fnexecution.FnService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Fn API control service which adds incoming SDK harness connections to a pool. */
public class FnApiControlClientPoolService extends BeamFnControlGrpc.BeamFnControlImplBase
    implements FnService {
  private static final Logger LOGGER = LoggerFactory.getLogger(FnApiControlClientPoolService.class);

  private final BlockingQueue<FnApiControlClient> clientPool;
  private final Collection<FnApiControlClient> vendedClients = new CopyOnWriteArrayList<>();
  private AtomicBoolean closed = new AtomicBoolean();

  private FnApiControlClientPoolService(BlockingQueue<FnApiControlClient> clientPool) {
    this.clientPool = clientPool;
  }

  /**
   * Creates a new {@link FnApiControlClientPoolService} which will enqueue and vend new SDK harness
   * connections.
   *
   * <p>Clients placed into the {@code clientPool} are owned by whichever consumer owns the pool.
   * That consumer is responsible for closing the clients when they are no longer needed.
   */
  public static FnApiControlClientPoolService offeringClientsToPool(
      BlockingQueue<FnApiControlClient> clientPool) {
    return new FnApiControlClientPoolService(clientPool);
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
    LOGGER.info("Beam Fn Control client connected.");
    FnApiControlClient newClient = FnApiControlClient.forRequestObserver(requestObserver);
    try {
      // Add the client to the pool of vended clients before making it available - we should close
      // the client when we close even if no one has picked it up yet. This can occur after the
      // service is closed, in which case the client will be discarded when the service is
      // discarded, which should be performed by a call to #shutdownNow. The remote caller must be
      // able to handle an unexpectedly terminated connection.
      vendedClients.add(newClient);
      clientPool.put(newClient);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    return newClient.asResponseObserver();
  }

  @Override
  public void close() {
    if (!closed.getAndSet(true)) {
      for (FnApiControlClient vended : vendedClients) {
        vended.close();
      }
    }
  }
}
