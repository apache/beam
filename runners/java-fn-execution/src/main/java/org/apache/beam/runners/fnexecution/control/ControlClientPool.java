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

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A pool of control clients that brokers incoming SDK harness connections (in the form of {@link
 * InstructionRequestHandler InstructionRequestHandlers}.
 *
 * <p>Incoming instruction handlers usually come from the control plane gRPC service. Typical use:
 *
 * <pre>
 *   // Within owner of the pool, who may or may not own the control plane server as well
 *   ControlClientPool pool = ...
 *   FnApiControlClientPoolService service =
 *       FnApiControlClientPoolService.offeringClientsToSink(pool.getSink(), headerAccessor)
 *   // Incoming gRPC control connections will now be added to the client pool.
 *
 *   // Within code that interacts with the instruction handler. The get call blocks until an
 *   // incoming client is available:
 *   ControlClientSource clientSource = ... InstructionRequestHandler
 *   instructionHandler = clientSource.get("worker-id");
 * </pre>
 *
 * <p>All {@link ControlClientPool} implementations must be thread-safe.
 */
@ThreadSafe
public interface ControlClientPool {

  /** Sink for control clients. */
  Sink getSink();

  /** Source of control clients. */
  Source getSource();

  /** A sink for {@link InstructionRequestHandler InstructionRequestHandlers} keyed by worker id. */
  @FunctionalInterface
  interface Sink {

    /**
     * Puts an {@link InstructionRequestHandler} into a client pool. Worker ids must be unique per
     * pool.
     */
    void put(String workerId, InstructionRequestHandler instructionHandler) throws Exception;
  }

  /** A source of {@link InstructionRequestHandler InstructionRequestHandlers}. */
  @FunctionalInterface
  interface Source {

    /**
     * Retrieves the {@link InstructionRequestHandler} for the given worker id, blocking until
     * available or the request times out. Worker ids must be unique per pool. A given worker id
     * must not be requested multiple times. Note that if the given worker id is never entered into
     * the pool, this call will never return.
     *
     * @throws TimeoutException if the request times out
     * @throws InterruptedException if interrupted while waiting
     */
    InstructionRequestHandler take(String workerId, Duration timeout) throws Exception;
  }
}
