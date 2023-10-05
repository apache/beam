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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/**
 * A {@link ControlClientPool} backed by a client map. It is expected that a given client id will be
 * requested at most once.
 */
public class MapControlClientPool implements ControlClientPool {

  /** Creates a {@link MapControlClientPool}. */
  public static MapControlClientPool create() {
    return new MapControlClientPool();
  }

  private final Map<String, CompletableFuture<InstructionRequestHandler>> clients =
      Maps.newConcurrentMap();

  private MapControlClientPool() {}

  @Override
  public Source getSource() {
    return this::getClient;
  }

  @Override
  public Sink getSink() {
    return this::putClient;
  }

  private void putClient(String workerId, InstructionRequestHandler client) {
    CompletableFuture<InstructionRequestHandler> future =
        clients.computeIfAbsent(workerId, MapControlClientPool::createClientFuture);
    boolean success = future.complete(client);
    if (!success) {
      throw new IllegalStateException(
          String.format("Control client for worker id %s failed to compete", workerId));
    }
  }

  private InstructionRequestHandler getClient(String workerId, Duration timeout)
      throws ExecutionException, InterruptedException, TimeoutException {
    CompletableFuture<InstructionRequestHandler> future =
        clients.computeIfAbsent(workerId, MapControlClientPool::createClientFuture);
    // TODO: Wire in health checking of clients so requests don't hang.
    future.get(timeout.getSeconds(), TimeUnit.SECONDS);
    InstructionRequestHandler client = future.get();
    clients.remove(workerId);
    return client;
  }

  private static CompletableFuture<InstructionRequestHandler> createClientFuture(String unused) {
    return new CompletableFuture<>();
  }
}
