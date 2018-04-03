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

import io.grpc.ServerServiceDefinition;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import org.apache.beam.runners.fnexecution.FnService;
import org.apache.beam.runners.fnexecution.HeaderAccessor;
import org.apache.beam.runners.fnexecution.data.FnDataService;

/**
 * A service providing {@link SdkHarnessClient} based on an internally managed {@link
 * FnApiControlClientPoolService}.
 */
public class SdkHarnessClientControlService implements FnService {
  private final FnApiControlClientPoolService clientPoolService;
  private final ControlClientPool<FnApiControlClient> pendingClients;

  private final Supplier<FnDataService> dataService;

  private final Collection<SdkHarnessClient> activeClients;

  public static SdkHarnessClientControlService create(
      Supplier<FnDataService> dataService, HeaderAccessor headerAccessor) {
    return new SdkHarnessClientControlService(dataService, headerAccessor);
  }

  private SdkHarnessClientControlService(
      Supplier<FnDataService> dataService, HeaderAccessor headerAccessor) {
    this.dataService = dataService;
    activeClients = new ConcurrentLinkedQueue<>();
    pendingClients = QueueControlClientPool.createSynchronous();
    clientPoolService =
        FnApiControlClientPoolService.offeringClientsToPool(pendingClients.getSink(),
            headerAccessor);
  }

  public SdkHarnessClient getClient() {
    try {
      // Block until a client is available.
      FnApiControlClient getClient = pendingClients.getSource().get();
      return SdkHarnessClient.usingFnApiClient(getClient, dataService.get());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while waiting for client", e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    for (SdkHarnessClient client : activeClients) {
      client.close();
    }
  }

  @Override
  public ServerServiceDefinition bindService() {
    return clientPoolService.bindService();
  }
}
