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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.dataflow.worker.fn.data.BeamFnDataGrpcService;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory to create SdkHarnessRegistry */
public class SdkHarnessRegistries {

  /** Create a registry which does not require sdkHarness registration for non fnapi worker. */
  public static SdkHarnessRegistry emptySdkHarnessRegistry() {
    return new EmptySdkHarnessRegistry();
  }

  /** Create a registry for fnapi worker. */
  public static SdkHarnessRegistry createFnApiSdkHarnessRegistry(
      ApiServiceDescriptor stateApiServiceDescriptor,
      GrpcStateService beamFnStateService,
      BeamFnDataGrpcService beamFnDataGrpcService) {
    return new WorkBalancingSdkHarnessRegistry(
        stateApiServiceDescriptor, beamFnStateService, beamFnDataGrpcService);
  }

  /** Registry used to manage all the connections (Control, Data, State) from SdkHarness */
  public static class WorkBalancingSdkHarnessRegistry implements SdkHarnessRegistry {
    private static final Logger LOG =
        LoggerFactory.getLogger(WorkBalancingSdkHarnessRegistry.class);
    private final ApiServiceDescriptor stateApiServiceDescriptor;
    private final GrpcStateService beamFnStateService;
    private final BeamFnDataGrpcService beamFnDataGrpcService;
    private final ConcurrentHashMap<FnApiControlClient, WorkCountingSdkWorkerHarness> workerMap =
        new ConcurrentHashMap<>();
    private final AtomicBoolean sdkHarnessesAreHealthy = new AtomicBoolean(true);

    private final PriorityBlockingQueue<WorkCountingSdkWorkerHarness> workers =
        new PriorityBlockingQueue<>(
            1,
            /* Prioritize the worker with least work */
            Comparator.comparingInt(o -> o.assignedWorkCount.get()));

    /** Create a registry for fnapi worker. */
    private WorkBalancingSdkHarnessRegistry(
        ApiServiceDescriptor stateApiServiceDescriptor,
        GrpcStateService beamFnStateService,
        BeamFnDataGrpcService beamFnDataGrpcService) {
      Preconditions.checkNotNull(beamFnStateService, "StateService can not be null.");
      Preconditions.checkNotNull(beamFnDataGrpcService, "DataService can not be null.");
      this.stateApiServiceDescriptor = stateApiServiceDescriptor;
      this.beamFnStateService = beamFnStateService;
      this.beamFnDataGrpcService = beamFnDataGrpcService;
    }

    private boolean validateAndCleanWorker(WorkCountingSdkWorkerHarness worker) {
      // Recheck and remove worker as it could have been closed by the unregister.
      if (worker.closed.get()) {
        workers.remove(worker);
        return false;
      }
      return true;
    }

    @Override
    public void registerWorkerClient(@Nullable FnApiControlClient controlClient) {
      controlClient = checkArgumentNotNull(controlClient);
      WorkCountingSdkWorkerHarness sdkWorkerHarness =
          new WorkCountingSdkWorkerHarness(controlClient);
      workerMap.put(controlClient, sdkWorkerHarness);
      workers.add(sdkWorkerHarness);
      LOG.info("Registered Control client {}", sdkWorkerHarness.getWorkerId());
    }

    @Override
    public void unregisterWorkerClient(FnApiControlClient controlClient) {
      // Find the worker, mark the worker closed and remove it.
      WorkCountingSdkWorkerHarness worker = workerMap.remove(controlClient);

      if (worker != null) {
        worker.closed.set(true);
        workers.remove(worker);
      }
      LOG.info("Unregistered Control client {}", worker != null ? worker.getWorkerId() : null);

      // unregisterWorkerClient() will be called only when the connection between SDK harness and
      // runner harness is broken or SDK harness respond to runner harness with an error. In either
      // case, the SDK should be marked as unhealthy.
      sdkHarnessesAreHealthy.set(false);
      LOG.info("SDK harness {} became unhealthy", worker != null ? worker.getWorkerId() : null);
    }

    @Override
    public boolean sdkHarnessesAreHealthy() {
      return sdkHarnessesAreHealthy.get();
    }

    /* Any modification to workers has race condition with unregisterWorkerClient. To resolve this
    we recheck worker state after picking a worker and clean the worker if needed.*/
    @Override
    public WorkCountingSdkWorkerHarness getAvailableWorkerAndAssignWork() {
      // Pick the client with least pending workItems.
      try {
        // Remove and re add the worker after incrementing the assignedWorkCount.
        // If the worker is closed then remove the worker and try to get next available worker.
        WorkCountingSdkWorkerHarness worker;
        do {
          worker = workers.take();
          worker.assignedWorkCount.incrementAndGet();
          // Put back the worker in the queue.
          workers.add(worker);
        } while (!validateAndCleanWorker(worker));
        return worker;
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting to get an available worker.");
        return null;
      }
    }

    @Override
    public void completeWork(SdkWorkerHarness worker) {
      // Remove worker -> decrement the workCount -> put the worker back -> Remove the worker if
      // worker is closed.
      if (!(worker instanceof WorkCountingSdkWorkerHarness)) {
        throw new IllegalArgumentException(
            String.format(
                "Worker should be of type %s. Found worker type %s",
                WorkCountingSdkWorkerHarness.class, worker.getClass()));
      }
      WorkCountingSdkWorkerHarness actualWorker = (WorkCountingSdkWorkerHarness) worker;
      if (workers.remove(actualWorker)) {
        actualWorker.assignedWorkCount.decrementAndGet();
        workers.add(actualWorker);
        // Recheck and remove worker as it could have been closed by the unregister.
        validateAndCleanWorker(actualWorker);
      }
    }

    @Override
    public @Nullable ApiServiceDescriptor beamFnStateApiServiceDescriptor() {
      return stateApiServiceDescriptor;
    }

    @Override
    public @Nullable ApiServiceDescriptor beamFnDataApiServiceDescriptor() {
      return beamFnDataGrpcService.getApiServiceDescriptor();
    }

    /** Class to keep client and associated data */
    public class WorkCountingSdkWorkerHarness implements SdkWorkerHarness {
      private final FnApiControlClient controlClientHandler;
      private final AtomicInteger assignedWorkCount;
      private final AtomicBoolean closed;

      private WorkCountingSdkWorkerHarness(FnApiControlClient controlClientHandler) {
        this.controlClientHandler = controlClientHandler;
        this.assignedWorkCount = new AtomicInteger(0);
        this.closed = new AtomicBoolean(false);
      }

      @Override
      public @Nullable FnApiControlClient getControlClientHandler() {
        return controlClientHandler;
      }

      @Override
      public @Nullable String getWorkerId() {
        return controlClientHandler.getWorkerId();
      }

      @Override
      public @Nullable GrpcFnServer<GrpcDataService> getGrpcDataFnServer() {
        return GrpcFnServer.create(
            beamFnDataGrpcService.getDataService(getWorkerId()), beamFnDataApiServiceDescriptor());
      }

      @Override
      public @Nullable GrpcFnServer<GrpcStateService> getGrpcStateFnServer() {
        return GrpcFnServer.create(beamFnStateService, beamFnDataApiServiceDescriptor());
      }
    }
  }

  /**
   * SdkHarnessRegistry which does not maintain any state and does not support {@link
   * EmptySdkHarnessRegistry#registerWorkerClient(FnApiControlClient)} and {@link
   * EmptySdkHarnessRegistry#unregisterWorkerClient(FnApiControlClient)}.
   *
   * <p>EmptySdkHarnessRegistry should be removed when we untangle the fnapi and non fnapi code from
   * {@link StreamingDataflowWorker}, {@link BatchDataflowWorker} and {@link DataflowRunnerHarness}.
   * The current implementation has to return null at multiple places in order to support the
   * tangled code for fnapi and non fnapi.
   */
  @Deprecated
  public static class EmptySdkHarnessRegistry implements SdkHarnessRegistry {

    private final SdkWorkerHarness sdkWorkerHarness =
        new SdkWorkerHarness() {
          @Nullable
          @Override
          public FnApiControlClient getControlClientHandler() {
            return null;
          }

          @Nullable
          @Override
          public String getWorkerId() {
            return null;
          }

          @Nullable
          @Override
          public GrpcFnServer<GrpcDataService> getGrpcDataFnServer() {
            return null;
          }

          @Nullable
          @Override
          public GrpcFnServer<GrpcStateService> getGrpcStateFnServer() {
            return null;
          }
        };

    @Override
    public void registerWorkerClient(@Nullable FnApiControlClient controlClient) {
      throw new UnsupportedOperationException(
          "EmptySdkHarnessRegistry does not support this operation");
    }

    @Override
    public void unregisterWorkerClient(FnApiControlClient controlClient) {
      throw new UnsupportedOperationException(
          "EmptySdkHarnessRegistry does not support this operation");
    }

    @Override
    public boolean sdkHarnessesAreHealthy() {
      return true;
    }

    @Override
    public SdkWorkerHarness getAvailableWorkerAndAssignWork() {
      return sdkWorkerHarness;
    }

    @Override
    public void completeWork(SdkWorkerHarness worker) {}

    @Nullable
    @Override
    public ApiServiceDescriptor beamFnStateApiServiceDescriptor() {
      return null;
    }

    @Nullable
    @Override
    public ApiServiceDescriptor beamFnDataApiServiceDescriptor() {
      return null;
    }
  }
}
