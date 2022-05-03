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

import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Factory to create SdkHarnessRegistry */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class SdkHarnessRegistries {

  /** Create a registry which does not require sdkHarness registration for non fnapi worker. */
  public static SdkHarnessRegistry emptySdkHarnessRegistry() {
    return new EmptySdkHarnessRegistry();
  }

  /** Create a registry for fnapi worker. */
  public static SdkHarnessRegistry createFnApiSdkHarnessRegistry() {
    return emptySdkHarnessRegistry();
  }

  /**
   * SdkHarnessRegistry which does not maintain any state and does not support {@link
   * EmptySdkHarnessRegistry#registerWorkerClient(FnApiControlClient)} and {@link
   * EmptySdkHarnessRegistry#unregisterWorkerClient(FnApiControlClient)}.
   *
   * <p>EmptySdkHarnessRegistry should be removed when we untangle the fnapi and non fnapi code from
   * {@link StreamingDataflowWorker} and {@link BatchDataflowWorker}. The current implementation has
   * to return null at multiple places in order to support the tangled code for fnapi and non fnapi.
   */
  @Deprecated
  public static class EmptySdkHarnessRegistry implements SdkHarnessRegistry {

    private final SdkWorkerHarness sdkWorkerHarness =
        new SdkWorkerHarness() {
          @Override
          public @Nullable FnApiControlClient getControlClientHandler() {
            return null;
          }

          @Override
          public @Nullable String getWorkerId() {
            return null;
          }

          @Override
          public @Nullable GrpcFnServer<GrpcDataService> getGrpcDataFnServer() {
            return null;
          }

          @Override
          public @Nullable GrpcFnServer<GrpcStateService> getGrpcStateFnServer() {
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

    @Override
    public @Nullable ApiServiceDescriptor beamFnStateApiServiceDescriptor() {
      return null;
    }

    @Override
    public @Nullable ApiServiceDescriptor beamFnDataApiServiceDescriptor() {
      return null;
    }
  }
}
