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

import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.FnApiControlClient;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;

/** Registry used to manage all the connections (Control, Data, State) from SdkHarness */
public interface SdkHarnessRegistry {
  /**
   * Register the {@link FnApiControlClient} to allocate work to the client
   *
   * @param controlClient
   */
  void registerWorkerClient(@Nullable FnApiControlClient controlClient);

  /**
   * Unregister the {@link FnApiControlClient} to stop allocating work to the client
   *
   * @param controlClient
   */
  void unregisterWorkerClient(FnApiControlClient controlClient);

  /** Returns true if all of the registered SDK harnesses are healthy. */
  boolean sdkHarnessesAreHealthy();

  /** Find the available worker and assign work to it or wait till a worker becomes available */
  SdkWorkerHarness getAvailableWorkerAndAssignWork();

  void completeWork(SdkWorkerHarness worker);

  @Nullable
  ApiServiceDescriptor beamFnStateApiServiceDescriptor();

  @Nullable
  ApiServiceDescriptor beamFnDataApiServiceDescriptor();

  /** Class to keep client and associated data */
  interface SdkWorkerHarness {

    @Nullable
    public FnApiControlClient getControlClientHandler();

    @Nullable
    public String getWorkerId();

    @Nullable
    public GrpcFnServer<GrpcDataService> getGrpcDataFnServer();

    @Nullable
    public GrpcFnServer<GrpcStateService> getGrpcStateFnServer();
  }
}
