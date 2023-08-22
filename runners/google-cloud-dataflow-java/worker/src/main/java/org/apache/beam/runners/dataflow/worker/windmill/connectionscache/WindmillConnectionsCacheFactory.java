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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import static org.apache.beam.runners.dataflow.worker.windmill.connectionscache.WindmillChannelFactory.localhostChannel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillApplianceGrpc;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.Credentials;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.Channel;

public final class WindmillConnectionsCacheFactory {
  private WindmillConnectionsCacheFactory() {}

  public static ReadWriteWindmillConnectionsCache forStreamingEngine(
      List<CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub> dispatcherStubs,
      int windmillServiceRpcChannelTimeoutSec,
      Credentials gcpCredentials) {
    return new WindmillConnectionsCache(
        new ArrayList<>(dispatcherStubs),
        /* dispatcherEndpoints= */ new HashSet<>(),
        /* windmillWorkerConnectionsState= */ new AtomicReference<>(),
        /* syncApplianceStub= */ null,
        windmillServiceRpcChannelTimeoutSec,
        WindmillGrpcStubFactory.remoteStubFactory(
            windmillServiceRpcChannelTimeoutSec, gcpCredentials));
  }

  /** Returns a connections cache for windmill Appliance. */
  public static ReadOnlyWindmillConnectionsCache forAppliance(int localhostPort) {
    Channel localChannel = localhostChannel(localhostPort);
    return new WindmillConnectionsCache(
        /* dispatcherStubs= */ new ArrayList<>(),
        /* dispatcherEndpoints= */ new HashSet<>(),
        /* windmillWorkerConnectionsState= */ new AtomicReference<>(),
        WindmillApplianceGrpc.newBlockingStub(localChannel),
        // Appliance just uses localhost channel. This is a no-op.
        /*windmillServiceRpcChannelTimeoutSec*/ -1,
        WindmillGrpcStubFactory.inProcessStubFactory(""));
  }
}
