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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.LOCALHOST;
import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.localhostChannel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages endpoints and stubs for connecting to the Windmill Dispatcher. */
@ThreadSafe
class GrpcDispatcherClient {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDispatcherClient.class);
  private final WindmillStubFactory windmillStubFactory;

  @GuardedBy("this")
  private final List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs;

  @GuardedBy("this")
  private final Set<HostAndPort> dispatcherEndpoints;

  @GuardedBy("this")
  private final Random rand;

  private GrpcDispatcherClient(
      WindmillStubFactory windmillStubFactory,
      List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs,
      Set<HostAndPort> dispatcherEndpoints,
      Random rand) {
    this.windmillStubFactory = windmillStubFactory;
    this.dispatcherStubs = dispatcherStubs;
    this.dispatcherEndpoints = dispatcherEndpoints;
    this.rand = rand;
  }

  static GrpcDispatcherClient create(WindmillStubFactory windmillStubFactory) {
    return new GrpcDispatcherClient(
        windmillStubFactory, new ArrayList<>(), new HashSet<>(), new Random());
  }

  @VisibleForTesting
  static GrpcDispatcherClient forTesting(
      WindmillStubFactory windmillGrpcStubFactory,
      List<CloudWindmillServiceV1Alpha1Stub> dispatcherStubs,
      Set<HostAndPort> dispatcherEndpoints) {
    Preconditions.checkArgument(dispatcherEndpoints.size() == dispatcherStubs.size());
    return new GrpcDispatcherClient(
        windmillGrpcStubFactory, dispatcherStubs, dispatcherEndpoints, new Random());
  }

  synchronized CloudWindmillServiceV1Alpha1Stub getDispatcherStub() {
    Preconditions.checkState(
        !dispatcherStubs.isEmpty(), "windmillServiceEndpoint has not been set");

    return (dispatcherStubs.size() == 1
        ? dispatcherStubs.get(0)
        : dispatcherStubs.get(rand.nextInt(dispatcherStubs.size())));
  }

  synchronized boolean isReady() {
    return !dispatcherStubs.isEmpty();
  }

  synchronized void consumeWindmillDispatcherEndpoints(
      ImmutableSet<HostAndPort> dispatcherEndpoints) {
    Preconditions.checkArgument(
        dispatcherEndpoints != null && !dispatcherEndpoints.isEmpty(),
        "Cannot set dispatcher endpoints to nothing.");
    if (this.dispatcherEndpoints.equals(dispatcherEndpoints)) {
      // The endpoints are equal don't recreate the stubs.
      return;
    }

    LOG.info("Creating a new windmill stub, endpoints: {}", dispatcherEndpoints);
    if (!this.dispatcherEndpoints.isEmpty()) {
      LOG.info("Previous windmill stub endpoints: {}", this.dispatcherEndpoints);
    }

    resetDispatcherEndpoints(dispatcherEndpoints);
  }

  private synchronized void resetDispatcherEndpoints(
      ImmutableSet<HostAndPort> newDispatcherEndpoints) {
    LOG.info("Initializing Streaming Engine GRPC client for endpoints: {}", newDispatcherEndpoints);
    this.dispatcherStubs.clear();
    this.dispatcherEndpoints.clear();
    this.dispatcherEndpoints.addAll(newDispatcherEndpoints);

    dispatcherEndpoints.stream()
        .map(this::createDispatcherStubForWindmillService)
        .forEach(dispatcherStubs::add);
  }

  private CloudWindmillServiceV1Alpha1Stub createDispatcherStubForWindmillService(
      HostAndPort endpoint) {
    if (LOCALHOST.equals(endpoint.getHost())) {
      return CloudWindmillServiceV1Alpha1Grpc.newStub(localhostChannel(endpoint.getPort()));
    }

    // Use an in-process stub if testing.
    return windmillStubFactory.getKind() == WindmillStubFactory.Kind.IN_PROCESS
        ? windmillStubFactory.inProcess().get()
        : windmillStubFactory.remote().apply(WindmillServiceAddress.create(endpoint));
  }
}
