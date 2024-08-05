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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages endpoints and stubs for connecting to the Windmill Dispatcher. */
@ThreadSafe
public class GrpcDispatcherClient {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDispatcherClient.class);
  private final WindmillStubFactory windmillStubFactory;
  private final CountDownLatch onInitializedEndpoints;

  /**
   * Current dispatcher endpoints and stubs used to communicate with Windmill Dispatcher.
   *
   * @implNote Reads are lock free, writes are synchronized.
   */
  private final AtomicReference<DispatcherStubs> dispatcherStubs;

  @GuardedBy("this")
  private final Random rand;

  private GrpcDispatcherClient(
      WindmillStubFactory windmillStubFactory,
      DispatcherStubs initialDispatcherStubs,
      Random rand) {
    this.windmillStubFactory = windmillStubFactory;
    this.rand = rand;
    this.dispatcherStubs = new AtomicReference<>(initialDispatcherStubs);
    this.onInitializedEndpoints = new CountDownLatch(1);
  }

  public static GrpcDispatcherClient create(WindmillStubFactory windmillStubFactory) {
    return new GrpcDispatcherClient(windmillStubFactory, DispatcherStubs.empty(), new Random());
  }

  @VisibleForTesting
  public static GrpcDispatcherClient forTesting(
      WindmillStubFactory windmillGrpcStubFactory,
      List<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs,
      List<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs,
      Set<HostAndPort> dispatcherEndpoints) {
    Preconditions.checkArgument(
        dispatcherEndpoints.size() == windmillServiceStubs.size()
            && windmillServiceStubs.size() == windmillMetadataServiceStubs.size());
    return new GrpcDispatcherClient(
        windmillGrpcStubFactory,
        DispatcherStubs.create(
            dispatcherEndpoints, windmillServiceStubs, windmillMetadataServiceStubs),
        new Random());
  }

  public CloudWindmillServiceV1Alpha1Stub getWindmillServiceStub() {
    ImmutableList<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs =
        dispatcherStubs.get().windmillServiceStubs();
    Preconditions.checkState(
        !windmillServiceStubs.isEmpty(), "windmillServiceEndpoint has not been set");

    return (windmillServiceStubs.size() == 1
        ? windmillServiceStubs.get(0)
        : randomlySelectNextStub(windmillServiceStubs));
  }

  ImmutableSet<HostAndPort> getDispatcherEndpoints() {
    return dispatcherStubs.get().dispatcherEndpoints();
  }

  /** Will block the calling thread until the initial endpoints are present. */
  public CloudWindmillMetadataServiceV1Alpha1Stub getWindmillMetadataServiceStubBlocking() {
    boolean initialized = false;
    long secondsWaited = 0;
    while (!initialized) {
      LOG.info(
          "Blocking until Windmill Service endpoint has been set. "
              + "Currently waited for [{}] seconds.",
          secondsWaited);
      try {
        initialized = onInitializedEndpoints.await(10, TimeUnit.SECONDS);
        secondsWaited += 10;
      } catch (InterruptedException e) {
        LOG.error(
            "Interrupted while waiting for initial Windmill Service endpoints. "
                + "These endpoints are required to do any pipeline processing.",
            e);
      }
    }

    ImmutableList<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs =
        dispatcherStubs.get().windmillMetadataServiceStubs();

    return (windmillMetadataServiceStubs.size() == 1
        ? windmillMetadataServiceStubs.get(0)
        : randomlySelectNextStub(windmillMetadataServiceStubs));
  }

  private synchronized <T> T randomlySelectNextStub(List<T> stubs) {
    return stubs.get(rand.nextInt(stubs.size()));
  }

  /**
   * Returns whether the {@link DispatcherStubs} have been set. Once initially set, {@link
   * #dispatcherStubs} will always have a value as empty updates will trigger an {@link
   * IllegalStateException}.
   */
  public boolean hasInitializedEndpoints() {
    return dispatcherStubs.get().hasInitializedEndpoints();
  }

  public synchronized void consumeWindmillDispatcherEndpoints(
      ImmutableSet<HostAndPort> dispatcherEndpoints) {
    ImmutableSet<HostAndPort> currentDispatcherEndpoints =
        dispatcherStubs.get().dispatcherEndpoints();
    Preconditions.checkArgument(
        dispatcherEndpoints != null && !dispatcherEndpoints.isEmpty(),
        "Cannot set dispatcher endpoints to nothing.");
    if (currentDispatcherEndpoints.equals(dispatcherEndpoints)) {
      // The endpoints are equal don't recreate the stubs.
      return;
    }

    LOG.info("Creating a new windmill stub, endpoints: {}", dispatcherEndpoints);
    if (!currentDispatcherEndpoints.isEmpty()) {
      LOG.info("Previous windmill stub endpoints: {}", currentDispatcherEndpoints);
    }

    LOG.info("Initializing Streaming Engine GRPC client for endpoints: {}", dispatcherEndpoints);
    dispatcherStubs.set(DispatcherStubs.create(dispatcherEndpoints, windmillStubFactory));
    onInitializedEndpoints.countDown();
  }

  /**
   * Endpoints and gRPC stubs used to communicate with the Windmill Dispatcher. {@link
   * #dispatcherEndpoints()}, {@link #windmillServiceStubs()}, and {@link
   * #windmillMetadataServiceStubs()} collections should all be of the same size.
   */
  @AutoValue
  abstract static class DispatcherStubs {

    private static DispatcherStubs empty() {
      return create(ImmutableSet.of(), ImmutableList.of(), ImmutableList.of());
    }

    private static DispatcherStubs create(
        Set<HostAndPort> endpoints,
        List<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs,
        List<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs) {
      Preconditions.checkState(
          endpoints.size() == windmillServiceStubs.size()
              && windmillServiceStubs.size() == windmillMetadataServiceStubs.size(),
          "Dispatcher should have the same number of endpoints and stubs");
      return new AutoValue_GrpcDispatcherClient_DispatcherStubs(
          ImmutableSet.copyOf(endpoints),
          ImmutableList.copyOf(windmillServiceStubs),
          ImmutableList.copyOf(windmillMetadataServiceStubs));
    }

    private static DispatcherStubs create(
        ImmutableSet<HostAndPort> newDispatcherEndpoints, WindmillStubFactory windmillStubFactory) {
      ImmutableList.Builder<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs =
          ImmutableList.builder();
      ImmutableList.Builder<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs =
          ImmutableList.builder();

      for (HostAndPort endpoint : newDispatcherEndpoints) {
        windmillServiceStubs.add(createWindmillServiceStub(endpoint, windmillStubFactory));
        windmillMetadataServiceStubs.add(
            createWindmillMetadataServiceStub(endpoint, windmillStubFactory));
      }

      return new AutoValue_GrpcDispatcherClient_DispatcherStubs(
          newDispatcherEndpoints,
          windmillServiceStubs.build(),
          windmillMetadataServiceStubs.build());
    }

    private static CloudWindmillServiceV1Alpha1Stub createWindmillServiceStub(
        HostAndPort endpoint, WindmillStubFactory windmillStubFactory) {
      if (LOCALHOST.equals(endpoint.getHost())) {
        return CloudWindmillServiceV1Alpha1Grpc.newStub(localhostChannel(endpoint.getPort()));
      }

      return windmillStubFactory.createWindmillServiceStub(WindmillServiceAddress.create(endpoint));
    }

    private static CloudWindmillMetadataServiceV1Alpha1Stub createWindmillMetadataServiceStub(
        HostAndPort endpoint, WindmillStubFactory windmillStubFactory) {
      if (LOCALHOST.equals(endpoint.getHost())) {
        return CloudWindmillMetadataServiceV1Alpha1Grpc.newStub(
            localhostChannel(endpoint.getPort()));
      }

      return windmillStubFactory.createWindmillMetadataServiceStub(
          WindmillServiceAddress.create(endpoint));
    }

    private int size() {
      return dispatcherEndpoints().size();
    }

    private boolean hasInitializedEndpoints() {
      return size() > 0;
    }

    abstract ImmutableSet<HostAndPort> dispatcherEndpoints();

    abstract ImmutableList<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs();

    abstract ImmutableList<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs();
  }
}
