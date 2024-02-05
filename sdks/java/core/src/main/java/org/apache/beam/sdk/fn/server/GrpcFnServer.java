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
package org.apache.beam.sdk.fn.server;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/**
 * A {@link Server gRPC Server} which manages a single {@link FnService}. The lifetime of the
 * service is bound to the {@link GrpcFnServer}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness",
  "keyfor"
}) // TODO(https://github.com/apache/beam/issues/20497)
public class GrpcFnServer<ServiceT extends FnService> implements AutoCloseable {
  /**
   * Create a {@link GrpcFnServer} for the provided {@link FnService} running on an arbitrary port.
   */
  public static <ServiceT extends FnService> GrpcFnServer<ServiceT> allocatePortAndCreateFor(
      ServiceT service, ServerFactory factory) throws IOException {
    ApiServiceDescriptor.Builder apiServiceDescriptor = ApiServiceDescriptor.newBuilder();
    Server server =
        factory.allocateAddressAndCreate(ImmutableList.of(service), apiServiceDescriptor);
    return new GrpcFnServer<>(server, service, apiServiceDescriptor.build());
  }

  /**
   * Create {@link GrpcFnServer}s for the provided {@link FnService}s running on an arbitrary port.
   */
  public static List<GrpcFnServer<? extends FnService>> allocatePortAndCreateFor(
      List<? extends FnService> services, ServerFactory factory) throws IOException {
    ApiServiceDescriptor.Builder apiServiceDescriptor = ApiServiceDescriptor.newBuilder();
    Server server =
        factory.allocateAddressAndCreate(
            Collections.unmodifiableList(services), apiServiceDescriptor);
    AtomicInteger countdown = new AtomicInteger(services.size());
    return Lists.transform(
        services,
        service ->
            new SharedGrpcFnServer<>(server, service, apiServiceDescriptor.build(), countdown));
  }

  /**
   * Create {@link GrpcFnServer}s for the provided {@link FnService}s running on a specified port.
   */
  public static List<GrpcFnServer<? extends FnService>> create(
      List<? extends FnService> services, ApiServiceDescriptor endpoint, ServerFactory factory)
      throws IOException {
    Server server = factory.create(Collections.unmodifiableList(services), endpoint);
    AtomicInteger countdown = new AtomicInteger(services.size());
    return Lists.transform(
        services, service -> new SharedGrpcFnServer<>(server, service, endpoint, countdown));
  }

  private static class SharedGrpcFnServer<ServiceT extends FnService>
      extends GrpcFnServer<ServiceT> {

    private AtomicInteger countdown;

    private SharedGrpcFnServer(
        Server server,
        ServiceT service,
        ApiServiceDescriptor apiServiceDescriptor,
        AtomicInteger countdown) {
      super(server, service, apiServiceDescriptor);
      this.countdown = countdown;
    }

    @Override
    public void close() throws Exception {
      if (countdown.addAndGet(-1) == 0) {
        super.close();
      } else {
        getService().close();
      }
    }
  }

  /**
   * Create a {@link GrpcFnServer} for the provided {@link FnService} which will run at the endpoint
   * specified in the {@link ApiServiceDescriptor}.
   */
  public static <ServiceT extends FnService> GrpcFnServer<ServiceT> create(
      ServiceT service, ApiServiceDescriptor endpoint, ServerFactory factory) throws IOException {
    return new GrpcFnServer<>(
        factory.create(ImmutableList.of(service), endpoint), service, endpoint);
  }

  /** @deprecated This create function is used for Dataflow migration purpose only. */
  @Deprecated
  public static <ServiceT extends FnService> GrpcFnServer<ServiceT> create(
      ServiceT service, ApiServiceDescriptor endpoint) {
    return new GrpcFnServer(null, service, endpoint) {
      @Override
      public void close() throws Exception {}
    };
  }

  private final Server server;
  private final ServiceT service;
  private final ApiServiceDescriptor apiServiceDescriptor;

  private GrpcFnServer(Server server, ServiceT service, ApiServiceDescriptor apiServiceDescriptor) {
    this.server = server;
    this.service = service;
    this.apiServiceDescriptor = apiServiceDescriptor;
  }

  /**
   * Get an {@link ApiServiceDescriptor} describing the endpoint this {@link GrpcFnServer} is bound
   * to.
   */
  public ApiServiceDescriptor getApiServiceDescriptor() {
    return apiServiceDescriptor;
  }

  /** Get the service exposed by this {@link GrpcFnServer}. */
  public ServiceT getService() {
    return service;
  }

  /** Get the underlying {@link Server} contained by this {@link GrpcFnServer}. */
  public Server getServer() {
    return server;
  }

  @Override
  public void close() throws Exception {
    try {
      // The server has been closed, and should not respond to any new incoming calls.
      server.shutdown();
      service.close();
      server.awaitTermination(60, TimeUnit.SECONDS);
    } finally {
      server.shutdownNow();
      server.awaitTermination();
    }
  }
}
