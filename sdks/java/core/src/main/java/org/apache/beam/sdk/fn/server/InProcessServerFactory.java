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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.BindableService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ServerInterceptors;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;

/**
 * A {@link ServerFactory} which creates {@link Server servers} with the {@link
 * InProcessServerBuilder}.
 */
public class InProcessServerFactory extends ServerFactory {
  private static final AtomicInteger serviceNameUniqifier = new AtomicInteger();

  public static InProcessServerFactory create() {
    return new InProcessServerFactory();
  }

  private InProcessServerFactory() {}

  @Override
  public Server allocateAddressAndCreate(
      List<BindableService> services, ApiServiceDescriptor.Builder builder) throws IOException {
    String name = String.format("InProcessServer_%s", serviceNameUniqifier.getAndIncrement());
    builder.setUrl(name);
    InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name);
    services.stream()
        .forEach(
            service ->
                serverBuilder.addService(
                    ServerInterceptors.intercept(
                        service, GrpcContextHeaderAccessorProvider.interceptor())));
    return serverBuilder.build().start();
  }

  @Override
  public Server create(List<BindableService> services, ApiServiceDescriptor serviceDescriptor)
      throws IOException {
    InProcessServerBuilder builder = InProcessServerBuilder.forName(serviceDescriptor.getUrl());
    services.stream()
        .forEach(
            service ->
                builder.addService(
                    ServerInterceptors.intercept(
                        service, GrpcContextHeaderAccessorProvider.interceptor())));
    return builder.build().start();
  }
}
