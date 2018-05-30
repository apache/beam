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

package org.apache.beam.runners.fnexecution;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;

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
  public Server allocatePortAndCreate(BindableService service, ApiServiceDescriptor.Builder builder)
      throws IOException {
    String name = String.format("InProcessServer_%s", serviceNameUniqifier.getAndIncrement());
    builder.setUrl(name);
    return InProcessServerBuilder.forName(name).addService(service).build().start();
  }

  @Override
  public Server create(BindableService service, ApiServiceDescriptor serviceDescriptor)
      throws IOException {
    return InProcessServerBuilder.forName(serviceDescriptor.getUrl())
        .addService(
            ServerInterceptors.intercept(service, GrpcContextHeaderAccessorProvider.interceptor()))
        .build()
        .start();
  }
}
