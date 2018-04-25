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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.net.HostAndPort;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.channel.SocketAddressFactory;

/** A {@link Server gRPC server} factory. */
public abstract class ServerFactory {
  /** Create a default {@link ServerFactory}. */
  public static ServerFactory createDefault() {
    return new InetSocketAddressServerFactory();
  }

  /**
   * Creates an instance of this server using an ephemeral port chosen automatically. The chosen
   * port is accessible to the caller from the URL set in the input {@link
   * Endpoints.ApiServiceDescriptor.Builder}. Server applies {@link
   * GrpcContextHeaderAccessorProvider#interceptor()} to all incoming requests.
   */
  public abstract Server allocatePortAndCreate(
      BindableService service, Endpoints.ApiServiceDescriptor.Builder builder) throws IOException;

  /**
   * Creates an instance of this server at the address specified by the given service descriptor.
   * Server applies {@link GrpcContextHeaderAccessorProvider#interceptor()} to all incoming
   * requests.
   */
  public abstract Server create(
      BindableService service, Endpoints.ApiServiceDescriptor serviceDescriptor) throws IOException;

  /**
   * Creates a {@link Server gRPC Server} using the default server factory.
   *
   * <p>The server is created listening any open port on "localhost".
   */
  public static class InetSocketAddressServerFactory extends ServerFactory {
    private InetSocketAddressServerFactory() {}

    @Override
    public Server allocatePortAndCreate(
        BindableService service, Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor)
        throws IOException {
      InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
      Server server = createServer(service, address);
      apiServiceDescriptor.setUrl(
          HostAndPort.fromParts(address.getHostName(), server.getPort()).toString());
      return server;
    }

    @Override
    public Server create(BindableService service, Endpoints.ApiServiceDescriptor serviceDescriptor)
        throws IOException {
      SocketAddress socketAddress = SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
      checkArgument(
          socketAddress instanceof InetSocketAddress,
          "%s %s requires a host:port socket address, got %s",
          getClass().getSimpleName(),
          ServerFactory.class.getSimpleName(),
          serviceDescriptor.getUrl());
      return createServer(service, (InetSocketAddress) socketAddress);
    }

    private static Server createServer(BindableService service, InetSocketAddress socket)
        throws IOException {
      // Note: Every ServerFactory should apply GrpcContextHeaderAccessorProvider to the service.
      Server server =
          NettyServerBuilder.forPort(socket.getPort())
              .addService(
                  ServerInterceptors.intercept(
                      service, GrpcContextHeaderAccessorProvider.interceptor()))
              // Set the message size to max value here. The actual size is governed by the
              // buffer size in the layers above.
              .maxMessageSize(Integer.MAX_VALUE)
              .build();
      server.start();
      return server;
    }
  }
}
