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
package org.apache.beam.runners.dataflow.worker.fn;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.net.HostAndPort;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.fnexecution.GrpcContextHeaderAccessorProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.BindableService;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.ServerBuilder;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.ServerInterceptors;
import org.apache.beam.vendor.grpc.v1_13_1.io.grpc.netty.NettyServerBuilder;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.channel.unix.DomainSocketAddress;
import org.apache.beam.vendor.grpc.v1_13_1.io.netty.util.internal.ThreadLocalRandom;

/**
 * A {@link Server gRPC Server} factory that returns a server based upon {@link PipelineOptions}
 * experiments. <br>
 * TODO: Kill {@link ServerFactory} instead use {@link
 * org.apache.beam.runners.fnexecution.ServerFactory}.
 */
@Deprecated
public abstract class ServerFactory {
  public static ServerFactory fromOptions(PipelineOptions options) {
    DataflowPipelineDebugOptions dataflowOptions = options.as(DataflowPipelineDebugOptions.class);
    if (DataflowRunner.hasExperiment(dataflowOptions, "beam_fn_api_epoll_domain_socket")) {
      return new EpollDomainSocket();
    } else if (DataflowRunner.hasExperiment(dataflowOptions, "beam_fn_api_epoll")) {
      return new EpollSocket();
    }
    return new Default();
  }

  /**
   * Allocates a port for a server using an ephemeral port chosen automatically. The chosen port is
   * accessible to the caller from the URL set in the input {@link
   * Endpoints.ApiServiceDescriptor.Builder}. Server applies {@link
   * GrpcContextHeaderAccessorProvider#interceptor()} to all incoming requests.
   */
  public abstract Server allocatePortAndCreate(
      Endpoints.ApiServiceDescriptor.Builder builder, List<BindableService> services)
      throws IOException;

  /**
   * Creates an instance of this server at the address specified by the given service descriptor.
   * Server applies {@link GrpcContextHeaderAccessorProvider#interceptor()} to all incoming
   * requests.
   */
  public abstract Server create(
      Endpoints.ApiServiceDescriptor serviceDescriptor, List<BindableService> services)
      throws IOException;

  /**
   * Creates a {@link Server gRPC Server} using a Unix domain socket. Note that this requires <a
   * href="http://netty.io/wiki/forked-tomcat-native.html">Netty TcNative</a> available to be able
   * to provide a {@link EpollServerDomainSocketChannel}.
   *
   * <p>The unix domain socket is located at ${java.io.tmpdir}/fnapi${random[0-10000)}.sock
   */
  private static class EpollDomainSocket extends ServerFactory {
    private static File getFileForPort(int port) {
      return new File(System.getProperty("java.io.tmpdir"), String.format("fnapi%d.sock", port));
    }

    @Override
    public Server allocatePortAndCreate(
        Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor, List<BindableService> services)
        throws IOException {
      File tmp;
      do {
        tmp = getFileForPort(ThreadLocalRandom.current().nextInt(10000));
      } while (tmp.exists());
      apiServiceDescriptor.setUrl("unix://" + tmp.getAbsolutePath());
      return create(apiServiceDescriptor.build(), services);
    }

    @Override
    public Server create(
        Endpoints.ApiServiceDescriptor serviceDescriptor, List<BindableService> services)
        throws IOException {
      SocketAddress socketAddress = SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
      checkArgument(
          socketAddress instanceof DomainSocketAddress,
          "%s requires a Unix domain socket address, got %s",
          EpollDomainSocket.class.getSimpleName(),
          serviceDescriptor.getUrl());
      return createServer((DomainSocketAddress) socketAddress, services);
    }

    private static Server createServer(
        DomainSocketAddress domainSocket, List<BindableService> services) throws IOException {
      NettyServerBuilder builder =
          NettyServerBuilder.forAddress(domainSocket)
              .channelType(EpollServerDomainSocketChannel.class)
              .workerEventLoopGroup(new EpollEventLoopGroup())
              .bossEventLoopGroup(new EpollEventLoopGroup())
              .maxMessageSize(Integer.MAX_VALUE);
      for (BindableService service : services) {
        // Wrap the service to extract headers
        builder.addService(
            ServerInterceptors.intercept(service, GrpcContextHeaderAccessorProvider.interceptor()));
      }
      return builder.build().start();
    }
  }

  /**
   * Creates a {@link Server gRPC Server} using an Epoll socket. Note that this requires <a
   * href="http://netty.io/wiki/forked-tomcat-native.html">Netty TcNative</a> available to be able
   * to provide a {@link EpollServerSocketChannel}.
   *
   * <p>The server is created listening any open port on "localhost".
   */
  private static class EpollSocket extends ServerFactory {
    @Override
    public Server allocatePortAndCreate(
        Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor, List<BindableService> services)
        throws IOException {
      InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
      Server server = createServer(address, services);
      apiServiceDescriptor.setUrl(
          HostAndPort.fromParts(address.getHostName(), server.getPort()).toString());
      return server;
    }

    @Override
    public Server create(
        Endpoints.ApiServiceDescriptor serviceDescriptor, List<BindableService> services)
        throws IOException {
      SocketAddress socketAddress = SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
      checkArgument(
          socketAddress instanceof InetSocketAddress,
          "%s requires a host:port socket address, got %s",
          EpollSocket.class.getSimpleName(),
          serviceDescriptor.getUrl());
      return createServer((InetSocketAddress) socketAddress, services);
    }

    private static Server createServer(InetSocketAddress socket, List<BindableService> services)
        throws IOException {
      ServerBuilder builder =
          NettyServerBuilder.forAddress(socket)
              .channelType(EpollServerSocketChannel.class)
              .workerEventLoopGroup(new EpollEventLoopGroup())
              .bossEventLoopGroup(new EpollEventLoopGroup())
              .maxMessageSize(Integer.MAX_VALUE);
      for (BindableService service : services) {
        // Wrap the service to extract headers
        builder.addService(
            ServerInterceptors.intercept(service, GrpcContextHeaderAccessorProvider.interceptor()));
      }
      return builder.build().start();
    }
  }

  /**
   * Creates a {@link Server gRPC Server} using the default server factory.
   *
   * <p>The server is created listening any open port on "localhost".
   */
  private static class Default extends ServerFactory {
    @Override
    public Server allocatePortAndCreate(
        Endpoints.ApiServiceDescriptor.Builder apiServiceDescriptor, List<BindableService> services)
        throws IOException {
      InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
      Server server = createServer(address, services);
      apiServiceDescriptor.setUrl(
          HostAndPort.fromParts(address.getHostName(), server.getPort()).toString());
      return server;
    }

    @Override
    public Server create(
        Endpoints.ApiServiceDescriptor serviceDescriptor, List<BindableService> services)
        throws IOException {
      SocketAddress socketAddress = SocketAddressFactory.createFrom(serviceDescriptor.getUrl());
      checkArgument(
          socketAddress instanceof InetSocketAddress,
          "Default ServerFactory requires a host:port socket address, got %s",
          serviceDescriptor.getUrl());
      return createServer((InetSocketAddress) socketAddress, services);
    }

    private static Server createServer(InetSocketAddress socket, List<BindableService> services)
        throws IOException {
      NettyServerBuilder builder =
          NettyServerBuilder.forPort(socket.getPort())
              // Set the message size to max value here. The actual size is governed by the
              // buffer size in the layers above.
              .maxMessageSize(Integer.MAX_VALUE);
      for (BindableService service : services) {
        // Wrap the service to extract headers
        builder.addService(
            ServerInterceptors.intercept(service, GrpcContextHeaderAccessorProvider.interceptor()));
      }
      return builder.build().start();
    }
  }
}
