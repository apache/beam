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
package org.apache.beam.sdk.fn.channel;

import java.net.SocketAddress;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.epoll.EpollDomainSocketChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.unix.DomainSocketAddress;

/** A Factory which creates an underlying {@link ManagedChannel} implementation. */
public abstract class ManagedChannelFactory {
  public static ManagedChannelFactory createDefault() {
    return new Default();
  }

  public static ManagedChannelFactory createEpoll() {
    org.apache.beam.vendor.grpc.v1p26p0.io.netty.channel.epoll.Epoll.ensureAvailability();
    return new Epoll();
  }

  public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
    return builderFor(apiServiceDescriptor).build();
  }

  /** Create a {@link ManagedChannelBuilder} for the provided {@link ApiServiceDescriptor}. */
  protected abstract ManagedChannelBuilder<?> builderFor(ApiServiceDescriptor descriptor);

  /**
   * Returns a {@link ManagedChannelFactory} like this one, but which will apply the provided {@link
   * ClientInterceptor ClientInterceptors} to any channel it creates.
   */
  public ManagedChannelFactory withInterceptors(List<ClientInterceptor> interceptors) {
    return new InterceptedManagedChannelFactory(this, interceptors);
  }

  /**
   * Creates a {@link ManagedChannel} backed by an {@link EpollDomainSocketChannel} if the address
   * is a {@link DomainSocketAddress}. Otherwise creates a {@link ManagedChannel} backed by an
   * {@link EpollSocketChannel}.
   */
  private static class Epoll extends ManagedChannelFactory {
    @Override
    public ManagedChannelBuilder<?> builderFor(ApiServiceDescriptor apiServiceDescriptor) {
      SocketAddress address = SocketAddressFactory.createFrom(apiServiceDescriptor.getUrl());
      return NettyChannelBuilder.forAddress(address)
          .channelType(
              address instanceof DomainSocketAddress
                  ? EpollDomainSocketChannel.class
                  : EpollSocketChannel.class)
          .eventLoopGroup(new EpollEventLoopGroup())
          .usePlaintext()
          // Set the message size to max value here. The actual size is governed by the
          // buffer size in the layers above.
          .maxInboundMessageSize(Integer.MAX_VALUE);
    }
  }

  /**
   * Creates a {@link ManagedChannel} relying on the {@link ManagedChannelBuilder} to create
   * instances.
   */
  private static class Default extends ManagedChannelFactory {
    @Override
    public ManagedChannelBuilder<?> builderFor(ApiServiceDescriptor apiServiceDescriptor) {
      return ManagedChannelBuilder.forTarget(apiServiceDescriptor.getUrl())
          .usePlaintext()
          // Set the message size to max value here. The actual size is governed by the
          // buffer size in the layers above.
          .maxInboundMessageSize(Integer.MAX_VALUE);
    }
  }

  private static class InterceptedManagedChannelFactory extends ManagedChannelFactory {
    private final ManagedChannelFactory channelFactory;
    private final List<ClientInterceptor> interceptors;

    private InterceptedManagedChannelFactory(
        ManagedChannelFactory managedChannelFactory, List<ClientInterceptor> interceptors) {
      this.channelFactory = managedChannelFactory;
      this.interceptors = interceptors;
    }

    @Override
    public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
      return builderFor(apiServiceDescriptor).intercept(interceptors).build();
    }

    @Override
    protected ManagedChannelBuilder<?> builderFor(ApiServiceDescriptor descriptor) {
      return channelFactory.builderFor(descriptor);
    }

    @Override
    public ManagedChannelFactory withInterceptors(List<ClientInterceptor> interceptors) {
      return new InterceptedManagedChannelFactory(channelFactory, interceptors);
    }
  }
}
