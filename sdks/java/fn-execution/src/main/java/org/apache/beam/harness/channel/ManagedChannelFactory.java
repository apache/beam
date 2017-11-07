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

package org.apache.beam.harness.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import java.net.SocketAddress;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;

/**
 * A Factory which creates an underlying {@link ManagedChannel} implementation.
 */
public abstract class ManagedChannelFactory {
  public static ManagedChannelFactory createDefault() {
    return new Default();
  }

  public static ManagedChannelFactory createEpoll() {
    io.netty.channel.epoll.Epoll.ensureAvailability();
    return new Epoll();
  }

  public abstract ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor);

  /**
   * Creates a {@link ManagedChannel} backed by an {@link EpollDomainSocketChannel} if the address
   * is a {@link DomainSocketAddress}. Otherwise creates a {@link ManagedChannel} backed by an
   * {@link EpollSocketChannel}.
   */
  private static class Epoll extends ManagedChannelFactory {
    @Override
    public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
      SocketAddress address = SocketAddressFactory.createFrom(apiServiceDescriptor.getUrl());
      return NettyChannelBuilder.forAddress(address)
          .channelType(address instanceof DomainSocketAddress
              ? EpollDomainSocketChannel.class : EpollSocketChannel.class)
          .eventLoopGroup(new EpollEventLoopGroup())
          .usePlaintext(true)
          // Set the message size to max value here. The actual size is governed by the
          // buffer size in the layers above.
          .maxInboundMessageSize(Integer.MAX_VALUE)
          .build();
    }
  }

  /**
   * Creates a {@link ManagedChannel} relying on the {@link ManagedChannelBuilder} to create
   * instances.
   */
  private static class Default extends ManagedChannelFactory {
    @Override
    public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
      return ManagedChannelBuilder.forTarget(apiServiceDescriptor.getUrl())
          .usePlaintext(true)
          // Set the message size to max value here. The actual size is governed by the
          // buffer size in the layers above.
          .maxInboundMessageSize(Integer.MAX_VALUE)
          .build();
    }
  }
}
