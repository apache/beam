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
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ClientInterceptor;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.epoll.EpollDomainSocketChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.unix.DomainSocketAddress;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** A Factory which creates {@link ManagedChannel} instances. */
public class ManagedChannelFactory {
  /**
   * Creates a {@link ManagedChannel} relying on the {@link ManagedChannelBuilder} to choose the
   * channel type.
   */
  public static ManagedChannelFactory createDefault() {
    return new ManagedChannelFactory(Type.DEFAULT, Collections.emptyList(), false);
  }

  /**
   * Creates a {@link ManagedChannelFactory} backed by an {@link EpollDomainSocketChannel} if the
   * address is a {@link DomainSocketAddress}. Otherwise creates a {@link ManagedChannel} backed by
   * an {@link EpollSocketChannel}.
   */
  public static ManagedChannelFactory createEpoll() {
    org.apache.beam.vendor.grpc.v1p60p1.io.netty.channel.epoll.Epoll.ensureAvailability();
    return new ManagedChannelFactory(Type.EPOLL, Collections.emptyList(), false);
  }

  /** Creates a {@link ManagedChannel} using an in-process channel. */
  public static ManagedChannelFactory createInProcess() {
    return new ManagedChannelFactory(Type.IN_PROCESS, Collections.emptyList(), false);
  }

  public ManagedChannel forDescriptor(ApiServiceDescriptor apiServiceDescriptor) {
    ManagedChannelBuilder<?> channelBuilder;
    switch (type) {
      case EPOLL:
        SocketAddress address = SocketAddressFactory.createFrom(apiServiceDescriptor.getUrl());
        channelBuilder =
            NettyChannelBuilder.forAddress(address)
                .channelType(
                    address instanceof DomainSocketAddress
                        ? EpollDomainSocketChannel.class
                        : EpollSocketChannel.class)
                .eventLoopGroup(new EpollEventLoopGroup());
        break;

      case DEFAULT:
        channelBuilder = ManagedChannelBuilder.forTarget(apiServiceDescriptor.getUrl());
        break;

      case IN_PROCESS:
        channelBuilder = InProcessChannelBuilder.forName(apiServiceDescriptor.getUrl());
        break;

      default:
        throw new IllegalStateException("Unknown type " + type);
    }

    channelBuilder =
        channelBuilder
            .usePlaintext()
            // Set the message size to max value here. The actual size is governed by the
            // buffer size in the layers above.
            .maxInboundMessageSize(Integer.MAX_VALUE)
            // Disable automatic retries as it introduces complexity and we send long-lived
            // rpcs which will exceed the per-rpc retry request buffer and not be retried
            // anyway. See
            // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#when-retries-are-valid
            .disableRetry()
            .intercept(interceptors);
    if (directExecutor) {
      channelBuilder = channelBuilder.directExecutor();
    }
    return channelBuilder.build();
  }

  /** The channel type. */
  private enum Type {
    EPOLL,
    DEFAULT,
    IN_PROCESS,
  }

  private final Type type;
  private final List<ClientInterceptor> interceptors;
  private final boolean directExecutor;

  private ManagedChannelFactory(
      Type type, List<ClientInterceptor> interceptors, boolean directExecutor) {
    this.type = type;
    this.interceptors = interceptors;
    this.directExecutor = directExecutor;
  }

  /**
   * Returns a {@link ManagedChannelFactory} like this one, but which will apply the provided {@link
   * ClientInterceptor ClientInterceptors} to any channel it creates.
   */
  public ManagedChannelFactory withInterceptors(List<ClientInterceptor> interceptors) {
    return new ManagedChannelFactory(
        type,
        ImmutableList.<ClientInterceptor>builder()
            .addAll(this.interceptors)
            .addAll(interceptors)
            .build(),
        directExecutor);
  }

  /**
   * Returns a {@link ManagedChannelFactory} like this one, but will construct the channel to use
   * the direct executor.
   */
  public ManagedChannelFactory withDirectExecutor() {
    return new ManagedChannelFactory(type, interceptors, true);
  }
}
