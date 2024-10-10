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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress.AuthenticatedGcpServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ForwardingChannelBuilder2;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.alts.AltsChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.netty.GrpcSslContexts;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.netty.NegotiationType;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Utility class used to create different RPC Channels. */
public final class WindmillChannelFactory {
  public static final String LOCALHOST = "localhost";
  private static final int MAX_REMOTE_TRACE_EVENTS = 100;

  private WindmillChannelFactory() {}

  public static ManagedChannel inProcessChannel(String channelName) {
    return InProcessChannelBuilder.forName(channelName).directExecutor().build();
  }

  public static Channel localhostChannel(int port) {
    return NettyChannelBuilder.forAddress(LOCALHOST, port)
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .negotiationType(NegotiationType.PLAINTEXT)
        .build();
  }

  public static ManagedChannel remoteChannel(
      WindmillServiceAddress windmillServiceAddress, int windmillServiceRpcChannelTimeoutSec) {
    switch (windmillServiceAddress.getKind()) {
      case GCP_SERVICE_ADDRESS:
        return remoteChannel(
            windmillServiceAddress.gcpServiceAddress(), windmillServiceRpcChannelTimeoutSec);
        // switch is exhaustive will never happen.
      case AUTHENTICATED_GCP_SERVICE_ADDRESS:
        return remoteDirectChannel(
            windmillServiceAddress.authenticatedGcpServiceAddress(),
            windmillServiceRpcChannelTimeoutSec);
      default:
        throw new UnsupportedOperationException(
            "Only IPV6, GCP_SERVICE_ADDRESS, AUTHENTICATED_GCP_SERVICE_ADDRESS are supported"
                + " WindmillServiceAddresses.");
    }
  }

  static ManagedChannel remoteDirectChannel(
      AuthenticatedGcpServiceAddress authenticatedGcpServiceAddress,
      int windmillServiceRpcChannelTimeoutSec) {
    return withDefaultChannelOptions(
            AltsChannelBuilder.forAddress(
                    authenticatedGcpServiceAddress.gcpServiceAddress().getHost(),
                    authenticatedGcpServiceAddress.gcpServiceAddress().getPort())
                .overrideAuthority(authenticatedGcpServiceAddress.authenticatingService()),
            windmillServiceRpcChannelTimeoutSec)
        .build();
  }

  public static ManagedChannel remoteChannel(
      HostAndPort endpoint, int windmillServiceRpcChannelTimeoutSec) {
    try {
      return createRemoteChannel(
          NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()),
          windmillServiceRpcChannelTimeoutSec);
    } catch (SSLException sslException) {
      throw new WindmillChannelCreationException(endpoint, sslException);
    }
  }

  public static Channel remoteChannel(
      Inet6Address directEndpoint, int port, int windmillServiceRpcChannelTimeoutSec) {
    try {
      return createRemoteChannel(
          NettyChannelBuilder.forAddress(new InetSocketAddress(directEndpoint, port)),
          windmillServiceRpcChannelTimeoutSec);
    } catch (SSLException sslException) {
      throw new WindmillChannelCreationException(directEndpoint.toString(), sslException);
    }
  }

  @SuppressWarnings("nullness")
  private static ManagedChannel createRemoteChannel(
      NettyChannelBuilder channelBuilder, int windmillServiceRpcChannelTimeoutSec)
      throws SSLException {
    return withDefaultChannelOptions(channelBuilder, windmillServiceRpcChannelTimeoutSec)
        .flowControlWindow(10 * 1024 * 1024)
        .negotiationType(NegotiationType.TLS)
        // Set ciphers(null) to not use GCM, which is disabled for Dataflow
        // due to it being horribly slow.
        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
        .build();
  }

  private static <T extends ForwardingChannelBuilder2<T>> T withDefaultChannelOptions(
      T channelBuilder, int windmillServiceRpcChannelTimeoutSec) {
    if (windmillServiceRpcChannelTimeoutSec > 0) {
      channelBuilder
          .keepAliveTime(windmillServiceRpcChannelTimeoutSec, TimeUnit.SECONDS)
          .keepAliveTimeout(windmillServiceRpcChannelTimeoutSec, TimeUnit.SECONDS)
          .keepAliveWithoutCalls(true);
    }

    return channelBuilder
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .maxTraceEvents(MAX_REMOTE_TRACE_EVENTS)
        .maxInboundMetadataSize(1024 * 1024);
  }

  public static class WindmillChannelCreationException extends IllegalStateException {
    private WindmillChannelCreationException(HostAndPort endpoint, SSLException sourceException) {
      super(
          String.format(
              "Exception thrown when trying to create channel to endpoint={host:%s; port:%d}",
              endpoint.getHost(), endpoint.getPort()),
          sourceException);
    }

    WindmillChannelCreationException(String directEndpoint, Throwable sourceException) {
      super(
          String.format(
              "Exception thrown when trying to create channel to endpoint={%s}", directEndpoint),
          sourceException);
    }
  }
}
