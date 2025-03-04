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

import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress.AuthenticatedGcpServiceAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Channel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.alts.AltsChannelCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.netty.GrpcSslContexts;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.netty.NegotiationType;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.netty.handler.ssl.SslContext;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/** Utility class used to create different RPC Channels. */
public final class WindmillChannelFactory {
  public static final String LOCALHOST = "localhost";
  private static final int MAX_REMOTE_TRACE_EVENTS = 100;
  // 10MiB.
  private static final int WINDMILL_MAX_FLOW_CONTROL_WINDOW =
      NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW * 10;

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
            "Only GCP_SERVICE_ADDRESS and AUTHENTICATED_GCP_SERVICE_ADDRESS are supported"
                + " WindmillServiceAddresses.");
    }
  }

  private static ManagedChannel remoteDirectChannel(
      AuthenticatedGcpServiceAddress authenticatedGcpServiceAddress,
      int windmillServiceRpcChannelTimeoutSec) {
    return withDefaultChannelOptions(
            NettyChannelBuilder.forAddress(
                    authenticatedGcpServiceAddress.gcpServiceAddress().getHost(),
                    authenticatedGcpServiceAddress.gcpServiceAddress().getPort(),
                    new AltsChannelCredentials.Builder().build())
                .overrideAuthority(authenticatedGcpServiceAddress.authenticatingService()),
            windmillServiceRpcChannelTimeoutSec)
        .build();
  }

  public static ManagedChannel remoteChannel(
      HostAndPort endpoint, int windmillServiceRpcChannelTimeoutSec) {
    return withDefaultChannelOptions(
            NettyChannelBuilder.forAddress(endpoint.getHost(), endpoint.getPort()),
            windmillServiceRpcChannelTimeoutSec)
        .negotiationType(NegotiationType.TLS)
        .sslContext(dataflowGrpcSslContext(endpoint))
        .build();
  }

  @SuppressWarnings("nullness")
  private static SslContext dataflowGrpcSslContext(HostAndPort endpoint) {
    try {
      // Set ciphers(null) to not use GCM, which is disabled for Dataflow
      // due to it being horribly slow.
      return GrpcSslContexts.forClient().ciphers(null).build();
    } catch (SSLException sslException) {
      throw new WindmillChannelCreationException(endpoint, sslException);
    }
  }

  private static NettyChannelBuilder withDefaultChannelOptions(
      NettyChannelBuilder channelBuilder, int windmillServiceRpcChannelTimeoutSec) {
    if (windmillServiceRpcChannelTimeoutSec > 0) {
      channelBuilder
          .keepAliveTime(windmillServiceRpcChannelTimeoutSec, TimeUnit.SECONDS)
          .keepAliveTimeout(windmillServiceRpcChannelTimeoutSec, TimeUnit.SECONDS)
          .keepAliveWithoutCalls(true);
    }

    return channelBuilder
        .maxInboundMessageSize(Integer.MAX_VALUE)
        .maxTraceEvents(MAX_REMOTE_TRACE_EVENTS)
        // 1MiB
        .maxInboundMetadataSize(1024 * 1024)
        .flowControlWindow(WINDMILL_MAX_FLOW_CONTROL_WINDOW);
  }

  private static class WindmillChannelCreationException extends IllegalStateException {
    private WindmillChannelCreationException(HostAndPort endpoint, SSLException sourceException) {
      super(
          String.format(
              "Exception thrown when trying to create channel to endpoint={host:%s; port:%d}",
              endpoint.getHost(), endpoint.getPort()),
          sourceException);
    }
  }
}
