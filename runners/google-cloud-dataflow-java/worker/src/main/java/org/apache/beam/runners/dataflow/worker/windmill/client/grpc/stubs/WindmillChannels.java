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
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.UserWorkerGrpcFlowControlSettings;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
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
public final class WindmillChannels {
  public static final String LOCALHOST = "localhost";
  private static final int MAX_REMOTE_TRACE_EVENTS = 100;
  // 1MiB.
  private static final int MAX_INBOUND_METADATA_SIZE_BYTES = 1024 * 1024;
  // 10MiB. Roughly 2x max message size.
  private static final int WINDMILL_MIN_FLOW_CONTROL_WINDOW =
      NettyChannelBuilder.DEFAULT_FLOW_CONTROL_WINDOW * 10;
  public static final UserWorkerGrpcFlowControlSettings DEFAULT_CLOUDPATH_FLOW_CONTROL_SETTINGS =
      UserWorkerGrpcFlowControlSettings.newBuilder()
          .setFlowControlWindowBytes(WINDMILL_MIN_FLOW_CONTROL_WINDOW)
          .setEnableAutoFlowControl(false)
          .build();
  // 100MiB.
  private static final int DIRECTPATH_MIN_FLOW_CONTROL_WINDOW =
      WINDMILL_MIN_FLOW_CONTROL_WINDOW * 10;
  // 100MiB.
  private static final int DIRECTPATH_ON_READY_THRESHOLD = WINDMILL_MIN_FLOW_CONTROL_WINDOW * 10;
  // User a bigger flow control window and onready threshold for directpath to prevent churn when
  // gRPC is trying to flush gRPCs over the wire. If onReadyThreshold and flowControlWindowBytes are
  // too low, it was observed in testing that the gRPC channel can get stuck in a "not-ready" state
  // until stream deadline.
  public static final UserWorkerGrpcFlowControlSettings DEFAULT_DIRECTPATH_FLOW_CONTROL_SETTINGS =
      UserWorkerGrpcFlowControlSettings.newBuilder()
          .setFlowControlWindowBytes(DIRECTPATH_MIN_FLOW_CONTROL_WINDOW)
          .setOnReadyThresholdBytes(DIRECTPATH_ON_READY_THRESHOLD)
          // Prevent gRPC from automatically resizing the window. If we have things to send/receive
          // from Windmill we want to do it right away. There are internal pushback mechanisms in
          // the user worker and Windmill that attempt to guard the process from OOMing (i.e
          // MemoryMonitor.java).
          .setEnableAutoFlowControl(false)
          .build();

  private WindmillChannels() {}

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
      WindmillServiceAddress windmillServiceAddress,
      int windmillServiceRpcChannelTimeoutSec,
      UserWorkerGrpcFlowControlSettings flowControlSettings) {
    switch (windmillServiceAddress.getKind()) {
      case GCP_SERVICE_ADDRESS:
        return remoteChannel(
            windmillServiceAddress.gcpServiceAddress(),
            windmillServiceRpcChannelTimeoutSec,
            flowControlSettings);
      case AUTHENTICATED_GCP_SERVICE_ADDRESS:
        return remoteDirectChannel(
            windmillServiceAddress.authenticatedGcpServiceAddress(),
            windmillServiceRpcChannelTimeoutSec,
            flowControlSettings);
      default:
        throw new UnsupportedOperationException(
            "Only GCP_SERVICE_ADDRESS and AUTHENTICATED_GCP_SERVICE_ADDRESS are supported"
                + " WindmillServiceAddresses.");
    }
  }

  private static ManagedChannel remoteDirectChannel(
      AuthenticatedGcpServiceAddress authenticatedGcpServiceAddress,
      int windmillServiceRpcChannelTimeoutSec,
      UserWorkerGrpcFlowControlSettings flowControlSettings) {
    NettyChannelBuilder channelBuilder =
        withDefaultChannelOptions(
            NettyChannelBuilder.forAddress(
                    authenticatedGcpServiceAddress.gcpServiceAddress().getHost(),
                    // Ports are required for direct channels.
                    authenticatedGcpServiceAddress.gcpServiceAddress().getPort(),
                    new AltsChannelCredentials.Builder().build())
                .overrideAuthority(authenticatedGcpServiceAddress.authenticatingService()),
            windmillServiceRpcChannelTimeoutSec);

    return withFlowControlSettings(
            channelBuilder, flowControlSettings, DIRECTPATH_MIN_FLOW_CONTROL_WINDOW)
        .build();
  }

  public static ManagedChannel remoteChannel(
      HostAndPort endpoint,
      int windmillServiceRpcChannelTimeoutSec,
      UserWorkerGrpcFlowControlSettings flowControlSettings) {
    NettyChannelBuilder channelBuilder =
        withDefaultChannelOptions(
                NettyChannelBuilder.forAddress(
                    endpoint.getHost(),
                    endpoint.hasPort()
                        ? endpoint.getPort()
                        : WindmillEndpoints.DEFAULT_WINDMILL_SERVICE_PORT),
                windmillServiceRpcChannelTimeoutSec)
            .negotiationType(NegotiationType.TLS)
            .sslContext(dataflowGrpcSslContext(endpoint));

    return withFlowControlSettings(
            channelBuilder, flowControlSettings, WINDMILL_MIN_FLOW_CONTROL_WINDOW)
        .build();
  }

  private static NettyChannelBuilder withFlowControlSettings(
      NettyChannelBuilder channelBuilder,
      UserWorkerGrpcFlowControlSettings flowControlSettings,
      int defaultFlowControlBytes) {
    int flowControlWindowSizeBytes =
        Math.max(defaultFlowControlBytes, flowControlSettings.getFlowControlWindowBytes());
    return flowControlSettings.getEnableAutoFlowControl()
        // Enable auto flow control with an initial window of flowControlWindowSizeBytes. gRPC may
        // resize this value based on throughput.
        ? channelBuilder.initialFlowControlWindow(flowControlWindowSizeBytes)
        : channelBuilder.flowControlWindow(flowControlWindowSizeBytes);
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
        .maxInboundMetadataSize(MAX_INBOUND_METADATA_SIZE_BYTES);
  }

  private static class WindmillChannelCreationException extends IllegalStateException {
    private WindmillChannelCreationException(HostAndPort endpoint, SSLException sourceException) {
      super(
          String.format("Exception thrown when trying to create channel to %s", endpoint),
          sourceException);
    }
  }
}
