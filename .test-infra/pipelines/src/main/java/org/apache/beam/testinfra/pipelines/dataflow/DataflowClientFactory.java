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
package org.apache.beam.testinfra.pipelines.dataflow;

import com.google.dataflow.v1beta3.JobsV1Beta3Grpc;
import com.google.dataflow.v1beta3.MetricsV1Beta3Grpc;
import io.grpc.ManagedChannel;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.netty.NettyChannelBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Produces and caches blocking stub gRPC clients for the Dataflow API. */
@SuppressWarnings("ForbidNonVendoredGrpcProtobuf")
@Internal
final class DataflowClientFactory {

  static final DataflowClientFactory INSTANCE = new DataflowClientFactory();

  private DataflowClientFactory() {
    Thread closeChannelsHook = new Thread(this::closeAllChannels);
    Runtime.getRuntime().addShutdownHook(closeChannelsHook);
  }

  private JobsV1Beta3Grpc.@Nullable JobsV1Beta3BlockingStub cachedJobsClient;
  private @Nullable ManagedChannel cachedJobsClientChannel;
  private MetricsV1Beta3Grpc.@Nullable MetricsV1Beta3BlockingStub cachedMetricsClient;
  private @Nullable ManagedChannel cachedMetricsClientChannel;

  JobsV1Beta3Grpc.JobsV1Beta3BlockingStub getOrCreateJobsClient(
      DataflowClientFactoryConfiguration configuration) {
    if (cachedJobsClient == null) {
      cachedJobsClient =
          JobsV1Beta3Grpc.newBlockingStub(getOrCreateJobsClientChannel(configuration))
              .withCallCredentials(MoreCallCredentials.from(configuration.getCredentials()));
    }
    return cachedJobsClient;
  }

  MetricsV1Beta3Grpc.MetricsV1Beta3BlockingStub getOrCreateMetricsClient(
      DataflowClientFactoryConfiguration configuration) {
    if (cachedMetricsClient == null) {
      cachedMetricsClient =
          MetricsV1Beta3Grpc.newBlockingStub(getOrCreateMetricsClientChannel(configuration))
              .withCallCredentials(MoreCallCredentials.from(configuration.getCredentials()));
    }
    return cachedMetricsClient;
  }

  private @NonNull ManagedChannel getOrCreateJobsClientChannel(
      DataflowClientFactoryConfiguration configuration) {
    if (cachedJobsClientChannel == null) {
      cachedJobsClientChannel = channel(configuration);
    }
    return cachedJobsClientChannel;
  }

  private @NonNull ManagedChannel getOrCreateMetricsClientChannel(
      DataflowClientFactoryConfiguration configuration) {
    if (cachedMetricsClientChannel == null) {
      cachedMetricsClientChannel = channel(configuration);
    }
    return cachedMetricsClientChannel;
  }

  private static ManagedChannel channel(DataflowClientFactoryConfiguration configuration) {
    return NettyChannelBuilder.forTarget(configuration.getDataflowTarget()).build();
  }

  void closeAllChannels() {
    close(INSTANCE.cachedJobsClientChannel);
    close(INSTANCE.cachedMetricsClientChannel);
  }

  private static void close(@Nullable ManagedChannel channel) {
    if (channel == null) {
      return;
    }
    channel.shutdown();
    try {
      boolean ignored = channel.awaitTermination(1L, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
    }
  }
}
