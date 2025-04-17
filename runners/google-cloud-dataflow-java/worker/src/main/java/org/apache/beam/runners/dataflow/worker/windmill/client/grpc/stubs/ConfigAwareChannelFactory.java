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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannels.remoteChannel;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Internal;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

/** Creates gRPC channels based on the current {@link StreamingGlobalConfig}. */
@Internal
@ThreadSafe
public final class ConfigAwareChannelFactory implements WindmillChannelFactory {
  private final int windmillServiceRpcChannelAliveTimeoutSec;
  @MonotonicNonNull private StreamingGlobalConfig currentConfig = null;

  public ConfigAwareChannelFactory(int windmillServiceRpcChannelAliveTimeoutSec) {
    this.windmillServiceRpcChannelAliveTimeoutSec = windmillServiceRpcChannelAliveTimeoutSec;
  }

  @Override
  public synchronized ManagedChannel create(
      Windmill.UserWorkerGrpcFlowControlSettings flowControlSettings,
      WindmillServiceAddress serviceAddress) {
    return currentConfig != null
            && currentConfig.userWorkerJobSettings().getUseWindmillIsolatedChannels()
        // IsolationChannel will create and manage separate RPC channels to the same
        // serviceAddress via calling the channelFactory, else just directly return
        // the RPC channel.
        ? IsolationChannel.create(
            () ->
                remoteChannel(
                    serviceAddress.getServiceAddress(),
                    windmillServiceRpcChannelAliveTimeoutSec,
                    flowControlSettings))
        : remoteChannel(
            serviceAddress.getServiceAddress(),
            windmillServiceRpcChannelAliveTimeoutSec,
            flowControlSettings);
  }

  public synchronized boolean tryConsumeJobConfig(StreamingGlobalConfig config) {
    if (currentConfig == null
        || config.userWorkerJobSettings().getUseWindmillIsolatedChannels()
            != currentConfig.userWorkerJobSettings().getUseWindmillIsolatedChannels()) {
      currentConfig = config;
      return true;
    }
    return false;
  }
}
