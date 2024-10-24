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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import com.google.auth.Credentials;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;

public class WindmillStubFactoryFactoryImpl implements WindmillStubFactoryFactory {

  private final int windmillServiceRpcChannelAliveTimeoutSec;
  private final Credentials gcpCredential;

  public WindmillStubFactoryFactoryImpl(DataflowWorkerHarnessOptions workerOptions) {
    this.gcpCredential = workerOptions.getGcpCredential();
    this.windmillServiceRpcChannelAliveTimeoutSec =
        workerOptions.getWindmillServiceRpcChannelAliveTimeoutSec();
  }

  @Override
  public WindmillStubFactory makeWindmillStubFactory(boolean useIsolatedChannels) {
    Function<WindmillServiceAddress, ManagedChannel> channelFactory =
        serviceAddress -> remoteChannel(serviceAddress, windmillServiceRpcChannelAliveTimeoutSec);
    ChannelCache channelCache =
        ChannelCache.create(
            serviceAddress ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress via calling the channelFactory, else just directly return the
                // RPC channel.
                useIsolatedChannels
                    ? IsolationChannel.create(() -> channelFactory.apply(serviceAddress))
                    : channelFactory.apply(serviceAddress));
    return ChannelCachingRemoteStubFactory.create(gcpCredential, channelCache);
  }
}
