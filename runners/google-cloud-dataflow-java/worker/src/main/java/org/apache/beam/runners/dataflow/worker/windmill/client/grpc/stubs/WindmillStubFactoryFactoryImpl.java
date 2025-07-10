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

import com.google.auth.Credentials;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;

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
    ChannelCache channelCache =
        ChannelCache.create(
            (flowControlSettings, serviceAddress) ->
                // IsolationChannel will create and manage separate RPC channels to the same
                // serviceAddress via calling the channelFactory, else just directly return the
                // RPC channel.
                useIsolatedChannels
                    ? IsolationChannel.create(
                        () ->
                            remoteChannel(
                                serviceAddress.getServiceAddress(),
                                windmillServiceRpcChannelAliveTimeoutSec,
                                flowControlSettings))
                    : remoteChannel(
                        serviceAddress.getServiceAddress(),
                        windmillServiceRpcChannelAliveTimeoutSec,
                        flowControlSettings));
    return ChannelCachingRemoteStubFactory.create(gcpCredential, channelCache);
  }
}
