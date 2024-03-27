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

import com.google.auth.Credentials;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.auth.VendoredCredentialsAdapter;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.auth.MoreCallCredentials;

/** Creates remote stubs to talk to Streaming Engine. */
@Internal
@ThreadSafe
public final class ChannelCachingRemoteStubFactory implements ChannelCachingStubFactory {
  private final Credentials gcpCredentials;
  private final ChannelCache channelCache;

  private ChannelCachingRemoteStubFactory(Credentials gcpCredentials, ChannelCache channelCache) {
    this.gcpCredentials = gcpCredentials;
    this.channelCache = channelCache;
  }

  public static ChannelCachingRemoteStubFactory create(
      Credentials gcpCredentials, ChannelCache channelCache) {
    return new ChannelCachingRemoteStubFactory(gcpCredentials, channelCache);
  }

  @Override
  public CloudWindmillServiceV1Alpha1Stub createWindmillServiceStub(
      WindmillServiceAddress serviceAddress) {
    CloudWindmillServiceV1Alpha1Stub windmillServiceStub =
        CloudWindmillServiceV1Alpha1Grpc.newStub(channelCache.get(serviceAddress));
    return serviceAddress.getKind() != WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS
        ? windmillServiceStub.withCallCredentials(
            MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials)))
        : windmillServiceStub;
  }

  @Override
  public CloudWindmillMetadataServiceV1Alpha1Stub createWindmillMetadataServiceStub(
      WindmillServiceAddress serviceAddress) {
    return CloudWindmillMetadataServiceV1Alpha1Grpc.newStub(channelCache.get(serviceAddress))
        .withCallCredentials(
            MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials)));
  }

  @Override
  public void remove(WindmillServiceAddress windmillServiceAddress) {
    channelCache.remove(windmillServiceAddress);
  }

  @Override
  public void shutdown() {
    channelCache.clear();
  }
}
