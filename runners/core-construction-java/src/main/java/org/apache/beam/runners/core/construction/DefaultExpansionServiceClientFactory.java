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
package org.apache.beam.runners.core.construction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannel;

/** Default factory for ExpansionServiceClient used by External transform. */
public class DefaultExpansionServiceClientFactory implements ExpansionServiceClientFactory {
  private Map<Endpoints.ApiServiceDescriptor, ExpansionServiceClient> expansionServiceMap;
  private Map<Endpoints.ApiServiceDescriptor, ArtifactServiceClient> artifactServiceMap;
  private Function<Endpoints.ApiServiceDescriptor, ManagedChannel> expansionChannelFactory;
  private Function<Endpoints.ApiServiceDescriptor, ManagedChannel> artifactChannelFactory;


  private DefaultExpansionServiceClientFactory(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> expansionChannelFactory,
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> artifactChannelFactory) {
    this.expansionServiceMap = new ConcurrentHashMap<>();
    this.artifactServiceMap = new ConcurrentHashMap<>();
    this.expansionChannelFactory = expansionChannelFactory;
    this.artifactChannelFactory = artifactChannelFactory;
  }

  public static DefaultExpansionServiceClientFactory create(
      Function<Endpoints.ApiServiceDescriptor, ManagedChannel> channelFactory) {
    return new DefaultExpansionServiceClientFactory(channelFactory, channelFactory);
  }

  public static DefaultExpansionServiceClientFactory create(
    Function<Endpoints.ApiServiceDescriptor, ManagedChannel> expansionChannelFactory,
    Function<Endpoints.ApiServiceDescriptor, ManagedChannel> artifactChannelFactory) {
  return new DefaultExpansionServiceClientFactory(expansionChannelFactory, artifactChannelFactory);
}

  @Override
  public void close() throws Exception {
    for (ExpansionServiceClient client : expansionServiceMap.values()) {
      try (AutoCloseable closer = client) {}
    }
    for (ArtifactServiceClient client : artifactServiceMap.values()) {
      try (AutoCloseable closer = client) {}
    }
  }

  @Override
  public ExpansionServiceClient getExpansionServiceClient(Endpoints.ApiServiceDescriptor endpoint) {
    return expansionServiceMap.computeIfAbsent(
        endpoint,
        e ->
            new ExpansionServiceClient() {
              private final ManagedChannel channel = expansionChannelFactory.apply(endpoint);
              private final ExpansionServiceGrpc.ExpansionServiceBlockingStub service =
                  ExpansionServiceGrpc.newBlockingStub(channel);

              @Override
              public ExpansionApi.ExpansionResponse expand(ExpansionApi.ExpansionRequest request) {
                return service.expand(request);
              }

              @Override
              public void close() throws Exception {
                channel.shutdown();
              }
            });
  }

  @Override
  public ArtifactServiceClient getArtifactServiceClient(Endpoints.ApiServiceDescriptor endpoint) {
    return artifactServiceMap.computeIfAbsent(
        endpoint,
        e ->
            new ArtifactServiceClient() {
              private final ManagedChannel channel = artifactChannelFactory.apply(endpoint);
              private final ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub service =
              ArtifactRetrievalServiceGrpc.newBlockingStub(channel);

              @Override
              ArtifactApi.ResolveArtifactsResponse resolveArtifacts(ArtifactApi.ResolveArtifactsRequest request) {
                return service.resolveArtifacts(request);
              }

              @Override
              Iterator<ArtifactApi.GetArtifactResponse> getArtifact(ArtifactApi.GetArtifactRequest request) {
                return service.getArtifact(request);
              }

              @Override
              public void close() throws Exception {
                channel.shutdown();
              }
            });
  }
}
