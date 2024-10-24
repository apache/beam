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
package org.apache.beam.sdk.transformservice;

import java.util.Iterator;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ArtifactService extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements FnService {

  final List<Endpoints.ApiServiceDescriptor> endpoints;

  final @Nullable ArtifactResolver artifactResolver;

  ArtifactService(
      List<Endpoints.ApiServiceDescriptor> endpoints, @Nullable ArtifactResolver artifactResolver) {
    this.endpoints = endpoints;
    this.artifactResolver = artifactResolver;
  }

  @Override
  public void resolveArtifacts(
      ArtifactApi.ResolveArtifactsRequest request,
      StreamObserver<ArtifactApi.ResolveArtifactsResponse> responseObserver) {
    // Trying out artifact services in order till one succeeds.
    // If all services fail, re-raises the last error.
    // TODO: when all services fail, return an aggregated error with errors from all services.
    RuntimeException lastError = null;
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      ArtifactResolver artifactResolver =
          this.artifactResolver != null
              ? this.artifactResolver
              : new EndpointBasedArtifactResolver(endpoint.getUrl());
      try {
        responseObserver.onNext(artifactResolver.resolveArtifacts(request));
        responseObserver.onCompleted();
        return;
      } catch (RuntimeException exn) {
        lastError = exn;
      } finally {
        if (this.artifactResolver == null) {
          artifactResolver.shutdown();
        }
      }
    }

    if (lastError == null) {
      lastError =
          new RuntimeException(
              "Could not successfully resolve the artifact for the request " + request);
    }
    throw lastError;
  }

  @Override
  public void getArtifact(
      ArtifactApi.GetArtifactRequest request,
      StreamObserver<ArtifactApi.GetArtifactResponse> responseObserver) {
    // Trying out artifact services in order till one succeeds.
    // If all services fail, re-raises the last error.
    // TODO: when all services fail, return an aggregated error with errors from all services.
    RuntimeException lastError = null;
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      ArtifactResolver artifactResolver =
          this.artifactResolver != null
              ? this.artifactResolver
              : new EndpointBasedArtifactResolver(endpoint.getUrl());
      try {
        Iterator<ArtifactApi.GetArtifactResponse> responseIterator =
            artifactResolver.getArtifact(request);
        while (responseIterator.hasNext()) {
          responseObserver.onNext(responseIterator.next());
        }

        responseObserver.onCompleted();
        return;
      } catch (RuntimeException exn) {
        lastError = exn;
      } finally {
        if (this.artifactResolver == null) {
          artifactResolver.shutdown();
        }
      }
    }

    if (lastError == null) {
      lastError =
          new RuntimeException(
              "Could not successfully get the artifact for the request " + request);
    }
    throw lastError;
  }

  @Override
  public void close() {
    // Nothing to close.
  }

  interface ArtifactResolver {
    ArtifactApi.ResolveArtifactsResponse resolveArtifacts(
        ArtifactApi.ResolveArtifactsRequest request);

    Iterator<ArtifactApi.GetArtifactResponse> getArtifact(ArtifactApi.GetArtifactRequest request);

    void shutdown();
  }

  static class EndpointBasedArtifactResolver implements ArtifactResolver {

    private ArtifactRetrievalServiceBlockingStub retrievalStub;
    private ManagedChannel channel;

    EndpointBasedArtifactResolver(String url) {
      channel =
          ManagedChannelBuilder.forTarget(url)
              .usePlaintext()
              .maxInboundMessageSize(Integer.MAX_VALUE)
              .build();
      this.retrievalStub = ArtifactRetrievalServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public ResolveArtifactsResponse resolveArtifacts(ResolveArtifactsRequest request) {
      return retrievalStub.resolveArtifacts(request);
    }

    @Override
    public Iterator<GetArtifactResponse> getArtifact(GetArtifactRequest request) {
      return retrievalStub.getArtifact(request);
    }

    @Override
    public void shutdown() {
      this.channel.shutdown();
    }
  }
}
