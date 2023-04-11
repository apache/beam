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
package org.apache.beam.transformservice.controller;

import java.util.Iterator;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.fn.server.FnService;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;

@SuppressWarnings("nullness")
public class ArtifactService extends ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceImplBase
    implements FnService {

  final List<Endpoints.ApiServiceDescriptor> endpoints;

  ArtifactService(List<Endpoints.ApiServiceDescriptor> endpoints) {
    this.endpoints = endpoints;
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
      try {
        ManagedChannel channel =
            ManagedChannelBuilder.forTarget(endpoint.getUrl())
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .build();

        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub =
            ArtifactRetrievalServiceGrpc.newBlockingStub(channel);
        responseObserver.onNext(retrievalStub.resolveArtifacts(request));

        responseObserver.onCompleted();
      } catch (RuntimeException exn) {
        lastError = exn;
      }
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
      try {
        ManagedChannel channel =
            ManagedChannelBuilder.forTarget(endpoint.getUrl())
                .usePlaintext()
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .build();

        ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalStub =
            ArtifactRetrievalServiceGrpc.newBlockingStub(channel);

        Iterator<ArtifactApi.GetArtifactResponse> responseIterator =
            retrievalStub.getArtifact(request);
        while (responseIterator.hasNext()) {
          responseObserver.onNext(responseIterator.next());
        }

        responseObserver.onCompleted();
      } catch (RuntimeException exn) {
        lastError = exn;
      }
    }

    throw lastError;
  }

  @Override
  public void close() {
    // Nothing to close.
  }
}
