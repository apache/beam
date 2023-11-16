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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.core.construction.DefaultExpansionServiceClientFactory;
import org.apache.beam.runners.core.construction.ExpansionServiceClientFactory;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p54p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExpansionService extends ExpansionServiceGrpc.ExpansionServiceImplBase
    implements AutoCloseable {

  private static final ExpansionServiceClientFactory DEFAULT_EXPANSION_SERVICE_CLIENT_FACTORY =
      DefaultExpansionServiceClientFactory.create(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  private final ExpansionServiceClientFactory expansionServiceClientFactory;

  final List<Endpoints.ApiServiceDescriptor> endpoints;

  ExpansionService(
      List<Endpoints.ApiServiceDescriptor> endpoints,
      @Nullable ExpansionServiceClientFactory clientFactory) {
    this.endpoints = endpoints;
    this.expansionServiceClientFactory =
        clientFactory != null ? clientFactory : DEFAULT_EXPANSION_SERVICE_CLIENT_FACTORY;
  }

  @Override
  public void expand(
      ExpansionApi.ExpansionRequest request,
      StreamObserver<ExpansionApi.ExpansionResponse> responseObserver) {
    try {
      responseObserver.onNext(processExpand(request));
      responseObserver.onCompleted();
    } catch (RuntimeException exn) {
      responseObserver.onNext(
          ExpansionApi.ExpansionResponse.newBuilder()
              .setError(Throwables.getStackTraceAsString(exn))
              .build());
      responseObserver.onCompleted();
    }
  }

  @Override
  public void discoverSchemaTransform(
      ExpansionApi.DiscoverSchemaTransformRequest request,
      StreamObserver<ExpansionApi.DiscoverSchemaTransformResponse> responseObserver) {
    try {
      responseObserver.onNext(processDiscover(request));
      responseObserver.onCompleted();
    } catch (RuntimeException exn) {
      responseObserver.onNext(
          ExpansionApi.DiscoverSchemaTransformResponse.newBuilder()
              .setError(Throwables.getStackTraceAsString(exn))
              .build());
      responseObserver.onCompleted();
    }
  }

  /*package*/ ExpansionApi.ExpansionResponse processExpand(ExpansionApi.ExpansionRequest request) {
    // Trying out expansion services in order till one succeeds.
    // If all services fail, re-raises the last error.
    // TODO: when all services fail, return an aggregated error with errors from all services.
    ExpansionApi.ExpansionResponse lastErrorResponse = null;
    RuntimeException lastException = null;
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      try {
        ExpansionApi.ExpansionResponse response =
            expansionServiceClientFactory.getExpansionServiceClient(endpoint).expand(request);
        if (!response.getError().isEmpty()) {
          lastErrorResponse = response;
          continue;
        }
        return response;
      } catch (RuntimeException e) {
        lastException = e;
      }
    }
    if (lastErrorResponse != null) {
      return lastErrorResponse;
    } else if (lastException != null) {
      throw new RuntimeException("Expansion request to transform service failed.", lastException);
    } else {
      throw new RuntimeException("Could not process the expansion request: " + request);
    }
  }

  ExpansionApi.DiscoverSchemaTransformResponse processDiscover(
      ExpansionApi.DiscoverSchemaTransformRequest request) {
    // Trying out expansion services and aggregating all successful results.
    // If all services fail, return the last successful response any.
    // If there are no successful responses, re-raises the last error.
    List<ExpansionApi.DiscoverSchemaTransformResponse> successfulResponses = new ArrayList<>();
    ExpansionApi.DiscoverSchemaTransformResponse lastErrorResponse = null;
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      try {
        ExpansionApi.DiscoverSchemaTransformResponse response =
            expansionServiceClientFactory.getExpansionServiceClient(endpoint).discover(request);
        if (response.getError().isEmpty()) {
          successfulResponses.add(response);
        } else {
          lastErrorResponse = response;
        }
      } catch (RuntimeException e) {
        // We just ignore this error and continue to try the next expansion service.
      }
    }
    if (successfulResponses.isEmpty()) {
      if (lastErrorResponse != null) {
        return lastErrorResponse;
      } else {
        // Returning a discovery response with an error message since none of the expansion services
        // supported discovery.
        return ExpansionApi.DiscoverSchemaTransformResponse.newBuilder()
            .setError("Did not find any expansion service that support the discovery API")
            .build();
      }
    } else {
      return aggregateDiscoveryRespones(successfulResponses);
    }
  }

  private ExpansionApi.DiscoverSchemaTransformResponse aggregateDiscoveryRespones(
      List<ExpansionApi.DiscoverSchemaTransformResponse> responses) {
    ExpansionApi.DiscoverSchemaTransformResponse.Builder responseBuilder =
        ExpansionApi.DiscoverSchemaTransformResponse.newBuilder();
    for (ExpansionApi.DiscoverSchemaTransformResponse response : responses) {
      responseBuilder.putAllSchemaTransformConfigs(response.getSchemaTransformConfigsMap());
    }

    return responseBuilder.build();
  }

  @Override
  public void close() throws Exception {}
}
