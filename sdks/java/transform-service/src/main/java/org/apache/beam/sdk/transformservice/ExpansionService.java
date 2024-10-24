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

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse;
import org.apache.beam.model.expansion.v1.ExpansionServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.util.construction.DefaultExpansionServiceClientFactory;
import org.apache.beam.sdk.util.construction.ExpansionServiceClientFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExpansionService extends ExpansionServiceGrpc.ExpansionServiceImplBase
    implements AutoCloseable {

  private static final ExpansionServiceClientFactory DEFAULT_EXPANSION_SERVICE_CLIENT_FACTORY =
      DefaultExpansionServiceClientFactory.create(
          endPoint -> ManagedChannelBuilder.forTarget(endPoint.getUrl()).usePlaintext().build());

  private final ExpansionServiceClientFactory expansionServiceClientFactory;

  final List<Endpoints.ApiServiceDescriptor> endpoints;

  private boolean checkedAllServices = false;

  private static final long SERVICE_CHECK_TIMEOUT_MILLIS = 60000;

  private boolean disableServiceCheck = false;

  ExpansionService(
      List<Endpoints.ApiServiceDescriptor> endpoints,
      @Nullable ExpansionServiceClientFactory clientFactory) {
    this.endpoints = endpoints;
    this.expansionServiceClientFactory =
        clientFactory != null ? clientFactory : DEFAULT_EXPANSION_SERVICE_CLIENT_FACTORY;
  }

  // Waits till all expansion services are ready.
  private void waitForAllServicesToBeReady() throws TimeoutException {
    if (disableServiceCheck) {
      // Service check disabled. Just returning.
      return;
    }

    outer:
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      long start = System.currentTimeMillis();
      long duration = 10;
      while (System.currentTimeMillis() - start < SERVICE_CHECK_TIMEOUT_MILLIS) {
        try {
          String url = endpoint.getUrl();
          int portIndex = url.lastIndexOf(":");
          if (portIndex <= 0) {
            throw new RuntimeException(
                "Expected the endpoint to be of the form <host>:<port> but received " + url);
          }
          int port = Integer.parseInt(url.substring(portIndex + 1));
          String host = url.substring(0, portIndex);
          new Socket(host, port).close();
          // Current service is up. Checking the next one.
          continue outer;
        } catch (IOException exn) {
          try {
            Thread.sleep(duration);
          } catch (InterruptedException e) {
            // Ignore
          }
          duration = (long) (duration * 1.2);
        }
      }
      throw new TimeoutException(
          "Timeout waiting for the service "
              + endpoint.getUrl()
              + " to startup after "
              + (System.currentTimeMillis() - start)
              + " milliseconds.");
    }
  }

  @VisibleForTesting
  void disableServiceCheck() {
    disableServiceCheck = true;
  }

  @Override
  public void expand(
      ExpansionApi.ExpansionRequest request,
      StreamObserver<ExpansionApi.ExpansionResponse> responseObserver) {
    if (!checkedAllServices) {
      try {
        waitForAllServicesToBeReady();
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }
      checkedAllServices = true;
    }
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
    if (!checkedAllServices) {
      try {
        waitForAllServicesToBeReady();
      } catch (TimeoutException e) {
        throw new RuntimeException(e);
      }
      checkedAllServices = true;
    }
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

  private ExpansionApi.ExpansionResponse getAggregatedErrorResponse(
      Map<String, ExpansionApi.ExpansionResponse> errorResponses) {
    StringBuilder errorMessageBuilder = new StringBuilder();

    errorMessageBuilder.append(
        "Aggregated errors from " + errorResponses.size() + " expansion services." + "\n");
    for (Map.Entry<String, ExpansionApi.ExpansionResponse> entry : errorResponses.entrySet()) {
      errorMessageBuilder.append(
          "Error from expansion service "
              + entry.getKey()
              + ": "
              + entry.getValue().getError()
              + "\n");
    }

    return errorResponses
        .values()
        .iterator()
        .next()
        .toBuilder()
        .setError(errorMessageBuilder.toString())
        .build();
  }

  ExpansionApi.ExpansionResponse processExpand(ExpansionApi.ExpansionRequest request) {
    // Trying out expansion services in order till one succeeds.
    // If all services fail, re-raises the last error.
    Map<String, ExpansionResponse> errorResponses = new HashMap<>();
    RuntimeException lastException = null;
    for (Endpoints.ApiServiceDescriptor endpoint : endpoints) {
      try {
        ExpansionApi.ExpansionResponse response =
            expansionServiceClientFactory.getExpansionServiceClient(endpoint).expand(request);
        if (!response.getError().isEmpty()) {
          errorResponses.put(endpoint.getUrl(), response);
          continue;
        }
        return response;
      } catch (RuntimeException e) {
        lastException = e;
      }
    }
    if (lastException != null) {
      throw new RuntimeException("Expansion request to transform service failed.", lastException);
    }
    if (!errorResponses.isEmpty()) {
      return getAggregatedErrorResponse(errorResponses);
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
