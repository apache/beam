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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformRequest;
import org.apache.beam.model.expansion.v1.ExpansionApi.DiscoverSchemaTransformResponse;
import org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionRequest;
import org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse;
import org.apache.beam.model.expansion.v1.ExpansionApi.SchemaTransformConfig;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.util.construction.ExpansionServiceClient;
import org.apache.beam.sdk.util.construction.ExpansionServiceClientFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Tests for {@link ExpansionService}. */
@RunWith(JUnit4.class)
public class ExpansionServiceTest {

  private ExpansionService expansionService;

  private ExpansionServiceClientFactory clientFactory;

  @Before
  public void setUp() throws Exception {
    List<ApiServiceDescriptor> endpoints = new ArrayList<>();
    ApiServiceDescriptor endpoint1 =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl("localhost:123").build();
    ApiServiceDescriptor endpoint2 =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl("localhost:456").build();
    endpoints.add(endpoint1);
    endpoints.add(endpoint2);
    clientFactory = Mockito.mock(ExpansionServiceClientFactory.class);
    expansionService = new ExpansionService(endpoints, clientFactory);
    // We do not run actual services in unit tests.
    expansionService.disableServiceCheck();
  }

  @Test
  public void testExpandFirstEndpoint() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.expand(Mockito.any()))
        .thenReturn(
            ExpansionResponse.newBuilder()
                .setTransform(
                    PTransform.newBuilder()
                        .setSpec(FunctionSpec.newBuilder().setUrn("dummy_urn_1")))
                .build());

    ExpansionRequest request = ExpansionRequest.newBuilder().build();
    StreamObserver<ExpansionResponse> responseObserver = Mockito.mock(StreamObserver.class);
    expansionService.expand(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(1)).expand(request);

    ArgumentCaptor<ExpansionResponse> expansionResponseCapture =
        ArgumentCaptor.forClass(ExpansionResponse.class);
    Mockito.verify(responseObserver).onNext(expansionResponseCapture.capture());
    assertEquals(
        "dummy_urn_1", expansionResponseCapture.getValue().getTransform().getSpec().getUrn());
  }

  @Test
  public void testExpandSecondEndpoint() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.expand(Mockito.any()))
        .thenReturn(ExpansionResponse.newBuilder().setError("expansion error").build())
        .thenReturn(
            ExpansionResponse.newBuilder()
                .setTransform(
                    PTransform.newBuilder()
                        .setSpec(FunctionSpec.newBuilder().setUrn("dummy_urn_1")))
                .build());

    ExpansionRequest request = ExpansionRequest.newBuilder().build();
    StreamObserver<ExpansionResponse> responseObserver = Mockito.mock(StreamObserver.class);
    expansionService.expand(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(2)).expand(request);

    ArgumentCaptor<ExpansionResponse> expansionResponseCapture =
        ArgumentCaptor.forClass(ExpansionResponse.class);
    Mockito.verify(responseObserver).onNext(expansionResponseCapture.capture());
    assertEquals(
        "dummy_urn_1", expansionResponseCapture.getValue().getTransform().getSpec().getUrn());
  }

  @Test
  public void testExpandFail() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.expand(Mockito.any()))
        .thenReturn(ExpansionResponse.newBuilder().setError("expansion error 1").build())
        .thenReturn(ExpansionResponse.newBuilder().setError("expansion error 2").build());

    ExpansionRequest request = ExpansionRequest.newBuilder().build();

    StreamObserver<ExpansionResponse> responseObserver = Mockito.mock(StreamObserver.class);
    expansionService.expand(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(2)).expand(request);

    ArgumentCaptor<ExpansionResponse> expansionResponseCapture =
        ArgumentCaptor.forClass(ExpansionResponse.class);
    Mockito.verify(responseObserver).onNext(expansionResponseCapture.capture());

    // Error response should contain errors from both expansion services.
    assertTrue(expansionResponseCapture.getValue().getError().contains("expansion error 1"));
    assertTrue(expansionResponseCapture.getValue().getError().contains("expansion error 2"));
  }

  @Test
  public void testObserverOneEndpointReturns() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.discover(Mockito.any()))
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder()
                .putSchemaTransformConfigs(
                    "schematransform_key_1", SchemaTransformConfig.newBuilder().build())
                .build())
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder().setError("discovery error 1").build());

    DiscoverSchemaTransformRequest request = DiscoverSchemaTransformRequest.newBuilder().build();
    StreamObserver<DiscoverSchemaTransformResponse> responseObserver =
        Mockito.mock(StreamObserver.class);
    expansionService.discoverSchemaTransform(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(2)).discover(request);

    ArgumentCaptor<DiscoverSchemaTransformResponse> discoverResponseCapture =
        ArgumentCaptor.forClass(DiscoverSchemaTransformResponse.class);
    Mockito.verify(responseObserver).onNext(discoverResponseCapture.capture());
    assertEquals(1, discoverResponseCapture.getValue().getSchemaTransformConfigsCount());
    assertTrue(
        discoverResponseCapture
            .getValue()
            .getSchemaTransformConfigsMap()
            .containsKey("schematransform_key_1"));
  }

  @Test
  public void testObserverMultipleEndpointsReturn() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.discover(Mockito.any()))
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder()
                .putSchemaTransformConfigs(
                    "schematransform_key_1", SchemaTransformConfig.newBuilder().build())
                .build())
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder()
                .putSchemaTransformConfigs(
                    "schematransform_key_2", SchemaTransformConfig.newBuilder().build())
                .build());

    DiscoverSchemaTransformRequest request = DiscoverSchemaTransformRequest.newBuilder().build();
    StreamObserver<DiscoverSchemaTransformResponse> responseObserver =
        Mockito.mock(StreamObserver.class);
    expansionService.discoverSchemaTransform(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(2)).discover(request);

    ArgumentCaptor<DiscoverSchemaTransformResponse> discoverResponseCapture =
        ArgumentCaptor.forClass(DiscoverSchemaTransformResponse.class);
    Mockito.verify(responseObserver).onNext(discoverResponseCapture.capture());
    assertEquals(2, discoverResponseCapture.getValue().getSchemaTransformConfigsCount());
    assertTrue(
        discoverResponseCapture
            .getValue()
            .getSchemaTransformConfigsMap()
            .containsKey("schematransform_key_1"));
    assertTrue(
        discoverResponseCapture
            .getValue()
            .getSchemaTransformConfigsMap()
            .containsKey("schematransform_key_2"));
  }

  @Test
  public void testObserverNoEndpointsReturn() {
    ExpansionServiceClient expansionServiceClient = Mockito.mock(ExpansionServiceClient.class);
    Mockito.when(clientFactory.getExpansionServiceClient(Mockito.any()))
        .thenReturn(expansionServiceClient);
    Mockito.when(expansionServiceClient.discover(Mockito.any()))
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder().setError("discovery error 1").build())
        .thenReturn(
            DiscoverSchemaTransformResponse.newBuilder().setError("discovery error 2").build());

    DiscoverSchemaTransformRequest request = DiscoverSchemaTransformRequest.newBuilder().build();
    StreamObserver<DiscoverSchemaTransformResponse> responseObserver =
        Mockito.mock(StreamObserver.class);
    expansionService.discoverSchemaTransform(request, responseObserver);
    Mockito.verify(expansionServiceClient, Mockito.times(2)).discover(request);

    ArgumentCaptor<DiscoverSchemaTransformResponse> discoverResponseCapture =
        ArgumentCaptor.forClass(DiscoverSchemaTransformResponse.class);
    Mockito.verify(responseObserver).onNext(discoverResponseCapture.capture());
    assertEquals(0, discoverResponseCapture.getValue().getSchemaTransformConfigsCount());
  }

  @Test
  public void testLoadTransformServiceConfig() throws Exception {
    URL transformServiceConfigFile = Resources.getResource("./test_transform_service_config.yml");
    TransformServiceConfig config =
        TransformServiceConfig.parseFromYamlStream(
            Files.newInputStream(Paths.get(transformServiceConfigFile.getPath())));
    assertEquals(2, config.getExpansionservices().size());
    assertTrue(config.getExpansionservices().contains("dummy-service-1:5001"));
    assertTrue(config.getExpansionservices().contains("dummy-service-2:5002"));
  }
}
