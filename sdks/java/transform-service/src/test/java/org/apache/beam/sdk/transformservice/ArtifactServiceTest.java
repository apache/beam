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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ResolveArtifactsResponse;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.sdk.transformservice.ArtifactService.ArtifactResolver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/** Tests for {@link ArtifactService}. */
public class ArtifactServiceTest {

  private ArtifactResolver artifactResolver;

  private ArtifactService artifactService;

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setUp() {
    List<ApiServiceDescriptor> endpoints = new ArrayList<>();
    ApiServiceDescriptor endpoint1 =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl("localhost:123").build();
    ApiServiceDescriptor endpoint2 =
        Endpoints.ApiServiceDescriptor.newBuilder().setUrl("localhost:456").build();
    endpoints.add(endpoint1);
    endpoints.add(endpoint2);
    this.artifactResolver = Mockito.mock(ArtifactResolver.class);
    this.artifactService = new ArtifactService(endpoints, artifactResolver);
  }

  @Test
  public void testArtifactResolveFirstEndpoint() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.ResolveArtifactsRequest request =
        ArtifactApi.ResolveArtifactsRequest.newBuilder().addArtifacts(fileArtifact).build();
    Mockito.when(artifactResolver.resolveArtifacts(request))
        .thenReturn(ArtifactApi.ResolveArtifactsResponse.newBuilder().build());

    StreamObserver<ResolveArtifactsResponse> responseObserver = Mockito.mock(StreamObserver.class);
    artifactService.resolveArtifacts(request, responseObserver);

    Mockito.verify(artifactResolver, Mockito.times(1)).resolveArtifacts(request);
  }

  @Test
  public void testArtifactResolveSecondEndpoint() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.ResolveArtifactsRequest request =
        ArtifactApi.ResolveArtifactsRequest.newBuilder().addArtifacts(fileArtifact).build();
    StreamObserver<ResolveArtifactsResponse> responseObserver = Mockito.mock(StreamObserver.class);

    Mockito.when(artifactResolver.resolveArtifacts(request))
        .thenThrow(new RuntimeException("Failing artifact resolve"))
        .thenReturn(ArtifactApi.ResolveArtifactsResponse.newBuilder().build());
    artifactService.resolveArtifacts(request, responseObserver);

    Mockito.verify(artifactResolver, Mockito.times(2)).resolveArtifacts(request);
  }

  @Test
  public void testArtifactResolveFail() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.ResolveArtifactsRequest request =
        ArtifactApi.ResolveArtifactsRequest.newBuilder().addArtifacts(fileArtifact).build();
    StreamObserver<ResolveArtifactsResponse> responseObserver = Mockito.mock(StreamObserver.class);

    Mockito.when(artifactResolver.resolveArtifacts(request))
        .thenThrow(new RuntimeException("Failing artifact resolve"));

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Failing artifact resolve");
    artifactService.resolveArtifacts(request, responseObserver);
  }

  @Test
  public void testArtifactGetFirstEndpoint() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.GetArtifactRequest request =
        ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(fileArtifact).build();
    Mockito.when(artifactResolver.getArtifact(request))
        .thenReturn(
            ImmutableList.of(ArtifactApi.GetArtifactResponse.newBuilder().build()).iterator());

    StreamObserver<GetArtifactResponse> responseObserver = Mockito.mock(StreamObserver.class);
    artifactService.getArtifact(request, responseObserver);

    Mockito.verify(artifactResolver, Mockito.times(1)).getArtifact(request);
  }

  @Test
  public void testArtifactGetSecondEndpoint() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.GetArtifactRequest request =
        ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(fileArtifact).build();
    StreamObserver<GetArtifactResponse> responseObserver = Mockito.mock(StreamObserver.class);

    Mockito.when(artifactResolver.getArtifact(request))
        .thenThrow(new RuntimeException("Failing artifact resolve"))
        .thenReturn(
            ImmutableList.of(ArtifactApi.GetArtifactResponse.newBuilder().build()).iterator());
    artifactService.getArtifact(request, responseObserver);

    Mockito.verify(artifactResolver, Mockito.times(2)).getArtifact(request);
  }

  @Test
  public void testArtifactGetFail() {
    Path path = Paths.get("dummypath");

    RunnerApi.ArtifactInformation fileArtifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.ArtifactFilePayload.newBuilder()
                    .setPath(path.toString())
                    .build()
                    .toByteString())
            .setRoleUrn("")
            .build();

    ArtifactApi.GetArtifactRequest request =
        ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(fileArtifact).build();
    StreamObserver<GetArtifactResponse> responseObserver = Mockito.mock(StreamObserver.class);

    Mockito.when(artifactResolver.getArtifact(request))
        .thenThrow(new RuntimeException("Failing artifact resolve"));

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("Failing artifact resolve");
    artifactService.getArtifact(request, responseObserver);
  }
}
