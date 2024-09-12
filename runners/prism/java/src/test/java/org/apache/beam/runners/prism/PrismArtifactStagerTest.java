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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService.EMBEDDED_ARTIFACT_URN;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismArtifactStager}. */
@RunWith(JUnit4.class)
public class PrismArtifactStagerTest {

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  final ArtifactStagingService stagingService =
      new ArtifactStagingService(new TestDestinationProvider());

  @Test
  public void givenValidArtifacts_stages()
      throws IOException, ExecutionException, InterruptedException {
    PrismArtifactStager underTest = prismArtifactStager(validArtifacts());
    assertThat(underTest.getManagedChannel().isShutdown()).isFalse();
    underTest.stage();
    assertThat(stagingService.getStagedArtifacts(underTest.getStagingSessionToken())).isNotEmpty();
    underTest.close();
    assertThat(underTest.getManagedChannel().isShutdown()).isTrue();
  }

  @Test
  public void givenErrors_performsGracefulCleanup() throws IOException {
    PrismArtifactStager underTest = prismArtifactStager(invalidArtifacts());
    assertThat(underTest.getManagedChannel().isShutdown()).isFalse();
    ExecutionException error = assertThrows(ExecutionException.class, underTest::stage);
    assertThat(error.getMessage()).contains("Unexpected artifact type: invalid-type-urn");
    assertThat(underTest.getManagedChannel().isShutdown()).isFalse();
    underTest.close();
    assertThat(underTest.getManagedChannel().isShutdown()).isTrue();
  }

  private PrismArtifactStager prismArtifactStager(
      Map<String, List<RunnerApi.ArtifactInformation>> artifacts) throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    ArtifactRetrievalService retrievalService = new ArtifactRetrievalService();
    String stagingToken = "staging-token";
    stagingService.registerJob(stagingToken, artifacts);

    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(stagingService)
            .addService(retrievalService)
            .build()
            .start());

    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).build());

    return PrismArtifactStager.builder()
        .setStagingEndpoint("ignore")
        .setStagingSessionToken(stagingToken)
        .setManagedChannel(channel)
        .build();
  }

  private Map<String, List<RunnerApi.ArtifactInformation>> validArtifacts() {
    return ImmutableMap.of(
        "env1",
        Collections.singletonList(
            RunnerApi.ArtifactInformation.newBuilder()
                .setTypeUrn(EMBEDDED_ARTIFACT_URN)
                .setTypePayload(
                    RunnerApi.EmbeddedFilePayload.newBuilder()
                        .setData(ByteString.copyFromUtf8("type-payload"))
                        .build()
                        .toByteString())
                .setRoleUrn("role-urn")
                .build()));
  }

  private Map<String, List<RunnerApi.ArtifactInformation>> invalidArtifacts() {
    return ImmutableMap.of(
        "env1",
        Collections.singletonList(
            RunnerApi.ArtifactInformation.newBuilder()
                .setTypeUrn("invalid-type-urn")
                .setTypePayload(
                    RunnerApi.EmbeddedFilePayload.newBuilder()
                        .setData(ByteString.copyFromUtf8("type-payload"))
                        .build()
                        .toByteString())
                .setRoleUrn("role-urn")
                .build()));
  }

  private static class TestDestinationProvider
      implements ArtifactStagingService.ArtifactDestinationProvider {

    @Override
    public ArtifactStagingService.ArtifactDestination getDestination(
        String stagingToken, String name) throws IOException {
      return ArtifactStagingService.ArtifactDestination.create(
          EMBEDDED_ARTIFACT_URN, ByteString.EMPTY, new ByteArrayOutputStream());
    }

    @Override
    public void removeStagedArtifacts(String stagingToken) throws IOException {}
  }
}
