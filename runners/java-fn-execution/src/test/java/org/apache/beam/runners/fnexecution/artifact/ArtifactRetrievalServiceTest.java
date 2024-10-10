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
package org.apache.beam.runners.fnexecution.artifact;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArtifactRetrievalServiceTest {
  private static final int TEST_BUFFER_SIZE = 1 << 10;
  private ArtifactRetrievalService retrievalService;
  private ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalBlockingStub;
  private Path stagingDir;
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Before
  public void setUp() throws Exception {
    retrievalService = new ArtifactRetrievalService(TEST_BUFFER_SIZE);
    grpcCleanup.register(
        InProcessServerBuilder.forName("server")
            .directExecutor()
            .addService(retrievalService)
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName("server").build());
    retrievalBlockingStub = ArtifactRetrievalServiceGrpc.newBlockingStub(channel);

    stagingDir = tempFolder.newFolder("staging").toPath();
  }

  private void stageFiles(Map<String, String> files) throws IOException {
    for (Map.Entry<String, String> entry : files.entrySet()) {
      Files.write(
          Paths.get(stagingDir.toString(), entry.getKey()),
          entry.getValue().getBytes(StandardCharsets.UTF_8));
    }
  }

  private RunnerApi.ArtifactInformation fileArtifact(Path path) {
    return fileArtifact(path, "");
  }

  private RunnerApi.ArtifactInformation fileArtifact(Path path, String role) {
    return RunnerApi.ArtifactInformation.newBuilder()
        .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
        .setTypePayload(
            RunnerApi.ArtifactFilePayload.newBuilder()
                .setPath(path.toString())
                .build()
                .toByteString())
        .setRoleUrn(role)
        .build();
  }

  @Test
  public void testResolveArtifacts() throws IOException {
    RunnerApi.ArtifactInformation artifact = fileArtifact(Paths.get("somePath"));
    ArtifactApi.ResolveArtifactsResponse resolved =
        retrievalBlockingStub.resolveArtifacts(
            ArtifactApi.ResolveArtifactsRequest.newBuilder().addArtifacts(artifact).build());
    assertEquals(1, resolved.getReplacementsCount());
    assertEquals(artifact, resolved.getReplacements(0));
  }

  private String getArtifact(RunnerApi.ArtifactInformation artifact) {
    ByteString all = ByteString.EMPTY;
    Iterator<ArtifactApi.GetArtifactResponse> response =
        retrievalBlockingStub.getArtifact(
            ArtifactApi.GetArtifactRequest.newBuilder().setArtifact(artifact).build());
    while (response.hasNext()) {
      all = all.concat(response.next().getData());
    }
    return all.toStringUtf8();
  }

  @SuppressWarnings("InlineMeInliner") // inline `Strings.repeat()` - Java 11+ API only
  @Test
  public void testRetrieveArtifacts() throws IOException, InterruptedException {
    Map<String, String> artifacts =
        ImmutableMap.of(
            "a.txt", "a", "b.txt", "bbb", "c.txt", Strings.repeat("cxy", TEST_BUFFER_SIZE * 3 / 4));
    stageFiles(artifacts);
    for (Map.Entry<String, String> artifact : artifacts.entrySet()) {
      assertEquals(
          artifact.getValue(),
          getArtifact(fileArtifact(Paths.get(stagingDir.toString(), artifact.getKey()))));
    }
  }
}
