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

import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ArtifactStagingServiceTest {
  private static final int TEST_BUFFER_SIZE = 1 << 10;
  private ArtifactStagingService stagingService;
  private ArtifactRetrievalService retrievalService;
  private ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stagingStub;
  private ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceBlockingStub retrievalBlockingStub;
  private Path stagingDir;
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Before
  public void setUp() throws Exception {
    stagingDir = tempFolder.newFolder("staging").toPath();
    stagingService =
        new ArtifactStagingService(
            ArtifactStagingService.beamFilesystemArtifactDestinationProvider(
                stagingDir.toString()));
    retrievalService = new ArtifactRetrievalService(TEST_BUFFER_SIZE);

    grpcCleanup.register(
        InProcessServerBuilder.forName("server")
            .directExecutor()
            .addService(stagingService)
            .addService(retrievalService)
            .build()
            .start());
    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName("server").build());

    stagingStub = ArtifactStagingServiceGrpc.newStub(channel);
    retrievalBlockingStub = ArtifactRetrievalServiceGrpc.newBlockingStub(channel);
  }

  private static class FakeArtifactRetrievalService extends ArtifactRetrievalService {

    @Override
    public void resolveArtifacts(
        ArtifactApi.ResolveArtifactsRequest request,
        StreamObserver<ArtifactApi.ResolveArtifactsResponse> responseObserver) {
      ArtifactApi.ResolveArtifactsResponse.Builder response =
          ArtifactApi.ResolveArtifactsResponse.newBuilder();
      for (RunnerApi.ArtifactInformation artifact : request.getArtifactsList()) {
        if (artifact.getTypeUrn().equals("resolved")) {
          response.addReplacements(artifact);
        } else if (artifact.getTypeUrn().equals("unresolved")) {
          response.addReplacements(artifact.toBuilder().setTypeUrn("resolved").build());
        } else {
          throw new UnsupportedOperationException(artifact.getTypeUrn());
        }
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void getArtifact(
        ArtifactApi.GetArtifactRequest request,
        StreamObserver<ArtifactApi.GetArtifactResponse> responseObserver) {
      if (request.getArtifact().getTypeUrn().equals("resolved")) {
        ByteString data = request.getArtifact().getTypePayload();
        responseObserver.onNext(
            ArtifactApi.GetArtifactResponse.newBuilder().setData(data.substring(0, 1)).build());
        responseObserver.onNext(
            ArtifactApi.GetArtifactResponse.newBuilder().setData(data.substring(1)).build());
        responseObserver.onCompleted();
      } else {
        throw new UnsupportedOperationException(request.getArtifact().getTypeUrn());
      }
    }

    public static RunnerApi.ArtifactInformation resolvedArtifact(String contents) {
      return RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn("resolved")
          .setTypePayload(ByteString.copyFromUtf8(contents))
          .setRoleUrn(contents)
          .build();
    }

    public static RunnerApi.ArtifactInformation unresolvedArtifact(String contents) {
      return RunnerApi.ArtifactInformation.newBuilder()
          .setTypeUrn("unresolved")
          .setTypePayload(ByteString.copyFromUtf8(contents))
          .setRoleUrn(contents)
          .build();
    }
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

  @Test
  public void testStageArtifacts() throws InterruptedException, ExecutionException {
    List<String> contentsList =
        ImmutableList.of("a", "bb", Strings.repeat("xyz", TEST_BUFFER_SIZE * 3 / 4));
    stagingService.registerJob(
        "stagingToken",
        ImmutableMap.of(
            "env1",
            Lists.transform(contentsList, FakeArtifactRetrievalService::resolvedArtifact),
            "env2",
            Lists.transform(contentsList, FakeArtifactRetrievalService::unresolvedArtifact)));
    ArtifactStagingService.offer(new FakeArtifactRetrievalService(), stagingStub, "stagingToken");
    Map<String, List<RunnerApi.ArtifactInformation>> staged =
        stagingService.getStagedArtifacts("stagingToken");
    assertEquals(2, staged.size());
    checkArtifacts(contentsList, staged.get("env1"));
    checkArtifacts(contentsList, staged.get("env2"));
  }

  private void checkArtifacts(
      Collection<String> expectedContents, List<RunnerApi.ArtifactInformation> staged) {
    assertEquals(
        expectedContents, Lists.transform(staged, RunnerApi.ArtifactInformation::getRoleUrn));
    assertEquals(expectedContents, Lists.transform(staged, this::getArtifact));
  }
}
