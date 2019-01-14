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
package org.apache.beam.runners.direct.portable.artifact;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p13p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v20_0.com.google.common.util.concurrent.Uninterruptibles;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LocalFileSystemArtifactStagerService}. */
@RunWith(JUnit4.class)
public class LocalFileSystemArtifactStagerServiceTest {
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stub;

  private LocalFileSystemArtifactStagerService stager;
  private Server server;

  @Before
  public void setup() throws Exception {
    stager = LocalFileSystemArtifactStagerService.forRootDirectory(temporaryFolder.newFolder());

    server =
        InProcessServerBuilder.forName("fs_stager")
            .directExecutor()
            .addService(stager)
            .build()
            .start();

    stub =
        ArtifactStagingServiceGrpc.newStub(
            InProcessChannelBuilder.forName("fs_stager").usePlaintext().build());
  }

  @After
  public void teardown() {
    server.shutdownNow();
  }

  @Test
  public void singleDataPutArtifactSucceeds() throws Exception {
    byte[] data = "foo-bar-baz".getBytes(UTF_8);
    RecordingStreamObserver<ArtifactApi.PutArtifactResponse> responseObserver =
        new RecordingStreamObserver<>();
    StreamObserver<ArtifactApi.PutArtifactRequest> requestObserver =
        stub.putArtifact(responseObserver);

    String name = "my-artifact";
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setMetadata(
                ArtifactApi.PutArtifactMetadata.newBuilder()
                    .setMetadata(ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build())
                    .setStagingSessionToken("token")
                    .build())
            .build());
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(data)).build())
            .build());
    requestObserver.onCompleted();

    responseObserver.awaitTerminalState();

    File staged = stager.getLocation().getArtifactFile(name);
    assertThat(staged.exists(), is(true));
    ByteBuffer buf = ByteBuffer.allocate(data.length);
    new FileInputStream(staged).getChannel().read(buf);
    Assert.assertArrayEquals(data, buf.array());
  }

  @Test
  public void multiPartPutArtifactSucceeds() throws Exception {
    byte[] partOne = "foo-".getBytes(UTF_8);
    byte[] partTwo = "bar-".getBytes(UTF_8);
    byte[] partThree = "baz".getBytes(UTF_8);
    RecordingStreamObserver<ArtifactApi.PutArtifactResponse> responseObserver =
        new RecordingStreamObserver<>();
    StreamObserver<ArtifactApi.PutArtifactRequest> requestObserver =
        stub.putArtifact(responseObserver);

    String name = "my-artifact";
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setMetadata(
                ArtifactApi.PutArtifactMetadata.newBuilder()
                    .setMetadata(ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build())
                    .setStagingSessionToken("token")
                    .build())
            .build());
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder()
                    .setData(ByteString.copyFrom(partOne))
                    .build())
            .build());
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder()
                    .setData(ByteString.copyFrom(partTwo))
                    .build())
            .build());
    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder()
                    .setData(ByteString.copyFrom(partThree))
                    .build())
            .build());
    requestObserver.onCompleted();

    responseObserver.awaitTerminalState();

    File staged = stager.getLocation().getArtifactFile(name);
    assertThat(staged.exists(), is(true));
    ByteBuffer buf = ByteBuffer.allocate("foo-bar-baz".length());
    new FileInputStream(staged).getChannel().read(buf);
    Assert.assertArrayEquals("foo-bar-baz".getBytes(UTF_8), buf.array());
  }

  @Test
  public void putArtifactBeforeNameFails() {
    byte[] data = "foo-".getBytes(UTF_8);
    RecordingStreamObserver<ArtifactApi.PutArtifactResponse> responseObserver =
        new RecordingStreamObserver<>();
    StreamObserver<ArtifactApi.PutArtifactRequest> requestObserver =
        stub.putArtifact(responseObserver);

    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(data)).build())
            .build());

    responseObserver.awaitTerminalState();

    assertThat(responseObserver.error, Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void putArtifactWithNoContentFails() {
    RecordingStreamObserver<ArtifactApi.PutArtifactResponse> responseObserver =
        new RecordingStreamObserver<>();
    StreamObserver<ArtifactApi.PutArtifactRequest> requestObserver =
        stub.putArtifact(responseObserver);

    requestObserver.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(ArtifactApi.ArtifactChunk.getDefaultInstance())
            .build());

    responseObserver.awaitTerminalState();

    assertThat(responseObserver.error, Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void commitManifestWithAllArtifactsSucceeds() {
    ArtifactApi.ArtifactMetadata firstArtifact =
        stageBytes("first-artifact", "foo, bar, baz, quux".getBytes(UTF_8));
    ArtifactApi.ArtifactMetadata secondArtifact =
        stageBytes("second-artifact", "spam, ham, eggs".getBytes(UTF_8));

    ArtifactApi.Manifest manifest =
        ArtifactApi.Manifest.newBuilder()
            .addArtifact(firstArtifact)
            .addArtifact(secondArtifact)
            .build();

    RecordingStreamObserver<ArtifactApi.CommitManifestResponse> commitResponseObserver =
        new RecordingStreamObserver<>();
    stub.commitManifest(
        ArtifactApi.CommitManifestRequest.newBuilder().setManifest(manifest).build(),
        commitResponseObserver);

    commitResponseObserver.awaitTerminalState();

    assertThat(commitResponseObserver.completed, is(true));
    assertThat(commitResponseObserver.responses, Matchers.hasSize(1));
    ArtifactApi.CommitManifestResponse commitResponse = commitResponseObserver.responses.get(0);
    assertThat(commitResponse.getRetrievalToken(), Matchers.not(Matchers.nullValue()));
  }

  @Test
  public void commitManifestWithMissingArtifactFails() {
    ArtifactApi.ArtifactMetadata firstArtifact =
        stageBytes("first-artifact", "foo, bar, baz, quux".getBytes(UTF_8));
    ArtifactApi.ArtifactMetadata absentArtifact =
        ArtifactApi.ArtifactMetadata.newBuilder().setName("absent").build();

    ArtifactApi.Manifest manifest =
        ArtifactApi.Manifest.newBuilder()
            .addArtifact(firstArtifact)
            .addArtifact(absentArtifact)
            .build();

    RecordingStreamObserver<ArtifactApi.CommitManifestResponse> commitResponseObserver =
        new RecordingStreamObserver<>();
    stub.commitManifest(
        ArtifactApi.CommitManifestRequest.newBuilder().setManifest(manifest).build(),
        commitResponseObserver);

    commitResponseObserver.awaitTerminalState();

    assertThat(commitResponseObserver.error, Matchers.not(Matchers.nullValue()));
  }

  private ArtifactApi.ArtifactMetadata stageBytes(String name, byte[] bytes) {
    StreamObserver<ArtifactApi.PutArtifactRequest> requests =
        stub.putArtifact(new RecordingStreamObserver<>());
    requests.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setMetadata(
                ArtifactApi.PutArtifactMetadata.newBuilder()
                    .setMetadata(ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build())
                    .setStagingSessionToken("token")
                    .build())
            .build());
    requests.onNext(
        ArtifactApi.PutArtifactRequest.newBuilder()
            .setData(
                ArtifactApi.ArtifactChunk.newBuilder().setData(ByteString.copyFrom(bytes)).build())
            .build());
    requests.onCompleted();
    return ArtifactApi.ArtifactMetadata.newBuilder().setName(name).build();
  }

  private static class RecordingStreamObserver<T> implements StreamObserver<T> {
    private List<T> responses = new ArrayList<>();
    @Nullable private Throwable error = null;
    private boolean completed = false;

    @Override
    public void onNext(T value) {
      failIfTerminal();
      responses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      failIfTerminal();
      error = t;
    }

    @Override
    public void onCompleted() {
      failIfTerminal();
      completed = true;
    }

    private boolean isTerminal() {
      return error != null || completed;
    }

    private void failIfTerminal() {
      if (isTerminal()) {
        Assert.fail(
            String.format(
                "Should have terminated after entering a terminal state: completed %s, error %s",
                completed, error));
      }
    }

    void awaitTerminalState() {
      while (!isTerminal()) {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
      }
    }
  }
}
