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

package org.apache.beam.artifact.local;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactChunk;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetArtifactRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestRequest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.Manifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactRetrievalServiceGrpc;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.apache.beam.runners.fnexecution.ServerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalFileSystemArtifactRetrievalService}.
 */
@RunWith(JUnit4.class)
public class LocalFileSystemArtifactRetrievalServiceTest {
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private File root;
  private ServerFactory serverFactory = InProcessServerFactory.create();

  private GrpcFnServer<LocalFileSystemArtifactStagerService> stagerServer;

  private GrpcFnServer<LocalFileSystemArtifactRetrievalService> retrievalServer;
  private ArtifactRetrievalServiceGrpc.ArtifactRetrievalServiceStub retrievalStub;

  @Before
  public void setup() throws Exception {
    root = tmp.newFolder();
    stagerServer =
        GrpcFnServer.allocatePortAndCreateFor(
            LocalFileSystemArtifactStagerService.withRootDirectory(root), serverFactory);
  }

  @After
  public void teardown() throws Exception {
    stagerServer.close();
    retrievalServer.close();
  }

  @Test
  public void retrieveManifest() throws Exception {
    Map<String, byte[]> artifacts = new HashMap<>();
    artifacts.put("foo", "bar, baz, quux".getBytes());
    artifacts.put("spam", new byte[] {127, -22, 5});
    stageAndCreateRetrievalService(artifacts);

    final AtomicReference<Manifest> returned = new AtomicReference<>();
    final CountDownLatch completed = new CountDownLatch(1);
    retrievalStub.getManifest(
        GetManifestRequest.getDefaultInstance(),
        new StreamObserver<GetManifestResponse>() {
          @Override
          public void onNext(GetManifestResponse value) {
            returned.set(value.getManifest());
          }

          @Override
          public void onError(Throwable t) {
            completed.countDown();
          }

          @Override
          public void onCompleted() {
            completed.countDown();
          }
        });

    completed.await();
    assertThat(returned.get(), not(nullValue()));

    List<String> manifestArtifacts = new ArrayList<>();
    for (ArtifactMetadata artifactMetadata : returned.get().getArtifactList()) {
      manifestArtifacts.add(artifactMetadata.getName());
    }
    assertThat(manifestArtifacts, containsInAnyOrder("foo", "spam"));
  }

  @Test
  public void retrieveArtifact() throws Exception {
    Map<String, byte[]> artifacts = new HashMap<>();
    byte[] fooContents = "bar, baz, quux".getBytes();
    artifacts.put("foo", fooContents);
    byte[] spamContents = {127, -22, 5};
    artifacts.put("spam", spamContents);
    stageAndCreateRetrievalService(artifacts);

    final CountDownLatch completed = new CountDownLatch(2);
    ByteArrayOutputStream returnedFooBytes = new ByteArrayOutputStream();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setName("foo").build(),
        new MultimapChunkAppender(returnedFooBytes, completed));
    ByteArrayOutputStream returnedSpamBytes = new ByteArrayOutputStream();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setName("spam").build(),
        new MultimapChunkAppender(returnedSpamBytes, completed));

    completed.await();
    assertArrayEquals(fooContents, returnedFooBytes.toByteArray());
    assertArrayEquals(spamContents, returnedSpamBytes.toByteArray());
  }

  @Test
  public void retrieveArtifactNotPresent() throws Exception {
    stageAndCreateRetrievalService(Collections.singletonMap("foo", "bar, baz, quux".getBytes()));

    final CountDownLatch completed = new CountDownLatch(1);
    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    retrievalStub.getArtifact(
        GetArtifactRequest.newBuilder().setName("spam").build(),
        new StreamObserver<ArtifactChunk>() {
          @Override
          public void onNext(ArtifactChunk value) {
            fail(
                "Should never receive an "
                    + ArtifactChunk.class.getSimpleName()
                    + " for a nonexistent artifact");
          }

          @Override
          public void onError(Throwable t) {
            thrown.set(t);
            completed.countDown();
          }

          @Override
          public void onCompleted() {
            completed.countDown();
          }
        });

    completed.await();
    assertThat(thrown.get(), not(nullValue()));
    assertThat(thrown.get().getMessage(), containsString("No such artifact"));
    assertThat(thrown.get().getMessage(), containsString("spam"));
  }

  private void stageAndCreateRetrievalService(Map<String, byte[]> artifacts) throws Exception {
    List<File> artifactFiles = new ArrayList<>();
    for (Map.Entry<String, byte[]> artifact : artifacts.entrySet()) {
      File artifactFile = tmp.newFile(artifact.getKey());
      new FileOutputStream(artifactFile).getChannel().write(ByteBuffer.wrap(artifact.getValue()));
      artifactFiles.add(artifactFile);
    }

    ArtifactServiceStager stager =
        ArtifactServiceStager.overChannel(
            InProcessChannelBuilder.forName(stagerServer.getApiServiceDescriptor().getUrl())
                .build());
    stager.stage(artifactFiles);

    retrievalServer =
        GrpcFnServer.allocatePortAndCreateFor(
            LocalFileSystemArtifactRetrievalService.forRootDirectory(root), serverFactory);
    retrievalStub =
        ArtifactRetrievalServiceGrpc.newStub(
            InProcessChannelBuilder.forName(retrievalServer.getApiServiceDescriptor().getUrl())
                .build());
  }

  private static class MultimapChunkAppender implements StreamObserver<ArtifactChunk> {
    private final ByteArrayOutputStream target;
    private final CountDownLatch completed;

    private MultimapChunkAppender(ByteArrayOutputStream target, CountDownLatch completed) {
      this.target = target;
      this.completed = completed;
    }

    @Override
    public void onNext(ArtifactChunk value) {
      try {
        target.write(value.getData().toByteArray());
      } catch (IOException e) {
        // This should never happen
        throw new AssertionError(e);
      }
    }

    @Override
    public void onError(Throwable t) {
      completed.countDown();
    }

    @Override
    public void onCompleted() {
      completed.countDown();
    }
  }
}
