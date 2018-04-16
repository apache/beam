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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.ServerImpl;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.runners.core.construction.ArtifactServiceStager.StagedFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ArtifactServiceStager}.
 */
@RunWith(JUnit4.class)
public class ArtifactServiceStagerTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private ServerImpl server;
  private InMemoryArtifactStagerService service;
  private ArtifactServiceStager stager;

  @Before
  public void setup() throws IOException {
    stager =
        ArtifactServiceStager.overChannel(
            InProcessChannelBuilder.forName("service_stager").build(), 6);
    service = new InMemoryArtifactStagerService();
    server =
        InProcessServerBuilder.forName("service_stager")
            .directExecutor()
            .addService(service)
            .build()
            .start();
  }

  @After
  public void teardown() {
    server.shutdownNow();
  }

  @Test
  public void testStage() throws Exception {
    File file = temp.newFile();
    byte[] content = "foo-bar-baz".getBytes();
    byte[] contentMd5 = MessageDigest.getInstance("MD5").digest(content);
    try (FileChannel contentChannel = new FileOutputStream(file).getChannel()) {
      contentChannel.write(ByteBuffer.wrap(content));
    }

    stager.stage(Collections.singleton(StagedFile.of(file, file.getName())));

    assertThat(service.getStagedArtifacts().entrySet(), hasSize(1));
    byte[] stagedContent = Iterables.getOnlyElement(service.getStagedArtifacts().values());
    assertThat(stagedContent, equalTo(content));

    ArtifactMetadata staged = service.getManifest().getArtifact(0);
    assertThat(staged.getName(), equalTo(file.getName()));
    byte[] manifestMd5 = BaseEncoding.base64().decode(staged.getMd5());
    assertArrayEquals(contentMd5, manifestMd5);

    assertThat(service.getManifest().getArtifactCount(), equalTo(1));
    assertThat(staged, equalTo(Iterables.getOnlyElement(service.getStagedArtifacts().keySet())));
  }

  @Test
  public void testStagingMultipleFiles() throws Exception {
    File file = temp.newFile();
    byte[] content = "foo-bar-baz".getBytes();
    try (FileChannel contentChannel = new FileOutputStream(file).getChannel()) {
      contentChannel.write(ByteBuffer.wrap(content));
    }

    File otherFile = temp.newFile();
    byte[] otherContent = "spam-ham-eggs".getBytes();
    try (FileChannel contentChannel = new FileOutputStream(otherFile).getChannel()) {
      contentChannel.write(ByteBuffer.wrap(otherContent));
    }

    File thirdFile = temp.newFile();
    byte[] thirdContent = "up, down, charm, top, bottom, strange".getBytes();
    try (FileChannel contentChannel = new FileOutputStream(thirdFile).getChannel()) {
      contentChannel.write(ByteBuffer.wrap(thirdContent));
    }

    stager.stage(
        ImmutableList.of(
            StagedFile.of(file, file.getName()),
            StagedFile.of(otherFile, otherFile.getName()),
            StagedFile.of(thirdFile, thirdFile.getName())));

    assertThat(service.getManifest().getArtifactCount(), equalTo(3));
    assertThat(service.getStagedArtifacts().entrySet(), hasSize(3));
    Set<File> stagedFiles = new HashSet<>();
    for (byte[] staged : service.getStagedArtifacts().values()) {
      if (Arrays.equals(staged, content)) {
        stagedFiles.add(file);
      } else if (Arrays.equals(staged, otherContent)) {
        stagedFiles.add(otherFile);
      } else if (Arrays.equals(staged, thirdContent)) {
        stagedFiles.add(thirdFile);
      }
    }
    assertThat("All of the files contents should be staged", stagedFiles, hasSize(3));
  }
}
