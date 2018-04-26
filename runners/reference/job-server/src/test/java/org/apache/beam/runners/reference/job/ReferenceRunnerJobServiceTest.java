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

package org.apache.beam.runners.reference.job;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobRequest;
import org.apache.beam.model.jobmanagement.v1.JobApi.PrepareJobResponse;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc.JobServiceBlockingStub;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.ArtifactServiceStager;
import org.apache.beam.runners.core.construction.ArtifactServiceStager.StagedFile;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.InProcessServerFactory;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ReferenceRunnerJobService}.
 */
@RunWith(JUnit4.class)
public class ReferenceRunnerJobServiceTest {
  @Rule public TemporaryFolder runnerTemp = new TemporaryFolder();
  @Rule public TemporaryFolder clientTemp = new TemporaryFolder();

  private InProcessServerFactory serverFactory = InProcessServerFactory.create();
  private ReferenceRunnerJobService service;
  private GrpcFnServer<ReferenceRunnerJobService> server;
  private JobServiceBlockingStub stub;

  @Before
  public void setup() throws Exception {
    service =
        ReferenceRunnerJobService.create(serverFactory)
            .withStagingPathSupplier(() -> runnerTemp.getRoot().toPath());
    server = GrpcFnServer.allocatePortAndCreateFor(service, serverFactory);
    stub =
        JobServiceGrpc.newBlockingStub(
            InProcessChannelBuilder.forName(server.getApiServiceDescriptor().getUrl()).build());
  }

  @After
  public void teardown() throws Exception {
    server.close();
  }

  @Test
  public void testPrepareJob() throws Exception {
    PrepareJobResponse response =
        stub.prepare(
            PrepareJobRequest.newBuilder()
                .setPipelineOptions(Struct.getDefaultInstance())
                .setPipeline(Pipeline.getDefaultInstance())
                .setJobName("myJobName")
                .build());

    ApiServiceDescriptor stagingEndpoint = response.getArtifactStagingEndpoint();
    ArtifactServiceStager stager =
        ArtifactServiceStager.overChannel(
            InProcessChannelBuilder.forName(stagingEndpoint.getUrl()).build());
    File foo = writeTempFile("foo", "foo, bar, baz".getBytes());
    File bar = writeTempFile("spam", "spam, ham, eggs".getBytes());
    stager.stage(
        ImmutableList.of(StagedFile.of(foo, foo.getName()), StagedFile.of(bar, bar.getName())));
    List<byte[]> tempDirFiles = readFlattenedFiles(runnerTemp.getRoot());
    assertThat(
        tempDirFiles,
        hasItems(
            arrayEquals(Files.readAllBytes(foo.toPath())),
            arrayEquals(Files.readAllBytes(bar.toPath()))));
    // TODO: 'run' the job with some sort of noop backend, to verify state is cleaned up.
  }

  private Matcher<byte[]> arrayEquals(final byte[] expected) {
    return new TypeSafeMatcher<byte[]>() {
      @Override
      protected boolean matchesSafely(byte[] actual) {
        return Arrays.equals(actual, expected);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("an array equal to ").appendValue(Arrays.toString(expected));
      }
    };
  }

  private List<byte[]> readFlattenedFiles(File root) throws Exception {
    if (root.isDirectory()) {
      List<byte[]> children = new ArrayList<>();
      for (File child : root.listFiles()) {
        children.addAll(readFlattenedFiles(child));
      }
      return children;
    } else {
      return Collections.singletonList(Files.readAllBytes(root.toPath()));
    }
  }

  private File writeTempFile(String fileName, byte[] contents) throws Exception {
    File file = clientTemp.newFile(fileName);
    try (FileOutputStream stream = new FileOutputStream(file);
        FileChannel channel = stream.getChannel()) {
      channel.write(ByteBuffer.wrap(contents));
    }
    return file;
  }
}
