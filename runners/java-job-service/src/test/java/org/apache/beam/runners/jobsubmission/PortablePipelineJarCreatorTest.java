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
package org.apache.beam.runners.jobsubmission;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

/** Unit tests for {@link PortablePipelineJarCreator}. */
@RunWith(JUnit4.class)
public class PortablePipelineJarCreatorTest implements Serializable {

  @Mock private JarFile inputJar;
  @Mock private JarOutputStream outputStream;
  @Mock private WritableByteChannel outputChannel;
  private PortablePipelineJarCreator jarCreator;

  @Before
  public void setup() throws IOException {
    initMocks(this);
    JarInputStream emptyInputStream = new JarInputStream(new ByteArrayInputStream(new byte[0]));
    when(inputJar.getInputStream(any())).thenReturn(emptyInputStream);
    jarCreator = new PortablePipelineJarCreator(null);
    jarCreator.outputStream = outputStream;
    jarCreator.outputChannel = outputChannel;
  }

  @Test
  public void testWriteArtifacts_copiesArtifactsIntoJar() throws IOException {
    RunnerApi.ArtifactInformation artifact =
        RunnerApi.ArtifactInformation.newBuilder()
            .setTypeUrn(ArtifactRetrievalService.EMBEDDED_ARTIFACT_URN)
            .setTypePayload(
                RunnerApi.EmbeddedFilePayload.newBuilder()
                    .setData(ByteString.copyFromUtf8("someData"))
                    .build()
                    .toByteString())
            .setRoleUrn("someRole")
            .build();
    RunnerApi.Pipeline pipeline =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(
                RunnerApi.Components.newBuilder()
                    .putEnvironments(
                        "env", RunnerApi.Environment.newBuilder().addDependencies(artifact).build())
                    .build())
            .build();
    RunnerApi.Pipeline newPipeline = jarCreator.writeArtifacts(pipeline, "jobName");
    verify(outputStream, times(1)).putNextEntry(any());
    RunnerApi.ArtifactInformation newArtifact =
        newPipeline.getComponents().getEnvironmentsMap().get("env").getDependencies(0);
    assertEquals(artifact.getRoleUrn(), newArtifact.getRoleUrn());
    assertNotEquals(artifact.getTypePayload(), newArtifact.getTypePayload());
  }

  @Test
  public void testCopyResourcesFromJar_copiesResources() throws IOException {
    List<JarEntry> entries =
        ImmutableList.of(new JarEntry("foo"), new JarEntry("bar"), new JarEntry("baz"));
    when(inputJar.entries()).thenReturn(Collections.enumeration(entries));
    jarCreator.copyResourcesFromJar(inputJar);
    verify(outputStream, times(3)).putNextEntry(any());
  }

  @Test
  public void testCopyResourcesFromJar_ignoresManifest() throws IOException {
    List<JarEntry> manifestEntry = ImmutableList.of(new JarEntry(JarFile.MANIFEST_NAME));
    when(inputJar.entries()).thenReturn(Collections.enumeration(manifestEntry));
    jarCreator.copyResourcesFromJar(inputJar);
    verify(outputStream, never()).putNextEntry(any());
  }

  @Test
  public void testCopyResourcesFromJar_ignoresDuplicates() throws IOException {
    List<JarEntry> duplicateEntries = ImmutableList.of(new JarEntry("foo"), new JarEntry("foo"));
    when(inputJar.entries()).thenReturn(Collections.enumeration(duplicateEntries));
    jarCreator.copyResourcesFromJar(inputJar);
    verify(outputStream, times(1)).putNextEntry(any());
  }

  private static class FakePipelineRunnner {
    public static void main(String[] args) {
      System.out.println("Hello world");
    }
  }

  @Test
  public void testCreateManifest_withMainMethod() {
    Manifest manifest = jarCreator.createManifest(FakePipelineRunnner.class, "job");
    assertEquals(
        FakePipelineRunnner.class.getName(),
        manifest.getMainAttributes().getValue(Name.MAIN_CLASS));
  }

  private static class EmptyPipelineRunner {}

  @Test
  public void testCreateManifest_withoutMainMethod() {
    Manifest manifest = jarCreator.createManifest(EmptyPipelineRunner.class, "job");
    assertNull(manifest.getMainAttributes().getValue(Name.MAIN_CLASS));
  }

  private static class EvilPipelineRunner {
    public static int main(String[] args) {
      return 0;
    }
  }

  @Test
  public void testCreateManifest_withInvalidMainMethod() {
    Manifest manifest = jarCreator.createManifest(EvilPipelineRunner.class, "job");
    assertNull(manifest.getMainAttributes().getValue(Name.MAIN_CLASS));
  }
}
