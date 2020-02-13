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
package org.apache.beam.runners.fnexecution.jobsubmission;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
import java.util.stream.Collectors;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ArtifactMetadata;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.GetManifestResponse;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest;
import org.apache.beam.model.jobmanagement.v1.ArtifactApi.ProxyManifest.Location;
import org.apache.beam.runners.fnexecution.jobsubmission.PortablePipelineJarCreator.ArtifactRetriever;
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
  @Mock private ArtifactRetriever retrievalServiceStub;
  private PortablePipelineJarCreator jarCreator;

  @Before
  public void setup() throws IOException {
    initMocks(this);
    JarInputStream emptyInputStream = new JarInputStream(new ByteArrayInputStream(new byte[0]));
    when(inputJar.getInputStream(any())).thenReturn(emptyInputStream);
    when(retrievalServiceStub.getArtifact(any())).thenReturn(Collections.emptyIterator());
    jarCreator = new PortablePipelineJarCreator(null);
    jarCreator.outputStream = outputStream;
    jarCreator.outputChannel = outputChannel;
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

  @Test
  public void testCopyStagedArtifacts_returnsProxyManifest() throws IOException {
    ArtifactMetadata artifact1 = ArtifactMetadata.newBuilder().setName("foo").build();
    ArtifactMetadata artifact2 = ArtifactMetadata.newBuilder().setName("bar").build();
    List<ArtifactMetadata> artifacts = ImmutableList.of(artifact1, artifact2);
    ArtifactApi.Manifest manifest =
        ArtifactApi.Manifest.newBuilder().addAllArtifact(artifacts).build();
    when(retrievalServiceStub.getManifest(any()))
        .thenReturn(GetManifestResponse.newBuilder().setManifest(manifest).build());

    ProxyManifest proxyManifest =
        jarCreator.copyStagedArtifacts("retrievalToken", retrievalServiceStub, "job");

    assertEquals(manifest, proxyManifest.getManifest());
    List<String> outputArtifactNames =
        proxyManifest.getLocationList().stream()
            .map(Location::getName)
            .collect(Collectors.toList());
    assertThat(outputArtifactNames, containsInAnyOrder("foo", "bar"));
  }

  @Test
  public void testCopyStagedArtifacts_copiesArtifacts() throws IOException {
    ArtifactMetadata artifact1 = ArtifactMetadata.newBuilder().setName("foo").build();
    ArtifactMetadata artifact2 = ArtifactMetadata.newBuilder().setName("bar").build();
    List<ArtifactMetadata> artifacts = ImmutableList.of(artifact1, artifact2);
    ArtifactApi.Manifest manifest =
        ArtifactApi.Manifest.newBuilder().addAllArtifact(artifacts).build();
    when(retrievalServiceStub.getManifest(any()))
        .thenReturn(GetManifestResponse.newBuilder().setManifest(manifest).build());

    jarCreator.copyStagedArtifacts("retrievalToken", retrievalServiceStub, "job");

    verify(outputStream, times(2)).putNextEntry(any());
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
