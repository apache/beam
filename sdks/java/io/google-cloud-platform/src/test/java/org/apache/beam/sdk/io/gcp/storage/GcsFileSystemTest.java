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
package org.apache.beam.sdk.io.gcp.storage;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.when;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link GcsFileSystem}.
 */
@RunWith(JUnit4.class)
public class GcsFileSystemTest {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();
  @Mock
  private GcsUtil mockGcsUtil;
  private GcsOptions gcsOptions;
  private GcsFileSystem gcsFileSystem;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcsUtil(mockGcsUtil);
    gcsFileSystem = new GcsFileSystem(gcsOptions);
  }

  @Test
  public void testGlobExpansion() throws IOException {
    Objects modelObjects = new Objects();
    List<StorageObject> items = new ArrayList<>();
    // A directory
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/"));

    // Files within the directory
    items.add(createStorageObject("gs://testbucket/testdirectory/file1name", 1L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/file2name", 2L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/file3name", 3L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/otherfile", 4L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/anotherfile", 5L /* fileSize */));
    items.add(createStorageObject(
        "gs://testbucket/testotherdirectory/file4name", 6L /* fileSize */));

    modelObjects.setItems(items);

    when(mockGcsUtil.listObjects(eq("testbucket"), anyString(), isNull(String.class)))
        .thenReturn(modelObjects);

    // Test patterns.
    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file*");
      List<String> expectedFiles = ImmutableList.of(
          "gs://testbucket/testdirectory/file1name",
          "gs://testbucket/testdirectory/file2name",
          "gs://testbucket/testdirectory/file3name");

      assertThat(
          expectedFiles,
          contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file*");
      List<String> expectedFiles = ImmutableList.of(
          "gs://testbucket/testdirectory/file1name",
          "gs://testbucket/testdirectory/file2name",
          "gs://testbucket/testdirectory/file3name");

      assertThat(
          expectedFiles,
          contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file[1-3]*");
      List<String> expectedFiles = ImmutableList.of(
          "gs://testbucket/testdirectory/file1name",
          "gs://testbucket/testdirectory/file2name",
          "gs://testbucket/testdirectory/file3name");

      assertThat(
          expectedFiles,
          contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file?name");
      List<String> expectedFiles = ImmutableList.of(
          "gs://testbucket/testdirectory/file1name",
          "gs://testbucket/testdirectory/file2name",
          "gs://testbucket/testdirectory/file3name");

      assertThat(
          expectedFiles,
          contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/test*ectory/fi*name");
      List<String> expectedFiles = ImmutableList.of(
          "gs://testbucket/testdirectory/file1name",
          "gs://testbucket/testdirectory/file2name",
          "gs://testbucket/testdirectory/file3name",
          "gs://testbucket/testotherdirectory/file4name");

      assertThat(
          expectedFiles,
          contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }
  }

  @Test
  public void testExpandNonGlob() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Glob expression: [testdirectory/otherfile] is not expandable.");
    gcsFileSystem.expand(GcsPath.fromUri("gs://testbucket/testdirectory/otherfile"));
  }

  // Patterns that contain recursive wildcards ('**') are not supported.
  @Test
  public void testRecursiveGlobExpansionFails() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported wildcard usage");
    gcsFileSystem.expand(GcsPath.fromUri("gs://testbucket/test**"));
  }

  private StorageObject createStorageObject(String gcsFilename, long fileSize) {
    GcsPath gcsPath = GcsPath.fromUri(gcsFilename);
    return new StorageObject()
        .setBucket(gcsPath.getBucket())
        .setName(gcsPath.getObject())
        .setSize(BigInteger.valueOf(fileSize));
  }

  private List<String> toFilenames(MatchResult matchResult) throws IOException {
    return FluentIterable
        .from(matchResult.metadata())
        .transform(new Function<Metadata, String>() {
          @Override
          public String apply(Metadata metadata) {
            return ((GcsResourceId) metadata.resourceId()).getGcsPath().toString();
          }})
        .toList();
  }
}
