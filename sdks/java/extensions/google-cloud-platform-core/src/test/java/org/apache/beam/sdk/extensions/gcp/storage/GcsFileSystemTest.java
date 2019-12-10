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
package org.apache.beam.sdk.extensions.gcp.storage;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.when;

import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link GcsFileSystem}. */
@RunWith(JUnit4.class)
public class GcsFileSystemTest {

  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Mock private GcsUtil mockGcsUtil;
  private GcsFileSystem gcsFileSystem;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcsUtil(mockGcsUtil);
    gcsFileSystem = new GcsFileSystem(gcsOptions);
  }

  @Test
  public void testMatch() throws Exception {
    Objects modelObjects = new Objects();
    List<StorageObject> items = new ArrayList<>();
    // A directory
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/"));

    // Files within the directory
    items.add(createStorageObject("gs://testbucket/testdirectory/file1name", 1L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/file2name", 2L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/file3name", 3L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/file4name", 4L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/otherfile", 5L /* fileSize */));
    items.add(createStorageObject("gs://testbucket/testdirectory/anotherfile", 6L /* fileSize */));

    modelObjects.setItems(items);
    when(mockGcsUtil.listObjects(eq("testbucket"), anyString(), isNull(String.class)))
        .thenReturn(modelObjects);

    List<GcsPath> gcsPaths =
        ImmutableList.of(
            GcsPath.fromUri("gs://testbucket/testdirectory/non-exist-file"),
            GcsPath.fromUri("gs://testbucket/testdirectory/otherfile"));

    when(mockGcsUtil.getObjects(eq(gcsPaths)))
        .thenReturn(
            ImmutableList.of(
                StorageObjectOrIOException.create(new FileNotFoundException()),
                StorageObjectOrIOException.create(
                    createStorageObject("gs://testbucket/testdirectory/otherfile", 4L))));

    List<String> specs =
        ImmutableList.of(
            "gs://testbucket/testdirectory/file[1-3]*",
            "gs://testbucket/testdirectory/non-exist-file",
            "gs://testbucket/testdirectory/otherfile");
    List<MatchResult> matchResults = gcsFileSystem.match(specs);
    assertEquals(3, matchResults.size());
    assertEquals(Status.OK, matchResults.get(0).status());
    assertThat(
        ImmutableList.of(
            "gs://testbucket/testdirectory/file1name",
            "gs://testbucket/testdirectory/file2name",
            "gs://testbucket/testdirectory/file3name"),
        contains(toFilenames(matchResults.get(0)).toArray()));
    assertEquals(Status.NOT_FOUND, matchResults.get(1).status());
    assertEquals(Status.OK, matchResults.get(2).status());
    assertThat(
        ImmutableList.of("gs://testbucket/testdirectory/otherfile"),
        contains(toFilenames(matchResults.get(2)).toArray()));
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
    items.add(
        createStorageObject("gs://testbucket/testotherdirectory/file4name", 6L /* fileSize */));

    modelObjects.setItems(items);

    when(mockGcsUtil.listObjects(eq("testbucket"), anyString(), isNull(String.class)))
        .thenReturn(modelObjects);

    // Test patterns.
    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file*");
      List<String> expectedFiles =
          ImmutableList.of(
              "gs://testbucket/testdirectory/file1name",
              "gs://testbucket/testdirectory/file2name",
              "gs://testbucket/testdirectory/file3name");

      assertThat(expectedFiles, contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file*");
      List<String> expectedFiles =
          ImmutableList.of(
              "gs://testbucket/testdirectory/file1name",
              "gs://testbucket/testdirectory/file2name",
              "gs://testbucket/testdirectory/file3name");

      assertThat(expectedFiles, contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file[1-3]*");
      List<String> expectedFiles =
          ImmutableList.of(
              "gs://testbucket/testdirectory/file1name",
              "gs://testbucket/testdirectory/file2name",
              "gs://testbucket/testdirectory/file3name");

      assertThat(expectedFiles, contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file?name");
      List<String> expectedFiles =
          ImmutableList.of(
              "gs://testbucket/testdirectory/file1name",
              "gs://testbucket/testdirectory/file2name",
              "gs://testbucket/testdirectory/file3name");

      assertThat(expectedFiles, contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/test*ectory/fi*name");
      List<String> expectedFiles =
          ImmutableList.of(
              "gs://testbucket/testdirectory/file1name",
              "gs://testbucket/testdirectory/file2name",
              "gs://testbucket/testdirectory/file3name",
              "gs://testbucket/testotherdirectory/file4name");

      assertThat(expectedFiles, contains(toFilenames(gcsFileSystem.expand(pattern)).toArray()));
    }
  }

  @Test
  public void testExpandNonGlob() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Glob expression: [testdirectory/otherfile] is not expandable.");
    gcsFileSystem.expand(GcsPath.fromUri("gs://testbucket/testdirectory/otherfile"));
  }

  @Test
  public void testMatchNonGlobs() throws Exception {
    List<StorageObjectOrIOException> items = new ArrayList<>();
    // Files within the directory
    items.add(
        StorageObjectOrIOException.create(
            createStorageObject("gs://testbucket/testdirectory/file1name", 1L /* fileSize */)));
    items.add(
        StorageObjectOrIOException.create(
            createStorageObject("gs://testbucket/testdirectory/dir2name/", 0L /* fileSize */)));
    items.add(StorageObjectOrIOException.create(new FileNotFoundException()));
    items.add(StorageObjectOrIOException.create(new IOException()));
    items.add(
        StorageObjectOrIOException.create(
            createStorageObject("gs://testbucket/testdirectory/file4name", 4L /* fileSize */)));

    List<GcsPath> gcsPaths =
        ImmutableList.of(
            GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
            GcsPath.fromUri("gs://testbucket/testdirectory/dir2name/"),
            GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
            GcsPath.fromUri("gs://testbucket/testdirectory/file3name"),
            GcsPath.fromUri("gs://testbucket/testdirectory/file4name"));

    when(mockGcsUtil.getObjects(eq(gcsPaths))).thenReturn(items);
    List<MatchResult> matchResults = gcsFileSystem.matchNonGlobs(gcsPaths);

    assertEquals(5, matchResults.size());
    assertThat(
        ImmutableList.of("gs://testbucket/testdirectory/file1name"),
        contains(toFilenames(matchResults.get(0)).toArray()));
    assertThat(
        ImmutableList.of("gs://testbucket/testdirectory/dir2name/"),
        contains(toFilenames(matchResults.get(1)).toArray()));
    assertEquals(Status.NOT_FOUND, matchResults.get(2).status());
    assertEquals(Status.ERROR, matchResults.get(3).status());
    assertThat(
        ImmutableList.of("gs://testbucket/testdirectory/file4name"),
        contains(toFilenames(matchResults.get(4)).toArray()));
  }

  private StorageObject createStorageObject(String gcsFilename, long fileSize) {
    GcsPath gcsPath = GcsPath.fromUri(gcsFilename);
    // Google APIs will use null for empty files.
    @Nullable BigInteger size = (fileSize == 0) ? null : BigInteger.valueOf(fileSize);
    return new StorageObject()
        .setBucket(gcsPath.getBucket())
        .setName(gcsPath.getObject())
        .setSize(size);
  }

  private List<String> toFilenames(MatchResult matchResult) throws IOException {
    return FluentIterable.from(matchResult.metadata())
        .transform(metadata -> ((GcsResourceId) metadata.resourceId()).getGcsPath().toString())
        .toList();
  }
}
