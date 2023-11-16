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
package org.apache.beam.sdk.io.hdfs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.helpers.MessageFormatter;

/** Tests for {@link HadoopFileSystem}. */
@RunWith(JUnit4.class)
public class HadoopFileSystemTest {

  @Rule public TestPipeline p = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public final ExpectedLogs expectedLogs = ExpectedLogs.none(HadoopFileSystem.class);
  private MiniDFSCluster hdfsCluster;
  private URI hdfsClusterBaseUri;
  private HadoopFileSystem fileSystem;

  @Before
  public void setUp() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.getRoot().getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");
    fileSystem =
        new HadoopFileSystem(Objects.requireNonNull(hdfsClusterBaseUri).getScheme(), configuration);
  }

  @After
  public void tearDown() {
    hdfsCluster.shutdown();
  }

  @Test
  public void testCreateAndReadFile() throws Exception {
    byte[] bytes = "testData".getBytes(StandardCharsets.UTF_8);
    create("testFile", bytes);
    assertArrayEquals(bytes, read("testFile", 0));
  }

  @Test
  public void testCreateAndReadFileWithShift() throws Exception {
    byte[] bytes = "testData".getBytes(StandardCharsets.UTF_8);
    create("testFile", bytes);
    int bytesToSkip = 3;
    byte[] expected = Arrays.copyOfRange(bytes, bytesToSkip, bytes.length);
    byte[] actual = read("testFile", bytesToSkip);
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testCreateAndReadFileWithShiftToEnd() throws Exception {
    byte[] bytes = "testData".getBytes(StandardCharsets.UTF_8);
    create("testFile", bytes);
    int bytesToSkip = bytes.length;
    byte[] expected = Arrays.copyOfRange(bytes, bytesToSkip, bytes.length);
    assertArrayEquals(expected, read("testFile", bytesToSkip));
  }

  @Test
  public void testCopy() throws Exception {
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));
    fileSystem.copy(
        ImmutableList.of(testPath("testFileA"), testPath("testFileB")),
        ImmutableList.of(testPath("copyTestFileA"), testPath("copyTestFileB")));
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("copyTestFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("copyTestFileB", 0));
  }

  @Test(expected = FileNotFoundException.class)
  public void testCopySourceMissing() throws Exception {
    fileSystem.copy(
        ImmutableList.of(testPath("missingFile")), ImmutableList.of(testPath("copyTestFile")));
  }

  @Test
  public void testDelete() throws Exception {
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));
    create("testFileC", "testDataC".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));
    assertArrayEquals("testDataC".getBytes(StandardCharsets.UTF_8), read("testFileC", 0));

    fileSystem.delete(ImmutableList.of(testPath("testFileA"), testPath("testFileC")));

    List<MatchResult> results =
        fileSystem.match(ImmutableList.of(testPath("testFile*").toString()));
    assertThat(
        results,
        contains(
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("testFileB"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testDataB".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("testFileB"))
                        .build()))));
  }

  /** Verifies that an attempt to delete a non existing file is silently ignored. */
  @Test
  public void testDeleteNonExisting() throws Exception {
    fileSystem.delete(ImmutableList.of(testPath("MissingFile")));
  }

  @Test
  public void testMatch() throws Exception {
    create("testFileAA", "testDataAA".getBytes(StandardCharsets.UTF_8));
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataAA".getBytes(StandardCharsets.UTF_8), read("testFileAA", 0));
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));

    List<MatchResult> results =
        fileSystem.match(ImmutableList.of(testPath("testFileA*").toString()));
    assertEquals(Status.OK, Iterables.getOnlyElement(results).status());
    assertThat(
        Iterables.getOnlyElement(results).metadata(),
        containsInAnyOrder(
            Metadata.builder()
                .setResourceId(testPath("testFileAA"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testDataAA".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("testFileAA"))
                .build(),
            Metadata.builder()
                .setResourceId(testPath("testFileA"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testDataA".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("testFileA"))
                .build()));
  }

  @Test
  public void testMatchDirectory() throws Exception {
    create("dir/file", "data".getBytes(StandardCharsets.UTF_8));
    final MatchResult matchResult =
        Iterables.getOnlyElement(
            fileSystem.match(Collections.singletonList(testPath("dir").toString())));
    assertThat(
        matchResult,
        equalTo(
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("dir"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes(0L)
                        .setLastModifiedMillis(lastModified("dir"))
                        .build()))));
  }

  @Test
  public void testMatchForNonExistentFile() throws Exception {
    create("testFileAA", "testDataAA".getBytes(StandardCharsets.UTF_8));
    create("testFileBB", "testDataBB".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataAA".getBytes(StandardCharsets.UTF_8), read("testFileAA", 0));
    assertArrayEquals("testDataBB".getBytes(StandardCharsets.UTF_8), read("testFileBB", 0));

    List<MatchResult> matchResults =
        fileSystem.match(
            ImmutableList.of(
                testPath("testFileAA").toString(),
                testPath("testFileA").toString(),
                testPath("testFileBB").toString()));

    assertThat(matchResults, hasSize(3));

    final List<MatchResult> expected =
        ImmutableList.of(
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("testFileAA"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testDataAA".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("testFileAA"))
                        .build())),
            MatchResult.create(Status.NOT_FOUND, ImmutableList.of()),
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("testFileBB"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testDataBB".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("testFileBB"))
                        .build())));
    assertThat(matchResults, equalTo(expected));
  }

  @Test
  public void testMatchForRecursiveGlob() throws Exception {
    create("1/testFile1", "testData1".getBytes(StandardCharsets.UTF_8));
    create("1/A/testFile1A", "testData1A".getBytes(StandardCharsets.UTF_8));
    create("1/A/A/testFile1AA", "testData1AA".getBytes(StandardCharsets.UTF_8));
    create("1/B/testFile1B", "testData1B".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testData1".getBytes(StandardCharsets.UTF_8), read("1/testFile1", 0));
    assertArrayEquals("testData1A".getBytes(StandardCharsets.UTF_8), read("1/A/testFile1A", 0));
    assertArrayEquals("testData1AA".getBytes(StandardCharsets.UTF_8), read("1/A/A/testFile1AA", 0));
    assertArrayEquals("testData1B".getBytes(StandardCharsets.UTF_8), read("1/B/testFile1B", 0));

    List<MatchResult> matchResults =
        fileSystem.match(ImmutableList.of(testPath("**testFile1*").toString()));

    assertThat(matchResults, hasSize(1));
    assertThat(
        Iterables.getOnlyElement(matchResults).metadata(),
        containsInAnyOrder(
            Metadata.builder()
                .setResourceId(testPath("1/testFile1"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testData1".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("1/testFile1"))
                .build(),
            Metadata.builder()
                .setResourceId(testPath("1/A/testFile1A"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testData1A".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("1/A/testFile1A"))
                .build(),
            Metadata.builder()
                .setResourceId(testPath("1/A/A/testFile1AA"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testData1AA".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("1/A/A/testFile1AA"))
                .build(),
            Metadata.builder()
                .setResourceId(testPath("1/B/testFile1B"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testData1B".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("1/B/testFile1B"))
                .build()));

    matchResults =
        fileSystem.match(
            ImmutableList.of(
                testPath("1**File1A").toString(),
                testPath("1**A**testFile1AA").toString(),
                testPath("1/B**").toString(),
                testPath("2**").toString()));

    final List<MatchResult> expected =
        ImmutableList.of(
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("1/A/testFile1A"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testData1A".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("1/A/testFile1A"))
                        .build())),
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("1/A/A/testFile1AA"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testData1AA".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("1/A/A/testFile1AA"))
                        .build())),
            MatchResult.create(
                Status.OK,
                ImmutableList.of(
                    Metadata.builder()
                        .setResourceId(testPath("1/B/testFile1B"))
                        .setIsReadSeekEfficient(true)
                        .setSizeBytes("testData1B".getBytes(StandardCharsets.UTF_8).length)
                        .setLastModifiedMillis(lastModified("1/B/testFile1B"))
                        .build())),
            MatchResult.create(Status.NOT_FOUND, ImmutableList.of()));

    assertThat(matchResults, hasSize(4));
    assertThat(matchResults, equalTo(expected));
  }

  @Test
  public void testRename() throws Exception {
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));

    fileSystem.rename(
        ImmutableList.of(testPath("testFileA"), testPath("testFileB")),
        ImmutableList.of(testPath("renameFileA"), testPath("renameFileB")));

    List<MatchResult> results = fileSystem.match(ImmutableList.of(testPath("*").toString()));
    assertEquals(Status.OK, Iterables.getOnlyElement(results).status());
    assertThat(
        Iterables.getOnlyElement(results).metadata(),
        containsInAnyOrder(
            Metadata.builder()
                .setResourceId(testPath("renameFileA"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testDataA".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("renameFileA"))
                .build(),
            Metadata.builder()
                .setResourceId(testPath("renameFileB"))
                .setIsReadSeekEfficient(true)
                .setSizeBytes("testDataB".getBytes(StandardCharsets.UTF_8).length)
                .setLastModifiedMillis(lastModified("renameFileB"))
                .build()));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("renameFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("renameFileB", 0));
  }

  /** Ensure that missing parent directories are created when required. */
  @Test
  public void testRenameMissingTargetDir() throws Exception {
    create("pathA/testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("pathA/testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("pathA/testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("pathA/testFileB", 0));

    // move to a directory that does not exist
    fileSystem.rename(
        ImmutableList.of(testPath("pathA/testFileA"), testPath("pathA/testFileB")),
        ImmutableList.of(testPath("pathB/testFileA"), testPath("pathB/pathC/pathD/testFileB")));

    // ensure the directories were created and the files can be read
    expectedLogs.verifyDebug(
        MessageFormatter.format(HadoopFileSystem.LOG_CREATE_DIRECTORY, "/pathB").getMessage());
    expectedLogs.verifyDebug(
        MessageFormatter.format(HadoopFileSystem.LOG_CREATE_DIRECTORY, "/pathB/pathC/pathD")
            .getMessage());
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("pathB/testFileA", 0));
    assertArrayEquals(
        "testDataB".getBytes(StandardCharsets.UTF_8), read("pathB/pathC/pathD/testFileB", 0));
  }

  @Test(expected = FileNotFoundException.class)
  public void testRenameMissingSource() throws Exception {
    fileSystem.rename(
        ImmutableList.of(testPath("missingFile")), ImmutableList.of(testPath("testFileA")));
  }

  /** Test that rename overwrites existing files. */
  @Test
  public void testRenameExistingDestination() throws Exception {
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileA", 0));
    assertArrayEquals("testDataB".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));

    fileSystem.rename(
        ImmutableList.of(testPath("testFileA")), ImmutableList.of(testPath("testFileB")));

    expectedLogs.verifyDebug(
        MessageFormatter.format(HadoopFileSystem.LOG_DELETING_EXISTING_FILE, "/testFileB")
            .getMessage());
    assertArrayEquals("testDataA".getBytes(StandardCharsets.UTF_8), read("testFileB", 0));
  }

  /** Test that rename throws predictably when source doesn't exist and destination does. */
  @Test(expected = FileNotFoundException.class)
  public void testRenameRetryScenario() throws Exception {
    testRename();
    // retry the knowing that sources are already moved to destination
    fileSystem.rename(
        ImmutableList.of(testPath("testFileA"), testPath("testFileB")),
        ImmutableList.of(testPath("renameFileA"), testPath("renameFileB")));
  }

  @Test
  public void testMatchNewResource() {
    // match file spec
    assertEquals(testPath("file"), fileSystem.matchNewResource(testPath("file").toString(), false));
    // match dir spec missing '/'
    assertEquals(testPath("dir/"), fileSystem.matchNewResource(testPath("dir").toString(), true));
    // match dir spec with '/'
    assertEquals(testPath("dir/"), fileSystem.matchNewResource(testPath("dir/").toString(), true));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected file path but received directory path");
    fileSystem.matchNewResource(testPath("dir/").toString(), false);
  }

  @Test
  @Ignore("TestPipeline needs a way to take in HadoopFileSystemOptions")
  public void testReadPipeline() throws Exception {
    create("testFileA", "testDataA".getBytes(StandardCharsets.UTF_8));
    create("testFileB", "testDataB".getBytes(StandardCharsets.UTF_8));
    create("testFileC", "testDataC".getBytes(StandardCharsets.UTF_8));

    HadoopFileSystemOptions options =
        TestPipeline.testingPipelineOptions().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(ImmutableList.of(fileSystem.configuration));
    FileSystems.setDefaultPipelineOptions(options);
    PCollection<String> pc = p.apply(TextIO.read().from(testPath("testFile*").toString()));
    PAssert.that(pc).containsInAnyOrder("testDataA", "testDataB", "testDataC");
    p.run();
  }

  private void create(String relativePath, byte[] contents) throws Exception {
    try (WritableByteChannel channel =
        fileSystem.create(
            testPath(relativePath),
            StandardCreateOptions.builder().setMimeType(MimeTypes.BINARY).build())) {
      channel.write(ByteBuffer.wrap(contents));
    }
  }

  private byte[] read(String relativePath, long bytesToSkip) throws Exception {
    try (ReadableByteChannel channel = fileSystem.open(testPath(relativePath))) {
      InputStream inputStream = Channels.newInputStream(channel);
      if (bytesToSkip > 0) {
        ByteStreams.skipFully(inputStream, bytesToSkip);
      }
      return ByteStreams.toByteArray(inputStream);
    }
  }

  private long lastModified(String relativePath) throws Exception {
    final Path testPath = testPath(relativePath).toPath();
    return testPath
        .getFileSystem(fileSystem.configuration)
        .getFileStatus(testPath(relativePath).toPath())
        .getModificationTime();
  }

  private HadoopResourceId testPath(String relativePath) {
    return new HadoopResourceId(hdfsClusterBaseUri.resolve(relativePath));
  }
}
