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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
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

/**
 * Tests for {@link HadoopFileSystem}.
 */
@RunWith(JUnit4.class)
public class HadoopFileSystemTest {

  @Rule public TestPipeline p = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();
  private Configuration configuration;
  private MiniDFSCluster hdfsCluster;
  private URI hdfsClusterBaseUri;
  private HadoopFileSystem fileSystem;

  @Before
  public void setUp() throws Exception {
    configuration = new Configuration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.getRoot().getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration);
    hdfsCluster = builder.build();
    hdfsClusterBaseUri = new URI(configuration.get("fs.defaultFS") + "/");
    fileSystem = new HadoopFileSystem(configuration);
  }

  @After
  public void tearDown() throws Exception {
    hdfsCluster.shutdown();
  }

  @Test
  public void testCreateAndReadFile() throws Exception {
    create("testFile", "testData".getBytes());
    assertArrayEquals("testData".getBytes(), read("testFile"));
  }

  @Test
  public void testCopy() throws Exception {
    create("testFileA", "testDataA".getBytes());
    create("testFileB", "testDataB".getBytes());
    fileSystem.copy(
        ImmutableList.of(
            testPath("testFileA"),
            testPath("testFileB")),
        ImmutableList.of(
            testPath("copyTestFileA"),
            testPath("copyTestFileB")));
    assertArrayEquals("testDataA".getBytes(), read("testFileA"));
    assertArrayEquals("testDataB".getBytes(), read("testFileB"));
    assertArrayEquals("testDataA".getBytes(), read("copyTestFileA"));
    assertArrayEquals("testDataB".getBytes(), read("copyTestFileB"));
  }

  @Test
  public void testDelete() throws Exception {
    create("testFileA", "testDataA".getBytes());
    create("testFileB", "testDataB".getBytes());
    create("testFileC", "testDataC".getBytes());

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(), read("testFileA"));
    assertArrayEquals("testDataB".getBytes(), read("testFileB"));
    assertArrayEquals("testDataC".getBytes(), read("testFileC"));

    fileSystem.delete(ImmutableList.of(
        testPath("testFileA"),
        testPath("testFileC")));

    List<MatchResult> results =
        fileSystem.match(ImmutableList.of(testPath("testFile*").toString()));
    assertThat(results, contains(MatchResult.create(Status.OK, ImmutableList.of(
        Metadata.builder()
            .setResourceId(testPath("testFileB"))
            .setIsReadSeekEfficient(true)
            .setSizeBytes("testDataB".getBytes().length)
            .build()))));
  }

  @Test
  public void testMatch() throws Exception {
    create("testFileAA", "testDataAA".getBytes());
    create("testFileA", "testDataA".getBytes());
    create("testFileB", "testDataB".getBytes());

    // ensure files exist
    assertArrayEquals("testDataAA".getBytes(), read("testFileAA"));
    assertArrayEquals("testDataA".getBytes(), read("testFileA"));
    assertArrayEquals("testDataB".getBytes(), read("testFileB"));

    List<MatchResult> results =
        fileSystem.match(ImmutableList.of(testPath("testFileA*").toString()));
    assertEquals(Status.OK, Iterables.getOnlyElement(results).status());
    assertThat(Iterables.getOnlyElement(results).metadata(), containsInAnyOrder(
        Metadata.builder()
            .setResourceId(testPath("testFileAA"))
            .setIsReadSeekEfficient(true)
            .setSizeBytes("testDataAA".getBytes().length)
            .build(),
        Metadata.builder()
            .setResourceId(testPath("testFileA"))
            .setIsReadSeekEfficient(true)
            .setSizeBytes("testDataA".getBytes().length)
            .build()));
  }

  @Test
  public void testRename() throws Exception {
    create("testFileA", "testDataA".getBytes());
    create("testFileB", "testDataB".getBytes());

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(), read("testFileA"));
    assertArrayEquals("testDataB".getBytes(), read("testFileB"));

    fileSystem.rename(
        ImmutableList.of(
            testPath("testFileA"), testPath("testFileB")),
        ImmutableList.of(
            testPath("renameFileA"), testPath("renameFileB")));

    List<MatchResult> results =
        fileSystem.match(ImmutableList.of(testPath("*").toString()));
    assertEquals(Status.OK, Iterables.getOnlyElement(results).status());
    assertThat(Iterables.getOnlyElement(results).metadata(), containsInAnyOrder(
        Metadata.builder()
            .setResourceId(testPath("renameFileA"))
            .setIsReadSeekEfficient(true)
            .setSizeBytes("testDataA".getBytes().length)
            .build(),
        Metadata.builder()
            .setResourceId(testPath("renameFileB"))
            .setIsReadSeekEfficient(true)
            .setSizeBytes("testDataB".getBytes().length)
            .build()));

    // ensure files exist
    assertArrayEquals("testDataA".getBytes(), read("renameFileA"));
    assertArrayEquals("testDataB".getBytes(), read("renameFileB"));
  }

  @Test
  public void testMatchNewResource() throws Exception {
    // match file spec
    assertEquals(testPath("file"),
        fileSystem.matchNewResource(testPath("file").toString(), false));
    // match dir spec missing '/'
    assertEquals(testPath("dir/"),
        fileSystem.matchNewResource(testPath("dir").toString(), true));
    // match dir spec with '/'
    assertEquals(testPath("dir/"),
        fileSystem.matchNewResource(testPath("dir/").toString(), true));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Expected file path but received directory path");
    fileSystem.matchNewResource(testPath("dir/").toString(), false);
  }

  @Test
  @Ignore("TestPipeline needs a way to take in HadoopFileSystemOptions")
  public void testReadPipeline() throws Exception {
    create("testFileA", "testDataA".getBytes());
    create("testFileB", "testDataB".getBytes());
    create("testFileC", "testDataC".getBytes());

    HadoopFileSystemOptions options = TestPipeline.testingPipelineOptions()
        .as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(ImmutableList.of(fileSystem.fileSystem.getConf()));
    FileSystems.setDefaultConfigInWorkers(options);
    PCollection<String> pc = p.apply(TextIO.Read.from(testPath("testFile*").toString()));
    PAssert.that(pc).containsInAnyOrder("testDataA", "testDataB", "testDataC");
    p.run();
  }

  private void create(String relativePath, byte[] contents) throws Exception {
    try (WritableByteChannel channel = fileSystem.create(
        testPath(relativePath),
        StandardCreateOptions.builder().setMimeType(MimeTypes.BINARY).build())) {
      channel.write(ByteBuffer.wrap(contents));
    }
  }

  private byte[] read(String relativePath) throws Exception {
    try (ReadableByteChannel channel = fileSystem.open(testPath(relativePath))) {
      return ByteStreams.toByteArray(Channels.newInputStream(channel));
    }
  }

  private HadoopResourceId testPath(String relativePath) {
    return new HadoopResourceId(hdfsClusterBaseUri.resolve(relativePath));
  }
}
