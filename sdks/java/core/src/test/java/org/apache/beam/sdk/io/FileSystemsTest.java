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
package org.apache.beam.sdk.io;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileSystems}. */
@RunWith(JUnit4.class)
public class FileSystemsTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();
  private LocalFileSystem localFileSystem = new LocalFileSystem();

  @Test
  public void testGetLocalFileSystem() throws Exception {
    // TODO: Java core test failing on windows, https://github.com/apache/beam/issues/20484
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    assertTrue(
        FileSystems.getFileSystemInternal(toLocalResourceId("~/home/").getScheme())
            instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(toLocalResourceId("file://home").getScheme())
            instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(toLocalResourceId("FILE://home").getScheme())
            instanceof LocalFileSystem);
    assertTrue(
        FileSystems.getFileSystemInternal(toLocalResourceId("File://home").getScheme())
            instanceof LocalFileSystem);
    if (SystemUtils.IS_OS_WINDOWS) {
      assertTrue(
          FileSystems.getFileSystemInternal(toLocalResourceId("c:\\home\\").getScheme())
              instanceof LocalFileSystem);
    }
  }

  @Test
  public void testVerifySchemesAreUnique() throws Exception {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Scheme: [file] has conflicting filesystems");
    FileSystems.verifySchemesAreUnique(
        PipelineOptionsFactory.create(),
        Sets.newHashSet(new LocalFileSystemRegistrar(), new LocalFileSystemRegistrar()));
  }

  @Test
  public void testDeleteIgnoreMissingFiles() throws Exception {
    Path existingPath = temporaryFolder.newFile().toPath();
    Path nonExistentPath = existingPath.resolveSibling("non-existent");

    createFileWithContent(existingPath, "content1");

    FileSystems.delete(
        toResourceIds(ImmutableList.of(existingPath, nonExistentPath), false /* isDirectory */));
  }

  @Test
  public void testCopyThrowsNoSuchFileException() throws Exception {
    Path existingPath = temporaryFolder.newFile().toPath();
    Path nonExistentPath = existingPath.resolveSibling("non-existent");

    Path destPath1 = existingPath.resolveSibling("dest1");
    Path destPath2 = nonExistentPath.resolveSibling("dest2");

    createFileWithContent(existingPath, "content1");

    thrown.expect(NoSuchFileException.class);
    FileSystems.copy(
        toResourceIds(ImmutableList.of(existingPath, nonExistentPath), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2), false /* isDirectory */));
  }

  @Test
  public void testCopyIgnoreMissingFiles() throws Exception {
    Path srcPath1 = temporaryFolder.newFile().toPath();
    Path nonExistentPath = srcPath1.resolveSibling("non-existent");
    Path srcPath3 = temporaryFolder.newFile().toPath();

    Path destPath1 = srcPath1.resolveSibling("dest1");
    Path destPath2 = nonExistentPath.resolveSibling("dest2");
    Path destPath3 = srcPath1.resolveSibling("dest3");

    createFileWithContent(srcPath1, "content1");
    createFileWithContent(srcPath3, "content3");

    FileSystems.copy(
        toResourceIds(
            ImmutableList.of(srcPath1, nonExistentPath, srcPath3), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2, destPath3), false /* isDirectory */),
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

    assertTrue(srcPath1.toFile().exists());
    assertTrue(srcPath3.toFile().exists());
    assertThat(
        Files.readLines(srcPath1.toFile(), StandardCharsets.UTF_8), containsInAnyOrder("content1"));
    assertFalse(destPath2.toFile().exists());
    assertThat(
        Files.readLines(srcPath3.toFile(), StandardCharsets.UTF_8), containsInAnyOrder("content3"));
  }

  @Test
  public void testRenameThrowsNoSuchFileException() throws Exception {
    Path existingPath = temporaryFolder.newFile().toPath();
    Path nonExistentPath = existingPath.resolveSibling("non-existent");

    Path destPath1 = existingPath.resolveSibling("dest1");
    Path destPath2 = nonExistentPath.resolveSibling("dest2");

    createFileWithContent(existingPath, "content1");

    thrown.expect(NoSuchFileException.class);
    FileSystems.rename(
        toResourceIds(ImmutableList.of(existingPath, nonExistentPath), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2), false /* isDirectory */));
  }

  @Test
  public void testCopySkipIfItExists() throws Exception {
    Path srcPath1 = temporaryFolder.newFile().toPath();
    Path srcPath2 = temporaryFolder.newFile().toPath();

    Path destPath1 = srcPath1.resolveSibling("dest1");
    Path destPath2 = srcPath2.resolveSibling("dest2");

    createFileWithContent(srcPath1, "content1");
    createFileWithContent(srcPath2, "content3");
    createFileWithContent(destPath2, "content");

    FileSystems.copy(
        toResourceIds(ImmutableList.of(srcPath1, srcPath2), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2), false /* isDirectory */),
        MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);

    assertTrue(srcPath1.toFile().exists());
    assertTrue(srcPath2.toFile().exists());
    assertThat(
        Files.readLines(destPath1.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder("content1"));
    // The file is overwritten because the content does not match.
    assertThat(
        Files.readLines(destPath2.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder("content3"));
  }

  @Test
  public void testRenameIgnoreMissingFiles() throws Exception {
    Path srcPath1 = temporaryFolder.newFile().toPath();
    Path nonExistentPath = srcPath1.resolveSibling("non-existent");
    Path srcPath3 = temporaryFolder.newFile().toPath();

    Path destPath1 = srcPath1.resolveSibling("dest1");
    Path destPath2 = nonExistentPath.resolveSibling("dest2");
    Path destPath3 = srcPath1.resolveSibling("dest3");

    createFileWithContent(srcPath1, "content1");
    createFileWithContent(srcPath3, "content3");

    FileSystems.rename(
        toResourceIds(
            ImmutableList.of(srcPath1, nonExistentPath, srcPath3), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2, destPath3), false /* isDirectory */),
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

    assertFalse(srcPath1.toFile().exists());
    assertFalse(srcPath3.toFile().exists());
    assertThat(
        Files.readLines(destPath1.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder("content1"));
    assertFalse(destPath2.toFile().exists());
    assertThat(
        Files.readLines(destPath3.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder("content3"));
  }

  @Test
  public void testRenameWithFilteringAfterUnsupportedOptions() throws Exception {
    FileSystem mockFileSystem = mock(FileSystem.class);

    Path srcPath1 = temporaryFolder.newFile().toPath();
    Path nonExistentPath = srcPath1.resolveSibling("non-existent");
    Path srcPath3 = temporaryFolder.newFile().toPath();

    Path destPath1 = srcPath1.resolveSibling("dest1");
    Path destPath2 = nonExistentPath.resolveSibling("dest2");
    Path destPath3 = srcPath1.resolveSibling("dest3");

    doThrow(new UnsupportedOperationException("move options not supported."))
        .when(mockFileSystem)
        .rename(
            toResourceIds(ImmutableList.of(srcPath1, nonExistentPath, srcPath3), false),
            toResourceIds(ImmutableList.of(destPath1, destPath2, destPath3), false),
            MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS,
            MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);
    when(mockFileSystem.match(
            ImmutableList.of(
                srcPath1.toString(),
                nonExistentPath.toString(),
                srcPath3.toString(),
                destPath1.toString(),
                destPath2.toString(),
                destPath3.toString())))
        .thenReturn(
            ImmutableList.of(
                MatchResult.create(
                    MatchResult.Status.OK,
                    ImmutableList.of(
                        MatchResult.Metadata.builder()
                            .setChecksum("1")
                            .setResourceId(LocalResourceId.fromPath(srcPath1, false))
                            .setSizeBytes(1)
                            .build())),
                MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException("")),
                MatchResult.create(
                    MatchResult.Status.OK,
                    ImmutableList.of(
                        MatchResult.Metadata.builder()
                            .setChecksum("3")
                            .setResourceId(LocalResourceId.fromPath(srcPath3, false))
                            .setSizeBytes(1)
                            .build())),
                MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException("")),
                MatchResult.create(
                    MatchResult.Status.OK,
                    ImmutableList.of(
                        MatchResult.Metadata.builder()
                            .setChecksum("2")
                            .setResourceId(LocalResourceId.fromPath(destPath2, false))
                            .setSizeBytes(1)
                            .build())),
                MatchResult.create(
                    MatchResult.Status.OK,
                    ImmutableList.of(
                        MatchResult.Metadata.builder()
                            .setChecksum("3")
                            .setResourceId(LocalResourceId.fromPath(destPath3, false))
                            .setSizeBytes(1)
                            .build()))));

    FileSystems.renameInternal(
        mockFileSystem,
        toResourceIds(
            ImmutableList.of(srcPath1, nonExistentPath, srcPath3), false /* isDirectory */),
        toResourceIds(ImmutableList.of(destPath1, destPath2, destPath3), false /* isDirectory */),
        MoveOptions.StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS,
        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES);

    verify(mockFileSystem)
        .rename(
            toResourceIds(ImmutableList.of(srcPath1), false /* isDirectory */),
            toResourceIds(ImmutableList.of(destPath1), false /* isDirectory */));
    verify(mockFileSystem)
        .delete(toResourceIds(ImmutableList.of(srcPath3), false /* isDirectory */));
  }

  @Test
  public void testValidMatchNewResourceForLocalFileSystem() {
    assertEquals("file", FileSystems.matchNewResource("/tmp/f1", false).getScheme());
    assertEquals("file", FileSystems.matchNewResource("tmp/f1", false).getScheme());
    assertEquals("file", FileSystems.matchNewResource("c:\\tmp\\f1", false).getScheme());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSchemaMatchNewResource() {
    assertEquals("file", FileSystems.matchNewResource("invalidschema://tmp/f1", false));
    assertEquals("file", FileSystems.matchNewResource("c:/tmp/f1", false));
  }

  @Test
  public void testMatchNewDirectory() {
    List<KV<String, KV<String, String[]>>> testCases =
        ImmutableList.<KV<String, KV<String, String[]>>>builder()
            .add(KV.of("/abc/d/", KV.of("/abc", new String[] {"d"})))
            .add(KV.of("/abc/d/", KV.of("/abc/", new String[] {"d"})))
            .add(KV.of("/abc/d/", KV.of("/abc", new String[] {"d/"})))
            .add(KV.of("/abc/d/e/f/", KV.of("/abc", new String[] {"d", "e", "f"})))
            .add(KV.of("/abc/", KV.of("/abc", new String[] {})))
            .build();
    for (KV<String, KV<String, String[]>> testCase : testCases) {
      ResourceId expected = FileSystems.matchNewResource(testCase.getKey(), true);
      ResourceId actual =
          FileSystems.matchNewDirectory(
              testCase.getValue().getKey(), testCase.getValue().getValue());
      assertEquals(expected, actual);
    }
  }

  private static List<ResourceId> toResourceIds(List<Path> paths, final boolean isDirectory) {
    return FluentIterable.from(paths)
        .transform(path -> (ResourceId) LocalResourceId.fromPath(path, isDirectory))
        .toList();
  }

  private void createFileWithContent(Path path, String content) throws Exception {
    try (Writer writer =
        Channels.newWriter(
            localFileSystem.create(
                LocalResourceId.fromPath(path, false /* isDirectory */),
                CreateOptions.StandardCreateOptions.builder().setMimeType(MimeTypes.TEXT).build()),
            StandardCharsets.UTF_8.name())) {
      writer.write(content);
    }
  }

  private LocalResourceId toLocalResourceId(String str) throws Exception {
    boolean isDirectory;
    if (SystemUtils.IS_OS_WINDOWS) {
      isDirectory = str.endsWith("\\");
    } else {
      isDirectory = str.endsWith("/");
    }
    return LocalResourceId.fromPath(Paths.get(str), isDirectory);
  }
}
