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

import static com.google.common.base.Preconditions.checkArgument;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.util.MimeTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link LocalFileSystem}.
 */
@RunWith(JUnit4.class)
public class LocalFileSystemTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private LocalFileSystem localFileSystem = new LocalFileSystem();

  @Test
  public void testCreateWithExistingFile() throws Exception {
    File existingFile = temporaryFolder.newFile();
    testCreate(existingFile.toPath());
  }

  @Test
  public void testCreateWithinExistingDirectory() throws Exception {
    testCreate(temporaryFolder.getRoot().toPath().resolve("file.txt"));
  }

  @Test
  public void testCreateWithNonExistentSubDirectory() throws Exception {
    testCreate(temporaryFolder.getRoot().toPath().resolve("non-existent-dir").resolve("file.txt"));
  }

  private void testCreate(Path path) throws Exception {
    String expected = "my test string";
    // First with the path string
    createFileWithContent(path, expected);
    assertThat(
        Files.readLines(path.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder(expected));

    // Delete the file before trying as URI
    assertTrue("Unable to delete file " + path, path.toFile().delete());

    // Second with the path URI
    createFileWithContent(Paths.get(path.toUri()), expected);
    assertThat(
        Files.readLines(path.toFile(), StandardCharsets.UTF_8),
        containsInAnyOrder(expected));
  }

  @Test
  public void testReadWithExistingFile() throws Exception {
    String expected = "my test string";
    File existingFile = temporaryFolder.newFile();
    Files.write(expected, existingFile, StandardCharsets.UTF_8);
    String data;
    try (Reader reader = Channels.newReader(
        localFileSystem.open(LocalResourceId.fromPath(
            existingFile.toPath(),
            false /* isDirectory */)),
        StandardCharsets.UTF_8.name())) {
      data = new LineReader(reader).readLine();
    }
    assertEquals(expected, data);
  }

  @Test
  public void testReadNonExistentFile() throws Exception {
    thrown.expect(FileNotFoundException.class);
    localFileSystem
        .open(LocalResourceId.fromPath(
            temporaryFolder.getRoot().toPath().resolve("non-existent-file.txt"),
            false /* isDirectory */))
        .close();
  }

  @Test
  public void testCopyWithExistingSrcFile() throws Exception {
    Path srcPath1 = temporaryFolder.newFile().toPath();
    Path srcPath2 = temporaryFolder.newFile().toPath();

    Path destPath1 = srcPath1.resolveSibling("dest1");
    Path destPath2 = srcPath2.resolveSibling("dest2");


    createFileWithContent(srcPath1, "content1");
    createFileWithContent(srcPath2, "content2");

    testCopy(
        ImmutableList.of(srcPath1, srcPath2),
        ImmutableList.of(destPath1, destPath2),
        ImmutableList.of("content1", "content2"));
  }

  private void testCopy(List<Path> srcFiles, List<Path> destFiles, List<String> contents)
      throws Exception {
    checkArgument(srcFiles.size() == destFiles.size());

    localFileSystem.copy(
        toLocalResourceIds(srcFiles, false /* isDirectory */),
        toLocalResourceIds(destFiles, false /* isDirectory */));
    for (int i = 0; i < srcFiles.size(); ++i) {
      assertThat(
          Files.readLines(destFiles.get(i).toFile(), StandardCharsets.UTF_8),
          containsInAnyOrder(contents.get(i)));
    }
  }

  private void createFileWithContent(Path path, String content) throws Exception {
    try (Writer writer = Channels.newWriter(
        localFileSystem.create(
            LocalResourceId.fromPath(path, false /* isDirectory */),
            StandardCreateOptions.builder().setMimeType(MimeTypes.TEXT).build()),
        StandardCharsets.UTF_8.name())) {
      writer.write(content);
    }
  }

  private List<LocalResourceId> toLocalResourceIds(List<Path> paths, final boolean isDirectory) {
    return FluentIterable
        .from(paths)
        .transform(new Function<Path, LocalResourceId>() {
          @Override
          public LocalResourceId apply(Path path) {
            return LocalResourceId.fromPath(path, isDirectory);
          }})
        .toList();
  }
}
