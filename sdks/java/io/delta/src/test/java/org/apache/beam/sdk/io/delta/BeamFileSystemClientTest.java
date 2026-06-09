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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.engine.FileReadRequest;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BeamFileSystemClient}. */
@RunWith(JUnit4.class)
public class BeamFileSystemClientTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private final BeamFileSystemClient client = new BeamFileSystemClient();

  // --------------------------------------------------------------------------
  // getFileStatus
  // --------------------------------------------------------------------------

  @Test
  public void testGetFileStatus() throws Exception {
    File file = createFile("test.json", "hello");

    FileStatus status = client.getFileStatus(file.getAbsolutePath());

    Assert.assertEquals(file.length(), status.getSize());
  }

  @Test(expected = IOException.class)
  public void testGetFileStatusNonExistentThrows() throws Exception {
    client.getFileStatus(tempFolder.getRoot().getAbsolutePath() + "/nonexistent.json");
  }

  // --------------------------------------------------------------------------
  // listFrom
  // --------------------------------------------------------------------------

  @Test
  public void testListFromReturnsFilesStartingFromPath() throws Exception {
    File dir = tempFolder.newFolder("list-test");

    createFile(dir, "00000000000000000000.json", "a");
    File second = createFile(dir, "00000000000000000001.json", "b");
    createFile(dir, "00000000000000000002.json", "c");

    Assert.assertEquals(
      Arrays.asList(
        "00000000000000000001.json",
        "00000000000000000002.json"),
      listFileNames(second.getAbsolutePath()));
  }

  @Test
  public void testListFromEmptyDirectoryReturnsEmpty() throws Exception {
    File dir = tempFolder.newFolder("empty-dir");

    String startPath =
      dir.getAbsolutePath() + "/00000000000000000000.json";

    Assert.assertTrue(listFileNames(startPath).isEmpty());
  }

  @Test
  public void testListFromIncludesStartFile() throws Exception {
    File dir = tempFolder.newFolder("list-inclusive");

    File only =
      createFile(dir, "00000000000000000005.json", "x");

    Assert.assertEquals(
      Arrays.asList("00000000000000000005.json"),
      listFileNames(only.getAbsolutePath()));
  }

  // --------------------------------------------------------------------------
  // resolvePath
  // --------------------------------------------------------------------------

  @Test
  public void testResolvePathExistingFile() throws Exception {
    File file = createFile("resolve-test.json", "data");

    String resolved = client.resolvePath(file.getAbsolutePath());

    Assert.assertNotNull(resolved);
    Assert.assertTrue(resolved.contains("resolve-test.json"));
  }

  @Test
  public void testResolvePathNonExistentFallsBackToMatchNewResource() throws Exception {
    String path =
      tempFolder.getRoot().getAbsolutePath() + "/does-not-exist.json";

    String resolved = client.resolvePath(path);

    Assert.assertNotNull(resolved);
    Assert.assertTrue(resolved.contains("does-not-exist.json"));
  }

  // --------------------------------------------------------------------------
  // readFiles
  // --------------------------------------------------------------------------

  @Test
  public void testReadFilesWithOffsetAndLength() throws Exception {
    File file = createFile("read-test.json", "0123456789");

    Assert.assertEquals(
      "2345",
      readContent(makeRequest(file.getAbsolutePath(), 2, 4)));
  }

  @Test
  public void testReadFilesFromStart() throws Exception {
    File file = createFile("read-start.json", "abcdef");

    Assert.assertEquals(
      "abc",
      readContent(makeRequest(file.getAbsolutePath(), 0, 3)));
  }

  // --------------------------------------------------------------------------
  // mkdirs
  // --------------------------------------------------------------------------

  @Test
  public void testMkdirsCreatesLocalDirectory() throws Exception {
    File nested =
      new File(tempFolder.getRoot(), "a/b/c");

    Assert.assertFalse(nested.exists());

    boolean result = client.mkdirs(nested.getAbsolutePath());

    Assert.assertTrue(result);
    Assert.assertTrue(nested.isDirectory());
  }

  @Test
  public void testMkdirsOnObjectStorageReturnsTrue() throws Exception {
    boolean result =
      client.mkdirs("gs://some-bucket/some/path/");

    Assert.assertTrue(result);
  }

  // --------------------------------------------------------------------------
  // delete
  // --------------------------------------------------------------------------

  @Test
  public void testDeleteExistingFile() throws Exception {
    File file = createFile("to-delete.json", "bye");

    Assert.assertTrue(file.exists());

    boolean result =
      client.delete(file.getAbsolutePath());

    Assert.assertTrue(result);
    Assert.assertFalse(file.exists());
  }

  @Test
  public void testDeleteNonExistentFileDoesNotThrow() throws Exception {
    String path =
      tempFolder.getRoot().getAbsolutePath() + "/ghost.json";

    boolean result = client.delete(path);

    Assert.assertTrue(result);
  }

  // --------------------------------------------------------------------------
  // copyFileAtomically
  // --------------------------------------------------------------------------

  @Test
  public void testCopyFileAtomicallyWithoutOverwrite() throws Exception {
    File src = createFile("src.json", "content");

    File dst =
      new File(tempFolder.getRoot(), "dst.json");

    Assert.assertFalse(dst.exists());

    client.copyFileAtomically(
      src.getAbsolutePath(),
      dst.getAbsolutePath(),
      false);

    verifyFileContent(dst, "content");
  }

  @Test
  public void testCopyFileAtomicallyWithOverwrite() throws Exception {
    File src =
      createFile("src-overwrite.json", "new-content");

    File dst =
      createFile("dst-overwrite.json", "old-content");

    client.copyFileAtomically(
      src.getAbsolutePath(),
      dst.getAbsolutePath(),
      true);

    verifyFileContent(dst, "new-content");
  }

  @Test(expected = IOException.class)
  public void testCopyFileAtomicallyWithoutOverwriteThrowsIfDestinationExists()
    throws Exception {

    File src =
      createFile("src-no-overwrite.json", "data");

    File dst =
      createFile("dst-no-overwrite.json", "existing");

    client.copyFileAtomically(
      src.getAbsolutePath(),
      dst.getAbsolutePath(),
      false);
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private File createFile(String name, String content) throws IOException {
    File file = tempFolder.newFile(name);

    Files.write(
      file.toPath(),
      content.getBytes(StandardCharsets.UTF_8));

    return file;
  }

  private File createFile(
    File directory,
    String name,
    String content)
    throws IOException {

    File file = new File(directory, name);

    Files.write(
      file.toPath(),
      content.getBytes(StandardCharsets.UTF_8));

    return file;
  }

  private void verifyFileContent(
    File file,
    String expected)
    throws IOException {

    Assert.assertEquals(
      expected,
      Files.readString(file.toPath()));
  }

  private List<String> listFileNames(String startPath) throws Exception {
    List<String> listed = new ArrayList<>();

    try (CloseableIterator<FileStatus> iter =
           client.listFrom(startPath)) {

      while (iter.hasNext()) {
        listed.add(
          new File(iter.next().getPath()).getName());
      }
    }

    return listed;
  }

  private String readContent(FileReadRequest request) throws Exception {
    try (CloseableIterator<ByteArrayInputStream> streams =
           client.readFiles(
             Utils.singletonCloseableIterator(request))) {

      Assert.assertTrue(streams.hasNext());

      return new String(
        streams.next().readAllBytes(),
        StandardCharsets.UTF_8);
    }
  }

  private static FileReadRequest makeRequest(
    String path,
    int startOffset,
    int readLength) {

    return new FileReadRequest() {
      @Override
      public String getPath() {
        return path;
      }

      @Override
      public int getStartOffset() {
        return startOffset;
      }

      @Override
      public int getReadLength() {
        return readLength;
      }
    };
  }
}
