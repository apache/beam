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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharSource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the {@link ZipFiles} class. These tests make sure that the handling of zip-files works
 * fine.
 */
@RunWith(JUnit4.class)
public class ZipFilesTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private File tmpDir;

  @Rule public TemporaryFolder tmpOutputFolder = new TemporaryFolder();
  private File zipFile;

  @Before
  public void setUp() throws Exception {
    tmpDir = tmpFolder.getRoot();
    zipFile = createZipFileHandle(); // the file is not actually created
  }

  /**
   * Verify that zipping and unzipping works fine. We zip a directory having some subdirectories,
   * unzip it again and verify the structure to be in place.
   */
  @Test
  public void testZipWithSubdirectories() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    File subDir1 = new File(zipDir, "subDir1");
    File subDir2 = new File(subDir1, "subdir2");
    assertTrue(subDir2.mkdirs());
    createFileWithContents(subDir2, "myTextFile.txt", "Simple Text");

    assertZipAndUnzipOfDirectoryMatchesOriginal(tmpDir);
  }

  /** An empty subdirectory must have its own zip-entry. */
  @Test
  public void testEmptySubdirectoryHasZipEntry() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    File subDirEmpty = new File(zipDir, "subDirEmpty");
    assertTrue(subDirEmpty.mkdirs());

    ZipFiles.zipDirectory(tmpDir, zipFile);
    assertZipOnlyContains("zip/subDirEmpty/");
  }

  /** A directory with contents should not have a zip entry. */
  @Test
  public void testSubdirectoryWithContentsHasNoZipEntry() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    File subDirContent = new File(zipDir, "subdirContent");
    assertTrue(subDirContent.mkdirs());
    createFileWithContents(subDirContent, "myTextFile.txt", "Simple Text");

    ZipFiles.zipDirectory(tmpDir, zipFile);
    assertZipOnlyContains("zip/subdirContent/myTextFile.txt");
  }

  @Test
  public void testZipDirectoryToOutputStream() throws Exception {
    createFileWithContents(tmpDir, "myTextFile.txt", "Simple Text");
    File[] sourceFiles = tmpDir.listFiles();
    Arrays.sort(sourceFiles);
    assertThat(sourceFiles, not(arrayWithSize(0)));

    try (FileOutputStream outputStream = new FileOutputStream(zipFile)) {
      ZipFiles.zipDirectory(tmpDir, outputStream);
    }
    File outputDir = Files.createTempDir();
    ZipFiles.unzipFile(zipFile, outputDir);
    File[] outputFiles = outputDir.listFiles();
    Arrays.sort(outputFiles);

    assertThat(outputFiles, arrayWithSize(sourceFiles.length));
    for (int i = 0; i < sourceFiles.length; i++) {
      compareFileContents(sourceFiles[i], outputFiles[i]);
    }

    removeRecursive(outputDir.toPath());
    assertTrue(zipFile.delete());
  }

  @Test
  public void testEntries() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    File subDir1 = new File(zipDir, "subDir1");
    File subDir2 = new File(subDir1, "subdir2");
    assertTrue(subDir2.mkdirs());
    createFileWithContents(subDir2, "myTextFile.txt", "Simple Text");

    ZipFiles.zipDirectory(tmpDir, zipFile);

    try (ZipFile zip = new ZipFile(zipFile)) {
      Enumeration<? extends ZipEntry> entries = zip.entries();
      for (ZipEntry entry : ZipFiles.entries(zip)) {
        assertTrue(entries.hasMoreElements());
        // ZipEntry doesn't override equals
        assertEquals(entry.getName(), entries.nextElement().getName());
      }
      assertFalse(entries.hasMoreElements());
    }
  }

  @Test
  public void testAsByteSource() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    assertTrue(zipDir.mkdirs());
    createFileWithContents(zipDir, "myTextFile.txt", "Simple Text");

    ZipFiles.zipDirectory(tmpDir, zipFile);

    try (ZipFile zip = new ZipFile(zipFile)) {
      ZipEntry entry = zip.getEntry("zip/myTextFile.txt");
      ByteSource byteSource = ZipFiles.asByteSource(zip, entry);
      if (entry.getSize() != -1) {
        assertEquals(entry.getSize(), byteSource.size());
      }
      assertArrayEquals("Simple Text".getBytes(StandardCharsets.UTF_8), byteSource.read());
    }
  }

  @Test
  public void testAsCharSource() throws Exception {
    File zipDir = new File(tmpDir, "zip");
    assertTrue(zipDir.mkdirs());
    createFileWithContents(zipDir, "myTextFile.txt", "Simple Text");

    ZipFiles.zipDirectory(tmpDir, zipFile);

    try (ZipFile zip = new ZipFile(zipFile)) {
      ZipEntry entry = zip.getEntry("zip/myTextFile.txt");
      CharSource charSource = ZipFiles.asCharSource(zip, entry, StandardCharsets.UTF_8);
      assertEquals("Simple Text", charSource.read());
    }
  }

  private void assertZipOnlyContains(String zipFileEntry) throws IOException {
    try (ZipFile zippedFile = new ZipFile(zipFile)) {
      assertEquals(1, zippedFile.size());
      ZipEntry entry = zippedFile.entries().nextElement();
      assertEquals(zipFileEntry, entry.getName());
    }
  }

  /** try to unzip to a non-existent directory and make sure that it fails. */
  @Test
  public void testInvalidTargetDirectory() throws IOException {
    File zipDir = new File(tmpDir, "zipdir");
    assertTrue(zipDir.mkdir());
    ZipFiles.zipDirectory(tmpDir, zipFile);
    File invalidDirectory = new File("/foo/bar");
    assertFalse(invalidDirectory.exists());
    try {
      ZipFiles.unzipFile(zipFile, invalidDirectory);
      fail("We expect the IllegalArgumentException, but it never occured");
    } catch (IllegalArgumentException e) {
      // This is the expected exception - we passed the test.
    }
  }

  /** Try to unzip to an existing directory, but failing to create directories. */
  @Test
  public void testDirectoryCreateFailed() throws IOException {
    File zipDir = new File(tmpDir, "zipdir");
    assertTrue(zipDir.mkdir());
    ZipFiles.zipDirectory(tmpDir, zipFile);
    File targetDirectory = Files.createTempDir();
    // Touch a file where the directory should be.
    Files.touch(new File(targetDirectory, "zipdir"));
    try {
      ZipFiles.unzipFile(zipFile, targetDirectory);
      fail("We expect the IOException, but it never occured");
    } catch (IOException e) {
      // This is the expected exception - we passed the test.
    }
  }

  /**
   * zip and unzip a certain directory, and verify the content afterward to be identical.
   *
   * @param sourceDir the directory to zip
   */
  private void assertZipAndUnzipOfDirectoryMatchesOriginal(File sourceDir) throws IOException {
    File[] sourceFiles = sourceDir.listFiles();
    Arrays.sort(sourceFiles);

    File zipFile = createZipFileHandle();
    ZipFiles.zipDirectory(sourceDir, zipFile);
    File outputDir = Files.createTempDir();
    ZipFiles.unzipFile(zipFile, outputDir);
    File[] outputFiles = outputDir.listFiles();
    Arrays.sort(outputFiles);

    assertThat(outputFiles, arrayWithSize(sourceFiles.length));
    for (int i = 0; i < sourceFiles.length; i++) {
      compareFileContents(sourceFiles[i], outputFiles[i]);
    }

    removeRecursive(outputDir.toPath());
    assertTrue(zipFile.delete());
  }

  /**
   * Compare the content of two files or directories recursively.
   *
   * @param expected the expected directory or file content
   * @param actual the actual directory or file content
   */
  private void compareFileContents(File expected, File actual) throws IOException {
    assertEquals(expected.isDirectory(), actual.isDirectory());
    assertEquals(expected.getName(), actual.getName());
    if (expected.isDirectory()) {
      // Go through the children step by step.
      File[] expectedChildren = expected.listFiles();
      Arrays.sort(expectedChildren);
      File[] actualChildren = actual.listFiles();
      Arrays.sort(actualChildren);
      assertThat(actualChildren, arrayWithSize(expectedChildren.length));
      for (int i = 0; i < expectedChildren.length; i++) {
        compareFileContents(expectedChildren[i], actualChildren[i]);
      }
    } else {
      // Compare the file content itself.
      assertTrue(Files.equal(expected, actual));
    }
  }

  /** Create a File object to which we can safely zip a file. */
  private File createZipFileHandle() throws IOException {
    File zipFile = File.createTempFile("test", "zip", tmpOutputFolder.getRoot());
    assertTrue(zipFile.delete());
    return zipFile;
  }

  // This is not generally safe as it does not handle symlinks, etc. However it is safe
  // enough for these tests.
  private static void removeRecursive(Path path) throws IOException {
    Iterable<File> files = Files.fileTraverser().depthFirstPostOrder(path.toFile());
    for (File f : files) {
      java.nio.file.Files.delete(f.toPath());
    }
  }

  /** Create file dir/fileName with contents fileContents. */
  private void createFileWithContents(File dir, String fileName, String fileContents)
      throws IOException {
    File txtFile = new File(dir, fileName);
    Files.asCharSink(txtFile, StandardCharsets.UTF_8).write(fileContents);
  }
}
