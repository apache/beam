/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.LineReader;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Tests for PackageUtil. */
@RunWith(JUnit4.class)
public class PackageUtilTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();

  @Mock
  GcsUtil mockGcsUtil;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPackageNamingWithFileHavingExtension() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target = PackageUtil.createPackage(tmpFile.getAbsolutePath(), gcsStaging, null);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A.txt", target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
        target.getLocation());
  }

  @Test
  public void testPackageNamingWithFileMissingExtension() throws Exception {
    File tmpFile = tmpFolder.newFile("file");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target = PackageUtil.createPackage(tmpFile.getAbsolutePath(), gcsStaging, null);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A", target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A",
        target.getLocation());
  }

  @Test
  public void testPackageNamingWithDirectory() throws Exception {
    File tmpDirectory = tmpFolder.newFolder("folder");
    File tmpFile = tmpFolder.newFile("folder/file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target =
        PackageUtil.createPackage(tmpDirectory.getAbsolutePath(), gcsStaging, null);

    assertEquals("folder-9MHI5fxducQ06t3IG9MC-g.zip", target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/folder-9MHI5fxducQ06t3IG9MC-g.zip",
                 target.getLocation());
  }

  @Test
  public void testPackageNamingWithFilesHavingSameContentsButDifferentNames() throws Exception {
    tmpFolder.newFolder("folder1");
    File tmpDirectory1 = tmpFolder.newFolder("folder1/folderA");
    File tmpFile1 = tmpFolder.newFile("folder1/folderA/uniqueName1");
    Files.write("This is a test!", tmpFile1, StandardCharsets.UTF_8);

    tmpFolder.newFolder("folder2");
    File tmpDirectory2 = tmpFolder.newFolder("folder2/folderA");
    File tmpFile2 = tmpFolder.newFile("folder2/folderA/uniqueName2");
    Files.write("This is a test!", tmpFile2, StandardCharsets.UTF_8);

    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target1 =
        PackageUtil.createPackage(tmpDirectory1.getAbsolutePath(), gcsStaging, null);
    DataflowPackage target2 =
        PackageUtil.createPackage(tmpDirectory2.getAbsolutePath(), gcsStaging, null);

    assertFalse(target1.getName().equals(target2.getName()));
    assertFalse(target1.getLocation().equals(target2.getLocation()));
  }

  @Test
  public void testPackageNamingWithDirectoriesHavingSameContentsButDifferentNames()
      throws Exception {
    tmpFolder.newFolder("folder1");
    File tmpDirectory1 = tmpFolder.newFolder("folder1/folderA");
    tmpFolder.newFolder("folder1/folderA/uniqueName1");

    tmpFolder.newFolder("folder2");
    File tmpDirectory2 = tmpFolder.newFolder("folder2/folderA");
    tmpFolder.newFolder("folder2/folderA/uniqueName2");

    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target1 =
        PackageUtil.createPackage(tmpDirectory1.getAbsolutePath(), gcsStaging, null);
    DataflowPackage target2 =
        PackageUtil.createPackage(tmpDirectory2.getAbsolutePath(), gcsStaging, null);

    assertFalse(target1.getName().equals(target2.getName()));
    assertFalse(target1.getLocation().equals(target2.getLocation()));
  }

  @Test
  public void testPackageUploadWithFileSucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(tmpFile.getAbsolutePath()), gcsStaging);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A.txt", target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
        target.getLocation());
    assertEquals("This is a test!",
        new LineReader(Channels.newReader(pipe.source(), "UTF-8")).readLine());
  }

  @Test
  public void testPackageUploadWithDirectorySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpDirectory = tmpFolder.newFolder("folder");
    tmpFolder.newFolder("folder/empty_directory");
    tmpFolder.newFolder("folder/directory");
    File tmpFile1 = tmpFolder.newFile("folder/file.txt");
    File tmpFile2 = tmpFolder.newFile("folder/directory/file.txt");
    Files.write("This is a test!", tmpFile1, StandardCharsets.UTF_8);
    Files.write("This is also a test!", tmpFile2, StandardCharsets.UTF_8);

    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    ZipInputStream inputStream = new ZipInputStream(Channels.newInputStream(pipe.source()));
    List<String> zipEntryNames = new ArrayList<>();
    for (ZipEntry entry = inputStream.getNextEntry(); entry != null;
        entry = inputStream.getNextEntry()) {
      zipEntryNames.add(entry.getName());
    }

    assertThat(zipEntryNames,
        containsInAnyOrder("directory/file.txt", "empty_directory/", "file.txt"));
  }

  @Test
  public void testPackageUploadWithEmptyDirectorySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpDirectory = tmpFolder.newFolder("folder");

    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals("folder-wstW9MW_ZW-soJhufroDCA.zip", target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/folder-wstW9MW_ZW-soJhufroDCA.zip",
        target.getLocation());
    assertNull(new ZipInputStream(Channels.newInputStream(pipe.source())).getNextEntry());
  }

  @Test(expected = RuntimeException.class)
  public void testPackageUploadFailsWhenIOExceptionThrown() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: Upload error"));

    try {
      PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
          ImmutableList.of(tmpFile.getAbsolutePath()), gcsStaging, fastNanoClockAndSleeper);
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil, times(5)).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadEventuallySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: 410 Gone")) // First attempt fails
        .thenReturn(pipe.sink());                               // second attempt succeeds

    try {
      PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
                                              ImmutableList.of(tmpFile.getAbsolutePath()),
                                              gcsStaging,
                                              fastNanoClockAndSleeper);
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil, times(2)).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadIsSkippedWhenFileAlreadyExists() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(tmpFile.length());

    PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(tmpFile.getAbsolutePath()), gcsStaging);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verifyNoMoreInteractions(mockGcsUtil);
  }

  @Test
  public void testPackageUploadIsNotSkippedWhenSizesAreDifferent() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpDirectory = tmpFolder.newFolder("folder");
    tmpFolder.newFolder("folder/empty_directory");
    tmpFolder.newFolder("folder/directory");
    File tmpFile1 = tmpFolder.newFile("folder/file.txt");
    File tmpFile2 = tmpFolder.newFile("folder/directory/file.txt");
    Files.write("This is a test!", tmpFile1, StandardCharsets.UTF_8);
    Files.write("This is also a test!", tmpFile2, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(Long.MAX_VALUE);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);
  }

  @Test
  public void testPackageUploadWithExplicitPackageName() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    final String overriddenName = "alias.txt";

    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(-1L);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElementsToGcs(mockGcsUtil,
        ImmutableList.of(overriddenName + "=" + tmpFile.getAbsolutePath()), gcsStaging);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals(overriddenName, target.getName());
    assertEquals("storage.googleapis.com/somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
        target.getLocation());
  }

}
