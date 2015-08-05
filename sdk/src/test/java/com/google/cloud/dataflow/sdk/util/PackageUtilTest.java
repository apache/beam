/*
 * Copyright (C) 2015 Google Inc.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineReader;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Tests for PackageUtil. */
@RunWith(JUnit4.class)
public class PackageUtilTest {
  @Rule public ExpectedLogs logged = ExpectedLogs.none(PackageUtil.class);
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule
  public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();

  @Mock
  GcsUtil mockGcsUtil;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGcsUtil(mockGcsUtil);

    IOChannelUtils.registerStandardIOFactories(pipelineOptions);
  }

  @Test
  public void testPackageNamingWithFileHavingExtension() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target =
        PackageUtil.createPackage(tmpFile, gcsStaging.toString(), null);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A.txt", target.getName());
    assertEquals("gs://somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
        target.getLocation());
  }

  @Test
  public void testPackageNamingWithFileMissingExtension() throws Exception {
    File tmpFile = tmpFolder.newFile("file");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target =
        PackageUtil.createPackage(tmpFile, gcsStaging.toString(), null);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A", target.getName());
    assertEquals("gs://somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A",
        target.getLocation());
  }

  @Test
  public void testPackageNamingWithDirectory() throws Exception {
    File tmpDirectory = tmpFolder.newFolder("folder");
    File tmpFile = tmpFolder.newFile("folder/file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");

    DataflowPackage target =
        PackageUtil.createPackage(tmpDirectory, gcsStaging.toString(), null);

    assertEquals("folder-9MHI5fxducQ06t3IG9MC-g.zip", target.getName());
    assertEquals("gs://somebucket/base/path/folder-9MHI5fxducQ06t3IG9MC-g.zip",
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
        PackageUtil.createPackage(tmpDirectory1, gcsStaging.toString(), null);
    DataflowPackage target2 =
        PackageUtil.createPackage(tmpDirectory2, gcsStaging.toString(), null);

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
        PackageUtil.createPackage(tmpDirectory1, gcsStaging.toString(), null);
    DataflowPackage target2 =
        PackageUtil.createPackage(tmpDirectory2, gcsStaging.toString(), null);

    assertFalse(target1.getName().equals(target2.getName()));
    assertFalse(target1.getLocation().equals(target2.getLocation()));
  }

  @Test
  public void testPackageUploadWithLargeClasspathLogsWarning() throws IOException {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    // all files will be present and cached so no upload needed.
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(tmpFile.length());

    List<String> classpathElements = Lists.newLinkedList();
    for (int i = 0; i < 1005; ++i) {
      String eltName = "element" + i;
      classpathElements.add(eltName + '=' + tmpFile.getAbsolutePath());
    }

    PackageUtil.stageClasspathElements(classpathElements, gcsStaging.toString());

    logged.verifyWarn("Your classpath contains 1005 elements, which Google Cloud Dataflow");
  }

  @Test
  public void testPackageUploadWithFileSucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpFile.getAbsolutePath()), gcsStaging.toString());
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals("file-SAzzqSB2zmoIgNHC9A2G0A.txt", target.getName());
    assertEquals("gs://somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
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
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging.toString());

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
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging.toString());
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals("folder-wstW9MW_ZW-soJhufroDCA.zip", target.getName());
    assertEquals("gs://somebucket/base/path/folder-wstW9MW_ZW-soJhufroDCA.zip",
        target.getLocation());
    assertNull(new ZipInputStream(Channels.newInputStream(pipe.source())).getNextEntry());
  }

  @Test(expected = RuntimeException.class)
  public void testPackageUploadFailsWhenIOExceptionThrown() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: Upload error"));

    try {
      PackageUtil.stageClasspathElements(
          ImmutableList.of(tmpFile.getAbsolutePath()),
          gcsStaging.toString(), fastNanoClockAndSleeper);
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil, times(5)).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadFailsWithPermissionsErrorGivesDetailedMessage() throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Failed to write to GCS path " + gcsStaging,
            googleJsonResponseException(
                HttpStatusCodes.STATUS_CODE_FORBIDDEN, "Permission denied", "Test message")));

    try {
      PackageUtil.stageClasspathElements(
          ImmutableList.of(tmpFile.getAbsolutePath()),
          gcsStaging.toString(), fastNanoClockAndSleeper);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertTrue("Expected IOException containing detailed message.",
          e.getCause() instanceof IOException);
      assertThat(e.getCause().getMessage(),
          Matchers.allOf(
              Matchers.containsString("Uploaded failed due to permissions error"),
              Matchers.containsString(
                  "Stale credentials can be resolved by executing 'gcloud auth login'")));
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadEventuallySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = tmpFolder.newFile("file.txt");
    Files.write("This is a test!", tmpFile, StandardCharsets.UTF_8);
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: 410 Gone")) // First attempt fails
        .thenReturn(pipe.sink());                               // second attempt succeeds

    try {
      PackageUtil.stageClasspathElements(
                                              ImmutableList.of(tmpFile.getAbsolutePath()),
                                              gcsStaging.toString(),
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

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpFile.getAbsolutePath()), gcsStaging.toString());

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

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), gcsStaging.toString());

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

    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(overriddenName + "=" + tmpFile.getAbsolutePath()), gcsStaging.toString());
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertEquals(overriddenName, target.getName());
    assertEquals("gs://somebucket/base/path/file-SAzzqSB2zmoIgNHC9A2G0A.txt",
        target.getLocation());
  }

  @Test
  public void testPackageUploadIsSkippedWithNonExistentResource() throws Exception {
    String nonExistentFile =
        IOChannelUtils.resolve(tmpFolder.getRoot().getPath(), "non-existent-file");
    GcsPath gcsStaging = GcsPath.fromComponents("somebucket", "base/path");
    assertEquals(Collections.EMPTY_LIST, PackageUtil.stageClasspathElements(
        ImmutableList.of(nonExistentFile), gcsStaging.toString()));
  }

  /**
   * Builds a fake GoogleJsonResponseException for testing API error handling.
   */
  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final String reason, final String message) throws IOException {
    final JsonFactory jsonFactory = new JacksonFactory();
    HttpTransport transport = new MockHttpTransport() {
      @Override
      public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
        ErrorInfo errorInfo = new ErrorInfo();
        errorInfo.setReason(reason);
        errorInfo.setMessage(message);
        errorInfo.setFactory(jsonFactory);
        GenericJson error = new GenericJson();
        error.set("code", status);
        error.set("errors", Arrays.asList(errorInfo));
        error.setFactory(jsonFactory);
        GenericJson errorResponse = new GenericJson();
        errorResponse.set("error", error);
        errorResponse.setFactory(jsonFactory);
        return new MockLowLevelHttpRequest().setResponse(
            new MockLowLevelHttpResponse().setContent(errorResponse.toPrettyString())
            .setContentType(Json.MEDIA_TYPE).setStatusCode(status));
        }
    };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }
}
