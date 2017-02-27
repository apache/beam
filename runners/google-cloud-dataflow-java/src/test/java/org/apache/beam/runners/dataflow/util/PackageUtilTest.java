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
package org.apache.beam.runners.dataflow.util;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineReader;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.Pipe.SinkChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.beam.runners.dataflow.util.PackageUtil.PackageAttributes;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.FastNanoClockAndSleeper;
import org.apache.beam.sdk.testing.RegexMatcher;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

  // 128 bits, base64 encoded is 171 bits, rounds to 22 bytes
  private static final String HASH_PATTERN = "[a-zA-Z0-9+-]{22}";

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGcsUtil(mockGcsUtil);

    IOChannelUtils.registerIOFactoriesAllowOverride(pipelineOptions);
  }

  private File makeFileWithContents(String name, String contents) throws Exception {
    File tmpFile = tmpFolder.newFile(name);
    Files.write(contents, tmpFile, StandardCharsets.UTF_8);
    tmpFile.setLastModified(0);  // required for determinism with directories
    return tmpFile;
  }

  static final String STAGING_PATH = GcsPath.fromComponents("somebucket", "base/path").toString();
  private static PackageAttributes makePackageAttributes(File file, String overridePackageName) {
    return PackageUtil.createPackageAttributes(file, STAGING_PATH, overridePackageName);
  }

  @Test
  public void testFileWithExtensionPackageNamingAndSize() throws Exception {
    String contents = "This is a test!";
    File tmpFile = makeFileWithContents("file.txt", contents);
    PackageAttributes attr = makePackageAttributes(tmpFile, null);
    DataflowPackage target = attr.getDataflowPackage();

    assertThat(target.getName(), RegexMatcher.matches("file-" + HASH_PATTERN + ".txt"));
    assertThat(target.getLocation(), equalTo(STAGING_PATH + '/' + target.getName()));
    assertThat(attr.getSize(), equalTo((long) contents.length()));
  }

  @Test
  public void testPackageNamingWithFileNoExtension() throws Exception {
    File tmpFile = makeFileWithContents("file", "This is a test!");
    DataflowPackage target = makePackageAttributes(tmpFile, null).getDataflowPackage();

    assertThat(target.getName(), RegexMatcher.matches("file-" + HASH_PATTERN));
    assertThat(target.getLocation(), equalTo(STAGING_PATH + '/' + target.getName()));
  }

  @Test
  public void testPackageNamingWithDirectory() throws Exception {
    File tmpDirectory = tmpFolder.newFolder("folder");
    DataflowPackage target = makePackageAttributes(tmpDirectory, null).getDataflowPackage();

    assertThat(target.getName(), RegexMatcher.matches("folder-" + HASH_PATTERN + ".jar"));
    assertThat(target.getLocation(), equalTo(STAGING_PATH + '/' + target.getName()));
  }

  @Test
  public void testPackageNamingWithFilesHavingSameContentsAndSameNames() throws Exception {
    File tmpDirectory1 = tmpFolder.newFolder("folder1", "folderA");
    makeFileWithContents("folder1/folderA/sameName", "This is a test!");
    DataflowPackage target1 = makePackageAttributes(tmpDirectory1, null).getDataflowPackage();

    File tmpDirectory2 = tmpFolder.newFolder("folder2", "folderA");
    makeFileWithContents("folder2/folderA/sameName", "This is a test!");
    DataflowPackage target2 = makePackageAttributes(tmpDirectory2, null).getDataflowPackage();

    assertEquals(target1.getName(), target2.getName());
    assertEquals(target1.getLocation(), target2.getLocation());
  }

  @Test
  public void testPackageNamingWithFilesHavingSameContentsButDifferentNames() throws Exception {
    File tmpDirectory1 = tmpFolder.newFolder("folder1", "folderA");
    makeFileWithContents("folder1/folderA/uniqueName1", "This is a test!");
    DataflowPackage target1 = makePackageAttributes(tmpDirectory1, null).getDataflowPackage();

    File tmpDirectory2 = tmpFolder.newFolder("folder2", "folderA");
    makeFileWithContents("folder2/folderA/uniqueName2", "This is a test!");
    DataflowPackage target2 = makePackageAttributes(tmpDirectory2, null).getDataflowPackage();

    assertNotEquals(target1.getName(), target2.getName());
    assertNotEquals(target1.getLocation(), target2.getLocation());
  }

  @Test
  public void testPackageNamingWithDirectoriesHavingSameContentsButDifferentNames()
      throws Exception {
    File tmpDirectory1 = tmpFolder.newFolder("folder1", "folderA");
    tmpFolder.newFolder("folder1", "folderA", "uniqueName1");
    DataflowPackage target1 = makePackageAttributes(tmpDirectory1, null).getDataflowPackage();

    File tmpDirectory2 = tmpFolder.newFolder("folder2", "folderA");
    tmpFolder.newFolder("folder2", "folderA", "uniqueName2");
    DataflowPackage target2 = makePackageAttributes(tmpDirectory2, null).getDataflowPackage();

    assertNotEquals(target1.getName(), target2.getName());
    assertNotEquals(target1.getLocation(), target2.getLocation());
  }

  @Test
  public void testPackageUploadWithLargeClasspathLogsWarning() throws Exception {
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    // all files will be present and cached so no upload needed.
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(tmpFile.length());

    List<String> classpathElements = Lists.newLinkedList();
    for (int i = 0; i < 1005; ++i) {
      String eltName = "element" + i;
      classpathElements.add(eltName + '=' + tmpFile.getAbsolutePath());
    }

    PackageUtil.stageClasspathElements(classpathElements, STAGING_PATH, mockGcsUtil);

    logged.verifyWarn("Your classpath contains 1005 elements, which Google Cloud Dataflow");
  }

  @Test
  public void testPackageUploadWithFileSucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    String contents = "This is a test!";
    File tmpFile = makeFileWithContents("file.txt", contents);
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpFile.getAbsolutePath()), STAGING_PATH, mockGcsUtil);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertThat(target.getName(), RegexMatcher.matches("file-" + HASH_PATTERN + ".txt"));
    assertThat(target.getLocation(), equalTo(STAGING_PATH + '/' + target.getName()));
    assertThat(new LineReader(Channels.newReader(pipe.source(), "UTF-8")).readLine(),
        equalTo(contents));
  }

  @Test
  public void testStagingPreservesClasspath() throws Exception {
    File smallFile = makeFileWithContents("small.txt", "small");
    File largeFile = makeFileWithContents("large.txt", "large contents");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenAnswer(new Answer<SinkChannel>() {
          @Override
          public SinkChannel answer(InvocationOnMock invocation) throws Throwable {
            return Pipe.open().sink();
          }
        });

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(smallFile.getAbsolutePath(), largeFile.getAbsolutePath()),
        STAGING_PATH, mockGcsUtil);
    // Verify that the packages are returned small, then large, matching input order even though
    // the large file would be uploaded first.
    assertThat(targets.get(0).getName(), startsWith("small"));
    assertThat(targets.get(1).getName(), startsWith("large"));
  }

  @Test
  public void testPackageUploadWithDirectorySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpDirectory = tmpFolder.newFolder("folder");
    tmpFolder.newFolder("folder", "empty_directory");
    tmpFolder.newFolder("folder", "directory");
    makeFileWithContents("folder/file.txt", "This is a test!");
    makeFileWithContents("folder/directory/file.txt", "This is also a test!");

    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), STAGING_PATH, mockGcsUtil);

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

    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), STAGING_PATH, mockGcsUtil);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertThat(target.getName(), RegexMatcher.matches("folder-" + HASH_PATTERN + ".jar"));
    assertThat(target.getLocation(), equalTo(STAGING_PATH + '/' + target.getName()));
    assertNull(new ZipInputStream(Channels.newInputStream(pipe.source())).getNextEntry());
  }

  @Test(expected = RuntimeException.class)
  public void testPackageUploadFailsWhenIOExceptionThrown() throws Exception {
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: Upload error"));

    try {
      PackageUtil.stageClasspathElements(
          ImmutableList.of(tmpFile.getAbsolutePath()),
          STAGING_PATH, fastNanoClockAndSleeper, MoreExecutors.newDirectExecutorService(),
          mockGcsUtil);
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil, times(5)).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadFailsWithPermissionsErrorGivesDetailedMessage() throws Exception {
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Failed to write to GCS path " + STAGING_PATH,
            googleJsonResponseException(
                HttpStatusCodes.STATUS_CODE_FORBIDDEN, "Permission denied", "Test message")));

    try {
      PackageUtil.stageClasspathElements(
          ImmutableList.of(tmpFile.getAbsolutePath()),
          STAGING_PATH, fastNanoClockAndSleeper, MoreExecutors.newDirectExecutorService(),
          mockGcsUtil);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertThat("Expected RuntimeException wrapping IOException.",
          e.getCause(), instanceOf(RuntimeException.class));
      assertThat("Expected IOException containing detailed message.",
          e.getCause().getCause(), instanceOf(IOException.class));
      assertThat(e.getCause().getCause().getMessage(),
          Matchers.allOf(
              Matchers.containsString("Uploaded failed due to permissions error"),
              Matchers.containsString(
                  "Stale credentials can be resolved by executing 'gcloud auth application-default "
                      + "login'")));
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadEventuallySucceeds() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString()))
        .thenThrow(new IOException("Fake Exception: 410 Gone")) // First attempt fails
        .thenReturn(pipe.sink());                               // second attempt succeeds

    try {
      PackageUtil.stageClasspathElements(
          ImmutableList.of(tmpFile.getAbsolutePath()), STAGING_PATH, fastNanoClockAndSleeper,
          MoreExecutors.newDirectExecutorService(), mockGcsUtil);
    } finally {
      verify(mockGcsUtil).fileSize(any(GcsPath.class));
      verify(mockGcsUtil, times(2)).create(any(GcsPath.class), anyString());
      verifyNoMoreInteractions(mockGcsUtil);
    }
  }

  @Test
  public void testPackageUploadIsSkippedWhenFileAlreadyExists() throws Exception {
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(tmpFile.length());

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpFile.getAbsolutePath()), STAGING_PATH, mockGcsUtil);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verifyNoMoreInteractions(mockGcsUtil);
  }

  @Test
  public void testPackageUploadIsNotSkippedWhenSizesAreDifferent() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpDirectory = tmpFolder.newFolder("folder");
    tmpFolder.newFolder("folder", "empty_directory");
    tmpFolder.newFolder("folder", "directory");
    makeFileWithContents("folder/file.txt", "This is a test!");
    makeFileWithContents("folder/directory/file.txt", "This is also a test!");
    when(mockGcsUtil.fileSize(any(GcsPath.class))).thenReturn(Long.MAX_VALUE);
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    PackageUtil.stageClasspathElements(
        ImmutableList.of(tmpDirectory.getAbsolutePath()), STAGING_PATH, mockGcsUtil);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);
  }

  @Test
  public void testPackageUploadWithExplicitPackageName() throws Exception {
    Pipe pipe = Pipe.open();
    File tmpFile = makeFileWithContents("file.txt", "This is a test!");
    final String overriddenName = "alias.txt";

    when(mockGcsUtil.fileSize(any(GcsPath.class)))
        .thenThrow(new FileNotFoundException("some/path"));
    when(mockGcsUtil.create(any(GcsPath.class), anyString())).thenReturn(pipe.sink());

    List<DataflowPackage> targets = PackageUtil.stageClasspathElements(
        ImmutableList.of(overriddenName + "=" + tmpFile.getAbsolutePath()), STAGING_PATH,
        mockGcsUtil);
    DataflowPackage target = Iterables.getOnlyElement(targets);

    verify(mockGcsUtil).fileSize(any(GcsPath.class));
    verify(mockGcsUtil).create(any(GcsPath.class), anyString());
    verifyNoMoreInteractions(mockGcsUtil);

    assertThat(target.getName(), equalTo(overriddenName));
    assertThat(target.getLocation(),
        RegexMatcher.matches(STAGING_PATH + "/file-" + HASH_PATTERN + ".txt"));
  }

  @Test
  public void testPackageUploadIsSkippedWithNonExistentResource() throws Exception {
    String nonExistentFile =
        IOChannelUtils.resolve(tmpFolder.getRoot().getPath(), "non-existent-file");
    assertEquals(Collections.EMPTY_LIST, PackageUtil.stageClasspathElements(
        ImmutableList.of(nonExistentFile), STAGING_PATH, mockGcsUtil));
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
