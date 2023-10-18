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
package org.apache.beam.it.gcp.storage;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link GcsResourceManager}. */
@RunWith(JUnit4.class)
public final class GcsResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Storage client;

  @Mock private Blob blob;
  private GcsResourceManager gcsClient;

  private static final String ARTIFACT_NAME = "test-artifact.json";
  private static final Path LOCAL_PATH;
  private static final byte[] TEST_ARTIFACT_CONTENTS;

  static {
    try {
      LOCAL_PATH = Paths.get(Resources.getResource(ARTIFACT_NAME).toURI());
      TEST_ARTIFACT_CONTENTS = Files.readAllBytes(LOCAL_PATH);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static final String BUCKET = "test-bucket";
  private static final String TEST_CLASS = "test-class-name";
  private static final String TEST_METHOD = "test-method-name";

  @Captor private ArgumentCaptor<String> bucketCaptor;
  @Captor private ArgumentCaptor<BlobInfo> blobInfoCaptor;
  @Captor private ArgumentCaptor<byte[]> contentsCaptor;
  @Captor private ArgumentCaptor<BlobListOption> listOptionsCaptor;
  @Captor private ArgumentCaptor<Iterable<BlobId>> blobIdCaptor;

  @Before
  public void setUp() {
    gcsClient = new GcsResourceManager(client, BUCKET, TEST_CLASS);
  }

  @After
  public void tearDown() {
    gcsClient.cleanupAll();
  }

  @Test
  public void testBuilderWithEmptyBucket() {
    assertThrows(
        IllegalArgumentException.class,
        () -> GcsResourceManager.builder("", TEST_CLASS, null).build());
  }

  @Test
  public void testBuilderWithEmptyTestClassName() {
    assertThrows(
        IllegalArgumentException.class, () -> GcsResourceManager.builder(BUCKET, "", null).build());
  }

  @Test
  public void testCreateArtifactInRunDir() {
    String artifactName = "artifact.txt";
    byte[] contents = new byte[] {0, 1, 2};
    when(client.create(any(BlobInfo.class), any(byte[].class))).thenReturn(blob);

    GcsArtifact actual = (GcsArtifact) gcsClient.createArtifact(artifactName, contents);

    verify(client).create(blobInfoCaptor.capture(), contentsCaptor.capture());
    BlobInfo actualInfo = blobInfoCaptor.getValue();

    assertThat(actual.blob).isSameInstanceAs(blob);
    assertThat(actualInfo.getBucket()).isEqualTo(BUCKET);
    assertThat(actualInfo.getName())
        .isEqualTo(String.format("%s/%s/%s", TEST_CLASS, gcsClient.runId(), artifactName));
    assertThat(contentsCaptor.getValue()).isEqualTo(contents);
  }

  @Test
  public void testUploadArtifact() throws IOException {
    when(client.create(any(BlobInfo.class), any(byte[].class))).thenReturn(blob);

    GcsArtifact actual = (GcsArtifact) gcsClient.uploadArtifact(ARTIFACT_NAME, LOCAL_PATH);

    verify(client).create(blobInfoCaptor.capture(), contentsCaptor.capture());
    BlobInfo actualInfo = blobInfoCaptor.getValue();

    assertThat(actual.blob).isSameInstanceAs(blob);
    assertThat(actualInfo.getBucket()).isEqualTo(BUCKET);
    assertThat(actualInfo.getName())
        .isEqualTo(String.format("%s/%s/%s", TEST_CLASS, gcsClient.runId(), ARTIFACT_NAME));
    assertThat(contentsCaptor.getValue()).isEqualTo(TEST_ARTIFACT_CONTENTS);
  }

  @Test
  public void testUploadArtifactInvalidLocalPath() {
    when(client.create(any(BlobInfo.class), any())).thenReturn(blob);
    assertThrows(
        IOException.class, () -> gcsClient.uploadArtifact(ARTIFACT_NAME, "/" + UUID.randomUUID()));
  }

  @Test
  public void testListArtifactsInMethodDirSinglePage() {
    // Arrange
    String name1 = "blob1";
    String name2 = "blob2";
    String name3 = "blob3";
    ImmutableList<Blob> page1 =
        ImmutableList.of(mock(Blob.class), mock(Blob.class), mock(Blob.class));
    when(page1.get(0).getName()).thenReturn(name1);
    when(page1.get(1).getName()).thenReturn(name2);
    when(page1.get(2).getName()).thenReturn(name3);

    TestBlobPage allPages = createPages(page1);
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);

    Pattern pattern = Pattern.compile(".*blob[13].*");

    // Act
    List<Artifact> actual = gcsClient.listArtifacts(TEST_METHOD, pattern);

    // Assert
    verify(client).list(bucketCaptor.capture(), listOptionsCaptor.capture());

    String actualBucket = bucketCaptor.getValue();
    BlobListOption actualOptions = listOptionsCaptor.getValue();

    assertThat(actual).hasSize(2);
    assertThat(actual.get(0).name()).isEqualTo(name1);
    assertThat(actual.get(1).name()).isEqualTo(name3);
    assertThat(actualBucket).isEqualTo(BUCKET);
    assertThat(actualOptions)
        .isEqualTo(
            BucketListOption.prefix(
                String.format("%s/%s/%s", TEST_CLASS, gcsClient.runId(), TEST_METHOD)));
  }

  @Test
  public void testListArtifactsInMethodDirMultiplePages() {
    // Arrange
    String name1 = "blob1";
    String name2 = "blob2";
    String name3 = "blob3";
    ImmutableList<Blob> page1 = ImmutableList.of(mock(Blob.class), mock(Blob.class));
    ImmutableList<Blob> page2 = ImmutableList.of(mock(Blob.class));
    when(page1.get(0).getName()).thenReturn(name1);
    when(page1.get(1).getName()).thenReturn(name2);
    when(page2.get(0).getName()).thenReturn(name3);

    TestBlobPage allPages = createPages(page1, page2);
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);

    Pattern pattern = Pattern.compile(".*blob[13].*");

    // Act
    List<Artifact> actual = gcsClient.listArtifacts(TEST_METHOD, pattern);

    // Assert
    verify(client).list(bucketCaptor.capture(), listOptionsCaptor.capture());

    String actualBucket = bucketCaptor.getValue();
    BlobListOption actualOptions = listOptionsCaptor.getValue();

    assertThat(actual).hasSize(2);
    assertThat(actual.get(0).name()).isEqualTo(name1);
    assertThat(actual.get(1).name()).isEqualTo(name3);
    assertThat(actualBucket).isEqualTo(BUCKET);
    assertThat(actualOptions)
        .isEqualTo(
            BucketListOption.prefix(
                String.format("%s/%s/%s", TEST_CLASS, gcsClient.runId(), TEST_METHOD)));
  }

  @Test
  public void testListArtifactsInMethodDirNoArtifacts() {
    TestBlobPage allPages = createPages(ImmutableList.of());
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);
    Pattern pattern = Pattern.compile(".*blob[13].*");

    List<Artifact> actual = gcsClient.listArtifacts(TEST_METHOD, pattern);

    verify(client).list(anyString(), any(BlobListOption.class));
    assertThat(actual).isEmpty();
  }

  @Test
  public void testCleanupRunSinglePage() {
    // Arrange
    BlobId id1 = BlobId.of(BUCKET, "blob1");
    BlobId id2 = BlobId.of(BUCKET, "blob2");
    BlobId id3 = BlobId.of(BUCKET, "blob3");
    ImmutableList<Blob> page1 =
        ImmutableList.of(mock(Blob.class), mock(Blob.class), mock(Blob.class));
    when(page1.get(0).getBlobId()).thenReturn(id1);
    when(page1.get(1).getBlobId()).thenReturn(id2);
    when(page1.get(2).getBlobId()).thenReturn(id3);

    TestBlobPage allPages = createPages(page1);
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);

    when(client.delete(anyIterable())).thenReturn(ImmutableList.of(true, false, true));

    // Act
    gcsClient.cleanupAll();

    // Assert
    verify(client).list(bucketCaptor.capture(), listOptionsCaptor.capture());
    verify(client).delete(blobIdCaptor.capture());

    String actualBucket = bucketCaptor.getValue();
    BlobListOption actualOption = listOptionsCaptor.getValue();
    Iterable<BlobId> actualIds = blobIdCaptor.getValue();

    assertThat(actualBucket).isEqualTo(BUCKET);
    assertThat(actualOption)
        .isEqualTo(BucketListOption.prefix(String.format("%s/%s", TEST_CLASS, gcsClient.runId())));
    assertThat(actualIds).containsExactly(id1, id2, id3);
  }

  @Test
  public void testCleanupRunMultiplePages() {
    // Arrange
    BlobId id1 = BlobId.of(BUCKET, "blob1");
    BlobId id2 = BlobId.of(BUCKET, "blob2");
    BlobId id3 = BlobId.of(BUCKET, "blob3");
    ImmutableList<Blob> page1 = ImmutableList.of(mock(Blob.class), mock(Blob.class));
    ImmutableList<Blob> page2 = ImmutableList.of(mock(Blob.class));
    when(page1.get(0).getBlobId()).thenReturn(id1);
    when(page1.get(1).getBlobId()).thenReturn(id2);
    when(page2.get(0).getBlobId()).thenReturn(id3);

    TestBlobPage allPages = createPages(page1, page2);
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);

    when(client.delete(anyIterable()))
        .thenReturn(ImmutableList.of(true, false))
        .thenReturn(ImmutableList.of(true));

    // Act
    gcsClient.cleanupAll();

    // Assert
    verify(client).list(bucketCaptor.capture(), listOptionsCaptor.capture());
    verify(client, times(2)).delete(blobIdCaptor.capture());

    String actualBucket = bucketCaptor.getValue();
    BlobListOption actualOption = listOptionsCaptor.getValue();
    List<Iterable<BlobId>> actualBlobIds = blobIdCaptor.getAllValues();

    assertThat(actualBucket).isEqualTo(BUCKET);
    assertThat(actualOption)
        .isEqualTo(BucketListOption.prefix(String.format("%s/%s", TEST_CLASS, gcsClient.runId())));
    assertThat(actualBlobIds.get(0)).containsExactly(id1, id2);
    assertThat(actualBlobIds.get(1)).containsExactly(id3);
  }

  @Test
  public void testDeleteArtifactsNoArtifacts() {
    TestBlobPage allPages = createPages(ImmutableList.of());
    when(client.list(anyString(), any(BlobListOption.class))).thenReturn(allPages);

    gcsClient.cleanupAll();

    verify(client, never()).delete(anyIterable());
  }

  private static TestBlobPage createPages(ImmutableList<Blob>... pageContents) {
    if (pageContents.length == 0) {
      return new TestBlobPage(ImmutableList.of());
    }
    TestBlobPage first = new TestBlobPage(pageContents[0]);
    TestBlobPage current = first;
    for (int i = 1; i < pageContents.length; ++i) {
      current.setNext(pageContents[i]);
      current = current.next;
    }
    return first;
  }

  private static final class TestBlobPage implements Page<Blob> {
    private TestBlobPage next;
    private final ImmutableList<Blob> contents;

    public TestBlobPage(ImmutableList<Blob> contents) {
      this.contents = contents;
      this.next = null;
    }

    public void setNext(ImmutableList<Blob> contents) {
      next = new TestBlobPage(contents);
    }

    @Override
    public boolean hasNextPage() {
      return next != null;
    }

    @Override
    public String getNextPageToken() {
      return "token";
    }

    @Override
    public Page<Blob> getNextPage() {
      return next;
    }

    @Override
    public Iterable<Blob> iterateAll() {
      return contents;
    }

    @Override
    public Iterable<Blob> getValues() {
      return contents;
    }
  }
}
