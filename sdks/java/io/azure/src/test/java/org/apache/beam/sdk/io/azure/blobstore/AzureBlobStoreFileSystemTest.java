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
package org.apache.beam.sdk.io.azure.blobstore;

import static java.util.UUID.randomUUID;
import static org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions.builder;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
@SuppressWarnings("CannotMockFinalClass") // Mockito 2 and above can mock final classes
public class AzureBlobStoreFileSystemTest {

  private static AzureBlobStoreFileSystem azureBlobStoreFileSystem;
  BlobstoreOptions options = PipelineOptionsFactory.as(BlobstoreOptions.class);
  BlobstoreOptions spyOptions = Mockito.spy(options);
  BlobServiceClient mockedServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient mockedContainerClient = Mockito.mock(BlobContainerClient.class);
  BlobClient mockedBlobClient = Mockito.mock(BlobClient.class);
  BlockBlobClient mockedBlockBlob = Mockito.mock(BlockBlobClient.class);
  BlobProperties mockedProperties = Mockito.mock(BlobProperties.class);
  PagedIterable<BlobItem> mockedPagedIterable = Mockito.mock(PagedIterable.class);
  BlobOutputStream mockedOutputStream = Mockito.mock(BlobOutputStream.class);
  BlobItem mockedBlobItem = Mockito.mock(BlobItem.class);
  BlobInputStream mockedInputStream = Mockito.mock(BlobInputStream.class);

  @Before
  public void beforeClass() {
    azureBlobStoreFileSystem = new AzureBlobStoreFileSystem(spyOptions);
    azureBlobStoreFileSystem.setClient(mockedServiceClient);

    boolean[] containerCreated = {false};
    when(mockedServiceClient.createBlobContainer(anyString()))
        .thenAnswer(
            (invocation) -> {
              containerCreated[0] = true;
              return mockedContainerClient;
            });
    when(mockedContainerClient.exists()).thenAnswer((invocation) -> containerCreated[0]);
    boolean[] blobCreated = {false};
    doAnswer(
            invocation -> {
              blobCreated[0] = true;
              return null;
            })
        .when(mockedBlobClient)
        .uploadFromFile(anyString());
    when(mockedBlobClient.exists()).thenAnswer((invocation) -> blobCreated[0]);
    when(azureBlobStoreFileSystem.getClient().getBlobContainerClient(anyString()))
        .thenReturn(mockedContainerClient);
    when(mockedContainerClient.getBlobClient(anyString())).thenReturn(mockedBlobClient);
    when(mockedBlobClient.getBlockBlobClient()).thenReturn(mockedBlockBlob);
    when(mockedBlobClient.getProperties()).thenReturn(mockedProperties);
    when(mockedProperties.getBlobSize()).thenReturn(Long.valueOf(1));
    when(mockedProperties.getLastModified()).thenReturn(OffsetDateTime.now());
    when(mockedContainerClient.listBlobs(any(ListBlobsOptions.class), any(Duration.class)))
        .thenReturn(mockedPagedIterable);
    when(mockedContainerClient.listBlobsByHierarchy(any(String.class)))
        .thenReturn(mockedPagedIterable);
    when(mockedBlockBlob.getBlobOutputStream())
        .thenAnswer(
            (i) -> {
              blobCreated[0] = true;
              return mockedOutputStream;
            });
    when(mockedBlobItem.getName()).thenReturn("name");
    when(spyOptions.getSasToken()).thenReturn("sas-token");
    when(mockedBlobClient.openInputStream()).thenReturn(mockedInputStream);
  }

  @Test
  public void testGetScheme() {
    assertEquals("azfs", azureBlobStoreFileSystem.getScheme());
  }

  @Test
  public void testGlobTranslation() {
    assertEquals("foo", AzureBlobStoreFileSystem.wildcardToRegexp("foo"));
    assertEquals("fo[^/]*o", AzureBlobStoreFileSystem.wildcardToRegexp("fo*o"));
    assertEquals("f[^/]*o\\.[^/]", AzureBlobStoreFileSystem.wildcardToRegexp("f*o.?"));
    assertEquals("foo-[0-9][^/]*", AzureBlobStoreFileSystem.wildcardToRegexp("foo-[0-9]*"));
    assertEquals("foo-[0-9].*", AzureBlobStoreFileSystem.wildcardToRegexp("foo-[0-9]**"));
    assertEquals(".*foo", AzureBlobStoreFileSystem.wildcardToRegexp("**/*foo"));
    assertEquals(".*foo", AzureBlobStoreFileSystem.wildcardToRegexp("**foo"));
    assertEquals("foo/[^/]*", AzureBlobStoreFileSystem.wildcardToRegexp("foo/*"));
    assertEquals("foo[^/]*", AzureBlobStoreFileSystem.wildcardToRegexp("foo*"));
    assertEquals("foo/[^/]*/[^/]*/[^/]*", AzureBlobStoreFileSystem.wildcardToRegexp("foo/*/*/*"));
    assertEquals("foo/[^/]*/.*", AzureBlobStoreFileSystem.wildcardToRegexp("foo/*/**"));
    assertEquals("foo.*baz", AzureBlobStoreFileSystem.wildcardToRegexp("foo**baz"));
  }

  @Test
  @SuppressWarnings("CheckReturnValue")
  public void testCopy() throws IOException {
    List<AzfsResourceId> src =
        new ArrayList<>(
            Arrays.asList(AzfsResourceId.fromComponents("account", "container", "from")));
    List<AzfsResourceId> dest =
        new ArrayList<>(Arrays.asList(AzfsResourceId.fromComponents("account", "container", "to")));
    when(mockedBlobClient.exists()).thenReturn(true);
    azureBlobStoreFileSystem.copy(src, dest);
    verify(mockedBlobClient, times(1)).copyFromUrl(any(String.class));
  }

  @Test
  public void testWriteAndRead() throws IOException {
    azureBlobStoreFileSystem.getClient().createBlobContainer("testcontainer");

    byte[] writtenArray = new byte[] {0};
    ByteBuffer bb = ByteBuffer.allocate(writtenArray.length);
    bb.put(writtenArray);

    // First create an object and write data to it
    AzfsResourceId path = AzfsResourceId.fromUri("azfs://account/testcontainer/foo/bar.txt");
    WritableByteChannel writableByteChannel =
        azureBlobStoreFileSystem.create(path, builder().setMimeType("application/text").build());
    writableByteChannel.write(bb);
    writableByteChannel.close();

    // Now read the same object
    ByteBuffer bb2 = ByteBuffer.allocate(writtenArray.length);
    ReadableByteChannel open = azureBlobStoreFileSystem.open(path);
    open.read(bb2);

    // And compare the content with the one that was written
    byte[] readArray = bb2.array();
    assertArrayEquals(readArray, writtenArray);
    open.close();
  }

  @Test
  @Ignore
  public void testGlobExpansion() throws IOException {
    // TODO: Write this test with mocks - see GcsFileSystemTest
    String container = "test-container" + randomUUID();
    BlobContainerClient blobContainerClient =
        azureBlobStoreFileSystem.getClient().createBlobContainer(container);

    // Create files
    List<String> blobNames = new ArrayList<>();
    blobNames.add("testdirectory/file1name");
    blobNames.add("testdirectory/file2name");
    blobNames.add("testdirectory/file3name");
    blobNames.add("testdirectory/otherfile");
    blobNames.add("testotherdirectory/file4name");
    for (String blob : blobNames) {
      blobContainerClient.getBlobClient(blob).uploadFromFile("src/test/resources/in.txt");
    }

    // Test patterns
    {
      AzfsResourceId pattern =
          AzfsResourceId.fromUri("azfs://account/" + container + "/testdirectory/file*");
      List<String> expectedFiles =
          ImmutableList.of(
              "azfs://account/" + container + "/testdirectory/file1name",
              "azfs://account/" + container + "/testdirectory/file2name",
              "azfs://account/" + container + "/testdirectory/file3name");

      assertThat(
          expectedFiles, contains(toFilenames(azureBlobStoreFileSystem.expand(pattern)).toArray()));
    }

    {
      AzfsResourceId pattern =
          AzfsResourceId.fromUri("azfs://account/" + container + "/testdirectory/file[1-3]*");
      List<String> expectedFiles =
          ImmutableList.of(
              "azfs://account/" + container + "/testdirectory/file1name",
              "azfs://account/" + container + "/testdirectory/file2name",
              "azfs://account/" + container + "/testdirectory/file3name");

      assertThat(
          expectedFiles, contains(toFilenames(azureBlobStoreFileSystem.expand(pattern)).toArray()));
    }

    {
      AzfsResourceId pattern =
          AzfsResourceId.fromUri("azfs://account/" + container + "/testdirectory/file?name");
      List<String> expectedFiles =
          ImmutableList.of(
              "azfs://account/" + container + "/testdirectory/file1name",
              "azfs://account/" + container + "/testdirectory/file2name",
              "azfs://account/" + container + "/testdirectory/file3name");

      assertThat(
          expectedFiles, contains(toFilenames(azureBlobStoreFileSystem.expand(pattern)).toArray()));
    }

    {
      AzfsResourceId pattern =
          AzfsResourceId.fromUri("azfs://account/" + container + "/test*ectory/fi*name");
      List<String> expectedFiles =
          ImmutableList.of(
              "azfs://account/" + container + "/testdirectory/file1name",
              "azfs://account/" + container + "/testdirectory/file2name",
              "azfs://account/" + container + "/testdirectory/file3name",
              "azfs://account/" + container + "/testotherdirectory/file4name");

      assertThat(
          expectedFiles, contains(toFilenames(azureBlobStoreFileSystem.expand(pattern)).toArray()));
    }

    // Clean up
    blobContainerClient.delete();
  }

  private List<String> toFilenames(MatchResult matchResult) throws IOException {
    return FluentIterable.from(matchResult.metadata())
        .transform(metadata -> (metadata.resourceId()).toString())
        .toList();
  }

  @Test
  @Ignore
  public void testMatch() throws Exception {
    // TODO: Write this test with mocks - see GcsFileSystemTest
    String container = "test-container" + randomUUID();
    BlobContainerClient blobContainerClient =
        azureBlobStoreFileSystem.getClient().createBlobContainer(container);

    // Create files
    List<String> blobNames = new ArrayList<>();
    blobNames.add("testdirectory/file1name");
    blobNames.add("testdirectory/file2name");
    blobNames.add("testdirectory/file3name");
    blobNames.add("testdirectory/file4name");
    blobNames.add("testdirectory/otherfile");
    blobNames.add("testotherdirectory/anotherfile");
    for (String blob : blobNames) {
      blobContainerClient.getBlobClient(blob).uploadFromFile("src/test/resources/in.txt");
    }

    List<String> specs =
        ImmutableList.of(
            "azfs://account/" + container + "/testdirectory/file[1-3]*",
            "azfs://account/" + container + "/testdirectory/non-exist-file",
            "azfs://account/" + container + "/testdirectory/otherfile");

    List<MatchResult> matchResults = azureBlobStoreFileSystem.match(specs);

    // Confirm that match results are as expected
    assertEquals(3, matchResults.size());
    assertEquals(MatchResult.Status.OK, matchResults.get(0).status());
    assertThat(
        ImmutableList.of(
            "azfs://account/" + container + "/testdirectory/file1name",
            "azfs://account/" + container + "/testdirectory/file2name",
            "azfs://account/" + container + "/testdirectory/file3name"),
        contains(toFilenames(matchResults.get(0)).toArray()));
    assertEquals(MatchResult.Status.NOT_FOUND, matchResults.get(1).status());
    assertEquals(MatchResult.Status.OK, matchResults.get(2).status());
    assertThat(
        ImmutableList.of("azfs://account/" + container + "/testdirectory/otherfile"),
        contains(toFilenames(matchResults.get(2)).toArray()));

    blobContainerClient.delete();
  }

  @Test
  public void testMatchNonGlobs() throws Exception {
    // TODO: Write this test with mocks - see GcsFileSystemTest
    String container = "test-container" + randomUUID();
    BlobContainerClient blobContainerClient =
        azureBlobStoreFileSystem.getClient().createBlobContainer(container);

    List<String> blobNames = new ArrayList<>();
    blobNames.add("testdirectory/file1name");
    blobNames.add("testdirectory/dir2name/");
    blobNames.add("testdirectory/file4name");
    // TODO: Also test match results where MatchResult.STATUS != OK (see gcs and s3 tests)

    for (String blob : blobNames) {
      blobContainerClient.getBlobClient(blob).uploadFromFile("src/test/resources/in.txt");
    }

    List<String> specs =
        ImmutableList.of(
            "azfs://account/" + container + "/testdirectory/file1name",
            "azfs://account/" + container + "/testdirectory/dir2name/",
            "azfs://account/" + container + "/testdirectory/file4name");

    List<MatchResult> matchResults = azureBlobStoreFileSystem.match(specs);

    assertEquals(3, matchResults.size());
    assertThat(
        ImmutableList.of("azfs://account/" + container + "/testdirectory/file1name"),
        contains(toFilenames(matchResults.get(0)).toArray()));
    assertThat(
        ImmutableList.of("azfs://account/" + container + "/testdirectory/dir2name/"),
        contains(toFilenames(matchResults.get(1)).toArray()));
    assertThat(
        ImmutableList.of("azfs://account/" + container + "/testdirectory/file4name"),
        contains(toFilenames(matchResults.get(2)).toArray()));

    blobContainerClient.delete();
  }
}
