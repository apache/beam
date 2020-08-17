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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
@SuppressWarnings("CannotMockFinalClass") // Mockito 2 and above can mock final classes
public class AzureBlobStoreFileSystemTest {

  private static AzureBlobStoreFileSystem azureBlobStoreFileSystem;

  @Before
  public void beforeClass() {
    BlobstoreOptions options = PipelineOptionsFactory.as(BlobstoreOptions.class);
    BlobServiceClient mockedServiceClient = Mockito.mock(BlobServiceClient.class);
    BlobContainerClient mockedContainerClient = Mockito.mock(BlobContainerClient.class);
    BlobClient mockedBlobClient = Mockito.mock(BlobClient.class);

    azureBlobStoreFileSystem = new AzureBlobStoreFileSystem(options);
    azureBlobStoreFileSystem.setClient(mockedServiceClient);

    boolean[] created = {false};

    when(azureBlobStoreFileSystem.getClient().createBlobContainer(anyString()))
        .thenAnswer(
            (invocation) -> {
              created[0] = true;
              return mockedContainerClient;
            });

    when(mockedContainerClient.exists()).thenAnswer((invocation) -> created[0]);
    when(azureBlobStoreFileSystem.getClient().getBlobContainerClient(anyString()))
        .thenReturn(mockedContainerClient);
    when(mockedContainerClient.getBlobClient(anyString())).thenReturn(mockedBlobClient);
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
  public void testDelete() throws IOException {
    String containerName = "test-container" + randomUUID();
    String blobName1 = "blob" + randomUUID();
    String blobName2 = "dir1/anotherBlob" + randomUUID();

    // Create files to delete
    BlobContainerClient blobContainerClient =
        azureBlobStoreFileSystem.getClient().createBlobContainer(containerName);
    assertTrue(blobContainerClient.exists());
    blobContainerClient.getBlobClient(blobName1).uploadFromFile("src/test/resources/in.txt");
    blobContainerClient.getBlobClient(blobName2).uploadFromFile("src/test/resources/in.txt");
    assertTrue(blobContainerClient.getBlobClient(blobName1).exists());
    assertTrue(blobContainerClient.getBlobClient(blobName2).exists());

    // Delete the files
    Collection<AzfsResourceId> toDelete = new ArrayList<>();
    String account = blobContainerClient.getAccountName();
    // delete blob
    toDelete.add(AzfsResourceId.fromComponents(account, containerName, blobName1));
    // delete directory
    toDelete.add(AzfsResourceId.fromComponents(account, containerName, "dir1/"));
    azureBlobStoreFileSystem.delete(toDelete);

    // Ensure exception is thrown, clean up
    assertFalse(blobContainerClient.getBlobClient(blobName1).exists());
    assertFalse(blobContainerClient.getBlobClient(blobName2).exists());
    assertThrows(FileNotFoundException.class, () -> azureBlobStoreFileSystem.delete(toDelete));
    blobContainerClient.delete();
  }

  @Test
  public void testCopy() throws IOException {
    BlobServiceClient blobServiceClient = azureBlobStoreFileSystem.getClient();
    String account = blobServiceClient.getAccountName();

    List<AzfsResourceId> src = new ArrayList<>();
    List<AzfsResourceId> dest = new ArrayList<>();
    String srcContainer = "source-container" + randomUUID();
    String destContainer = "dest-container" + randomUUID();

    // Create source file
    BlobContainerClient srcContainerClient = blobServiceClient.createBlobContainer(srcContainer);
    srcContainerClient.getBlobClient("src-blob").uploadFromFile("src/test/resources/in.txt");

    // Copy source file to destination
    src.add(AzfsResourceId.fromComponents(account, srcContainer, "src-blob"));
    dest.add(AzfsResourceId.fromComponents(account, destContainer, "dest-blob"));
    azureBlobStoreFileSystem.copy(src, dest);

    // Confirm the destination container was created
    BlobContainerClient destContainerClient =
        blobServiceClient.getBlobContainerClient(destContainer);
    assertTrue(destContainerClient.getBlobClient("dest-blob").exists());

    // Confirm that the source and destination files are the same
    srcContainerClient.getBlobClient("src-blob").downloadToFile("./src/test/resources/blob1");
    destContainerClient.getBlobClient("dest-blob").downloadToFile("./src/test/resources/blob2");
    File file1 = new File("./src/test/resources/blob1");
    File file2 = new File("./src/test/resources/blob2");
    assertTrue("The files differ!", FileUtils.contentEquals(file1, file2));

    // Clean up
    assertTrue(file1.delete());
    assertTrue(file2.delete());
    blobServiceClient.deleteBlobContainer(srcContainer);
    blobServiceClient.deleteBlobContainer(destContainer);
  }

  @Test
  public void testWriteAndRead() throws IOException {
    BlobServiceClient client = azureBlobStoreFileSystem.getClient();
    String containerName = "test-container" + randomUUID();
    client.createBlobContainer(containerName);

    byte[] writtenArray = new byte[] {0};
    ByteBuffer bb = ByteBuffer.allocate(writtenArray.length);
    bb.put(writtenArray);

    // Create an object and write data to it
    AzfsResourceId path =
        AzfsResourceId.fromUri(
            "azfs://" + client.getAccountName() + "/" + containerName + "/foo/bar.txt");
    WritableByteChannel writableByteChannel =
        azureBlobStoreFileSystem.create(path, builder().setMimeType("application/text").build());
    writableByteChannel.write(bb);
    writableByteChannel.close();

    // Read the same object
    ByteBuffer bb2 = ByteBuffer.allocate(writtenArray.length);
    ReadableByteChannel open = azureBlobStoreFileSystem.open(path);
    open.read(bb2);

    // Compare the content with the one that was written
    byte[] readArray = bb2.array();
    assertArrayEquals(readArray, writtenArray);
    open.close();

    // Clean up
    client.getBlobContainerClient(containerName).delete();
  }

  @Test
  public void testGlobExpansion() throws IOException {
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
  public void testMatch() throws Exception {
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
