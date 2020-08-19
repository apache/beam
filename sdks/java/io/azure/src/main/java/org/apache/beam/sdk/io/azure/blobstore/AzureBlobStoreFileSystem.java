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

import static java.nio.channels.Channels.newChannel;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.SharedAccessAccountPolicy;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.azure.options.BlobstoreClientBuilderFactory;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AzureBlobStoreFileSystem extends FileSystem<AzfsResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreFileSystem.class);

  private static final ImmutableSet<String> NON_READ_SEEK_EFFICIENT_ENCODINGS =
      ImmutableSet.of("gzip");

  private Supplier<BlobServiceClient> client;
  private final BlobstoreOptions options;

  AzureBlobStoreFileSystem(BlobstoreOptions options) {
    this.options = checkNotNull(options, "options");

    BlobServiceClientBuilder builder =
        InstanceBuilder.ofType(BlobstoreClientBuilderFactory.class)
            .fromClass(options.getBlobstoreClientFactoryClass())
            .build()
            .createBuilder(options);

    // The Supplier is to make sure we don't call .build() unless we are actually using Azure.
    client = Suppliers.memoize(builder::buildClient);
  }

  @VisibleForTesting
  void setClient(BlobServiceClient client) {
    this.client = Suppliers.ofInstance(client);
  }

  @VisibleForTesting
  BlobServiceClient getClient() {
    return client.get();
  }

  @Override
  protected String getScheme() {
    return AzfsResourceId.SCHEME;
  }

  @Override
  protected List<MatchResult> match(List<String> specs) {
    List<AzfsResourceId> paths =
        specs.stream().map(AzfsResourceId::fromUri).collect(Collectors.toList());
    List<AzfsResourceId> globs = new ArrayList<>();
    List<AzfsResourceId> nonGlobs = new ArrayList<>();
    List<Boolean> isGlobBooleans = new ArrayList<>();

    for (AzfsResourceId path : paths) {
      if (path.isWildcard()) {
        globs.add(path);
        isGlobBooleans.add(true);
      } else {
        nonGlobs.add(path);
        isGlobBooleans.add(false);
      }
    }

    Iterator<MatchResult> globMatches = matchGlobPaths(globs).iterator();
    Iterator<MatchResult> nonGlobMatches = matchNonGlobPaths(nonGlobs).iterator();

    ImmutableList.Builder<MatchResult> matchResults = ImmutableList.builder();
    for (Boolean isGlob : isGlobBooleans) {
      if (isGlob) {
        checkState(globMatches.hasNext(), "Expect globMatches has next.");
        matchResults.add(globMatches.next());
      } else {
        checkState(nonGlobMatches.hasNext(), "Expect nonGlobMatches has next.");
        matchResults.add(nonGlobMatches.next());
      }
    }
    checkState(!globMatches.hasNext(), "Expect no more elements in globMatches.");
    checkState(!nonGlobMatches.hasNext(), "Expect no more elements in nonGlobMatches.");

    return matchResults.build();
  }

  /**
   * Expands glob expressions to regular expressions.
   *
   * @param globExp the glob expression to expand
   * @return a string with the regular expression this glob expands to
   */
  @VisibleForTesting
  static String wildcardToRegexp(String globExp) {
    StringBuilder dst = new StringBuilder();
    char[] src = globExp.replace("**/*", "**").toCharArray();
    int i = 0;
    while (i < src.length) {
      char c = src[i++];
      switch (c) {
        case '*':
          // One char lookahead for **
          if (i < src.length && src[i] == '*') {
            dst.append(".*");
            ++i;
          } else {
            dst.append("[^/]*");
          }
          break;
        case '?':
          dst.append("[^/]");
          break;
        case '.':
        case '+':
        case '{':
        case '}':
        case '(':
        case ')':
        case '|':
        case '^':
        case '$':
          // These need to be escaped in regular expressions
          dst.append('\\').append(c);
          break;
        case '\\':
          i = doubleSlashes(dst, src, i);
          break;
        default:
          dst.append(c);
          break;
      }
    }
    return dst.toString();
  }

  private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
    // Emit the next character without special interpretation
    dst.append("\\\\");
    if ((i - 1) != src.length) {
      dst.append(src[i]);
      i++;
    } else {
      // A backslash at the very end is treated like an escaped backslash
      dst.append('\\');
    }
    return i;
  }

  private List<MatchResult> matchGlobPaths(List<AzfsResourceId> globs) {
    return FluentIterable.from(globs).transform(this::expand).toList();
  }

  /** Expands a pattern into {@link MatchResult}. */
  @VisibleForTesting
  MatchResult expand(AzfsResourceId azfsPattern) {

    checkArgument(azfsPattern.isWildcard(), "is Wildcard");
    String blobPrefix = azfsPattern.getBlobNonWildcardPrefix();
    Pattern wildcardAsRegexp = Pattern.compile(wildcardToRegexp(azfsPattern.getBlob()));

    LOG.debug(
        "matching files in container {}, prefix {} against pattern {}",
        azfsPattern.getContainer(),
        blobPrefix,
        wildcardAsRegexp.toString());

    ListBlobsOptions listOptions = new ListBlobsOptions().setPrefix(blobPrefix);
    Duration timeout = Duration.ZERO.plusMinutes(1);

    String account = azfsPattern.getAccount();
    String container = azfsPattern.getContainer();
    BlobContainerClient blobContainerClient = client.get().getBlobContainerClient(container);
    PagedIterable<BlobItem> blobs = blobContainerClient.listBlobs(listOptions, timeout);
    List<MatchResult.Metadata> results = new ArrayList<>();

    blobs.forEach(
        blob -> {
          String name = blob.getName();
          if (wildcardAsRegexp.matcher(name).matches() && !name.endsWith("/")) {
            LOG.debug("Matched object: {}", name);

            BlobProperties properties = blobContainerClient.getBlobClient(name).getProperties();
            AzfsResourceId rid =
                AzfsResourceId.fromComponents(account, container, name)
                    .withSize(properties.getBlobSize())
                    .withLastModified(Date.from(properties.getLastModified().toInstant()));

            results.add(toMetadata(rid, properties.getContentEncoding()));
          }
        });

    return MatchResult.create(MatchResult.Status.OK, results);
  }

  private MatchResult.Metadata toMetadata(AzfsResourceId path, String contentEncoding) {

    checkArgument(path.getSize().isPresent(), "path has size");
    boolean isReadSeekEfficient = !NON_READ_SEEK_EFFICIENT_ENCODINGS.contains(contentEncoding);

    return MatchResult.Metadata.builder()
        .setIsReadSeekEfficient(isReadSeekEfficient)
        .setResourceId(path)
        .setSizeBytes(path.getSize().get())
        .setLastModifiedMillis(path.getLastModified().transform(Date::getTime).or(0L))
        .build();
  }

  /**
   * Returns {@link MatchResult MatchResults} for the given {@link AzfsResourceId paths}.
   *
   * <p>The number of returned {@link MatchResult MatchResults} equals to the number of given {@link
   * AzfsResourceId paths}. Each {@link MatchResult} contains one {@link MatchResult.Metadata}.
   */
  @VisibleForTesting
  private Iterable<MatchResult> matchNonGlobPaths(List<AzfsResourceId> paths) {
    ImmutableList.Builder<MatchResult> toReturn = ImmutableList.builder();
    for (AzfsResourceId path : paths) {
      toReturn.add(toMatchResult(path));
    }
    return toReturn.build();
  }

  private MatchResult toMatchResult(AzfsResourceId path) {
    BlobClient blobClient =
        client.get().getBlobContainerClient(path.getContainer()).getBlobClient(path.getBlob());
    BlobProperties blobProperties;

    try {
      blobProperties = blobClient.getProperties();
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == 404) {
        return MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException());
      }
      return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
    }

    return MatchResult.create(
        MatchResult.Status.OK,
        ImmutableList.of(
            toMetadata(
                path.withSize(blobProperties.getBlobSize())
                    .withLastModified(Date.from(blobProperties.getLastModified().toInstant())),
                blobProperties.getContentEncoding())));
  }

  @Override
  protected WritableByteChannel create(AzfsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    BlobContainerClient blobContainerClient =
        client.get().getBlobContainerClient(resourceId.getContainer());
    if (!blobContainerClient.exists()) {
      throw new UnsupportedOperationException("create does not create containers.");
    }

    BlobClient blobClient = blobContainerClient.getBlobClient(resourceId.getBlob());
    // The getBlobOutputStream method overwrites existing blobs,
    // so throw an error in this case to prevent data loss
    if (blobClient.exists()) {
      throw new IOException("This filename is already in use.");
    }

    OutputStream outputStream;
    try {
      outputStream = blobClient.getBlockBlobClient().getBlobOutputStream();
    } catch (BlobStorageException e) {
      throw (IOException) e.getCause();
    }
    return newChannel(outputStream);
  }

  @Override
  protected ReadableByteChannel open(AzfsResourceId resourceId) throws IOException {
    BlobContainerClient containerClient =
        client.get().getBlobContainerClient(resourceId.getContainer());
    if (!containerClient.exists()) {
      throw new FileNotFoundException("The requested file doesn't exist.");
    }
    BlobClient blobClient = containerClient.getBlobClient(resourceId.getBlob());
    if (!blobClient.exists()) {
      throw new FileNotFoundException("The requested file doesn't exist.");
    }
    return new AzureReadableSeekableByteChannel(blobClient);
  }

  @Override
  protected void copy(List<AzfsResourceId> srcPaths, List<AzfsResourceId> destPaths)
      throws IOException {
    checkArgument(
        srcPaths.size() == destPaths.size(),
        "sizes of source paths and destination paths do not match");

    Iterator<AzfsResourceId> sourcePathsIterator = srcPaths.iterator();
    Iterator<AzfsResourceId> destinationPathsIterator = destPaths.iterator();
    while (sourcePathsIterator.hasNext()) {
      final AzfsResourceId sourcePath = sourcePathsIterator.next();
      final AzfsResourceId destinationPath = destinationPathsIterator.next();
      copy(sourcePath, destinationPath);
    }
  }

  @VisibleForTesting
  void copy(AzfsResourceId sourcePath, AzfsResourceId destinationPath) throws IOException {
    checkArgument(
        sourcePath.getBlob() != null && destinationPath.getBlob() != null,
        "This method is intended to copy file-like resources, not directories.");

    // get source blob client
    BlobClient srcBlobClient =
        client
            .get()
            .getBlobContainerClient(sourcePath.getContainer())
            .getBlobClient(sourcePath.getBlob());
    if (!srcBlobClient.exists()) {
      throw new FileNotFoundException("The copy source does not exist.");
    }

    // get destination blob client
    BlobContainerClient destBlobContainerClient =
        client.get().getBlobContainerClient(destinationPath.getContainer());
    if (!destBlobContainerClient.exists()) {
      client.get().createBlobContainer(destinationPath.getContainer());
    }
    BlobClient destBlobClient = destBlobContainerClient.getBlobClient(destinationPath.getBlob());

    destBlobClient.copyFromUrl(srcBlobClient.getBlobUrl() + generateSasToken());
  }

  @VisibleForTesting
  String generateSasToken() throws IOException {
    if (!Strings.isNullOrEmpty(options.getSasToken())) {
      return options.getSasToken();
    }

    SharedAccessAccountPolicy sharedAccessAccountPolicy = new SharedAccessAccountPolicy();
    long date = new Date().getTime();
    long expiryDate = new Date(date + 8640000).getTime();

    sharedAccessAccountPolicy.setPermissionsFromString("racwdlup");
    sharedAccessAccountPolicy.setSharedAccessStartTime(new Date(date));
    sharedAccessAccountPolicy.setSharedAccessExpiryTime(new Date(expiryDate));
    sharedAccessAccountPolicy.setResourceTypeFromString(
        "co"); // container, object, add s for service
    sharedAccessAccountPolicy.setServiceFromString("b"); // blob, add "fqt" for file, queue, table

    String storageConnectionString;
    if (!Strings.isNullOrEmpty(options.getAzureConnectionString())) {
      storageConnectionString = options.getAzureConnectionString();
    } else if (!Strings.isNullOrEmpty(options.getAccessKey())) {
      storageConnectionString =
          "DefaultEndpointsProtocol=https;AccountName="
              + client.get().getAccountName()
              + ";AccountKey="
              + options.getAccessKey()
              + ";EndpointSuffix=core.windows.net";
    } else {
      throw new IOException(
          "Copying blobs requires that a SAS token, connection string, or account key be provided.");
    }

    try {
      CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
      return "?" + storageAccount.generateSharedAccessSignature(sharedAccessAccountPolicy);
    } catch (Exception e) {
      throw (IOException) e.getCause();
    }
  }

  @Override
  protected void rename(List<AzfsResourceId> srcResourceIds, List<AzfsResourceId> destResourceIds)
      throws IOException {
    copy(srcResourceIds, destResourceIds);
    delete(srcResourceIds);
  }

  // This method with delete a virtual folder or a blob
  @Override
  protected void delete(Collection<AzfsResourceId> resourceIds) throws IOException {
    for (AzfsResourceId resourceId : resourceIds) {
      if (resourceId.getBlob() == null) {
        throw new IOException("delete does not delete containers.");
      }

      BlobContainerClient container =
          client.get().getBlobContainerClient(resourceId.getContainer());

      // deleting a blob that is not a directory
      if (!resourceId.isDirectory()) {
        BlobClient blob = container.getBlobClient(resourceId.getBlob());
        if (!blob.exists()) {
          throw new FileNotFoundException("The resource to delete does not exist.");
        }
        blob.delete();
      }

      // deleting a directory (not a container)
      else {
        PagedIterable<BlobItem> blobsInDirectory =
            container.listBlobsByHierarchy(resourceId.getBlob());
        blobsInDirectory.forEach(
            blob -> {
              String blobName = blob.getName();
              container.getBlobClient(blobName).delete();
            });
      }
    }
  }

  @Override
  protected AzfsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (isDirectory) {
      if (!singleResourceSpec.endsWith("/")) {
        singleResourceSpec += "/";
      }
    } else {
      checkArgument(
          !singleResourceSpec.endsWith("/"),
          "Expected a file path, but [%s] ends with '/'. This is unsupported in AzfsFileSystem.",
          singleResourceSpec);
    }
    return AzfsResourceId.fromUri(singleResourceSpec);
  }
}
