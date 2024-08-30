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
package org.apache.beam.sdk.io.aws.s3;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.auto.value.AutoValue;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.util.MoreFutures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Suppliers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for storage systems that use the S3 protocol.
 *
 * @see S3FileSystemSchemeRegistrar
 * @deprecated Module <code>beam-sdks-java-io-amazon-web-services</code> is deprecated and will be
 *     eventually removed. Please migrate to module <code>beam-sdks-java-io-amazon-web-services2
 *     </code>.
 */
@Deprecated
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class S3FileSystem extends FileSystem<S3ResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(S3FileSystem.class);

  // Amazon S3 API: You can create a copy of your object up to 5 GB in a single atomic operation
  // Ref. https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsExamples.html
  private static final long MAX_COPY_OBJECT_SIZE_BYTES = 5_368_709_120L;

  // S3 API, delete-objects: "You may specify up to 1000 keys."
  private static final int MAX_DELETE_OBJECTS_PER_REQUEST = 1000;

  private static final ImmutableSet<String> NON_READ_SEEK_EFFICIENT_ENCODINGS =
      ImmutableSet.of("gzip");

  // Non-final for testing.
  private Supplier<AmazonS3> amazonS3;
  private final S3FileSystemConfiguration config;
  private final ListeningExecutorService executorService;

  S3FileSystem(S3FileSystemConfiguration config) {
    this.config = checkNotNull(config, "config");
    // The Supplier is to make sure we don't call .build() unless we are actually using S3.
    amazonS3 = Suppliers.memoize(config.getS3ClientBuilder()::build);

    checkNotNull(config.getS3StorageClass(), "storageClass");
    checkArgument(config.getS3ThreadPoolSize() > 0, "threadPoolSize");
    executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                config.getS3ThreadPoolSize(), new ThreadFactoryBuilder().setDaemon(true).build()));

    LOG.warn(
        "You are using a deprecated file system for S3. Please migrate to module "
            + "'org.apache.beam:beam-sdks-java-io-amazon-web-services2'.");
  }

  S3FileSystem(S3Options options) {
    this(S3FileSystemConfiguration.fromS3Options(options).build());
  }

  @Override
  protected String getScheme() {
    return config.getScheme();
  }

  @VisibleForTesting
  void setAmazonS3Client(AmazonS3 amazonS3) {
    this.amazonS3 = Suppliers.ofInstance(amazonS3);
  }

  @VisibleForTesting
  AmazonS3 getAmazonS3Client() {
    return this.amazonS3.get();
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<S3ResourceId> paths =
        specs.stream().map(S3ResourceId::fromUri).collect(Collectors.toList());
    List<S3ResourceId> globs = new ArrayList<>();
    List<S3ResourceId> nonGlobs = new ArrayList<>();
    List<Boolean> isGlobBooleans = new ArrayList<>();

    for (S3ResourceId path : paths) {
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
        checkState(
            globMatches.hasNext(),
            "Internal error encountered in S3Filesystem: expected more elements in globMatches.");
        matchResults.add(globMatches.next());
      } else {
        checkState(
            nonGlobMatches.hasNext(),
            "Internal error encountered in S3Filesystem: expected more elements in nonGlobMatches.");
        matchResults.add(nonGlobMatches.next());
      }
    }
    checkState(
        !globMatches.hasNext(),
        "Internal error encountered in S3Filesystem: expected no more elements in globMatches.");
    checkState(
        !nonGlobMatches.hasNext(),
        "Internal error encountered in S3Filesystem: expected no more elements in nonGlobMatches.");

    return matchResults.build();
  }

  /** Gets {@link MatchResult} representing all objects that match wildcard-containing paths. */
  @VisibleForTesting
  List<MatchResult> matchGlobPaths(Collection<S3ResourceId> globPaths) throws IOException {
    List<Callable<ExpandedGlob>> expandTasks = new ArrayList<>(globPaths.size());
    for (final S3ResourceId path : globPaths) {
      expandTasks.add(() -> expandGlob(path));
    }

    Map<S3ResourceId, ExpandedGlob> expandedGlobByGlobPath = new HashMap<>();
    List<Callable<PathWithEncoding>> contentTypeTasks = new ArrayList<>(globPaths.size());
    for (ExpandedGlob expandedGlob : callTasks(expandTasks)) {
      expandedGlobByGlobPath.put(expandedGlob.getGlobPath(), expandedGlob);
      if (expandedGlob.getExpandedPaths() != null) {
        for (final S3ResourceId path : expandedGlob.getExpandedPaths()) {
          contentTypeTasks.add(() -> getPathContentEncoding(path));
        }
      }
    }

    Map<S3ResourceId, PathWithEncoding> exceptionByPath = new HashMap<>();
    for (PathWithEncoding pathWithException : callTasks(contentTypeTasks)) {
      exceptionByPath.put(pathWithException.getPath(), pathWithException);
    }

    List<MatchResult> results = new ArrayList<>(globPaths.size());
    for (S3ResourceId globPath : globPaths) {
      ExpandedGlob expandedGlob = expandedGlobByGlobPath.get(globPath);

      if (expandedGlob.getException() != null) {
        results.add(MatchResult.create(MatchResult.Status.ERROR, expandedGlob.getException()));

      } else {
        List<MatchResult.Metadata> metadatas = new ArrayList<>();
        IOException exception = null;
        for (S3ResourceId expandedPath : expandedGlob.getExpandedPaths()) {
          PathWithEncoding pathWithEncoding = exceptionByPath.get(expandedPath);

          if (pathWithEncoding.getException() != null) {
            exception = pathWithEncoding.getException();
            break;
          } else {
            // TODO(https://github.com/apache/beam/issues/20755): Support file checksum in this
            // method.
            metadatas.add(
                createBeamMetadata(
                    pathWithEncoding.getPath(), pathWithEncoding.getContentEncoding(), null));
          }
        }

        if (exception != null) {
          if (exception instanceof FileNotFoundException) {
            results.add(MatchResult.create(MatchResult.Status.NOT_FOUND, exception));
          } else {
            results.add(MatchResult.create(MatchResult.Status.ERROR, exception));
          }
        } else {
          results.add(MatchResult.create(MatchResult.Status.OK, metadatas));
        }
      }
    }

    return ImmutableList.copyOf(results);
  }

  @AutoValue
  abstract static class ExpandedGlob {

    abstract S3ResourceId getGlobPath();

    abstract @Nullable List<S3ResourceId> getExpandedPaths();

    abstract @Nullable IOException getException();

    static ExpandedGlob create(S3ResourceId globPath, List<S3ResourceId> expandedPaths) {
      checkNotNull(globPath, "globPath");
      checkNotNull(expandedPaths, "expandedPaths");
      return new AutoValue_S3FileSystem_ExpandedGlob(globPath, expandedPaths, null);
    }

    static ExpandedGlob create(S3ResourceId globPath, IOException exception) {
      checkNotNull(globPath, "globPath");
      checkNotNull(exception, "exception");
      return new AutoValue_S3FileSystem_ExpandedGlob(globPath, null, exception);
    }
  }

  @AutoValue
  abstract static class PathWithEncoding {

    abstract S3ResourceId getPath();

    abstract @Nullable String getContentEncoding();

    abstract @Nullable IOException getException();

    static PathWithEncoding create(S3ResourceId path, String contentEncoding) {
      checkNotNull(path, "path");
      checkNotNull(contentEncoding, "contentEncoding");
      return new AutoValue_S3FileSystem_PathWithEncoding(path, contentEncoding, null);
    }

    static PathWithEncoding create(S3ResourceId path, IOException exception) {
      checkNotNull(path, "path");
      checkNotNull(exception, "exception");
      return new AutoValue_S3FileSystem_PathWithEncoding(path, null, exception);
    }
  }

  private ExpandedGlob expandGlob(S3ResourceId glob) {
    // The S3 API can list objects, filtered by prefix, but not by wildcard.
    // Here, we find the longest prefix without wildcard "*",
    // then filter the results with a regex.
    checkArgument(glob.isWildcard(), "isWildcard");
    String keyPrefix = glob.getKeyNonWildcardPrefix();
    Pattern wildcardRegexp = Pattern.compile(wildcardToRegexp(glob.getKey()));

    LOG.debug(
        "expanding bucket {}, prefix {}, against pattern {}",
        glob.getBucket(),
        keyPrefix,
        wildcardRegexp.toString());

    ImmutableList.Builder<S3ResourceId> expandedPaths = ImmutableList.builder();
    String continuationToken = null;

    do {
      ListObjectsV2Request request =
          new ListObjectsV2Request()
              .withBucketName(glob.getBucket())
              .withPrefix(keyPrefix)
              .withContinuationToken(continuationToken);
      ListObjectsV2Result result;
      try {
        result = amazonS3.get().listObjectsV2(request);
      } catch (AmazonClientException e) {
        return ExpandedGlob.create(glob, new IOException(e));
      }
      continuationToken = result.getNextContinuationToken();

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        // Filter against regex.
        if (wildcardRegexp.matcher(objectSummary.getKey()).matches()) {
          S3ResourceId expandedPath =
              S3ResourceId.fromComponents(
                      glob.getScheme(), objectSummary.getBucketName(), objectSummary.getKey())
                  .withSize(objectSummary.getSize())
                  .withLastModified(objectSummary.getLastModified());
          LOG.debug("Expanded S3 object path {}", expandedPath);
          expandedPaths.add(expandedPath);
        }
      }
    } while (continuationToken != null);

    return ExpandedGlob.create(glob, expandedPaths.build());
  }

  private PathWithEncoding getPathContentEncoding(S3ResourceId path) {
    ObjectMetadata s3Metadata;
    try {
      s3Metadata = getObjectMetadata(path);
    } catch (AmazonClientException e) {
      if (e instanceof AmazonS3Exception && ((AmazonS3Exception) e).getStatusCode() == 404) {
        return PathWithEncoding.create(path, new FileNotFoundException());
      }
      return PathWithEncoding.create(path, new IOException(e));
    }
    return PathWithEncoding.create(path, Strings.nullToEmpty(s3Metadata.getContentEncoding()));
  }

  private List<MatchResult> matchNonGlobPaths(Collection<S3ResourceId> paths) throws IOException {
    List<Callable<MatchResult>> tasks = new ArrayList<>(paths.size());
    for (final S3ResourceId path : paths) {
      tasks.add(() -> matchNonGlobPath(path));
    }

    return callTasks(tasks);
  }

  private ObjectMetadata getObjectMetadata(S3ResourceId s3ResourceId) throws AmazonClientException {
    GetObjectMetadataRequest request =
        new GetObjectMetadataRequest(s3ResourceId.getBucket(), s3ResourceId.getKey());
    request.setSSECustomerKey(config.getSSECustomerKey());
    return amazonS3.get().getObjectMetadata(request);
  }

  @VisibleForTesting
  MatchResult matchNonGlobPath(S3ResourceId path) {
    ObjectMetadata s3Metadata;
    try {
      s3Metadata = getObjectMetadata(path);
    } catch (AmazonClientException e) {
      if (e instanceof AmazonS3Exception && ((AmazonS3Exception) e).getStatusCode() == 404) {
        return MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException());
      }
      return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
    }

    return MatchResult.create(
        MatchResult.Status.OK,
        ImmutableList.of(
            createBeamMetadata(
                path.withSize(s3Metadata.getContentLength())
                    .withLastModified(s3Metadata.getLastModified()),
                Strings.nullToEmpty(s3Metadata.getContentEncoding()),
                s3Metadata.getETag())));
  }

  private static MatchResult.Metadata createBeamMetadata(
      S3ResourceId path, String contentEncoding, String eTag) {
    checkArgument(path.getSize().isPresent(), "The resource id should have a size.");
    checkNotNull(contentEncoding, "contentEncoding");
    boolean isReadSeekEfficient = !NON_READ_SEEK_EFFICIENT_ENCODINGS.contains(contentEncoding);

    MatchResult.Metadata.Builder ret =
        MatchResult.Metadata.builder()
            .setIsReadSeekEfficient(isReadSeekEfficient)
            .setResourceId(path)
            .setSizeBytes(path.getSize().get())
            .setLastModifiedMillis(path.getLastModified().transform(Date::getTime).or(0L));
    if (eTag != null) {
      ret.setChecksum(eTag);
    }
    return ret.build();
  }

  @Override
  protected WritableByteChannel create(S3ResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return new S3WritableByteChannel(amazonS3.get(), resourceId, createOptions.mimeType(), config);
  }

  @Override
  protected ReadableByteChannel open(S3ResourceId resourceId) throws IOException {
    return new S3ReadableSeekableByteChannel(amazonS3.get(), resourceId, config);
  }

  @Override
  protected void copy(List<S3ResourceId> sourcePaths, List<S3ResourceId> destinationPaths)
      throws IOException {
    checkArgument(
        sourcePaths.size() == destinationPaths.size(),
        "sizes of sourcePaths and destinationPaths do not match");

    List<Callable<Void>> tasks = new ArrayList<>(sourcePaths.size());

    Iterator<S3ResourceId> sourcePathsIterator = sourcePaths.iterator();
    Iterator<S3ResourceId> destinationPathsIterator = destinationPaths.iterator();
    while (sourcePathsIterator.hasNext()) {
      final S3ResourceId sourcePath = sourcePathsIterator.next();
      final S3ResourceId destinationPath = destinationPathsIterator.next();

      tasks.add(
          () -> {
            copy(sourcePath, destinationPath);
            return null;
          });
    }

    callTasks(tasks);
  }

  @VisibleForTesting
  void copy(S3ResourceId sourcePath, S3ResourceId destinationPath) throws IOException {
    try {
      ObjectMetadata sourceObjectMetadata = getObjectMetadata(sourcePath);
      if (sourceObjectMetadata.getContentLength() < MAX_COPY_OBJECT_SIZE_BYTES) {
        atomicCopy(sourcePath, destinationPath, sourceObjectMetadata);
      } else {
        multipartCopy(sourcePath, destinationPath, sourceObjectMetadata);
      }
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  CopyObjectResult atomicCopy(
      S3ResourceId sourcePath, S3ResourceId destinationPath, ObjectMetadata sourceObjectMetadata)
      throws AmazonClientException {
    CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(
            sourcePath.getBucket(),
            sourcePath.getKey(),
            destinationPath.getBucket(),
            destinationPath.getKey());
    copyObjectRequest.setNewObjectMetadata(sourceObjectMetadata);
    copyObjectRequest.setStorageClass(config.getS3StorageClass());
    copyObjectRequest.setSourceSSECustomerKey(config.getSSECustomerKey());
    copyObjectRequest.setDestinationSSECustomerKey(config.getSSECustomerKey());
    return amazonS3.get().copyObject(copyObjectRequest);
  }

  @VisibleForTesting
  CompleteMultipartUploadResult multipartCopy(
      S3ResourceId sourcePath, S3ResourceId destinationPath, ObjectMetadata sourceObjectMetadata)
      throws AmazonClientException {
    InitiateMultipartUploadRequest initiateUploadRequest =
        new InitiateMultipartUploadRequest(destinationPath.getBucket(), destinationPath.getKey())
            .withStorageClass(config.getS3StorageClass())
            .withObjectMetadata(sourceObjectMetadata)
            .withSSECustomerKey(config.getSSECustomerKey());

    InitiateMultipartUploadResult initiateUploadResult =
        amazonS3.get().initiateMultipartUpload(initiateUploadRequest);
    final String uploadId = initiateUploadResult.getUploadId();

    List<PartETag> eTags = new ArrayList<>();

    final long objectSize = sourceObjectMetadata.getContentLength();
    // extra validation in case a caller calls directly S3FileSystem.multipartCopy
    // without using S3FileSystem.copy in the future
    if (objectSize == 0) {
      final CopyPartRequest copyPartRequest =
          new CopyPartRequest()
              .withSourceBucketName(sourcePath.getBucket())
              .withSourceKey(sourcePath.getKey())
              .withDestinationBucketName(destinationPath.getBucket())
              .withDestinationKey(destinationPath.getKey())
              .withUploadId(uploadId)
              .withPartNumber(1);
      copyPartRequest.setSourceSSECustomerKey(config.getSSECustomerKey());
      copyPartRequest.setDestinationSSECustomerKey(config.getSSECustomerKey());

      CopyPartResult copyPartResult = amazonS3.get().copyPart(copyPartRequest);
      eTags.add(copyPartResult.getPartETag());
    } else {
      long bytePosition = 0;
      // Amazon parts are 1-indexed, not zero-indexed.
      for (int partNumber = 1; bytePosition < objectSize; partNumber++) {
        final CopyPartRequest copyPartRequest =
            new CopyPartRequest()
                .withSourceBucketName(sourcePath.getBucket())
                .withSourceKey(sourcePath.getKey())
                .withDestinationBucketName(destinationPath.getBucket())
                .withDestinationKey(destinationPath.getKey())
                .withUploadId(uploadId)
                .withPartNumber(partNumber)
                .withFirstByte(bytePosition)
                .withLastByte(
                    Math.min(objectSize - 1, bytePosition + MAX_COPY_OBJECT_SIZE_BYTES - 1));
        copyPartRequest.setSourceSSECustomerKey(config.getSSECustomerKey());
        copyPartRequest.setDestinationSSECustomerKey(config.getSSECustomerKey());

        CopyPartResult copyPartResult = amazonS3.get().copyPart(copyPartRequest);
        eTags.add(copyPartResult.getPartETag());

        bytePosition += MAX_COPY_OBJECT_SIZE_BYTES;
      }
    }

    CompleteMultipartUploadRequest completeUploadRequest =
        new CompleteMultipartUploadRequest()
            .withBucketName(destinationPath.getBucket())
            .withKey(destinationPath.getKey())
            .withUploadId(uploadId)
            .withPartETags(eTags);
    return amazonS3.get().completeMultipartUpload(completeUploadRequest);
  }

  @Override
  protected void rename(
      List<S3ResourceId> sourceResourceIds,
      List<S3ResourceId> destinationResourceIds,
      MoveOptions... moveOptions)
      throws IOException {
    if (moveOptions.length > 0) {
      throw new UnsupportedOperationException("Support for move options is not yet implemented.");
    }
    copy(sourceResourceIds, destinationResourceIds);
    delete(sourceResourceIds);
  }

  @Override
  protected void delete(Collection<S3ResourceId> resourceIds) throws IOException {
    List<S3ResourceId> nonDirectoryPaths =
        resourceIds.stream()
            .filter(s3ResourceId -> !s3ResourceId.isDirectory())
            .collect(Collectors.toList());
    Multimap<String, String> keysByBucket = ArrayListMultimap.create();
    for (S3ResourceId path : nonDirectoryPaths) {
      keysByBucket.put(path.getBucket(), path.getKey());
    }

    List<Callable<Void>> tasks = new ArrayList<>();
    for (final String bucket : keysByBucket.keySet()) {
      for (final List<String> keysPartition :
          Iterables.partition(keysByBucket.get(bucket), MAX_DELETE_OBJECTS_PER_REQUEST)) {
        tasks.add(
            () -> {
              delete(bucket, keysPartition);
              return null;
            });
      }
    }

    callTasks(tasks);
  }

  private void delete(String bucket, Collection<String> keys) throws IOException {
    checkArgument(
        keys.size() <= MAX_DELETE_OBJECTS_PER_REQUEST,
        "only %s keys can be deleted per request, but got %s",
        MAX_DELETE_OBJECTS_PER_REQUEST,
        keys.size());
    List<KeyVersion> deleteKeyVersions =
        keys.stream().map(KeyVersion::new).collect(Collectors.toList());
    DeleteObjectsRequest request =
        new DeleteObjectsRequest(bucket).withKeys(deleteKeyVersions).withQuiet(true);
    try {
      amazonS3.get().deleteObjects(request);
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected S3ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    if (isDirectory) {
      if (!singleResourceSpec.endsWith("/")) {
        singleResourceSpec += "/";
      }
    } else {
      checkArgument(
          !singleResourceSpec.endsWith("/"),
          "Expected a file path, but [%s] ends with '/'. This is unsupported in S3FileSystem.",
          singleResourceSpec);
    }
    return S3ResourceId.fromUri(singleResourceSpec);
  }

  @Override
  protected void reportLineage(S3ResourceId resourceId, Lineage lineage) {
    lineage.add("s3", ImmutableList.of(resourceId.getBucket(), resourceId.getKey()));
  }

  /**
   * Invokes tasks in a thread pool, then unwraps the resulting {@link Future Futures}.
   *
   * <p>Any task exception is wrapped in {@link IOException}.
   */
  private <T> List<T> callTasks(Collection<Callable<T>> tasks) throws IOException {

    try {
      List<CompletionStage<T>> futures = new ArrayList<>(tasks.size());
      for (Callable<T> task : tasks) {
        futures.add(MoreFutures.supplyAsync(task::call, executorService));
      }
      return MoreFutures.get(MoreFutures.allAsList(futures));

    } catch (ExecutionException e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new IOException(e.getCause());
      }
      throw new IOException(e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("executor service was interrupted");
    }
  }
}
