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
package org.apache.beam.sdk.io.aws2.s3;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.aws2.options.S3Options;
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
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;

/**
 * {@link FileSystem} implementation for storage systems that use the S3 protocol.
 *
 * @see S3FileSystemSchemeRegistrar
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class S3FileSystem extends FileSystem<S3ResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(S3FileSystem.class);

  // Amazon S3 API: You can create a copy of your object up to 5 GB in a single atomic operation
  // Ref. https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsExamples.html
  @VisibleForTesting static final long MAX_COPY_OBJECT_SIZE_BYTES = 5_368_709_120L;

  // S3 API, delete-objects: "You may specify up to 1000 keys."
  private static final int MAX_DELETE_OBJECTS_PER_REQUEST = 1000;

  private static final ImmutableSet<String> NON_READ_SEEK_EFFICIENT_ENCODINGS =
      ImmutableSet.of("gzip");

  // Non-final for testing.
  private Supplier<S3Client> s3Client;
  private final S3FileSystemConfiguration config;
  private final ListeningExecutorService executorService;

  S3FileSystem(S3Options options) {
    this(S3FileSystemConfiguration.fromS3Options(options));
  }

  S3FileSystem(S3FileSystemConfiguration config) {
    this.config = checkNotNull(config, "config");
    // The Supplier is to make sure we don't call .build() unless we are actually using S3.
    s3Client = Suppliers.memoize(config.getS3ClientBuilder()::build);

    checkNotNull(config.getS3StorageClass(), "storageClass");
    checkArgument(config.getS3ThreadPoolSize() > 0, "threadPoolSize");
    executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                config.getS3ThreadPoolSize(), new ThreadFactoryBuilder().setDaemon(true).build()));
  }

  @Override
  protected String getScheme() {
    return config.getScheme();
  }

  @VisibleForTesting
  void setS3Client(S3Client s3) {
    this.s3Client = Suppliers.ofInstance(s3);
  }

  @VisibleForTesting
  S3Client getS3Client() {
    return this.s3Client.get();
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<S3ResourceId> paths =
        specs.stream().map(S3ResourceId::fromUri).collect(Collectors.toList());
    List<S3ResourceId> globs = new ArrayList<>();
    List<S3ResourceId> nonGlobs = new ArrayList<>();
    List<Boolean> isGlobBooleans = new ArrayList<>();

    paths.forEach(
        path -> {
          if (path.isWildcard()) {
            globs.add(path);
            isGlobBooleans.add(true);
          } else {
            nonGlobs.add(path);
            isGlobBooleans.add(false);
          }
        });

    Iterator<MatchResult> globMatches = matchGlobPaths(globs).iterator();
    Iterator<MatchResult> nonGlobMatches = matchNonGlobPaths(nonGlobs).iterator();

    ImmutableList.Builder<MatchResult> matchResults = ImmutableList.builder();
    isGlobBooleans.forEach(
        isGlob -> {
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
        });
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
    Stream<Callable<ExpandedGlob>> expandTasks =
        globPaths.stream().map(path -> () -> expandGlob(path));

    Map<S3ResourceId, ExpandedGlob> expandedGlobByGlobPath =
        callTasks(expandTasks).stream()
            .collect(Collectors.toMap(ExpandedGlob::getGlobPath, expandedGlob -> expandedGlob));

    Stream<Callable<PathWithEncoding>> contentTypeTasks =
        expandedGlobByGlobPath.values().stream()
            .map(ExpandedGlob::getExpandedPaths)
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .map(path -> () -> getPathContentEncoding(path));

    Map<S3ResourceId, PathWithEncoding> exceptionByPath =
        callTasks(contentTypeTasks).stream()
            .collect(
                Collectors.toMap(PathWithEncoding::getPath, pathWithEncoding -> pathWithEncoding));

    List<MatchResult> results = new ArrayList<>(globPaths.size());
    globPaths.forEach(
        globPath -> {
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
        });

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
        wildcardRegexp);

    ImmutableList.Builder<S3ResourceId> expandedPaths = ImmutableList.builder();
    String continuationToken = null;

    do {
      ListObjectsV2Request request =
          ListObjectsV2Request.builder()
              .bucket(glob.getBucket())
              .prefix(keyPrefix)
              .continuationToken(continuationToken)
              .build();
      ListObjectsV2Response response;
      try {
        response = s3Client.get().listObjectsV2(request);
      } catch (SdkServiceException e) {
        return ExpandedGlob.create(glob, new IOException(e));
      }
      continuationToken = response.nextContinuationToken();
      List<S3Object> contents = response.contents();

      contents.stream()
          .filter(s3Object -> wildcardRegexp.matcher(s3Object.key()).matches())
          .forEach(
              s3Object -> {
                S3ResourceId expandedPath =
                    S3ResourceId.fromComponents(glob.getScheme(), glob.getBucket(), s3Object.key())
                        .withSize(s3Object.size())
                        .withLastModified(Date.from(s3Object.lastModified()));
                LOG.debug("Expanded S3 object path {}", expandedPath);
                expandedPaths.add(expandedPath);
              });
    } while (continuationToken != null);

    return ExpandedGlob.create(glob, expandedPaths.build());
  }

  private PathWithEncoding getPathContentEncoding(S3ResourceId path) {
    HeadObjectResponse s3ObjectHead;
    try {
      s3ObjectHead = getObjectHead(path);
    } catch (SdkServiceException e) {
      if (e instanceof S3Exception && e.statusCode() == 404) {
        return PathWithEncoding.create(path, new FileNotFoundException());
      }
      return PathWithEncoding.create(path, new IOException(e));
    }
    return PathWithEncoding.create(path, Strings.nullToEmpty(s3ObjectHead.contentEncoding()));
  }

  private List<MatchResult> matchNonGlobPaths(Collection<S3ResourceId> paths) throws IOException {
    return callTasks(paths.stream().map(path -> () -> matchNonGlobPath(path)));
  }

  private HeadObjectResponse getObjectHead(S3ResourceId s3ResourceId) throws SdkServiceException {
    HeadObjectRequest request =
        HeadObjectRequest.builder()
            .bucket(s3ResourceId.getBucket())
            .key(s3ResourceId.getKey())
            .sseCustomerKey(config.getSSECustomerKey().getKey())
            .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .build();
    return s3Client.get().headObject(request);
  }

  @VisibleForTesting
  MatchResult matchNonGlobPath(S3ResourceId path) {
    HeadObjectResponse s3ObjectHead;
    try {
      s3ObjectHead = getObjectHead(path);
    } catch (SdkServiceException e) {
      if (e instanceof S3Exception && e.statusCode() == 404) {
        return MatchResult.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException());
      }
      return MatchResult.create(MatchResult.Status.ERROR, new IOException(e));
    }

    return MatchResult.create(
        MatchResult.Status.OK,
        ImmutableList.of(
            createBeamMetadata(
                path.withSize(s3ObjectHead.contentLength())
                    .withLastModified(Date.from(s3ObjectHead.lastModified())),
                Strings.nullToEmpty(s3ObjectHead.contentEncoding()),
                s3ObjectHead.eTag())));
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
    return new S3WritableByteChannel(s3Client.get(), resourceId, createOptions.mimeType(), config);
  }

  @Override
  protected ReadableByteChannel open(S3ResourceId resourceId) throws IOException {
    return new S3ReadableSeekableByteChannel(s3Client.get(), resourceId, config);
  }

  @Override
  protected void copy(List<S3ResourceId> sourcePaths, List<S3ResourceId> destinationPaths)
      throws IOException {
    checkArgument(
        sourcePaths.size() == destinationPaths.size(),
        "sizes of sourcePaths and destinationPaths do not match");

    Stream.Builder<Callable<Void>> tasks = Stream.builder();

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

    callTasks(tasks.build());
  }

  @VisibleForTesting
  void copy(S3ResourceId sourcePath, S3ResourceId destinationPath) throws IOException {
    try {
      HeadObjectResponse sourceObjectHead = getObjectHead(sourcePath);
      if (sourceObjectHead.contentLength() < MAX_COPY_OBJECT_SIZE_BYTES) {
        atomicCopy(sourcePath, destinationPath, sourceObjectHead);
      } else {
        multipartCopy(sourcePath, destinationPath, sourceObjectHead);
      }
    } catch (SdkServiceException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  CopyObjectResponse atomicCopy(
      S3ResourceId sourcePath, S3ResourceId destinationPath, HeadObjectResponse objectHead)
      throws SdkServiceException {
    CopyObjectRequest copyObjectRequest =
        CopyObjectRequest.builder()
            .sourceBucket(sourcePath.getBucket())
            .sourceKey(sourcePath.getKey())
            .destinationBucket(destinationPath.getBucket())
            .destinationKey(destinationPath.getKey())
            .metadata(objectHead.metadata())
            .storageClass(config.getS3StorageClass())
            .serverSideEncryption(config.getSSEAlgorithm())
            .ssekmsKeyId(config.getSSEKMSKeyId())
            .sseCustomerKey(config.getSSECustomerKey().getKey())
            .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .copySourceSSECustomerKey(config.getSSECustomerKey().getKey())
            .copySourceSSECustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
            .copySourceSSECustomerKeyMD5(config.getSSECustomerKey().getMD5())
            .build();
    return s3Client.get().copyObject(copyObjectRequest);
  }

  @VisibleForTesting
  CompleteMultipartUploadResponse multipartCopy(
      S3ResourceId sourcePath, S3ResourceId destinationPath, HeadObjectResponse sourceObjectHead)
      throws SdkServiceException {
    CreateMultipartUploadRequest initiateUploadRequest =
        CreateMultipartUploadRequest.builder()
            .bucket(destinationPath.getBucket())
            .key(destinationPath.getKey())
            .storageClass(config.getS3StorageClass())
            .metadata(sourceObjectHead.metadata())
            .serverSideEncryption(config.getSSEAlgorithm())
            .ssekmsKeyId(config.getSSEKMSKeyId())
            .sseCustomerKey(config.getSSECustomerKey().getKey())
            .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
            .build();

    CreateMultipartUploadResponse createMultipartUploadResponse =
        s3Client.get().createMultipartUpload(initiateUploadRequest);
    final String uploadId = createMultipartUploadResponse.uploadId();

    List<CompletedPart> completedParts = new ArrayList<>();

    final long objectSize = sourceObjectHead.contentLength();
    CopyPartResult copyPartResult;
    CompletedPart completedPart;
    // extra validation in case a caller calls directly S3FileSystem.multipartCopy
    // without using S3FileSystem.copy in the future
    if (objectSize == 0) {
      final UploadPartCopyRequest uploadPartCopyRequest =
          UploadPartCopyRequest.builder()
              .destinationBucket(destinationPath.getBucket())
              .destinationKey(destinationPath.getKey())
              .sourceBucket(sourcePath.getBucket())
              .sourceKey(sourcePath.getKey())
              .uploadId(uploadId)
              .partNumber(1)
              .sseCustomerKey(config.getSSECustomerKey().getKey())
              .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
              .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
              .copySourceSSECustomerKey(config.getSSECustomerKey().getKey())
              .copySourceSSECustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
              .copySourceSSECustomerKeyMD5(config.getSSECustomerKey().getMD5())
              .build();

      copyPartResult = s3Client.get().uploadPartCopy(uploadPartCopyRequest).copyPartResult();
      completedPart = CompletedPart.builder().partNumber(1).eTag(copyPartResult.eTag()).build();
      completedParts.add(completedPart);
    } else {
      long bytePosition = 0;
      // Amazon parts are 1-indexed, not zero-indexed.
      for (int partNumber = 1; bytePosition < objectSize; partNumber++) {
        final UploadPartCopyRequest uploadPartCopyRequest =
            UploadPartCopyRequest.builder()
                .destinationBucket(destinationPath.getBucket())
                .destinationKey(destinationPath.getKey())
                .sourceBucket(sourcePath.getBucket())
                .sourceKey(sourcePath.getKey())
                .uploadId(uploadId)
                .partNumber(partNumber)
                .copySourceRange(
                    String.format(
                        "bytes=%s-%s",
                        bytePosition,
                        Math.min(objectSize - 1, bytePosition + MAX_COPY_OBJECT_SIZE_BYTES - 1)))
                .sseCustomerKey(config.getSSECustomerKey().getKey())
                .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
                .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
                .copySourceSSECustomerKey(config.getSSECustomerKey().getKey())
                .copySourceSSECustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
                .copySourceSSECustomerKeyMD5(config.getSSECustomerKey().getMD5())
                .build();

        copyPartResult = s3Client.get().uploadPartCopy(uploadPartCopyRequest).copyPartResult();
        completedPart =
            CompletedPart.builder().partNumber(partNumber).eTag(copyPartResult.eTag()).build();
        completedParts.add(completedPart);

        bytePosition += MAX_COPY_OBJECT_SIZE_BYTES;
      }
    }
    CompletedMultipartUpload completedMultipartUpload =
        CompletedMultipartUpload.builder().parts(completedParts).build();

    CompleteMultipartUploadRequest completeUploadRequest =
        CompleteMultipartUploadRequest.builder()
            .bucket(destinationPath.getBucket())
            .key(destinationPath.getKey())
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();
    return s3Client.get().completeMultipartUpload(completeUploadRequest);
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
    nonDirectoryPaths.forEach(path -> keysByBucket.put(path.getBucket(), path.getKey()));

    Stream.Builder<Callable<Void>> tasks = Stream.builder();
    keysByBucket
        .keySet()
        .forEach(
            bucket ->
                Iterables.partition(keysByBucket.get(bucket), MAX_DELETE_OBJECTS_PER_REQUEST)
                    .forEach(
                        keysPartition ->
                            tasks.add(
                                () -> {
                                  delete(bucket, keysPartition);
                                  return null;
                                })));
    callTasks(tasks.build());
  }

  private void delete(String bucket, Collection<String> keys) throws IOException {
    checkArgument(
        keys.size() <= MAX_DELETE_OBJECTS_PER_REQUEST,
        "only %s keys can be deleted per request, but got %s",
        MAX_DELETE_OBJECTS_PER_REQUEST,
        keys.size());

    List<ObjectIdentifier> deleteKeyVersions =
        keys.stream()
            .map((key) -> ObjectIdentifier.builder().key(key).build())
            .collect(Collectors.toList());
    Delete delete = Delete.builder().objects(deleteKeyVersions).quiet(true).build();
    DeleteObjectsRequest deleteObjectsRequest =
        DeleteObjectsRequest.builder().bucket(bucket).delete(delete).build();
    try {
      s3Client.get().deleteObjects(deleteObjectsRequest);
    } catch (SdkServiceException e) {
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
    lineage.add("s3", ImmutableList.of(resourceId.getBucket()));
  }

  /**
   * Invokes tasks in a thread pool, then unwraps the resulting {@link Future Futures}.
   *
   * <p>Any task exception is wrapped in {@link IOException}.
   */
  private <T> List<T> callTasks(Stream<Callable<T>> tasks) throws IOException {

    try {
      return MoreFutures.get(
          MoreFutures.allAsList(
              tasks
                  .map(task -> MoreFutures.supplyAsync(task::call, executorService))
                  .collect(Collectors.toList())));

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
