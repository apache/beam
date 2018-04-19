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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.amazonaws.AmazonClientException;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.util.MoreFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class S3FileSystem extends FileSystem<S3ResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(S3FileSystem.class);

  // Amazon S3 API docs: Each part must be at least 5 MB in size, except the last part.
  private static final int MINIMUM_UPLOAD_BUFFER_SIZE_BYTES = 5 * 1024 * 1024;
  private static final int DEFAULT_UPLOAD_BUFFER_SIZE_BYTES =
      Runtime.getRuntime().maxMemory() < 512 * 1024 * 1024
          ? MINIMUM_UPLOAD_BUFFER_SIZE_BYTES
          : 64 * 1024 * 1024;

  // Amazon S3 API: You can create a copy of your object up to 5 GB in a single atomic operation
  // Ref. https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjectsExamples.html
  private static final long MAX_COPY_OBJECT_SIZE_BYTES = 5L * 1024L * 1024L * 1024L;

  // S3 API, delete-objects: "You may specify up to 1000 keys."
  private static final int MAX_DELETE_OBJECTS_PER_REQUEST = 1000;

  private static final Set<String> NON_READ_SEEK_EFFICIENT_ENCODINGS = ImmutableSet.of("gzip");

  // Non-final for testing.
  private AmazonS3 amazonS3;
  private final String storageClass;
  private final int s3UploadBufferSizeBytes;
  private final ListeningExecutorService executorService;

  S3FileSystem(S3Options options) {
    checkNotNull(options, "options");

    if (Strings.isNullOrEmpty(options.getAwsRegion())) {
      LOG.info(
          "The AWS S3 Beam extension was included in this build, but the awsRegion flag "
              + "was not specified. If you don't plan to use S3, then ignore this message.");
    }

    AmazonS3ClientBuilder builder =
        AmazonS3ClientBuilder.standard().withCredentials(options.getAwsCredentialsProvider());
    if (Strings.isNullOrEmpty(options.getAwsServiceEndpoint())) {
      builder = builder.withRegion(options.getAwsRegion());
    } else {
      builder =
          builder.withEndpointConfiguration(
              new EndpointConfiguration(options.getAwsServiceEndpoint(), options.getAwsRegion()));
    }
    amazonS3 = builder.build();

    this.storageClass = checkNotNull(options.getS3StorageClass(), "storageClass");

    int uploadBufferSizeBytes;
    if (options.getS3UploadBufferSizeBytes() != null) {
      uploadBufferSizeBytes = options.getS3UploadBufferSizeBytes();
    } else {
      uploadBufferSizeBytes = DEFAULT_UPLOAD_BUFFER_SIZE_BYTES;
    }
    this.s3UploadBufferSizeBytes =
        Math.max(MINIMUM_UPLOAD_BUFFER_SIZE_BYTES, uploadBufferSizeBytes);

    checkArgument(options.getS3ThreadPoolSize() > 0, "threadPoolSize");
    executorService =
        MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(
                options.getS3ThreadPoolSize(), new ThreadFactoryBuilder().setDaemon(true).build()));
  }

  @Override
  protected String getScheme() {
    return S3ResourceId.SCHEME;
  }

  @VisibleForTesting
  void setAmazonS3Client(AmazonS3 amazonS3) {
    this.amazonS3 = amazonS3;
  }

  @VisibleForTesting
  AmazonS3 getAmazonS3Client() {
    return this.amazonS3;
  }

  @VisibleForTesting
  int getS3UploadBufferSizeBytes() {
    return s3UploadBufferSizeBytes;
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<S3ResourceId> paths =
        FluentIterable.from(specs).transform(spec -> S3ResourceId.fromUri(spec)).toList();
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
            metadatas.add(
                createBeamMetadata(
                    pathWithEncoding.getPath(), pathWithEncoding.getContentEncoding()));
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

    @Nullable
    abstract List<S3ResourceId> getExpandedPaths();

    @Nullable
    abstract IOException getException();

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

    @Nullable
    abstract String getContentEncoding();

    @Nullable
    abstract IOException getException();

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
        result = amazonS3.listObjectsV2(request);
      } catch (AmazonClientException e) {
        return ExpandedGlob.create(glob, new IOException(e));
      }
      continuationToken = result.getNextContinuationToken();

      for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
        // Filter against regex.
        if (wildcardRegexp.matcher(objectSummary.getKey()).matches()) {
          S3ResourceId expandedPath =
              S3ResourceId.fromComponents(objectSummary.getBucketName(), objectSummary.getKey())
                  .withSize(objectSummary.getSize());
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
      s3Metadata = amazonS3.getObjectMetadata(path.getBucket(), path.getKey());
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

  @VisibleForTesting
  MatchResult matchNonGlobPath(S3ResourceId path) {
    ObjectMetadata s3Metadata;
    try {
      s3Metadata = amazonS3.getObjectMetadata(path.getBucket(), path.getKey());
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
                path.withSize(s3Metadata.getContentLength()),
                Strings.nullToEmpty(s3Metadata.getContentEncoding()))));
  }

  private static MatchResult.Metadata createBeamMetadata(
      S3ResourceId path, String contentEncoding) {
    checkArgument(path.getSize().isPresent(), "path has size");
    checkNotNull(contentEncoding, "contentEncoding");
    boolean isReadSeekEfficient = !NON_READ_SEEK_EFFICIENT_ENCODINGS.contains(contentEncoding);
    return MatchResult.Metadata.builder()
        .setIsReadSeekEfficient(isReadSeekEfficient)
        .setResourceId(path)
        .setSizeBytes(path.getSize().get())
        .build();
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

  @Override
  protected WritableByteChannel create(S3ResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return new S3WritableByteChannel(
        amazonS3, resourceId, createOptions.mimeType(), storageClass, s3UploadBufferSizeBytes);
  }

  @Override
  protected ReadableByteChannel open(S3ResourceId resourceId) throws IOException {
    return new S3ReadableSeekableByteChannel(amazonS3, resourceId);
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
      ObjectMetadata objectMetadata =
          amazonS3.getObjectMetadata(sourcePath.getBucket(), sourcePath.getKey());
      if (objectMetadata.getContentLength() < MAX_COPY_OBJECT_SIZE_BYTES) {
        atomicCopy(sourcePath, destinationPath);
      } else {
        multipartCopy(sourcePath, destinationPath, objectMetadata);
      }
    } catch (AmazonClientException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void atomicCopy(S3ResourceId sourcePath, S3ResourceId destinationPath)
      throws AmazonClientException {
    CopyObjectRequest copyObjectRequest =
        new CopyObjectRequest(
            sourcePath.getBucket(),
            sourcePath.getKey(),
            destinationPath.getBucket(),
            destinationPath.getKey());
    copyObjectRequest.setStorageClass(storageClass);

    amazonS3.copyObject(copyObjectRequest);
  }

  @VisibleForTesting
  void multipartCopy(
      S3ResourceId sourcePath, S3ResourceId destinationPath, ObjectMetadata objectMetadata)
      throws AmazonClientException {
    InitiateMultipartUploadRequest initiateUploadRequest =
        new InitiateMultipartUploadRequest(destinationPath.getBucket(), destinationPath.getKey())
            .withStorageClass(storageClass)
            .withObjectMetadata(objectMetadata);

    InitiateMultipartUploadResult initiateUploadResult =
        amazonS3.initiateMultipartUpload(initiateUploadRequest);
    final String uploadId = initiateUploadResult.getUploadId();

    List<PartETag> eTags = new ArrayList<>();

    final long objectSize = objectMetadata.getContentLength();
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

      CopyPartResult copyPartResult = amazonS3.copyPart(copyPartRequest);
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
                .withLastByte(Math.min(objectSize - 1, bytePosition + s3UploadBufferSizeBytes - 1));

        CopyPartResult copyPartResult = amazonS3.copyPart(copyPartRequest);
        eTags.add(copyPartResult.getPartETag());

        bytePosition += s3UploadBufferSizeBytes;
      }
    }

    CompleteMultipartUploadRequest completeUploadRequest =
        new CompleteMultipartUploadRequest()
            .withBucketName(destinationPath.getBucket())
            .withKey(destinationPath.getKey())
            .withUploadId(uploadId)
            .withPartETags(eTags);
    amazonS3.completeMultipartUpload(completeUploadRequest);
  }

  @Override
  protected void rename(
      List<S3ResourceId> sourceResourceIds, List<S3ResourceId> destinationResourceIds)
      throws IOException {
    copy(sourceResourceIds, destinationResourceIds);
    delete(sourceResourceIds);
  }

  @Override
  protected void delete(Collection<S3ResourceId> resourceIds) throws IOException {
    List<S3ResourceId> nonDirectoryPaths =
        FluentIterable.from(resourceIds)
            .filter(s3ResourceId -> !s3ResourceId.isDirectory())
            .toList();
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
        "only %d keys can be deleted per request, but got %d",
        MAX_DELETE_OBJECTS_PER_REQUEST,
        keys.size());
    List<KeyVersion> deleteKeyVersions =
        FluentIterable.from(keys).transform(key -> new KeyVersion(key)).toList();
    DeleteObjectsRequest request = new DeleteObjectsRequest(bucket).withKeys(deleteKeyVersions);
    try {
      amazonS3.deleteObjects(request);
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

  /**
   * Invokes tasks in a thread pool, then unwraps the resulting {@link Future Futures}.
   *
   * <p>Any task exception is wrapped in {@link IOException}.
   */
  private <T> List<T> callTasks(Collection<Callable<T>> tasks) throws IOException {

    try {
      List<CompletionStage<T>> futures = new ArrayList<>(tasks.size());
      for (Callable<T> task : tasks) {
        futures.add(MoreFutures.supplyAsync(() -> task.call(), executorService));
      }
      return MoreFutures.get(MoreFutures.allAsList(futures));

    } catch (ExecutionException e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof IOException) {
          throw ((IOException) e.getCause());
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
