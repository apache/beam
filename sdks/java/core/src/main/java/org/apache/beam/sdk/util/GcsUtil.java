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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageWriteChannel;
import com.google.cloud.hadoop.gcsio.ObjectWriteConditions;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides operations on GCS.
 */
public class GcsUtil {
  /**
   * This is a {@link DefaultValueFactory} able to create a {@link GcsUtil} using
   * any transport flags specified on the {@link PipelineOptions}.
   */
  public static class GcsUtilFactory implements DefaultValueFactory<GcsUtil> {
    /**
     * Returns an instance of {@link GcsUtil} based on the
     * {@link PipelineOptions}.
     *
     * <p>If no instance has previously been created, one is created and the value
     * stored in {@code options}.
     */
    @Override
    public GcsUtil create(PipelineOptions options) {
      LOG.debug("Creating new GcsUtil");
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      Storage.Builder storageBuilder = Transport.newStorageClient(gcsOptions);
      return new GcsUtil(
          storageBuilder.build(),
          storageBuilder.getHttpRequestInitializer(),
          gcsOptions.getExecutorService(),
          gcsOptions.getGcsUploadBufferSizeBytes());
    }

    /**
     * Returns an instance of {@link GcsUtil} based on the given parameters.
     */
    public static GcsUtil create(
        Storage storageClient,
        HttpRequestInitializer httpRequestInitializer,
        ExecutorService executorService,
        @Nullable Integer uploadBufferSizeBytes) {
      return new GcsUtil(
          storageClient, httpRequestInitializer, executorService, uploadBufferSizeBytes);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(GcsUtil.class);

  /** Maximum number of items to retrieve per Objects.List request. */
  private static final long MAX_LIST_ITEMS_PER_CALL = 1024;

  /** Matches a glob containing a wildcard, capturing the portion before the first wildcard. */
  private static final Pattern GLOB_PREFIX = Pattern.compile("(?<PREFIX>[^\\[*?]*)[\\[*?].*");

  private static final String RECURSIVE_WILDCARD = "[*]{2}";

  /**
   * A {@link Pattern} for globs with a recursive wildcard.
   */
  private static final Pattern RECURSIVE_GCS_PATTERN =
      Pattern.compile(".*" + RECURSIVE_WILDCARD + ".*");

  /**
   * Maximum number of requests permitted in a GCS batch request.
   */
  private static final int MAX_REQUESTS_PER_BATCH = 100;
  /**
   * Maximum number of concurrent batches of requests executing on GCS.
   */
  private static final int MAX_CONCURRENT_BATCHES = 256;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(3).withInitialBackoff(Duration.millis(200));

  /////////////////////////////////////////////////////////////////////////////

  /** Client for the GCS API. */
  private Storage storageClient;
  private final HttpRequestInitializer httpRequestInitializer;
  /** Buffer size for GCS uploads (in bytes). */
  @Nullable private final Integer uploadBufferSizeBytes;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

  // Exposed for testing.
  final ExecutorService executorService;

  /**
   * Returns true if the given GCS pattern is supported otherwise fails with an
   * exception.
   */
  public static boolean isGcsPatternSupported(String gcsPattern) {
    if (RECURSIVE_GCS_PATTERN.matcher(gcsPattern).matches()) {
      throw new IllegalArgumentException("Unsupported wildcard usage in \"" + gcsPattern + "\": "
          + " recursive wildcards are not supported.");
    }
    return true;
  }

  /**
   * Returns the prefix portion of the glob that doesn't contain wildcards.
   */
  public static String getGlobPrefix(String globExp) {
    checkArgument(isGcsPatternSupported(globExp));
    Matcher m = GLOB_PREFIX.matcher(globExp);
    checkArgument(
        m.matches(),
        String.format("Glob expression: [%s] is not expandable.", globExp));
    return m.group("PREFIX");
  }

  /**
   * Expands glob expressions to regular expressions.
   *
   * @param globExp the glob expression to expand
   * @return a string with the regular expression this glob expands to
   */
  public static String globToRegexp(String globExp) {
    StringBuilder dst = new StringBuilder();
    char[] src = globExp.toCharArray();
    int i = 0;
    while (i < src.length) {
      char c = src[i++];
      switch (c) {
        case '*':
          dst.append("[^/]*");
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

  private GcsUtil(
      Storage storageClient,
      HttpRequestInitializer httpRequestInitializer,
      ExecutorService executorService,
      @Nullable Integer uploadBufferSizeBytes) {
    this.storageClient = storageClient;
    this.httpRequestInitializer = httpRequestInitializer;
    this.uploadBufferSizeBytes = uploadBufferSizeBytes;
    this.executorService = executorService;
  }

  // Use this only for testing purposes.
  protected void setStorageClient(Storage storageClient) {
    this.storageClient = storageClient;
  }

  /**
   * Expands a pattern into matched paths. The pattern path may contain globs, which are expanded
   * in the result. For patterns that only match a single object, we ensure that the object
   * exists.
   */
  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    checkArgument(isGcsPatternSupported(gcsPattern.getObject()));
    Pattern p = null;
    String prefix = null;
    if (!GLOB_PREFIX.matcher(gcsPattern.getObject()).matches()) {
      // Not a glob.
      try {
        // Use a get request to fetch the metadata of the object, and ignore the return value.
        // The request has strong global consistency.
        getObject(gcsPattern);
        return ImmutableList.of(gcsPattern);
      } catch (FileNotFoundException e) {
        // If the path was not found, return an empty list.
        return ImmutableList.of();
      }
    } else {
      // Part before the first wildcard character.
      prefix = getGlobPrefix(gcsPattern.getObject());
      p = Pattern.compile(globToRegexp(gcsPattern.getObject()));
    }

    LOG.debug("matching files in bucket {}, prefix {} against pattern {}", gcsPattern.getBucket(),
        prefix, p.toString());

    String pageToken = null;
    List<GcsPath> results = new LinkedList<>();
    do {
      Objects objects = listObjects(gcsPattern.getBucket(), prefix, pageToken);
      if (objects.getItems() == null) {
        break;
      }

      // Filter objects based on the regex.
      for (StorageObject o : objects.getItems()) {
        String name = o.getName();
        // Skip directories, which end with a slash.
        if (p.matcher(name).matches() && !name.endsWith("/")) {
          LOG.debug("Matched object: {}", name);
          results.add(GcsPath.fromObject(o));
        }
      }
      pageToken = objects.getNextPageToken();
    } while (pageToken != null);

    return results;
  }

  @VisibleForTesting
  @Nullable
  Integer getUploadBufferSizeBytes() {
    return uploadBufferSizeBytes;
  }

  /**
   * Returns the file size from GCS or throws {@link FileNotFoundException}
   * if the resource does not exist.
   */
  public long fileSize(GcsPath path) throws IOException {
    return getObject(path).getSize().longValue();
  }

  /**
   * Returns the {@link StorageObject} for the given {@link GcsPath}.
   */
  public StorageObject getObject(GcsPath gcsPath) throws IOException {
    return getObject(gcsPath, BACKOFF_FACTORY.backoff(), Sleeper.DEFAULT);
  }

  @VisibleForTesting
  StorageObject getObject(GcsPath gcsPath, BackOff backoff, Sleeper sleeper) throws IOException {
    Storage.Objects.Get getObject =
        storageClient.objects().get(gcsPath.getBucket(), gcsPath.getObject());
    try {
      return ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(getObject),
          backoff,
          RetryDeterminer.SOCKET_ERRORS,
          IOException.class,
          sleeper);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (e instanceof IOException && errorExtractor.itemNotFound((IOException) e)) {
        throw new FileNotFoundException(gcsPath.toString());
      }
      throw new IOException(
          String.format("Unable to get the file object for path %s.", gcsPath),
          e);
    }
  }

  /**
   * Lists {@link Objects} given the {@code bucket}, {@code prefix}, {@code pageToken}.
   */
  public Objects listObjects(String bucket, String prefix, @Nullable String pageToken)
      throws IOException {
    // List all objects that start with the prefix (including objects in sub-directories).
    Storage.Objects.List listObject = storageClient.objects().list(bucket);
    listObject.setMaxResults(MAX_LIST_ITEMS_PER_CALL);
    listObject.setPrefix(prefix);

    if (pageToken != null) {
      listObject.setPageToken(pageToken);
    }

    try {
      return ResilientOperation.retry(
          ResilientOperation.getGoogleRequestCallable(listObject),
          BACKOFF_FACTORY.backoff(),
          RetryDeterminer.SOCKET_ERRORS,
          IOException.class);
    } catch (Exception e) {
      throw new IOException(
          String.format("Unable to match files in bucket %s, prefix %s.", bucket, prefix),
          e);
    }
  }

  /**
   * Returns the file size from GCS or throws {@link FileNotFoundException}
   * if the resource does not exist.
   */
  @VisibleForTesting
  List<Long> fileSizes(Collection<GcsPath> paths) throws IOException {
    List<StorageObject[]> results = Lists.newArrayList();
    executeBatches(makeGetBatches(paths, results));

    ImmutableList.Builder<Long> ret = ImmutableList.builder();
    for (StorageObject[] result : results) {
      ret.add(result[0].getSize().longValue());
    }
    return ret.build();
  }

  /**
   * Opens an object in GCS.
   *
   * <p>Returns a SeekableByteChannel that provides access to data in the bucket.
   *
   * @param path the GCS filename to read from
   * @return a SeekableByteChannel that can read the object data
   */
  public SeekableByteChannel open(GcsPath path)
      throws IOException {
    return new GoogleCloudStorageReadChannel(storageClient, path.getBucket(),
            path.getObject(), errorExtractor,
            new ClientRequestHelper<StorageObject>());
  }

  /**
   * Creates an object in GCS.
   *
   * <p>Returns a WritableByteChannel that can be used to write data to the
   * object.
   *
   * @param path the GCS file to write to
   * @param type the type of object, eg "text/plain".
   * @return a Callable object that encloses the operation.
   */
  public WritableByteChannel create(GcsPath path,
      String type) throws IOException {
    GoogleCloudStorageWriteChannel channel = new GoogleCloudStorageWriteChannel(
        executorService,
        storageClient,
        new ClientRequestHelper<StorageObject>(),
        path.getBucket(),
        path.getObject(),
        AsyncWriteChannelOptions.newBuilder().build(),
        new ObjectWriteConditions(),
        Collections.<String, String>emptyMap(),
        type);
    if (uploadBufferSizeBytes != null) {
      channel.setUploadBufferSize(uploadBufferSizeBytes);
    }
    channel.initialize();
    return channel;
  }

  /**
   * Returns whether the GCS bucket exists and is accessible.
   */
  public boolean bucketAccessible(GcsPath path) throws IOException {
    return bucketAccessible(
        path,
        BACKOFF_FACTORY.backoff(),
        Sleeper.DEFAULT);
  }

  /**
   * Returns the project number of the project which owns this bucket.
   * If the bucket exists, it must be accessible otherwise the permissions
   * exception will be propagated.  If the bucket does not exist, an exception
   * will be thrown.
   */
  public long bucketOwner(GcsPath path) throws IOException {
    return getBucket(
        path,
        BACKOFF_FACTORY.backoff(),
        Sleeper.DEFAULT).getProjectNumber().longValue();
  }

  /**
   * Creates a {@link Bucket} under the specified project in Cloud Storage or
   * propagates an exception.
   */
  public void createBucket(String projectId, Bucket bucket) throws IOException {
    createBucket(
        projectId, bucket, BACKOFF_FACTORY.backoff(), Sleeper.DEFAULT);
  }

  /**
   * Returns whether the GCS bucket exists. This will return false if the bucket
   * is inaccessible due to permissions.
   */
  @VisibleForTesting
  boolean bucketAccessible(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    try {
      return getBucket(path, backoff, sleeper) != null;
    } catch (AccessDeniedException | FileNotFoundException e) {
      return false;
    }
  }

  @VisibleForTesting
  @Nullable
  Bucket getBucket(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    Storage.Buckets.Get getBucket =
        storageClient.buckets().get(path.getBucket());

      try {
        Bucket bucket = ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(getBucket),
            backoff,
            new RetryDeterminer<IOException>() {
              @Override
              public boolean shouldRetry(IOException e) {
                if (errorExtractor.itemNotFound(e) || errorExtractor.accessDenied(e)) {
                  return false;
                }
                return RetryDeterminer.SOCKET_ERRORS.shouldRetry(e);
              }
            },
            IOException.class,
            sleeper);

        return bucket;
      } catch (GoogleJsonResponseException e) {
        if (errorExtractor.accessDenied(e)) {
          throw new AccessDeniedException(path.toString(), null, e.getMessage());
        }
        if (errorExtractor.itemNotFound(e)) {
          throw new FileNotFoundException(e.getMessage());
        }
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Error while attempting to verify existence of bucket gs://%s",
                path.getBucket()), e);
     }
  }

  @VisibleForTesting
  void createBucket(String projectId, Bucket bucket, BackOff backoff, Sleeper sleeper)
        throws IOException {
    Storage.Buckets.Insert insertBucket =
        storageClient.buckets().insert(projectId, bucket);
    insertBucket.setPredefinedAcl("projectPrivate");
    insertBucket.setPredefinedDefaultObjectAcl("projectPrivate");

    try {
      ResilientOperation.retry(
        ResilientOperation.getGoogleRequestCallable(insertBucket),
        backoff,
        new RetryDeterminer<IOException>() {
          @Override
          public boolean shouldRetry(IOException e) {
            if (errorExtractor.itemAlreadyExists(e) || errorExtractor.accessDenied(e)) {
              return false;
            }
            return RetryDeterminer.SOCKET_ERRORS.shouldRetry(e);
          }
        },
        IOException.class,
        sleeper);
      return;
    } catch (GoogleJsonResponseException e) {
      if (errorExtractor.accessDenied(e)) {
        throw new AccessDeniedException(bucket.getName(), null, e.getMessage());
      }
      if (errorExtractor.itemAlreadyExists(e)) {
        throw new FileAlreadyExistsException(bucket.getName(), null, e.getMessage());
      }
      throw e;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
        String.format("Error while attempting to create bucket gs://%s for rproject %s",
                      bucket.getName(), projectId), e);
    }
  }

  private static void executeBatches(List<BatchRequest> batches) throws IOException {
    ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(MAX_CONCURRENT_BATCHES, MAX_CONCURRENT_BATCHES,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>())));

    List<ListenableFuture<Void>> futures = new LinkedList<>();
    for (final BatchRequest batch : batches) {
      futures.add(executor.submit(new Callable<Void>() {
        public Void call() throws IOException {
          batch.execute();
          return null;
        }
      }));
    }

    try {
      Futures.allAsList(futures).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while executing batch GCS request", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        throw (FileNotFoundException) e.getCause();
      }
      throw new IOException("Error executing batch GCS request", e);
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Makes get {@link BatchRequest BatchRequests}.
   *
   * @param paths {@link GcsPath GcsPaths}.
   * @param results mutable {@link List} for return values.
   * @return {@link BatchRequest BatchRequests} to execute.
   * @throws IOException
   */
  @VisibleForTesting
  List<BatchRequest> makeGetBatches(
      Collection<GcsPath> paths,
      List<StorageObject[]> results) throws IOException {
    List<BatchRequest> batches = new LinkedList<>();
    for (List<GcsPath> filesToGet :
        Lists.partition(Lists.newArrayList(paths), MAX_REQUESTS_PER_BATCH)) {
      BatchRequest batch = createBatchRequest();
      for (GcsPath path : filesToGet) {
        results.add(enqueueGetFileSize(path, batch));
      }
      batches.add(batch);
    }
    return batches;
  }

  public void copy(List<String> srcFilenames, List<String> destFilenames) throws IOException {
    executeBatches(makeCopyBatches(srcFilenames, destFilenames));
  }

  List<BatchRequest> makeCopyBatches(List<String> srcFilenames, List<String> destFilenames)
      throws IOException {
    checkArgument(
        srcFilenames.size() == destFilenames.size(),
        "Number of source files %s must equal number of destination files %s",
        srcFilenames.size(),
        destFilenames.size());

    List<BatchRequest> batches = new LinkedList<>();
    BatchRequest batch = createBatchRequest();
    for (int i = 0; i < srcFilenames.size(); i++) {
      final GcsPath sourcePath = GcsPath.fromUri(srcFilenames.get(i));
      final GcsPath destPath = GcsPath.fromUri(destFilenames.get(i));
      enqueueCopy(sourcePath, destPath, batch);
      if (batch.size() >= MAX_REQUESTS_PER_BATCH) {
        batches.add(batch);
        batch = createBatchRequest();
      }
    }
    if (batch.size() > 0) {
      batches.add(batch);
    }
    return batches;
  }

  List<BatchRequest> makeRemoveBatches(Collection<String> filenames) throws IOException {
    List<BatchRequest> batches = new LinkedList<>();
    for (List<String> filesToDelete :
        Lists.partition(Lists.newArrayList(filenames), MAX_REQUESTS_PER_BATCH)) {
      BatchRequest batch = createBatchRequest();
      for (String file : filesToDelete) {
        enqueueDelete(GcsPath.fromUri(file), batch);
      }
      batches.add(batch);
    }
    return batches;
  }

  public void remove(Collection<String> filenames) throws IOException {
    executeBatches(makeRemoveBatches(filenames));
  }

  private StorageObject[] enqueueGetFileSize(final GcsPath path, BatchRequest batch)
      throws IOException {
    final StorageObject[] storageObject = new StorageObject[1];

    Storage.Objects.Get getRequest = storageClient.objects()
        .get(path.getBucket(), path.getObject());
    getRequest.queue(batch, new JsonBatchCallback<StorageObject>() {
      @Override
      public void onSuccess(StorageObject response, HttpHeaders httpHeaders) throws IOException {
        storageObject[0] = response;
      }

      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders httpHeaders) throws IOException {
        if (errorExtractor.itemNotFound(e)) {
          throw new FileNotFoundException(path.toString());
        } else {
          throw new IOException(String.format("Error trying to get %s: %s", path, e));
        }
      }
    });
    return storageObject;
  }

  private void enqueueCopy(final GcsPath from, final GcsPath to, BatchRequest batch)
      throws IOException {
    Storage.Objects.Copy copyRequest = storageClient.objects()
        .copy(from.getBucket(), from.getObject(), to.getBucket(), to.getObject(), null);
    copyRequest.queue(batch, new JsonBatchCallback<StorageObject>() {
      @Override
      public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
        LOG.debug("Successfully copied {} to {}", from, to);
      }

      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
        if (errorExtractor.itemNotFound(e)) {
          // Do nothing on item not found.
          LOG.debug("{} does not exist, assuming this is a retry after deletion.", from);
          return;
        }
        throw new IOException(
            String.format("Error trying to copy %s to %s: %s", from, to, e));
      }
    });
  }

  private void enqueueDelete(final GcsPath file, BatchRequest batch) throws IOException {
    Storage.Objects.Delete deleteRequest = storageClient.objects()
        .delete(file.getBucket(), file.getObject());
    deleteRequest.queue(batch, new JsonBatchCallback<Void>() {
      @Override
      public void onSuccess(Void obj, HttpHeaders responseHeaders) {
        LOG.debug("Successfully deleted {}", file);
      }

      @Override
      public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
        if (errorExtractor.itemNotFound(e)) {
          // Do nothing on item not found.
          LOG.debug("{} does not exist.", file);
          return;
        }
        throw new IOException(String.format("Error trying to delete %s: %s", file, e));
      }
    });
  }

  private BatchRequest createBatchRequest() {
    return storageClient.batch(httpRequestInitializer);
  }

  private static int doubleSlashes(StringBuilder dst, char[] src, int i) {
    // Emit the next character without special interpretation
    dst.append('\\');
    if ((i - 1) != src.length) {
      dst.append(src[i]);
      i++;
    } else {
      // A backslash at the very end is treated like an escaped backslash
      dst.append('\\');
    }
    return i;
  }
}
