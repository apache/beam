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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
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
      return new GcsUtil(Transport.newStorageClient(gcsOptions).build(),
          gcsOptions.getExecutorService(), gcsOptions.getGcsUploadBufferSizeBytes());
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
  public boolean isGcsPatternSupported(String gcsPattern) {
    if (RECURSIVE_GCS_PATTERN.matcher(gcsPattern).matches()) {
      throw new IllegalArgumentException("Unsupported wildcard usage in \"" + gcsPattern + "\": "
          + " recursive wildcards are not supported.");
    }

    return true;
  }

  private GcsUtil(
      Storage storageClient, ExecutorService executorService,
      @Nullable Integer uploadBufferSizeBytes) {
    this.storageClient = storageClient;
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
    Matcher m = GLOB_PREFIX.matcher(gcsPattern.getObject());
    Pattern p = null;
    String prefix = null;
    if (!m.matches()) {
      // Not a glob.
      Storage.Objects.Get getObject = storageClient.objects().get(
          gcsPattern.getBucket(), gcsPattern.getObject());
      try {
        // Use a get request to fetch the metadata of the object,
        // the request has strong global consistency.
        ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(getObject),
            BACKOFF_FACTORY.backoff(),
            RetryDeterminer.SOCKET_ERRORS,
            IOException.class);
        return ImmutableList.of(gcsPattern);
      } catch (IOException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (e instanceof IOException && errorExtractor.itemNotFound((IOException) e)) {
          // If the path was not found, return an empty list.
          return ImmutableList.of();
        }
        throw new IOException("Unable to match files for pattern " + gcsPattern, e);
      }
    } else {
      // Part before the first wildcard character.
      prefix = m.group("PREFIX");
      p = Pattern.compile(globToRegexp(gcsPattern.getObject()));
    }

    LOG.debug("matching files in bucket {}, prefix {} against pattern {}", gcsPattern.getBucket(),
        prefix, p.toString());

    // List all objects that start with the prefix (including objects in sub-directories).
    Storage.Objects.List listObject = storageClient.objects().list(gcsPattern.getBucket());
    listObject.setMaxResults(MAX_LIST_ITEMS_PER_CALL);
    listObject.setPrefix(prefix);

    String pageToken = null;
    List<GcsPath> results = new LinkedList<>();
    do {
      if (pageToken != null) {
        listObject.setPageToken(pageToken);
      }

      Objects objects;
      try {
        objects = ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(listObject),
            BACKOFF_FACTORY.backoff(),
            RetryDeterminer.SOCKET_ERRORS,
            IOException.class);
      } catch (Exception e) {
        throw new IOException("Unable to match files in bucket " + gcsPattern.getBucket()
            +  ", prefix " + prefix + " against pattern " + p.toString(), e);
      }
      //Objects objects = listObject.execute();
      checkNotNull(objects);

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
    return fileSize(
        path,
        BACKOFF_FACTORY.backoff(),
        Sleeper.DEFAULT);
  }

  /**
   * Returns the file size from GCS or throws {@link FileNotFoundException}
   * if the resource does not exist.
   */
  @VisibleForTesting
  long fileSize(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
      Storage.Objects.Get getObject =
          storageClient.objects().get(path.getBucket(), path.getObject());
      try {
        StorageObject object = ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(getObject),
            backoff,
            RetryDeterminer.SOCKET_ERRORS,
            IOException.class,
            sleeper);
        return object.getSize().longValue();
      } catch (Exception e) {
        if (e instanceof IOException && errorExtractor.itemNotFound((IOException) e)) {
          throw new FileNotFoundException(path.toString());
        }
        throw new IOException("Unable to get file size", e);
     }
  }

  /**
   * Opens an object in GCS.
   *
   * <p>Returns a SeekableByteChannel that provides access to data in the bucket.
   *
   * @param path the GCS filename to read from
   * @return a SeekableByteChannel that can read the object data
   * @throws IOException
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
   * @throws IOException
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
      throw new IOException("Error executing batch GCS request", e);
    } finally {
      executor.shutdown();
    }
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
    BatchRequest batch = storageClient.batch();
    for (int i = 0; i < srcFilenames.size(); i++) {
      final GcsPath sourcePath = GcsPath.fromUri(srcFilenames.get(i));
      final GcsPath destPath = GcsPath.fromUri(destFilenames.get(i));
      enqueueCopy(sourcePath, destPath, batch);
      if (batch.size() >= MAX_REQUESTS_PER_BATCH) {
        batches.add(batch);
        batch = storageClient.batch();
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
      BatchRequest batch = storageClient.batch();
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

  /**
   * Expands glob expressions to regular expressions.
   *
   * @param globExp the glob expression to expand
   * @return a string with the regular expression this glob expands to
   */
  static String globToRegexp(String globExp) {
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
