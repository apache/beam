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

import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Sleeper;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageRequest;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

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
  private static final int MAX_REQUESTS_PER_BATCH = 1000;

  /////////////////////////////////////////////////////////////////////////////

  /** Client for the GCS API. */
  private Storage storageClient;
  /** Buffer size for GCS uploads (in bytes). */
  @Nullable private final Integer uploadBufferSizeBytes;

  // Helper delegate for turning IOExceptions from API calls into higher-level semantics.
  private final ApiErrorExtractor errorExtractor = new ApiErrorExtractor();

  // Exposed for testing.
  final ExecutorService executorService;

  private final BatchHelper batchHelper;
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
    this.batchHelper = new BatchHelper(
        storageClient.getRequestFactory().getInitializer(), storageClient, MAX_REQUESTS_PER_BATCH);
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
            new AttemptBoundedExponentialBackOff(3, 200),
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
            new AttemptBoundedExponentialBackOff(3, 200),
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
    return fileSize(path, new AttemptBoundedExponentialBackOff(4, 200), Sleeper.DEFAULT);
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
   * Returns whether the GCS bucket exists. If the bucket exists, it must
   * be accessible otherwise the permissions exception will be propagated.
   */
  public boolean bucketExists(GcsPath path) throws IOException {
    return bucketExists(path, new AttemptBoundedExponentialBackOff(4, 200), Sleeper.DEFAULT);
  }

  /**
   * Returns whether the GCS bucket exists. This will return false if the bucket
   * is inaccessible due to permissions.
   */
  @VisibleForTesting
  boolean bucketExists(GcsPath path, BackOff backoff, Sleeper sleeper) throws IOException {
    Storage.Buckets.Get getBucket =
        storageClient.buckets().get(path.getBucket());

      try {
        ResilientOperation.retry(
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
        return true;
      } catch (GoogleJsonResponseException e) {
        if (errorExtractor.itemNotFound(e) || errorExtractor.accessDenied(e)) {
          return false;
        }
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            String.format("Error while attempting to verify existence of bucket gs://%s",
                path.getBucket()), e);
     }
  }

  public void copy(List<String> srcFilenames, List<String> destFilenames) throws IOException {
    checkArgument(
        srcFilenames.size() == destFilenames.size(),
        "Number of source files %s must equal number of destination files %s",
        srcFilenames.size(),
        destFilenames.size());
    for (int i = 0; i < srcFilenames.size(); i++) {
      final GcsPath sourcePath = GcsPath.fromUri(srcFilenames.get(i));
      final GcsPath destPath = GcsPath.fromUri(destFilenames.get(i));
      LOG.debug("Copying {} to {}", sourcePath, destPath);
      Storage.Objects.Copy copyObject = storageClient.objects().copy(sourcePath.getBucket(),
          sourcePath.getObject(), destPath.getBucket(), destPath.getObject(), null);
      batchHelper.queue(copyObject, new JsonBatchCallback<StorageObject>() {
        @Override
        public void onSuccess(StorageObject obj, HttpHeaders responseHeaders) {
          LOG.debug("Successfully copied {} to {}", sourcePath, destPath);
        }

        @Override
        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
          // Do nothing on item not found.
          if (!errorExtractor.itemNotFound(e)) {
            throw new IOException(e.toString());
          }
          LOG.debug("{} does not exist.", sourcePath);
        }
      });
    }
    batchHelper.flush();
  }

  public void remove(Collection<String> filenames) throws IOException {
    for (String filename : filenames) {
      final GcsPath path = GcsPath.fromUri(filename);
      LOG.debug("Removing: " + path);
      Storage.Objects.Delete deleteObject =
          storageClient.objects().delete(path.getBucket(), path.getObject());
      batchHelper.queue(deleteObject, new JsonBatchCallback<Void>() {
        @Override
        public void onSuccess(Void obj, HttpHeaders responseHeaders) throws IOException {
          LOG.debug("Successfully removed {}", path);
        }

        @Override
        public void onFailure(GoogleJsonError e, HttpHeaders responseHeaders) throws IOException {
          // Do nothing on item not found.
          if (!errorExtractor.itemNotFound(e)) {
            throw new IOException(e.toString());
          }
          LOG.debug("{} does not exist.", path);
        }
      });
    }
    batchHelper.flush();
  }

  /**
   * BatchHelper abstracts out the logic for the maximum requests per batch for GCS.
   *
   * <p>Copy of
   * https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/src/main/java/com/google/cloud/hadoop/gcsio/BatchHelper.java
   *
   * <p>Copied to prevent Dataflow from depending on the Hadoop-related dependencies that are not
   * used in Dataflow.  Hadoop-related dependencies will be removed from the Google Cloud Storage
   * Connector (https://cloud.google.com/hadoop/google-cloud-storage-connector) so that this project
   * and others may use the connector without introducing unnecessary dependencies.
   *
   * <p>This class is not thread-safe; create a new BatchHelper instance per single-threaded logical
   * grouping of requests.
   */
  @NotThreadSafe
  private static class BatchHelper {
    /**
     * Callback that causes a single StorageRequest to be added to the BatchRequest.
     */
    protected static interface QueueRequestCallback {
      void enqueue() throws IOException;
    }

    private final List<QueueRequestCallback> pendingBatchEntries;
    private final BatchRequest batch;

    // Number of requests that can be queued into a single actual HTTP request
    // before a sub-batch is sent.
    private final long maxRequestsPerBatch;

    // Flag that indicates whether there is an in-progress flush.
    private boolean flushing = false;

    /**
     * Primary constructor, generally accessed only via the inner Factory class.
     */
    public BatchHelper(
        HttpRequestInitializer requestInitializer, Storage gcs, long maxRequestsPerBatch) {
      this.pendingBatchEntries = new LinkedList<>();
      this.batch = gcs.batch(requestInitializer);
      this.maxRequestsPerBatch = maxRequestsPerBatch;
    }

    /**
     * Adds an additional request to the batch, and possibly flushes the current contents of the
     * batch if {@code maxRequestsPerBatch} has been reached.
     */
    public <T> void queue(final StorageRequest<T> req, final JsonBatchCallback<T> callback)
        throws IOException {
      QueueRequestCallback queueCallback = new QueueRequestCallback() {
        @Override
        public void enqueue() throws IOException {
          req.queue(batch, callback);
        }
      };
      pendingBatchEntries.add(queueCallback);

      flushIfPossibleAndRequired();
    }

    // Flush our buffer if we have more pending entries than maxRequestsPerBatch
    private void flushIfPossibleAndRequired() throws IOException {
      if (pendingBatchEntries.size() > maxRequestsPerBatch) {
        flushIfPossible();
      }
    }

    // Flush our buffer if we are not already in a flush operation and we have data to flush.
    private void flushIfPossible() throws IOException {
      if (!flushing && pendingBatchEntries.size() > 0) {
        flushing = true;
        try {
          while (batch.size() < maxRequestsPerBatch && pendingBatchEntries.size() > 0) {
            QueueRequestCallback head = pendingBatchEntries.remove(0);
            head.enqueue();
          }

          batch.execute();
        } finally {
          flushing = false;
        }
      }
    }


    /**
     * Sends any currently remaining requests in the batch; should be called at the end of any
     * series of batched requests to ensure everything has been sent.
     */
    public void flush() throws IOException {
      flushIfPossible();
    }
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
