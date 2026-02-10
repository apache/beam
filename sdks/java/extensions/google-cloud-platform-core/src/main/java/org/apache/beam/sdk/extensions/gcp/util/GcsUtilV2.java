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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.apache.beam.sdk.io.FileSystemUtils.wildcardToRegexp;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.paging.Page;
import com.google.auto.value.AutoValue;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

class GcsUtilV2 {
  private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(GcsUtilV2.class);

  public static class GcsUtilFactory implements DefaultValueFactory<GcsUtilV2> {
    @Override
    public GcsUtilV2 create(PipelineOptions options) {
      // GcsOptions gcsOptions = options.as(GcsOptions.class);
      // Storage.Builder storageBuilder = Transport.newStorageClient(gcsOptions);
      return new GcsUtilV2(options);
    }
  }

  private Storage storage;

  /** Maximum number of items to retrieve per Objects.List request. */
  private static final long MAX_LIST_BLOBS_PER_CALL = 1024;

  /** Maximum number of requests permitted in a GCS batch request. */
  private static final int MAX_REQUESTS_PER_BATCH = 100;

  /**
   * Limit the number of bytes Cloud Storage will attempt to copy before responding to an individual
   * request. If you see Read Timeout errors, try reducing this value.
   */
  private static final long MEGABYTES_COPIED_PER_CHUNK = 2048L;

  GcsUtilV2(PipelineOptions options) {
    String projectId = options.as(GcpOptions.class).getProject();
    storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  @SuppressWarnings({
    "nullness" // For Creating AccessDeniedException and FileAlreadyExistsException with null.
  })
  private IOException translateStorageException(GcsPath gcsPath, StorageException e) {
    switch (e.getCode()) {
      case 403:
        return new AccessDeniedException(gcsPath.toString(), null, e.getMessage());
      case 409:
        return new FileAlreadyExistsException(gcsPath.toString(), null, e.getMessage());
      default:
        return new IOException(e);
    }
  }

  private IOException translateStorageException(
      String bucketName, @Nullable String blobName, StorageException e) {
    return translateStorageException(GcsPath.fromComponents(bucketName, blobName), e);
  }

  public Blob getBlob(GcsPath gcsPath, BlobGetOption... options) throws IOException {
    try {
      Blob blob = storage.get(gcsPath.getBucket(), gcsPath.getObject(), options);
      if (blob == null) {
        throw new FileNotFoundException(
            String.format("The specified file does not exist: %s", gcsPath.toString()));
      }
      return blob;
    } catch (StorageException e) {
      throw translateStorageException(gcsPath, e);
    }
  }

  public long fileSize(GcsPath gcsPath) throws IOException {
    return getBlob(gcsPath, BlobGetOption.fields(BlobField.SIZE)).getSize();
  }

  /** A class that holds either a {@link Blob} or an {@link IOException}. */
  @AutoValue
  public abstract static class BlobResult {

    /** Returns the {@link Blob}. */
    public abstract @Nullable Blob blob();

    /** Returns the {@link IOException}. */
    public abstract @Nullable IOException ioException();

    @VisibleForTesting
    public static BlobResult create(Blob blob) {
      return new AutoValue_GcsUtilV2_BlobResult(checkNotNull(blob, "blob"), null /* ioException */);
    }

    @VisibleForTesting
    public static BlobResult create(IOException ioException) {
      return new AutoValue_GcsUtilV2_BlobResult(
          null /* blob */, checkNotNull(ioException, "ioException"));
    }
  }

  public List<BlobResult> getBlobs(Iterable<GcsPath> gcsPaths, BlobGetOption... options)
      throws IOException {

    List<BlobResult> results = new ArrayList<>();

    for (List<GcsPath> pathPartition :
        Lists.partition(Lists.newArrayList(gcsPaths), MAX_REQUESTS_PER_BATCH)) {

      // Create a new empty batch every time
      StorageBatch batch = storage.batch();
      List<StorageBatchResult<Blob>> batchResultFutures = new ArrayList<>();

      for (GcsPath path : pathPartition) {
        batchResultFutures.add(batch.get(path.getBucket(), path.getObject(), options));
      }
      batch.submit();

      for (int i = 0; i < batchResultFutures.size(); i++) {
        StorageBatchResult<Blob> future = batchResultFutures.get(i);
        try {
          Blob blob = future.get();
          if (blob != null) {
            results.add(BlobResult.create(blob));
          } else {
            results.add(
                BlobResult.create(
                    new FileNotFoundException(
                        String.format(
                            "The specified file does not exist: %s",
                            pathPartition.get(i).toString()))));
          }
        } catch (StorageException e) {
          results.add(BlobResult.create(translateStorageException(pathPartition.get(i), e)));
        }
      }
    }
    return results;
  }

  /** Lists {@link Blob}s given the {@code bucket}, {@code prefix}, {@code pageToken}. */
  public Page<Blob> listBlobs(
      String bucket,
      String prefix,
      @Nullable String pageToken,
      @Nullable String delimiter,
      BlobListOption... options)
      throws IOException {
    List<BlobListOption> blobListOptions = new ArrayList<>();
    blobListOptions.add(BlobListOption.pageSize(MAX_LIST_BLOBS_PER_CALL));
    if (pageToken != null) {
      blobListOptions.add(BlobListOption.pageToken(pageToken));
    }
    if (prefix != null) {
      blobListOptions.add(BlobListOption.prefix(prefix));
    }
    if (delimiter != null) {
      blobListOptions.add(BlobListOption.delimiter(delimiter));
    }
    if (options != null && options.length > 0) {
      for (BlobListOption option : options) {
        blobListOptions.add(option);
      }
    }

    try {
      return storage.list(bucket, blobListOptions.toArray(new BlobListOption[0]));
    } catch (StorageException e) {
      throw translateStorageException(bucket, prefix, e);
    }
  }

  public Page<Blob> listBlobs(
      String bucket, String prefix, @Nullable String pageToken, BlobListOption... options)
      throws IOException {
    return listBlobs(bucket, prefix, pageToken, null, options);
  }

  /**
   * Expands a pattern into matched paths. The pattern path may contain globs, which are expanded in
   * the result. For patterns that only match a single object, we ensure that the object exists.
   */
  public List<GcsPath> expand(GcsPath gcsPattern) throws IOException {
    // Handle Non-Wildcard Path
    if (!GcsPath.isWildcard(gcsPattern)) {
      try {
        // Use a get request to fetch the metadata of the object, and ignore the return value.
        // The request has strong global consistency.
        getBlob(gcsPattern, BlobGetOption.fields(BlobField.NAME));
        return ImmutableList.of(gcsPattern);
      } catch (FileNotFoundException e) {
        // If the path was not found, return an empty list.
        return ImmutableList.of();
      }
    }

    // Handle Wildcard Path
    // TODO: check out BlobListOption.matchGlob() for a similar function.
    String prefix = GcsPath.getNonWildcardPrefix(gcsPattern.getObject());
    Pattern p = Pattern.compile(wildcardToRegexp(gcsPattern.getObject()));

    LOG.debug(
        "matching files in bucket {}, prefix {} against pattern {}",
        gcsPattern.getBucket(),
        prefix,
        p.toString());

    List<GcsPath> results = new ArrayList<>();
    Page<Blob> blobs =
        listBlobs(
            gcsPattern.getBucket(),
            prefix,
            null,
            BlobListOption.fields(BlobField.NAME, BlobField.BUCKET));
    // Iterate through all elements page by page (lazily)
    for (Blob b : blobs.iterateAll()) {
      String name = b.getName();
      // Filter objects based on the regex. Skip directories, which end with a slash.
      if (p.matcher(name).matches() && !name.endsWith("/")) {
        LOG.debug("Matched object: {}", name);
        results.add(GcsPath.fromComponents(b.getBucket(), b.getName()));
      }
    }
    return results;
  }

  public void remove(Iterable<GcsPath> paths, BlobSourceOption... options) throws IOException {
    for (List<GcsPath> pathPartition :
        Lists.partition(Lists.newArrayList(paths), MAX_REQUESTS_PER_BATCH)) {

      // Create a new empty batch every time
      StorageBatch batch = storage.batch();
      List<StorageBatchResult<Boolean>> batchResultFutures = new ArrayList<>();

      for (GcsPath path : pathPartition) {
        batchResultFutures.add(batch.delete(path.getBucket(), path.getObject(), options));
      }
      batch.submit();

      for (int i = 0; i < batchResultFutures.size(); i++) {
        StorageBatchResult<Boolean> future = batchResultFutures.get(i);
        try {
          Boolean deleted = future.get();
          if (!deleted) {
            throw new FileNotFoundException(
                String.format(
                    "The specified file does not exist: %s", pathPartition.get(i).toString()));
          }
        } catch (StorageException e) {
          throw translateStorageException(pathPartition.get(i), e);
        }
      }
    }
  }

  public enum CopyStrategy {
    NO_OVERWRITE, // Fail if target exists
    SAFE_OVERWRITE, // Overwrite only if the generation matches (atomic)
    UNSAFE_OVERWRITE // Overwrite regardless of state
  }

  public void copy(Iterable<GcsPath> srcPaths, Iterable<GcsPath> dstPaths, CopyStrategy strategy)
      throws IOException {
    List<GcsPath> srcList = Lists.newArrayList(srcPaths);
    List<GcsPath> dstList = Lists.newArrayList(dstPaths);
    checkArgument(
        srcList.size() == dstList.size(),
        "Number of source files %s must equal number of destination files %s",
        srcList.size(),
        dstList.size());

    for (int i = 0; i < srcList.size(); i++) {
      GcsPath srcPath = srcList.get(i);
      GcsPath dstPath = dstList.get(i);
      BlobId srcId = BlobId.of(srcPath.getBucket(), srcPath.getObject());
      BlobId dstId = BlobId.of(dstPath.getBucket(), dstPath.getObject());

      CopyRequest.Builder copyRequestBuilder =
          CopyRequest.newBuilder()
              .setSource(srcId)
              .setMegabytesCopiedPerChunk(MEGABYTES_COPIED_PER_CHUNK); // 2GiB

      if (strategy == CopyStrategy.UNSAFE_OVERWRITE) {
        copyRequestBuilder.setTarget(dstId);
      } else {
        // Both NO_OVERWRITE and SAFE_OVERWRITE require checking the existing blob
        BlobInfo existingTarget = null;
        try {
          existingTarget = storage.get(dstId);
        } catch (StorageException e) {
          throw translateStorageException(dstPath, e);
        }

        Storage.BlobTargetOption precondition;
        if (existingTarget == null) {
          precondition = Storage.BlobTargetOption.doesNotExist();
        } else if (strategy == CopyStrategy.SAFE_OVERWRITE) {
          // SAFE_OVERWRITE: match the generation of the existing object
          precondition = Storage.BlobTargetOption.generationMatch(existingTarget.getGeneration());
        } else {
          // Target exists and we aren't allowed to overwrite (strategy == NO_OVERWRITE)
          throw new FileAlreadyExistsException(
              srcPath.toString(),
              dstPath.toString(),
              "Target object already exists and CopyStrategy is NO_OVERWRITE");
        }
        copyRequestBuilder.setTarget(dstId, precondition);
      }

      CopyWriter copyWriter = storage.copy(copyRequestBuilder.build());

      try {
        copyWriter.getResult();
      } catch (StorageException e) {
        throw translateStorageException(srcPath, e);
      }
    }
  }

  /** Get the {@link Bucket} from Cloud Storage path or propagates an exception. */
  public Bucket getBucket(GcsPath path, BucketGetOption... options) throws IOException {
    String bucketName = path.getBucket();
    try {
      Bucket bucket = storage.get(bucketName, options);
      if (bucket == null) {
        throw new FileNotFoundException(
            String.format("The specified bucket does not exist: gs://%s", bucketName));
      }
      return bucket;
    } catch (StorageException e) {
      throw translateStorageException(bucketName, null, e);
    }
  }

  /** Returns whether the GCS bucket exists and is accessible. */
  public boolean bucketAccessible(GcsPath path) {
    try {
      // Fetch only the name field to minimize data transfer
      getBucket(path, BucketGetOption.fields(BucketField.NAME));
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Checks whether the GCS bucket exists. Similar to {@link #bucketAccessible(GcsPath)}, but throws
   * exception if the bucket is inaccessible due to permissions or does not exist.
   */
  public void verifyBucketAccessible(GcsPath path) throws IOException {
    // Fetch only the name field to minimize data transfer
    getBucket(path, BucketGetOption.fields(BucketField.NAME));
  }

  /**
   * Returns the project number of the project which owns this bucket. If the bucket exists, it must
   * be accessible otherwise the permissions exception will be propagated. If the bucket does not
   * exist, an exception will be thrown.
   */
  public long bucketProject(GcsPath path) throws IOException {
    Bucket bucket = getBucket(path, BucketGetOption.fields(BucketField.PROJECT));
    return bucket.getProject().longValue();
  }

  public void createBucket(BucketInfo bucketInfo) throws IOException {
    try {
      storage.create(bucketInfo);
    } catch (StorageException e) {
      throw translateStorageException(bucketInfo.getName(), null, e);
    }
  }

  public void removeBucket(BucketInfo bucketInfo) throws IOException {
    try {
      if (!storage.delete(bucketInfo.getName())) {
        throw new FileNotFoundException(
            String.format("The specified bucket does not exist: gs://%s", bucketInfo.getName()));
      }
    } catch (StorageException e) {
      throw translateStorageException(bucketInfo.getName(), null, e);
    }
  }
}
