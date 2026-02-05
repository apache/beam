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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

class GcsUtilV2 {
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

  GcsUtilV2(PipelineOptions options) {
    String projectId = options.as(GcpOptions.class).getProject();
    storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
  }

  public Blob getBlob(GcsPath gcsPath, BlobGetOption... blobGetOptions) throws IOException {
    return storage.get(gcsPath.getBucket(), gcsPath.getObject(), blobGetOptions);
  }

  public long fileSize(GcsPath gcsPath) throws IOException {
    return getBlob(gcsPath).getSize();
  }

  /** Lists {@link Blob}s given the {@code bucket}, {@code prefix}, {@code pageToken}. */
  public List<Blob> listBlobs(
      String bucket, String prefix, @Nullable String pageToken, @Nullable String delimiter)
      throws IOException {
    List<BlobListOption> options = new ArrayList<>();
    options.add(BlobListOption.pageSize(MAX_LIST_BLOBS_PER_CALL));
    if (pageToken != null) {
      options.add(BlobListOption.pageToken(pageToken));
    }
    if (prefix != null) {
      options.add(BlobListOption.prefix(prefix));
    }
    if (delimiter != null) {
      options.add(BlobListOption.delimiter(delimiter));
    }

    Page<Blob> blobs = storage.list(bucket, options.toArray(new BlobListOption[0]));
    List<Blob> blobList = blobs.streamValues().collect(Collectors.toList());
    return blobList;
  }

  public List<Blob> listBlobs(String bucket, String prefix, @Nullable String pageToken)
      throws IOException {
    return listBlobs(bucket, prefix, pageToken, null);
  }

  @SuppressWarnings({
    "nullness" // For Creating AccessDeniedException with null.
  })
  /** Get the {@link Bucket} from Cloud Storage path or propagates an exception. */
  public @Nullable Bucket getBucket(GcsPath path) throws IOException {
    String bucketName = path.getBucket();
    try {
      Bucket bucket = storage.get(bucketName);
      if (bucket == null) {
        throw new FileNotFoundException(
            String.format("The specified bucket does not exist: %s", bucketName));
      }
      return bucket;
    } catch (StorageException e) {
      if (e.getCode() == 403) { // 403 Forbidden
        throw new AccessDeniedException(String.format("gs://%s", bucketName), null, e.getMessage());
      }

      // rethrow other exceptions
      throw e;
    }
  }

  /** Returns whether the GCS bucket exists and is accessible. */
  public boolean bucketAccessible(GcsPath path) throws IOException {
    try {
      // Only select bucket name as a minimal set of returned fields.
      return storage.get(path.getBucket(), BucketGetOption.fields(BucketField.NAME)) != null;
    } catch (StorageException e) {
      return false;
    }
  }

  /**
   * Checks whether the GCS bucket exists. Similar to {@link #bucketAccessible(GcsPath)}, but throws
   * exception if the bucket is inaccessible due to permissions or does not exist.
   */
  public void verifyBucketAccessible(GcsPath path) throws IOException {
    storage.get(path.getBucket(), BucketGetOption.fields(BucketField.NAME));
  }

  /**
   * Returns the project number of the project which owns this bucket. If the bucket exists, it must
   * be accessible otherwise the permissions exception will be propagated. If the bucket does not
   * exist, an exception will be thrown.
   */
  public long bucketProject(GcsPath path) throws IOException {
    Bucket bucket = storage.get(path.getBucket(), BucketGetOption.fields(BucketField.PROJECT));
    return bucket.getProject().longValue();
  }

  @SuppressWarnings({
    "nullness" // For Creating AccessDeniedException with null.
  })
  public void createBucket(BucketInfo bucketInfo) throws IOException {
    String bucketName = bucketInfo.getName();
    try {
      storage.create(bucketInfo);
    } catch (StorageException e) {
      if (e.getCode() == 403) { // 403 Forbidden
        throw new AccessDeniedException(String.format("gs://%s", bucketName), null, e.getMessage());
      } else if (e.getCode() == 409) { // 409 Conflict
        throw new FileAlreadyExistsException(bucketName, null, e.getMessage());
      }

      // rethrow other exceptions
      throw e;
    }
  }

  @SuppressWarnings({
    "nullness" // For Creating AccessDeniedException with null.
  })
  public void removeBucket(BucketInfo bucketInfo) throws IOException {
    String bucketName = bucketInfo.getName();
    try {
      Bucket bucket = storage.get(bucketName, BucketGetOption.fields(BucketField.NAME));
      if (bucket == null) {
        throw new FileNotFoundException(
            String.format("The specified bucket does not exist: %s", bucketName));
      }
      bucket.delete();
    } catch (StorageException e) {
      if (e.getCode() == 403) { // 403 Forbidden
        throw new AccessDeniedException(String.format("gs://%s", bucketName), null, e.getMessage());
      }

      // rethrow other exceptions
      throw e;
    }
  }
}
