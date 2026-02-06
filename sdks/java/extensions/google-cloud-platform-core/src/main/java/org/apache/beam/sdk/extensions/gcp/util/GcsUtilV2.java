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

  @SuppressWarnings({
    "nullness" // For Creating AccessDeniedException and FileAlreadyExistsException with null.
  })
  private IOException translateStorageException(
      String bucketName, @Nullable String blobName, StorageException e) {
    String path = "gs://" + bucketName + (blobName == null ? "" : "/" + blobName);

    switch (e.getCode()) {
      case 403:
        return new AccessDeniedException(path, null, e.getMessage());
      case 409:
        return new FileAlreadyExistsException(path, null, e.getMessage());
      default:
        return new IOException(e);
    }
  }

  public Blob getBlob(GcsPath gcsPath, BlobGetOption... blobGetOptions) throws IOException {
    try {
      Blob blob = storage.get(gcsPath.getBucket(), gcsPath.getObject(), blobGetOptions);
      if (blob == null) {
        throw new FileNotFoundException(
            String.format("The specified file does not exist: %s", gcsPath.toString()));
      }
      return blob;
    } catch (StorageException e) {
      throw translateStorageException(gcsPath.getBucket(), gcsPath.getObject(), e);
    }
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
    Bucket bucket =
        getBucket(
            GcsPath.fromComponents(bucketInfo.getName(), null),
            BucketGetOption.fields(BucketField.NAME));

    try {
      bucket.delete();
    } catch (StorageException e) {
      throw translateStorageException(bucketInfo.getName(), null, e);
    }
  }
}
