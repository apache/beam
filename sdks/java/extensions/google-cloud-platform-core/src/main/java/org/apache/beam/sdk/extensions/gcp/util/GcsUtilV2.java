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
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
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
}
