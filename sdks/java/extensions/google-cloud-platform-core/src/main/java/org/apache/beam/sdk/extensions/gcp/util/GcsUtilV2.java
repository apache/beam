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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.PipelineOptions;

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
}
