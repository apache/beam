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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.DataflowPackage;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.PackageUtil.StagedFile;
import org.apache.beam.sdk.extensions.gcp.storage.GcsCreateOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.MimeTypes;

/** Utility class for staging files to GCS. */
public class GcsStager implements Stager {
  private DataflowPipelineOptions options;

  private GcsStager(DataflowPipelineOptions options) {
    this.options = options;
  }

  @SuppressWarnings("unused") // used via reflection
  public static GcsStager fromOptions(PipelineOptions options) {
    return new GcsStager(options.as(DataflowPipelineOptions.class));
  }

  /**
   * Stages files to {@link DataflowPipelineOptions#getStagingLocation()}, suffixed with their md5
   * hash to avoid collisions.
   *
   * <p>Uses {@link DataflowPipelineOptions#getGcsUploadBufferSizeBytes()}.
   */
  @Override
  public List<DataflowPackage> stageFiles(List<StagedFile> filesToStage) {
    try (PackageUtil packageUtil = PackageUtil.withDefaultThreadPool()) {
      return packageUtil.stageClasspathElements(
          filesToStage, options.getStagingLocation(), buildCreateOptions());
    }
  }

  @Override
  public DataflowPackage stageToFile(byte[] bytes, String baseName) {
    try (PackageUtil packageUtil = PackageUtil.withDefaultThreadPool()) {
      return packageUtil.stageToFile(
          bytes, baseName, options.getStagingLocation(), buildCreateOptions());
    }
  }

  private GcsCreateOptions buildCreateOptions() {
    // Default is 1M, to avoid excessive memory use when uploading, but can be changed with
    // {@link DataflowPipelineOptions#getGcsUploadBufferSizeBytes()}.
    int uploadSizeBytes = firstNonNull(options.getGcsUploadBufferSizeBytes(), 1024 * 1024);
    checkArgument(uploadSizeBytes > 0, "gcsUploadBufferSizeBytes must be > 0");

    return GcsCreateOptions.builder()
        .setGcsUploadBufferSizeBytes(uploadSizeBytes)
        .setMimeType(MimeTypes.BINARY)
        .build();
  }
}
