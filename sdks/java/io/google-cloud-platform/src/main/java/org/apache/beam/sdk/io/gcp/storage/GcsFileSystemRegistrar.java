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
package org.apache.beam.sdk.io.gcp.storage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link AutoService} registrar for the {@link GcsFileSystem}.
 */
@AutoService(FileSystemRegistrar.class)
public class GcsFileSystemRegistrar implements FileSystemRegistrar {

  static final String GCS_SCHEME = "gs";

  @Override
  public FileSystem fromOptions(@Nonnull PipelineOptions options) {
    checkNotNull(
        options,
        "Expect the runner have called FileSystems.setDefaultConfigInWorkers().");
    return new GcsFileSystem(options.as(GcsOptions.class));
  }

  @Override
  public String getScheme() {
    return GCS_SCHEME;
  }
}
