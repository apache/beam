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
package org.apache.beam.sdk.extensions.gcp.storage;

import com.google.auto.value.AutoValue;
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An abstract class that contains common configuration options for creating resources. */
@AutoValue
public abstract class GcsCreateOptions extends CreateOptions {

  /**
   * The buffer size (in bytes) to use when uploading files to GCS. Please see the documentation for
   * {@link AbstractGoogleAsyncWriteChannel#setUploadBufferSize} for more information on the
   * restrictions and performance implications of this value.
   */
  public abstract @Nullable Integer gcsUploadBufferSizeBytes();

  // TODO: Add other GCS options when needed.

  /** Returns a {@link GcsCreateOptions.Builder}. */
  public static GcsCreateOptions.Builder builder() {
    return new AutoValue_GcsCreateOptions.Builder();
  }

  /** A builder for {@link GcsCreateOptions}. */
  @AutoValue.Builder
  public abstract static class Builder extends CreateOptions.Builder<GcsCreateOptions.Builder> {
    public abstract GcsCreateOptions build();

    public abstract GcsCreateOptions.Builder setGcsUploadBufferSizeBytes(@Nullable Integer bytes);
  }
}
