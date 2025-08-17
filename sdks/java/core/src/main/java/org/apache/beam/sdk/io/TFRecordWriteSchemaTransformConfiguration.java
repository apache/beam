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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Configuration for reading from TFRecord.
 *
 * <p>This class is meant to be used with {@link TFRecordWriteSchemaTransformProvider}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class TFRecordWriteSchemaTransformConfiguration {

  public void validate() {
    String invalidConfigMessage = "Invalid TFRecord Write configuration: ";

    ErrorHandling errorHandling = getErrorHandling();
    if (errorHandling != null) {
      checkArgument(
          !Strings.isNullOrEmpty(errorHandling.getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }
  }

  /** Instantiates a {@link TFRecordWriteSchemaTransformConfiguration.Builder} instance. */
  public static TFRecordWriteSchemaTransformConfiguration.Builder builder() {
    return new AutoValue_TFRecordWriteSchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("The directory to which files will be written.")
  public abstract String getOutputPrefix();

  @SchemaFieldDescription(
      "The suffix of each file written, combined with prefix and shardTemplate.")
  @Nullable
  public abstract String getFilenameSuffix();

  @SchemaFieldDescription("The number of shards to use, or 0 for automatic.")
  public abstract int getNumShards();

  @SchemaFieldDescription(
      "The shard template of each file written, combined with prefix and suffix.")
  @Nullable
  public abstract String getShardTemplate();

  @SchemaFieldDescription("Option to indicate the output sink's compression type. Default is NONE.")
  public abstract String getCompression();

  @SchemaFieldDescription(
      "Whether to skip the spilling of data caused by having maxNumWritersPerBundle.")
  @Nullable
  public abstract Boolean getNoSpilling();

  @SchemaFieldDescription(
      "Maximum number of writers created in a bundle before spilling to shuffle.")
  @Nullable
  public abstract Integer getMaxNumWritersPerBundle();

  @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setOutputPrefix(String value);

    public abstract Builder setShardTemplate(String value);

    public abstract Builder setFilenameSuffix(String value);

    public abstract Builder setNumShards(int value);

    public abstract Builder setCompression(String value);

    public abstract Builder setNoSpilling(Boolean value);

    public abstract Builder setMaxNumWritersPerBundle(@Nullable Integer maxNumWritersPerBundle);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    /** Builds the {@link TFRecordWriteSchemaTransformConfiguration} configuration. */
    public abstract TFRecordWriteSchemaTransformConfiguration build();
  }
}
