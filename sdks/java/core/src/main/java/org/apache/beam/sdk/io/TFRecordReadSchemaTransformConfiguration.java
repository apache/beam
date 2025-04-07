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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * Configuration for reading from TFRecord.
 *
 * <p>This class is meant to be used with {@link TFRecordReadSchemaTransformProvider}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class TFRecordReadSchemaTransformConfiguration implements Serializable {

  public void validate() {
    String invalidConfigMessage = "Invalid TFRecord Read configuration: ";

    if (getValidate()) {
      String filePattern = getFilePattern();
      try {
        MatchResult matches = FileSystems.match(filePattern);
        checkState(
            !matches.metadata().isEmpty(), "Unable to find any files matching %s", filePattern);
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format(invalidConfigMessage + "Failed to validate %s", filePattern), e);
      }
    }

    ErrorHandling errorHandling = getErrorHandling();
    if (errorHandling != null) {
      checkArgument(
          !Strings.isNullOrEmpty(errorHandling.getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }
  }

  /** Instantiates a {@link TFRecordReadSchemaTransformConfiguration.Builder} instance. */
  public static TFRecordReadSchemaTransformConfiguration.Builder builder() {
    return new AutoValue_TFRecordReadSchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("Validate file pattern.")
  public abstract boolean getValidate();

  @SchemaFieldDescription("Decompression type to use when reading input files.")
  public abstract String getCompression();

  @SchemaFieldDescription("Filename or file pattern used to find input files.")
  public abstract String getFilePattern();

  @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
  public abstract @Nullable ErrorHandling getErrorHandling();

  abstract Builder toBuilder();

  /** Builder for {@link TFRecordReadSchemaTransformConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setValidate(boolean value);

    public abstract Builder setCompression(String value);

    public abstract Builder setFilePattern(String value);

    public abstract Builder setErrorHandling(@Nullable ErrorHandling errorHandling);

    /** Builds the {@link TFRecordReadSchemaTransformConfiguration} configuration. */
    public abstract TFRecordReadSchemaTransformConfiguration build();
  }
}
