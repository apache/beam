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
package org.apache.beam.sdk.io.datadog;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * Configuration for writing to Datadog.
 *
 * <p>This class is meant to be used with {@link DatadogWriteSchemaTransformProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class DatadogWriteSchemaTransformConfiguration {

  public void validate() {
    String invalidConfigMessage = "Invalid Datadog Write configuration: ";

    ErrorHandling errorHandling = getErrorHandling();
    if (errorHandling != null) {
      checkArgument(
          !Strings.isNullOrEmpty(errorHandling.getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }
  }

  /** Instantiates a {@link DatadogWriteSchemaTransformConfiguration.Builder} instance. */
  public static DatadogWriteSchemaTransformConfiguration.Builder builder() {
    return new AutoValue_DatadogWriteSchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("The Datadog API URL.")
  public abstract String getUrl();

  @SchemaFieldDescription("The Datadog API key.")
  public abstract String getApiKey();

  @SchemaFieldDescription("The number of events to batch together for each write.")
  public abstract @Nullable Integer getBatchCount();

  @SchemaFieldDescription("The maximum buffer size in bytes.")
  public abstract @Nullable Long getMaxBufferSize();

  @SchemaFieldDescription("The degree of parallelism for writing.")
  public abstract @Nullable Integer getParallelism();

  @SchemaFieldDescription("Specifies how to handle errors.")
  public abstract @Nullable ErrorHandling getErrorHandling();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUrl(String url);

    public abstract Builder setApiKey(String apiKey);

    public abstract Builder setBatchCount(Integer batchCount);

    public abstract Builder setMaxBufferSize(Long maxBufferSize);

    public abstract Builder setParallelism(Integer parallelism);

    public abstract Builder setErrorHandling(@Nullable ErrorHandling errorHandling);

    /** Builds the {@link DatadogWriteSchemaTransformConfiguration} configuration. */
    public abstract DatadogWriteSchemaTransformConfiguration build();
  }
}
