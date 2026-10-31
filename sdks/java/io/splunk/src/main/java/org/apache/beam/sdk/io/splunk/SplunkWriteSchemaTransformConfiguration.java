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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Configuration for writing to Splunk's Http Event Collector (HEC).
 *
 * <p>This class is meant to be used with {@link SplunkWriteSchemaTransformProvider}.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SplunkWriteSchemaTransformConfiguration {

  public void validate() {
    String invalidConfigMessage = "Invalid Splunk Write configuration: ";
    checkArgument(!getUrl().isEmpty(), invalidConfigMessage + "url must be specified.");
    checkArgument(!getToken().isEmpty(), invalidConfigMessage + "token must be specified.");
    Integer batchCount = getBatchCount();
    if (batchCount != null) {
      checkArgument(batchCount > 0, invalidConfigMessage + "batchCount must be greater than 0.");
    }
    Integer parallelism = getParallelism();
    if (parallelism != null) {
      checkArgument(parallelism > 0, invalidConfigMessage + "parallelism must be greater than 0.");
    }
    ErrorHandling errorHandling = getErrorHandling();
    if (errorHandling != null) {
      checkArgument(
          !Strings.isNullOrEmpty(errorHandling.getOutput()),
          invalidConfigMessage + "Output must not be empty if error handling specified.");
    }
  }

  /** Instantiates a {@link SplunkWriteSchemaTransformConfiguration.Builder} instance. */
  public static SplunkWriteSchemaTransformConfiguration.Builder builder() {
    return new AutoValue_SplunkWriteSchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("The Splunk HEC endpoint URL, e.g. https://splunk-host:8088.")
  public abstract String getUrl();

  @SchemaFieldDescription("The Splunk HEC authentication token.")
  public abstract String getToken();

  @SchemaFieldDescription("The number of events to batch together for each write.")
  public abstract @Nullable Integer getBatchCount();

  @SchemaFieldDescription("The number of parallel requests to the HEC endpoint.")
  public abstract @Nullable Integer getParallelism();

  @SchemaFieldDescription(
      "Whether to disable SSL certificate validation, e.g. for self-signed certificates.")
  public abstract @Nullable Boolean getDisableCertificateValidation();

  @SchemaFieldDescription("Path to a root CA certificate used to validate the HEC endpoint.")
  public abstract @Nullable String getRootCaCertificatePath();

  @SchemaFieldDescription("Whether to log the result of each batch write.")
  public abstract @Nullable Boolean getEnableBatchLogs();

  @SchemaFieldDescription(
      "Whether requests sent to the HEC endpoint should be GZIP encoded. Defaults to true.")
  public abstract @Nullable Boolean getEnableGzipHttpCompression();

  @SchemaFieldDescription("Specifies how to handle errors.")
  public abstract @Nullable ErrorHandling getErrorHandling();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUrl(String url);

    public abstract Builder setToken(String token);

    public abstract Builder setBatchCount(Integer batchCount);

    public abstract Builder setParallelism(Integer parallelism);

    public abstract Builder setDisableCertificateValidation(Boolean disableCertificateValidation);

    public abstract Builder setRootCaCertificatePath(String rootCaCertificatePath);

    public abstract Builder setEnableBatchLogs(Boolean enableBatchLogs);

    public abstract Builder setEnableGzipHttpCompression(Boolean enableGzipHttpCompression);

    public abstract Builder setErrorHandling(@Nullable ErrorHandling errorHandling);

    /** Builds the {@link SplunkWriteSchemaTransformConfiguration} configuration. */
    public abstract SplunkWriteSchemaTransformConfiguration build();
  }
}
