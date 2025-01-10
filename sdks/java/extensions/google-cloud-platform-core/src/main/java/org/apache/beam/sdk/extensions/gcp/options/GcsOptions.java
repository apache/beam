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
package org.apache.beam.sdk.extensions.gcp.options;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import java.util.concurrent.ExecutorService;
import org.apache.beam.sdk.extensions.gcp.storage.GcsPathValidator;
import org.apache.beam.sdk.extensions.gcp.storage.PathValidator;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExecutorOptions;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options used to configure Google Cloud Storage. */
public interface GcsOptions extends ApplicationNameOptions, GcpOptions, PipelineOptions {
  /** The GcsUtil instance that should be used to communicate with Google Cloud Storage. */
  @JsonIgnore
  @Description("The GcsUtil instance that should be used to communicate with Google Cloud Storage.")
  @Default.InstanceFactory(GcsUtil.GcsUtilFactory.class)
  @Hidden
  GcsUtil getGcsUtil();

  void setGcsUtil(GcsUtil value);

  @JsonIgnore
  @Description(
      "The GoogleCloudStorageReadOptions instance that should be used to read from Google Cloud Storage.")
  @Default.InstanceFactory(GcsUtil.GcsReadOptionsFactory.class)
  @Hidden
  GoogleCloudStorageReadOptions getGoogleCloudStorageReadOptions();

  void setGoogleCloudStorageReadOptions(GoogleCloudStorageReadOptions value);

  /**
   * The ExecutorService instance to use to create threads, can be overridden to specify an
   * ExecutorService that is compatible with the user's environment. If unset, the default is to use
   * {@link ExecutorOptions#getScheduledExecutorService()}.
   *
   * @deprecated use {@link ExecutorOptions#getScheduledExecutorService()} instead
   */
  @JsonIgnore
  @Default.InstanceFactory(ExecutorServiceFactory.class)
  @Hidden
  @Deprecated
  ExecutorService getExecutorService();

  /**
   * @deprecated use {@link ExecutorOptions#setScheduledExecutorService} instead. If set, it may
   *     result in multiple ExecutorServices, and therefore thread pools, in the runtime.
   */
  @Deprecated
  void setExecutorService(ExecutorService value);

  /** GCS endpoint to use. If unspecified, uses the default endpoint. */
  @JsonIgnore
  @Hidden
  @Description("The URL for the GCS API.")
  String getGcsEndpoint();

  void setGcsEndpoint(String value);

  /**
   * The buffer size (in bytes) to use when uploading files to GCS. Please see the documentation for
   * {@link AsyncWriteChannelOptions#getUploadChunkSize} for more information on the restrictions
   * and performance implications of this value.
   */
  @Description(
      "The buffer size (in bytes) to use when uploading files to GCS. Please see the "
          + "documentation for AsyncWriteChannelOptions.getUploadChunkSize for more "
          + "information on the restrictions and performance implications of this value.\n\n"
          + "https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/util/src/main/java/"
          + "com/google/cloud/hadoop/util/AsyncWriteChannelOptions.java")
  @Nullable
  Integer getGcsUploadBufferSizeBytes();

  void setGcsUploadBufferSizeBytes(@Nullable Integer bytes);

  /**
   * The class of the validator that should be created and used to validate paths. If pathValidator
   * has not been set explicitly, an instance of this class will be constructed and used as the path
   * validator.
   */
  @Description(
      "The class of the validator that should be created and used to validate paths. "
          + "If pathValidator has not been set explicitly, an instance of this class will be "
          + "constructed and used as the path validator.")
  @Default.Class(GcsPathValidator.class)
  Class<? extends PathValidator> getPathValidatorClass();

  void setPathValidatorClass(Class<? extends PathValidator> validatorClass);

  /**
   * The path validator instance that should be used to validate paths. If no path validator has
   * been set explicitly, the default is to use the instance factory that constructs a path
   * validator based upon the currently set pathValidatorClass.
   */
  @JsonIgnore
  @Description(
      "The path validator instance that should be used to validate paths. "
          + "If no path validator has been set explicitly, the default is to use the instance factory "
          + "that constructs a path validator based upon the currently set pathValidatorClass.")
  @Default.InstanceFactory(PathValidatorFactory.class)
  PathValidator getPathValidator();

  void setPathValidator(PathValidator validator);

  /** If true, reports metrics of certain operations, such as batch copies. */
  @Description("Whether to report performance metrics of certain GCS operations.")
  @Default.Boolean(false)
  Boolean getGcsPerformanceMetrics();

  void setGcsPerformanceMetrics(Boolean reportPerformanceMetrics);

  @Description("Read timeout for gcs http requests")
  @Nullable
  Integer getGcsHttpRequestReadTimeout();

  void setGcsHttpRequestReadTimeout(@Nullable Integer timeoutMs);

  @Description("Write timeout for gcs http requests.")
  @Nullable
  Integer getGcsHttpRequestWriteTimeout();

  void setGcsHttpRequestWriteTimeout(@Nullable Integer timeoutMs);

  @Description("Batching limit for rewrite ops which will copy data.")
  @Nullable
  Integer getGcsRewriteDataOpBatchLimit();

  void setGcsRewriteDataOpBatchLimit(@Nullable Integer timeoutMs);

  /** If true, reports number of bytes written to each gcs bucket. */
  @Description("Whether to report number of bytes written per GCS bucket.")
  @Default.Boolean(false)
  Boolean getEnableBucketWriteMetricCounter();

  void setEnableBucketWriteMetricCounter(Boolean enableBucketWriteMetricCounter);

  /** If true, reports number of bytes read from each gcs bucket. */
  @Description("Whether to report number of bytes read per GCS bucket.")
  @Default.Boolean(false)
  Boolean getEnableBucketReadMetricCounter();

  void setEnableBucketReadMetricCounter(Boolean enableBucketReadMetricCounter);

  @Description(
      "Prefix for the metric that counts the number of bytes read per GCS bucket. The resulting"
          + " metric name will be formatted according to this template: <prefix>_<bucket_name>.")
  @Default.String("GCS_read_bytes_counter")
  String getGcsReadCounterPrefix();

  void setGcsReadCounterPrefix(String gcsReadCounterPrefix);

  @Description(
      "Prefix for the metric that counts the number of bytes written per GCS bucket. The resulting"
          + " metric name will be formatted according to this template: <prefix>_<bucket_name>.")
  @Default.String("GCS_write_bytes_counter")
  String getGcsWriteCounterPrefix();

  void setGcsWriteCounterPrefix(String gcsReadCounterPrefix);

  /**
   * Returns the default {@link ExecutorService} to use within the Apache Beam SDK. The {@link
   * ExecutorService} is compatible with AppEngine.
   */
  class ExecutorServiceFactory implements DefaultValueFactory<ExecutorService> {
    @Override
    public ExecutorService create(PipelineOptions options) {
      return options.as(ExecutorOptions.class).getScheduledExecutorService();
    }
  }

  /**
   * Creates a {@link PathValidator} object using the class specified in {@link
   * #getPathValidatorClass()}.
   */
  class PathValidatorFactory implements DefaultValueFactory<PathValidator> {
    @Override
    public PathValidator create(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      return InstanceBuilder.ofType(PathValidator.class)
          .fromClass(gcsOptions.getPathValidatorClass())
          .fromFactoryMethod("fromOptions")
          .withArg(PipelineOptions.class, options)
          .build();
    }
  }
}
