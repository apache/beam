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
import com.google.cloud.hadoop.util.AbstractGoogleAsyncWriteChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.extensions.gcp.storage.GcsPathValidator;
import org.apache.beam.sdk.extensions.gcp.storage.PathValidator;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
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

  /**
   * The ExecutorService instance to use to create threads, can be overridden to specify an
   * ExecutorService that is compatible with the user's environment. If unset, the default is to
   * create an ExecutorService with an unbounded number of threads; this is compatible with Google
   * AppEngine.
   */
  @JsonIgnore
  @Description(
      "The ExecutorService instance to use to create multiple threads. Can be overridden "
          + "to specify an ExecutorService that is compatible with the user's environment. If unset, "
          + "the default is to create an ExecutorService with an unbounded number of threads; this "
          + "is compatible with Google AppEngine.")
  @Default.InstanceFactory(ExecutorServiceFactory.class)
  @Hidden
  ExecutorService getExecutorService();

  void setExecutorService(ExecutorService value);

  /** GCS endpoint to use. If unspecified, uses the default endpoint. */
  @JsonIgnore
  @Hidden
  @Description("The URL for the GCS API.")
  String getGcsEndpoint();

  void setGcsEndpoint(String value);

  /**
   * The buffer size (in bytes) to use when uploading files to GCS. Please see the documentation for
   * {@link AbstractGoogleAsyncWriteChannel#setUploadBufferSize} for more information on the
   * restrictions and performance implications of this value.
   */
  @Description(
      "The buffer size (in bytes) to use when uploading files to GCS. Please see the "
          + "documentation for AbstractGoogleAsyncWriteChannel.setUploadBufferSize for more "
          + "information on the restrictions and performance implications of this value.\n\n"
          + "https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/util/src/main/java/"
          + "com/google/cloud/hadoop/util/AbstractGoogleAsyncWriteChannel.java")
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
  @Description("Experimental. Whether to report performance metrics of certain GCS operations.")
  @Default.Boolean(false)
  @Experimental(Kind.FILESYSTEM)
  Boolean getGcsPerformanceMetrics();

  void setGcsPerformanceMetrics(Boolean reportPerformanceMetrics);

  /**
   * Returns the default {@link ExecutorService} to use within the Apache Beam SDK. The {@link
   * ExecutorService} is compatible with AppEngine.
   */
  class ExecutorServiceFactory implements DefaultValueFactory<ExecutorService> {
    @SuppressWarnings("deprecation") // IS_APP_ENGINE is deprecated for internal use only.
    @Override
    public ExecutorService create(PipelineOptions options) {
      ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
      threadFactoryBuilder.setThreadFactory(MoreExecutors.platformThreadFactory());
      threadFactoryBuilder.setDaemon(true);
      /* The SDK requires an unbounded thread pool because a step may create X writers
       * each requiring their own thread to perform the writes otherwise a writer may
       * block causing deadlock for the step because the writers buffer is full.
       * Also, the MapTaskExecutor launches the steps in reverse order and completes
       * them in forward order thus requiring enough threads so that each step's writers
       * can be active.
       */
      return new ThreadPoolExecutor(
          0,
          Integer.MAX_VALUE, // Allow an unlimited number of re-usable threads.
          Long.MAX_VALUE,
          TimeUnit.NANOSECONDS, // Keep non-core threads alive forever.
          new SynchronousQueue<>(),
          threadFactoryBuilder.build());
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
