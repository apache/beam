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
package org.apache.beam.sdk.options;

import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.Validation.Required;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline options common to all portable runners. */
@Experimental(Kind.PORTABILITY)
public interface PortablePipelineOptions extends PipelineOptions {

  // TODO: https://issues.apache.org/jira/browse/BEAM-4106: Consider pulling this out into a new
  // options interface, e.g., FileStagingOptions.
  /**
   * List of local files to make available to workers.
   *
   * <p>Files are placed on the worker's classpath.
   *
   * <p>The default value is the list of jars from the main program's classpath.
   */
  @Description(
      "Files to stage to the artifact service and make available to workers. Files are placed on "
          + "the worker's classpath. The default value is all files from the classpath.")
  List<String> getFilesToStage();

  void setFilesToStage(List<String> value);

  @Description(
      "Job service endpoint to use. Should be in the form of address and port, e.g. localhost:3000")
  @Required
  String getJobEndpoint();

  void setJobEndpoint(String endpoint);

  @Description(
      "Set the default environment type for running user code. "
          + "Possible options are DOCKER and PROCESS.")
  @Nullable
  String getDefaultEnvironmentType();

  void setDefaultEnvironmentType(String environmentType);

  @Description(
      "Set environment configuration for running the user code.\n"
          + " For DOCKER: Url for the docker image.\n"
          + " For PROCESS: json of the form "
          + "{\"os\": \"<OS>\", \"arch\": \"<ARCHITECTURE>\", \"command\": \"<process to execute>\", "
          + "\"env\":{\"<Environment variables 1>\": \"<ENV_VAL>\"} }. "
          + "All fields in the json are optional except command.")
  @Nullable
  String getDefaultEnvironmentConfig();

  void setDefaultEnvironmentConfig(@Nullable String config);

  @Description(
      "Sets the number of sdk worker processes that will run on each worker node. Default is 1. If"
          + " 0, it will be automatically set by the runner by looking at different parameters "
          + "(e.g. number of CPU cores on the worker machine).")
  @Default.Integer(1)
  int getSdkWorkerParallelism();

  void setSdkWorkerParallelism(int parallelism);

  @Description("Duration in milliseconds for environment cache within a job. 0 means no caching.")
  @Default.Integer(0)
  int getEnvironmentCacheMillis();

  void setEnvironmentCacheMillis(int environmentCacheMillis);

  @Description("Duration in milliseconds for environment expiration. 0 means no expiration.")
  @Default.Integer(0)
  int getEnvironmentExpirationMillis();

  void setEnvironmentExpirationMillis(int environmentExpirationMillis);

  @Description(
      "Specifies if bundles should be distributed to the next available free SDK worker. By default SDK workers are pinned to runner tasks for the duration of the pipeline. This option can help for pipelines with long and skewed bundle execution times to increase throughput and improve worker utilization.")
  @Default.Boolean(false)
  boolean getLoadBalanceBundles();

  void setLoadBalanceBundles(boolean loadBalanceBundles);

  @Description("The output path for the executable file to be created.")
  @Nullable
  String getOutputExecutablePath();

  void setOutputExecutablePath(String outputExecutablePath);

  /** Enumeration of the different implementations of the artifact retrieval service. */
  enum RetrievalServiceType {
    /** Artifacts are to be retrieved from a {@link org.apache.beam.sdk.io.FileSystem}. */
    FILE_SYSTEM,
    /** Artifacts are to be retrieved from the runtime {@link ClassLoader}. */
    CLASSLOADER,
  }

  @Description("The artifact retrieval service to be used.")
  @Default.Enum("FILE_SYSTEM")
  RetrievalServiceType getRetrievalServiceType();

  void setRetrievalServiceType(RetrievalServiceType retrievalServiceType);
}
