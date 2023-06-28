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
import org.apache.beam.sdk.options.Validation.Required;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline options common to all portable runners. */
public interface PortablePipelineOptions extends PipelineOptions, FileStagingOptions {

  @Description(
      "Job service endpoint to use. Should be in the form of address and port, e.g. localhost:3000")
  @Required
  String getJobEndpoint();

  void setJobEndpoint(String endpoint);

  @Description(
      "Job service request timeout in seconds. The timeout "
          + "determines the max time the driver program will wait to "
          + "get a response from the job server. NOTE: the timeout does not "
          + "apply to the actual pipeline run time. The driver program will "
          + "still wait for job completion indefinitely.")
  @Default.Integer(60)
  int getJobServerTimeout();

  void setJobServerTimeout(int timeout);

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

  @Description(
      "Options for configuring the default environment of portable workers. This environment will be used for all executable stages except for external transforms. Recognized options depend on the value of defaultEnvironmentType:\n"
          + "DOCKER: docker_container_image (optional), e.g. 'apache/beam_java8_sdk:latest'. If unset, will default to the latest official release of the Beam Java SDK corresponding to your Java runtime version (8 or 11).\n"
          + "EXTERNAL: external_service_address (required), e.g. 'localhost:50000'\n"
          + "PROCESS: process_command (required), process_variables (optional). process_command must be the location of an executable file that starts a Beam SDK worker. process_variables is a comma-separated list of environment variable assignments which will be set before running the process, e.g. 'FOO=a,BAR=b'\n\n"
          + "environmentOptions and defaultEnvironmentConfig are mutually exclusive. Prefer environmentOptions.")
  List<String> getEnvironmentOptions();

  void setEnvironmentOptions(List<String> value);

  /** Return the value for the specified environment option or empty string if not present. */
  static String getEnvironmentOption(
      PortablePipelineOptions options, String environmentOptionName) {
    List<String> environmentOptions = options.getEnvironmentOptions();
    if (environmentOptions == null) {
      return "";
    }

    for (String environmentEntry : environmentOptions) {
      String[] tokens = environmentEntry.split(environmentOptionName + "=", -1);
      if (tokens.length > 1) {
        return tokens[1];
      }
    }

    return "";
  }

  /**
   * If {@literal true} and PipelineOption tempLocation is set, save a heap dump before shutting
   * down the JVM due to GC thrashing or out of memory. The heap will be dumped to local disk and
   * then uploaded to the tempLocation.
   *
   * <p>CAUTION: Heap dumps can take up more disk than the JVM memory. Ensure the local disk is
   * configured to have sufficient free space before enabling this option.
   */
  @Description(
      "If {@literal true} and PipelineOption tempLocation is set, save a heap dump before shutting"
          + " down the JVM due to GC thrashing or out of memory. The heap will be dumped to local"
          + " disk and then uploaded to the tempLocation.")
  @Default.Boolean(false)
  boolean getEnableHeapDumps();

  void setEnableHeapDumps(boolean enableHeapDumps);
}
