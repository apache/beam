/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.options;

import com.google.cloud.dataflow.sdk.annotations.Experimental;

import java.util.HashMap;

/**
 * Options for controlling profiling of pipeline execution.
 */
@Description("[Experimental] Used to configure profiling of the Dataflow pipeline")
@Experimental
@Hidden
public interface DataflowProfilingOptions {

  @Description(
      "This option is deprecated and ignored. Using --saveProfilesToGcs=<GCS path> is preferred.")
  boolean getEnableProfilingAgent();
  void setEnableProfilingAgent(boolean enabled);

  @Description(
      "When set to a non-empty value, enables recording profiles and saving them to GCS.\n"
      + "Profiles will continue until the pipeline is stopped or updated without this option.\n")
  String getSaveProfilesToGcs();
  void setSaveProfilesToGcs(String gcsPath);

  @Description(
      "[INTERNAL] Additional configuration for the profiling agent. Not typically necessary.")
  @Hidden
  DataflowProfilingAgentConfiguration getProfilingAgentConfiguration();
  void setProfilingAgentConfiguration(DataflowProfilingAgentConfiguration configuration);

  /**
   * Configuration the for profiling agent.
   */
  class DataflowProfilingAgentConfiguration extends HashMap<String, Object> {
  }
}
