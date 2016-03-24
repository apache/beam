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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Options that can be used to configure the {@link DirectPipeline}.
 */
public interface DirectPipelineOptions extends
    ApplicationNameOptions, BigQueryOptions, GcsOptions, GcpOptions,
    PipelineOptions, StreamingOptions {

  /**
   * The random seed to use for pseudorandom behaviors in the {@link DirectPipelineRunner}.
   * If not explicitly specified, a random seed will be generated.
   */
  @JsonIgnore
  @Description("The random seed to use for pseudorandom behaviors in the DirectPipelineRunner."
      + " If not explicitly specified, a random seed will be generated.")
  Long getDirectPipelineRunnerRandomSeed();
  void setDirectPipelineRunnerRandomSeed(Long value);

  /**
   * Controls whether the runner should ensure that all of the elements of
   * the pipeline, such as DoFns, can be serialized.
   */
  @JsonIgnore
  @Description("Controls whether the runner should ensure that all of the elements of the "
      + "pipeline, such as DoFns, can be serialized.")
  @Default.Boolean(true)
  boolean isTestSerializability();
  void setTestSerializability(boolean testSerializability);

  /**
   * Controls whether the runner should ensure that all of the elements of
   * every {@link PCollection} can be encoded using the appropriate
   * {@link Coder}.
   */
  @JsonIgnore
  @Description("Controls whether the runner should ensure that all of the elements of every "
      + "PCollection can be encoded using the appropriate Coder.")
  @Default.Boolean(true)
  boolean isTestEncodability();
  void setTestEncodability(boolean testEncodability);

  /**
   * Controls whether the runner should randomize the order of each
   * {@link PCollection}.
   */
  @JsonIgnore
  @Description("Controls whether the runner should randomize the order of each PCollection.")
  @Default.Boolean(true)
  boolean isTestUnorderedness();
  void setTestUnorderedness(boolean testUnorderedness);
}
