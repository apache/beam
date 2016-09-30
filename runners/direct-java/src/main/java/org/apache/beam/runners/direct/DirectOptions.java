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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure the {@link org.apache.beam.runners.direct.DirectRunner}.
 */
public interface DirectOptions extends PipelineOptions, ApplicationNameOptions {
  @Default.Boolean(true)
  @Description(
      "If the pipeline should shut down producers which have reached the maximum "
          + "representable watermark. If this is set to true, a pipeline in which all PTransforms "
          + "have reached the maximum watermark will be shut down, even if there are unbounded "
          + "sources that could produce additional (late) data. By default, if the pipeline "
          + "contains any unbounded PCollections, it will run until explicitly shut down.")
  boolean isShutdownUnboundedProducersWithMaxWatermark();

  void setShutdownUnboundedProducersWithMaxWatermark(boolean shutdown);

  @Default.Boolean(true)
  @Description(
      "If the pipeline should block awaiting completion of the pipeline. If set to true, "
          + "a call to Pipeline#run() will block until all PTransforms are complete. Otherwise, "
          + "the Pipeline will execute asynchronously. If set to false, the completion of the "
          + "pipeline can be awaited on by use of DirectPipelineResult#awaitCompletion().")
  boolean isBlockOnRun();

  void setBlockOnRun(boolean b);

  @Default.Boolean(true)
  @Description(
      "Controls whether the DirectRunner should ensure that all of the elements of every "
          + "PCollection are not mutated. PTransforms are not permitted to mutate input elements "
          + "at any point, or output elements after they are output.")
  boolean isEnforceImmutability();

  void setEnforceImmutability(boolean test);

  @Default.Boolean(true)
  @Description(
      "Controls whether the DirectRunner should ensure that all of the elements of every "
          + "PCollection are encodable. All elements in a PCollection must be encodable.")
  boolean isEnforceEncodability();
  void setEnforceEncodability(boolean test);
}
