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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure the {@link org.apache.beam.runners.direct.DirectRunner}.
 */
public interface DirectOptions extends PipelineOptions, ApplicationNameOptions {
  @Default.Boolean(true)
  @Description(
      "If the pipeline should block awaiting completion of the pipeline. If set to true, "
          + "a call to Pipeline#run() will block until all PTransforms are complete. Otherwise, "
          + "the Pipeline will execute asynchronously. If set to false, use "
          + "PipelineResult#waitUntilFinish() to block until the Pipeline is complete.")
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
          + "PCollection can be encoded and decoded by that PCollection's Coder.")
  boolean isEnforceEncodability();
  void setEnforceEncodability(boolean test);

  @Default.InstanceFactory(AvailableParallelismFactory.class)
  @Description(
      "Controls the amount of target parallelism the DirectRunner will use. Defaults to"
          + " the greater of the number of available processors and 3. Must be a value greater"
          + " than zero.")
  int getTargetParallelism();
  void setTargetParallelism(int target);

  /**
   * A {@link DefaultValueFactory} that returns the result of {@link Runtime#availableProcessors()}
   * from the {@link #create(PipelineOptions)} method. Uses {@link Runtime#getRuntime()} to obtain
   * the {@link Runtime}.
   */
  class AvailableParallelismFactory implements DefaultValueFactory<Integer> {
    private static final int MIN_PARALLELISM = 3;

    @Override
    public Integer create(PipelineOptions options) {
      return Math.max(Runtime.getRuntime().availableProcessors(), MIN_PARALLELISM);
    }
  }

  @Experimental(Kind.CORE_RUNNERS_ONLY)
  @Default.Boolean(false)
  @Description("Control whether toProto/fromProto translations are applied to original Pipeline")
  boolean isProtoTranslation();
  void setProtoTranslation(boolean b);
}
