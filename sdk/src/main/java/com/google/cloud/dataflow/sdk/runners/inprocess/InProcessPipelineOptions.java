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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation.Required;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.concurrent.ExecutorService;

/**
 * Options that can be used to configure the {@link InProcessPipelineRunner}.
 */
public interface InProcessPipelineOptions extends PipelineOptions, ApplicationNameOptions {
  @JsonIgnore
  @Default.InstanceFactory(CachedThreadPoolExecutorServiceFactory.class)
  ExecutorService getExecutorService();

  void setExecutorService(ExecutorService executorService);

  /**
   * Gets the {@link Clock} used by this pipeline. The clock is used in place of accessing the
   * system time when time values are required by the evaluator.
   */
  @Default.InstanceFactory(NanosOffsetClock.Factory.class)
  @Required
  @Description(
      "The processing time source used by the pipeline. When the current time is "
          + "needed by the evaluator, the result of clock#now() is used.")
  Clock getClock();

  void setClock(Clock clock);

  @Default.Boolean(false)
  @Description("If the pipelien should block awaiting completion of the pipeline. If set to true, "
      + "a call to Pipeline#run() will block until all PTransforms are complete. Otherwise, the "
      + "Pipeline will execute asynchronously. If set to false, the completion of the pipeline can "
      + "be awaited on by use of InProcessPipelineResult#awaitCompletion().")
  boolean isBlockOnRun();

  void setBlockOnRun(boolean b);
}
