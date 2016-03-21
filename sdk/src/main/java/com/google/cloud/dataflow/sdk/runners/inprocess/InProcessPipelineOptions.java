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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.Hidden;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Options that can be used to configure the {@link InProcessPipelineRunner}.
 */
public interface InProcessPipelineOptions extends PipelineOptions, ApplicationNameOptions {
  /**
   * Gets the {@link ExecutorServiceFactory} to use to create instances of {@link ExecutorService}
   * to execute {@link PTransform PTransforms}.
   *
   * <p>Note that {@link ExecutorService ExecutorServices} returned by the factory must ensure that
   * it cannot enter a state in which it will not schedule additional pending work unless currently
   * scheduled work completes, as this may cause the {@link Pipeline} to cease processing.
   *
   * <p>Defaults to a {@link CachedThreadPoolExecutorServiceFactory}, which produces instances of
   * {@link Executors#newCachedThreadPool()}.
   */
  @JsonIgnore
  @Required
  @Hidden
  @Default.InstanceFactory(CachedThreadPoolExecutorServiceFactory.class)
  ExecutorServiceFactory getExecutorServiceFactory();

  void setExecutorServiceFactory(ExecutorServiceFactory executorService);

  /**
   * Gets the {@link Clock} used by this pipeline. The clock is used in place of accessing the
   * system time when time values are required by the evaluator.
   */
  @Default.InstanceFactory(NanosOffsetClock.Factory.class)
  @JsonIgnore
  @Required
  @Hidden
  @Description(
      "The processing time source used by the pipeline. When the current time is "
          + "needed by the evaluator, the result of clock#now() is used.")
  Clock getClock();

  void setClock(Clock clock);

  @Default.Boolean(false)
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
          + "pipeline can be awaited on by use of InProcessPipelineResult#awaitCompletion().")
  boolean isBlockOnRun();

  void setBlockOnRun(boolean b);
}
