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

/**
 * Options that are used exclusively within the Dataflow worker harness.
 * These options have no effect at pipeline creation time.
 */
@Description("[Internal] Options that are used exclusively within the Dataflow worker harness. "
    + "These options have no effect at pipeline creation time.")
@Hidden
public interface DataflowWorkerHarnessOptions extends DataflowPipelineOptions {
  /**
   * The identity of the worker running this pipeline.
   */
  @Description("The identity of the worker running this pipeline.")
  String getWorkerId();
  void setWorkerId(String value);

  /**
   * The identity of the Dataflow job.
   */
  @Description("The identity of the Dataflow job.")
  String getJobId();
  void setJobId(String value);

  /**
   * The size of the worker's in-memory cache, in megabytes.
   *
   * <p>Currently, this cache is used for storing read values of side inputs.
   */
  @Description("The size of the worker's in-memory cache, in megabytes.")
  @Default.Integer(100)
  Integer getWorkerCacheMb();
  void setWorkerCacheMb(Integer value);
}
