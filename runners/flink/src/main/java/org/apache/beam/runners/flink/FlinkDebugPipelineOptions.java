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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Debug options which shouldn't normally be used. */
public interface FlinkDebugPipelineOptions extends PipelineOptions {

  @Description(
      "If not null, reports the checkpoint duration of each ParDo stage in the provided metric namespace.")
  String getReportCheckpointDuration();

  void setReportCheckpointDuration(String metricNamespace);

  @Description(
      "Shuts down sources which have been idle for the configured time of milliseconds. Once a source has been "
          + "shut down, chekpointing is not possible anymore. Shutting down the sources eventually leads to pipeline "
          + "shutdown once all input has been processed.")
  @Default.Long(0)
  Long getShutdownSourcesAfterIdleMs();

  void setShutdownSourcesAfterIdleMs(Long timeout);
}
