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
package org.apache.beam.runners.dataflow.options;

import com.google.api.services.clouddebugger.v2.model.Debuggee;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options for controlling Cloud Debugger. */
@Description("[Experimental] Used to configure the Cloud Debugger")
@Experimental
@Hidden
public interface CloudDebuggerOptions extends PipelineOptions {

  /** Whether to enable the Cloud Debugger snapshot agent for the current job. */
  @Description("Whether to enable the Cloud Debugger snapshot agent for the current job.")
  boolean getEnableCloudDebugger();

  void setEnableCloudDebugger(boolean enabled);

  /** The Cloud Debugger debuggee to associate with. This should not be set directly. */
  @Description("The Cloud Debugger debuggee to associate with. This should not be set directly.")
  @Hidden
  @Nullable
  Debuggee getDebuggee();

  void setDebuggee(Debuggee debuggee);

  /** The maximum cost (as a ratio of CPU time) allowed for evaluating conditional snapshots. */
  @Description(
      "The maximum cost (as a ratio of CPU time) allowed for evaluating conditional snapshots. "
          + "Should be a double between 0 and 1. "
          + "Snapshots will be cancelled if evaluating conditions takes more than this ratio of time.")
  @Default.Double(0.01)
  double getMaxConditionCost();

  void setMaxConditionCost(double maxConditionCost);
}
