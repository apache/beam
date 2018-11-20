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
package org.apache.beam.runners.apex;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options that configure the Apex pipeline. */
public interface ApexPipelineOptions extends PipelineOptions {

  @Description("set unique application name for Apex runner")
  void setApplicationName(String name);

  String getApplicationName();

  @Description("execute the pipeline with embedded cluster")
  void setEmbeddedExecution(boolean embedded);

  @Default.Boolean(true)
  boolean isEmbeddedExecution();

  @Description("configure embedded execution with debug friendly options")
  void setEmbeddedExecutionDebugMode(boolean embeddedDebug);

  @Default.Boolean(true)
  boolean isEmbeddedExecutionDebugMode();

  @Description("output data received and emitted on ports (for debugging)")
  void setTupleTracingEnabled(boolean enabled);

  @Default.Boolean(false)
  boolean isTupleTracingEnabled();

  @Description("how long the client should wait for the pipeline to run")
  void setRunMillis(long runMillis);

  @Default.Long(0)
  long getRunMillis();

  @Description("configuration properties file for the Apex engine")
  void setConfigFile(String name);

  @Default.String("classpath:/beam-runners-apex.properties")
  String getConfigFile();

  @Description("configure whether to perform ParDo fusion")
  void setParDoFusionEnabled(boolean enabled);

  @Default.Boolean(true)
  boolean isParDoFusionEnabled();
}
