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
package org.apache.beam.examples.advanced.subprocess;


import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Options for running a sub process within a DoFn.
 */
@DefaultCoder(AvroCoder.class)
public interface SubProcessPipelineOptions extends PipelineOptions {

  @Description("Source GCS directory where the C++ library is located gs://bucket/tests")
  @Required
  String getSourcePath();

  void setSourcePath(String sourcePath);

  @Description("Working directory for the process I/O")
  @Default.String("/tmp/grid_working_files")
  String getWorkerPath();

  void setWorkerPath(String workerPath);

  @Description("The maximum time to wait for the sub-process to complete")
  @Default.Integer(3600)
  Integer getWaitTime();

  void setWaitTime(Integer waitTime);

  @Description("As sub-processes can be heavy weight define the level of concurrency level")
  @Required
  Integer getConcurrency();

  void setConcurrency(Integer concurrency);

  @Description("Should log files only be uploaded if error.")
  @Default.Boolean(true)
  Boolean getOnlyUpLoadLogsOnError();

  void setOnlyUpLoadLogsOnError(Boolean onlyUpLoadLogsOnError);

}
