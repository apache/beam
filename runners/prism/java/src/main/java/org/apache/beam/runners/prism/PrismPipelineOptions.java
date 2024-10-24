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
package org.apache.beam.runners.prism;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PortablePipelineOptions;

/**
 * {@link org.apache.beam.sdk.options.PipelineOptions} for running a {@link
 * org.apache.beam.sdk.Pipeline} on the {@link PrismRunner}.
 */
public interface PrismPipelineOptions extends PortablePipelineOptions {

  String JOB_PORT_FLAG_NAME = "job_port";

  @Description(
      "Path or URL to a prism binary, or zipped binary for the current "
          + "platform (Operating System and Architecture). May also be an Apache "
          + "Beam Github Release page URL, which be used to construct  download URL for "
          + " the current platform. ")
  String getPrismLocation();

  void setPrismLocation(String prismLocation);

  @Description(
      "Override the SDK's version for deriving the Github Release URLs for "
          + "downloading a zipped prism binary, for the current platform. If "
          + "the --prismLocation flag is set to a Github Release page URL, "
          + "then it will use that release page as a base when constructing the download URL.")
  String getPrismVersionOverride();

  void setPrismVersionOverride(String prismVersionOverride);

  @Description("Enable or disable Prism Web UI")
  @Default.Boolean(true)
  Boolean getEnableWebUI();

  void setEnableWebUI(Boolean enableWebUI);

  @Description(
      "Duration, represented as a String, that prism will wait for a new job before shutting itself down. Negative durations disable auto shutdown. Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\".")
  @Default.String("5m")
  String getIdleShutdownTimeout();

  void setIdleShutdownTimeout(String idleShutdownTimeout);

  @Description(
      "Sets the log level for Prism. Can be set to 'debug', 'info', 'warn', or 'error'.")
  @Default.String("warn")
  String getPrismLogLevel();

  void setPrismLogLevel(String idleShutdownTimeout);
}
