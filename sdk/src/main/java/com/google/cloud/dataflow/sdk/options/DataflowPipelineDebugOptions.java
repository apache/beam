/*
 * Copyright (C) 2014 Google Inc.
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

import java.util.List;

/**
 * Options used for testing and debugging the Dataflow SDK.
 */
public interface DataflowPipelineDebugOptions extends PipelineOptions {
  /**
   * Dataflow endpoint to use.
   *
   * <p> Defaults to the current version of the Google Cloud Dataflow
   * API, at the time the current SDK version was released.
   *
   * <p> If the string contains "://", then this is treated as a url,
   * otherwise {@link #getApiRootUrl()} is used as the root
   * url.
   */
  @Description("Cloud Dataflow Endpoint")
  @Default.String("dataflow/v1b3/projects/")
  String getDataflowEndpoint();
  void setDataflowEndpoint(String value);

  /**
   * The list of backend experiments to enable.
   *
   * <p> Dataflow provides a number of experimental features that can be enabled
   * with this flag.
   *
   * <p> Please sync with the Dataflow team when enabling any experiments.
   */
  @Description("Backend experiments to enable.")
  List<String> getExperiments();
  void setExperiments(List<String> value);

  /**
   * The API endpoint to use when communicating with the Dataflow service.
   */
  @Description("Google Cloud root API")
  @Default.String("https://www.googleapis.com/")
  String getApiRootUrl();
  void setApiRootUrl(String value);

  /**
   * The path to write the translated Dataflow specification out to
   * at job submission time.
   */
  @Description("File for writing dataflow job descriptions")
  String getDataflowJobFile();
  void setDataflowJobFile(String value);
}
