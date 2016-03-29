/*
 * Copyright (C) 2016 Google Inc.
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
 * Properties that can be set when using Pubsub with the Dataflow SDK.
 */
@Description("Options that are used to configure BigQuery. See "
    + "https://cloud.google.com/bigquery/what-is-bigquery for details on BigQuery.")
public interface PubsubOptions extends ApplicationNameOptions, GcpOptions,
    PipelineOptions, StreamingOptions {

  /**
   * Root URL for use with the Pubsub API.
   */
  @Description("Root URL for use with the Pubsub API")
  @Default.String("https://pubsub.googleapis.com")
  @Hidden
  String getPubsubRootUrl();
  void setPubsubRootUrl(String value);
}
