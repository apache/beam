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

/**
 * Options that allow setting the application name.
 */
public interface ApplicationNameOptions extends PipelineOptions {
  /**
   * Name of application, for display purposes.
   * <p>
   * Defaults to the name of the class which constructs the
   * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner}.
   */
  @Description("Application name. Defaults to the name of the class which "
      + "constructs the Pipeline.")
  String getAppName();
  void setAppName(String value);
}
