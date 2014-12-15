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
 * [Whitelisting Required] Options used to configure the streaming backend.
 *
 * <p> <b>Important:</b> Streaming support is experimental. It is only supported in the
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner} for users whitelisted in a
 * streaming early access program.
 *
 * <p> You should expect this class to change significantly in future
 * versions of the SDK or be removed entirely.
 */
public interface StreamingOptions extends
    ApplicationNameOptions, GcpOptions, PipelineOptions {
  /**
   * Note that this feature is currently experimental and only available to users whitelisted in
   * a streaming early access program.
   */
  @Description("True if running in streaming mode (experimental)")
  boolean isStreaming();
  void setStreaming(boolean value);
}
