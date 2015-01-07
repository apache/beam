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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Options for controlling Cloud Debugger. These options are experimental and subject to change.
 */
public interface CloudDebuggerOptions {

  /**
   * User defined application version. Cloud Debugger uses it to group all
   * running debuggee processes. Version should be different if users have
   * multiple parallel runs of the same application with different inputs.
   */
  String getCdbgVersion();
  void setCdbgVersion(String value);

  /**
   * Return a JSON string for the Debugger metadata item.
   */
  public static class DebuggerConfig {
    private String version;
    public String getVersion() { return version; }
    public void setVersion(String version) { this.version = version; }

    /**
     * Compute the string of Debugger config.
     * @return JSON string of Debugger config metadata.
     * @throws JsonProcessingException when converting to Json fails.
     */
    public String computeMetadataString() throws JsonProcessingException {
      ObjectMapper mapper = new ObjectMapper();
      String debuggerConfigString = mapper.writeValueAsString(this);
      return debuggerConfigString;
    }
  }
}

