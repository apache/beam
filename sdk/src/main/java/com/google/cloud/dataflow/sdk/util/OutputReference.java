/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;

/**
 * A representation used by {@link com.google.api.services.dataflow.model.Step}s
 * to reference the output of other {@code Step}s.
 */
public final class OutputReference extends GenericJson {
  @Key("@type")
  public final String type = "OutputReference";

  @Key("step_name")
  private final String stepName;

  @Key("output_name")
  private final String outputName;

  public OutputReference(String stepName, String outputName) {
    this.stepName = checkNotNull(stepName);
    this.outputName = checkNotNull(outputName);
  }
}
