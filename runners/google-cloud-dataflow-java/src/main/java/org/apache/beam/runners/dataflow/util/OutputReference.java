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
package org.apache.beam.runners.dataflow.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A representation used by {@link com.google.api.services.dataflow.model.Step}s to reference the
 * output of other {@code Step}s.
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

  @Override
  public boolean equals(@Nullable Object otherObject) {
    if (!(otherObject instanceof OutputReference)) {
      return false;
    }
    OutputReference other = (OutputReference) otherObject;
    return Objects.equals(type, other.type)
        && Objects.equals(stepName, other.stepName)
        && Objects.equals(outputName, other.outputName)
        && super.equals(other);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, stepName, outputName, super.hashCode());
  }
}
