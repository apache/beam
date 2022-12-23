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
package org.apache.beam.sdk.extensions.spd.description;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.Nullable;

public class Profile {
  String target = ""; // Default environment

  public String getTarget() {
    return target;
  }

  public void setTarget(String target) {
    this.target = target;
  }

  @Nullable ObjectNode inputs;

  public @Nullable ObjectNode getInputs() {
    return inputs == null ? outputs : inputs;
  }

  public void setInputs(ObjectNode inputs) {
    this.inputs = inputs;
  }

  @Nullable ObjectNode outputs;

  public @Nullable ObjectNode getOutputs() {
    return outputs;
  }

  public void setOutputs(ObjectNode outputs) {
    this.outputs = outputs;
  }
}
