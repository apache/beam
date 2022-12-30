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
package org.apache.beam.sdk.extensions.spd;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Reader;

/** Class for managing dbt-style profiles */
public class StructuredPipelineProfile {

  String project;
  String target;
  JsonNode runner;
  JsonNode output;
  JsonNode input;

  StructuredPipelineProfile(
      String project, String target, JsonNode runner, JsonNode output, @Nullable JsonNode input) {
    this.project = project;
    this.target = target;
    this.runner = runner;
    this.output = output;
    this.input = input == null ? output : input;
  }

  public static StructuredPipelineProfile from(
      Reader reader, String project, @Nullable String target) throws Exception {
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    JsonNode node = mapper.readTree(reader);
    node = node.path(project);

    //If not specified, use the target in the profile itself
    if (target == null || "".equals(target)) {
      target = node.path("target").asText();
    }

    JsonNode outputs = node.path("outputs").path(target);
    if (outputs.isEmpty())
      throw new Exception(
          "Unable to resolve profile for project " + project + " with target " + target);
    JsonNode runner = node.path("runners").path(target);
    JsonNode inputs = node.path("inputs").path(target);

    return new StructuredPipelineProfile(
        project, target, runner, outputs, inputs.isEmpty() ? null : inputs);
  }

  public static StructuredPipelineProfile from(Reader reader, String project) throws Exception {
    return from(reader, project, null);
  }
}
