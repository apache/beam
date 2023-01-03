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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

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

  public String getProject() {
    return project;
  }

  public String getTarget() {
    return target;
  }

  public String getTargetRunner() {
    return runner.path("runner").asText("");
  }

  public PipelineOptionsFactory.Builder targetPipelineOptions() {
    // This is a little cheesy, but here we are. Convert all the values from the YAML
    // into "arguments" so the pipeline builder can read them.
    String[] args =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(runner.fields(), Spliterator.ORDERED), false)
            .filter(e -> e.getValue().isTextual() && !"runner".equals(e.getKey()))
            .map(
                e -> {
                  return "--" + e.getKey() + "=" + e.getValue().asText("");
                })
            .collect(Collectors.toList())
            .toArray(new String[0]);
    return PipelineOptionsFactory.fromArgs(args);
  }

  Table getTableFromJsonObject(String name,JsonNode node) {
    Table.Builder builder = Table.builder();
    return builder.build();
  }

  public Table getInputTable(String name) {
    return getTableFromJsonObject(name,input);
  }

  public Table getOutputTable(String name) {
    return getTableFromJsonObject(name,output);
  }

  public static StructuredPipelineProfile from(
      Reader reader, String project, @Nullable String target) throws Exception {
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    JsonNode node = mapper.readTree(reader);
    node = node.path(project);

    // If not specified, use the target in the profile itself
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
