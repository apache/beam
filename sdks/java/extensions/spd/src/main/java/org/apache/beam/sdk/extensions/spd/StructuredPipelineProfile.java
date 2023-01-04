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
import org.apache.beam.sdk.extensions.spd.description.Column;
import org.apache.beam.sdk.extensions.spd.description.Source;
import org.apache.beam.sdk.extensions.spd.description.TableDesc;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for managing dbt-style profiles */
public class StructuredPipelineProfile {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineProfile.class);

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

  Table getTableFromJsonObject(String name, JsonNode node) {
    Table.Builder builder =
        Table.builder()
            .name(name)
            .type(node.path("type").asText())
            .schema(Schema.builder().build());
    return builder.build();
  }

  public Table getInputTable(String name, Source source, TableDesc desc) {
    return getTableFromJsonObject(name, input)
        .toBuilder()
        .schema(Column.asSchema(desc.getColumns()))
        .build();
  }

  public Table getOutputTable(String name, PCollection<Row> source) {
    return getTableFromJsonObject(name, output).toBuilder().schema(source.getSchema()).build();
  }

  public static StructuredPipelineProfile from(
      Reader reader, String project, @Nullable String target) throws Exception {
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    JsonNode node = mapper.readTree(reader);
    LOG.info("Profile: " + mapper.writeValueAsString(node));
    node = node.path(project);

    LOG.info("Profile for project: " + mapper.writeValueAsString(node));

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
