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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
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

/** Class for managing dbt-style profiles */
public class StructuredPipelineProfile {
  // private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineProfile.class);

  String project;
  String target;
  JsonNode runner;
  JsonNode output;
  JsonNode input;
  JsonNode error;

  StructuredPipelineProfile(
      String project,
      String target,
      JsonNode runner,
      JsonNode output,
      @Nullable JsonNode input,
      @Nullable JsonNode error) {
    this.project = project;
    this.target = target;
    this.runner = runner;
    this.output = output;
    this.input = input == null ? output : input;
    this.error = error == null ? output : error;
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

  public Table getErrorTable(String name, PCollection<Row> source) {
    return getTableFromJsonObject(name, error).toBuilder().schema(source.getSchema()).build();
  }

  public static class StructuredProfileSelector {
    ObjectMapper mapper;

    HashMap<String, JsonNode> profiles = new HashMap<>();
    JsonNode config;

    public StructuredProfileSelector(Reader reader) throws Exception {
      mapper =
          new ObjectMapper(new YAMLFactory())
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      JsonNode node = mapper.readTree(reader);
      reader.close();
      for (Iterator<Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
        Entry<String, JsonNode> e = it.next();
        if ("config".equals(e.getKey())) {
          this.config = e.getValue();
        } else {
          profiles.put(e.getKey(), e.getValue());
        }
      }
      if (config == null) {
        config = mapper.createObjectNode();
      }
    }

    public @Nullable StructuredPipelineProfile select(String profile, @Nullable String target)
        throws Exception {
      JsonNode node = profiles.get(profile);
      if (node == null) {
        throw new Exception("No profile found for `" + profile + "`");
      }

      if (target == null || "".equals(target)) {
        if (!node.has("target")) {
          throw new Exception("Profile target not provided.");
        }
        target = node.path("target").asText();
      }

      JsonNode outputs = node.path("outputs").path(target);
      if (outputs.isEmpty()) {
        throw new Exception("Unable resolve target `" + target + "` for profile `" + profile + "`");
      }
      JsonNode runner = node.path("runners").path(target);
      JsonNode inputs = node.path("inputs").path(target);
      JsonNode errors = node.path("errors").path(target);

      return new StructuredPipelineProfile(
          profile,
          target,
          runner,
          outputs,
          inputs.isEmpty() ? null : inputs,
          errors.isEmpty() ? null : errors);
    }
  }

  public static StructuredProfileSelector from(Reader reader) throws Exception {
    return new StructuredProfileSelector(reader);
  }

  public static StructuredProfileSelector from(Path path) throws Exception {
    return from(Files.newBufferedReader(path));
  }

  public static StructuredPipelineProfile from(
      Reader reader, String project, @Nullable String target) throws Exception {
    StructuredPipelineProfile profile = from(reader).select(project, target);
    if (profile == null) {
      throw new Exception("Unable to find profile `" + project + "`");
    }
    return profile;
  }

  public static StructuredPipelineProfile from(Reader reader, String project) throws Exception {
    return from(reader, project, null);
  }

  public static StructuredPipelineProfile from(Path path, String project, @Nullable String target)
      throws Exception {
    StructuredPipelineProfile profile = from(path).select(project, target);
    if (profile == null) {
      throw new Exception("Unable to find profile `" + project + "`");
    }
    return profile;
  }

  public static StructuredPipelineProfile from(Path path, String project) throws Exception {
    return from(path, project, null);
  }
}
