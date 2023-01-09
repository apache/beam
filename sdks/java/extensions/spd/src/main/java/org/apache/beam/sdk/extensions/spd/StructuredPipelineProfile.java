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

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.Map.Entry;
import org.apache.beam.sdk.extensions.spd.description.Column;
import org.apache.beam.sdk.extensions.spd.description.Source;
import org.apache.beam.sdk.extensions.spd.description.TableDesc;
import org.apache.beam.sdk.extensions.spd.macros.MacroContext;
import org.apache.beam.sdk.extensions.spd.profile.adapter.ProfileAdapter;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for managing dbt-style profiles */
@SuppressWarnings({"nullness"})
public class StructuredPipelineProfile {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineProfile.class);

  String project;
  String target;
  JsonNode runner;
  JsonNode output;
  JsonNode input;
  JsonNode error;

  @Nullable ProfileAdapter inputAdapter = null;

  @Nullable ProfileAdapter outputAdapter = null;

  @Nullable ProfileAdapter errorAdapter = null;

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

    LOG.info("PROFILE OUTPUT");
    LOG.info(this.output.toPrettyString());
    LOG.info("PROFILE INPUT");
    LOG.info(this.input.toPrettyString());
    LOG.info("PROFILE ERROR");
    LOG.info(this.error.toPrettyString());

    String inputType = this.input.path("type").asText("default");
    String outputType = this.output.path("type").asText("default");
    String errorType = this.error.path("type").asText("default");

    ProfileAdapter defaultAdapter = null;
    for (ProfileAdapter adapter : ServiceLoader.load(ProfileAdapter.class)) {
      if ("default".equals(adapter.getName())) {
        defaultAdapter = adapter;
      }
      if (inputType.equals(adapter.getName())) {
        inputAdapter = adapter;
      }
      if (outputType.equals(adapter.getName())) {
        outputAdapter = adapter;
      }
      if (errorType.equals(adapter.getName())) {
        errorAdapter = adapter;
      }
    }

    if (inputAdapter == null) {
      inputAdapter = defaultAdapter;
    }
    if (outputAdapter == null) {
      outputAdapter = defaultAdapter;
    }
    if (errorAdapter == null) {
      errorAdapter = defaultAdapter;
    }
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

  public PipelineOptionsFactory.Builder targetPipelineOptions(
      MacroContext context, Map<String, String> localEnv) {
    LOG.info("" + runner);
    Map<String, ?> binding = ImmutableMap.of("_env", localEnv);
    // This is a little cheesy, but here we are. Convert all the values from the YAML
    // into "arguments" so the pipeline builder can read them.
    ArrayList<String> pipelineArgs = new ArrayList<>();
    pipelineArgs.add("--appName=spd_" + project);
    for (Iterator<Entry<String, JsonNode>> it = runner.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      pipelineArgs.add(
          "--" + e.getKey() + "=" + context.eval(e.getValue().asText(""), binding).getOutput());
    }
    String[] args = pipelineArgs.toArray(new String[pipelineArgs.size()]);
    LOG.info("PipelineOptions: " + String.join(" ", args));
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
    Table.Builder builder =
        inputAdapter
            .getSourceTable(input, getTableFromJsonObject(name, input))
            .toBuilder()
            .schema(Column.asSchema(desc.columns));
    // Override whatever the base table builder may have done with the table description
    if (desc.external != null && !desc.external.isEmpty()) {
      JsonNode location = desc.external.path("location");
      if (!location.isEmpty() && location.isTextual()) {
        builder = builder.location(location.asText());
      }
    }
    return builder.build();
  }

  public Table getOutputTable(String name, PCollection<Row> source) {
    Table.Builder builder =
        outputAdapter
            .getMaterializedTable(output, getTableFromJsonObject(name, output))
            .toBuilder()
            .schema(source.getSchema());
    return builder.build();
  }

  public Table getErrorTable(String name, PCollection<Row> source) {
    return getTableFromJsonObject(name, error).toBuilder().schema(source.getSchema()).build();
  }

  public void preLoadHook(StructuredPipelineDescription spd) {
    inputAdapter.preLoadSourceHook(spd);
    outputAdapter.preLoadMaterializationHook(spd);

    inputAdapter.preLoadHook(spd);
    if (inputAdapter != outputAdapter) {
      outputAdapter.preLoadHook(spd);
    }
  }

  public void preExecuteHook(StructuredPipelineDescription spd) {
    inputAdapter.preExecuteSourceHook(spd);
    outputAdapter.preExecuteMaterializationHook(spd);

    inputAdapter.preExecuteHook(spd);
    if (inputAdapter != outputAdapter) {
      outputAdapter.preExecuteHook(spd);
    }
  }

  public void postExecuteHook(StructuredPipelineDescription spd) {
    inputAdapter.postExecuteSourceHook(spd);
    outputAdapter.postExecuteMaterializationHook(spd);

    inputAdapter.postExecuteHook(spd);
    if (inputAdapter != outputAdapter) {
      outputAdapter.postExecuteHook(spd);
    }
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
      LOG.info("TARGET PROFILE");
      LOG.info("" + node);

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
