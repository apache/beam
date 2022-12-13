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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.interpret.TemplateError;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.spd.description.Column;
import org.apache.beam.sdk.extensions.spd.description.Model;
import org.apache.beam.sdk.extensions.spd.description.Project;
import org.apache.beam.sdk.extensions.spd.description.Schemas;
import org.apache.beam.sdk.extensions.spd.description.Seed;
import org.apache.beam.sdk.extensions.spd.models.SqlModel;
import org.apache.beam.sdk.extensions.spd.models.StructuredModel;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv.BeamSqlEnvBuilder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressFBWarnings
public class StructuredPipelineDescription {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineDescription.class);
  private static final String DEFAULT_PLANNER =
      "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner";

  private Pipeline pipeline;
  private InMemoryMetaStore metaTableProvider;
  private BeamSqlEnv env;
  private Map<String, BeamSqlTable> tableMap;
  private Map<String, StructuredModel> modelMap;

  @Nullable Project project = null;

  public StructuredPipelineDescription(Pipeline pipeline) {
    this.pipeline = pipeline;
    this.tableMap = new HashMap<>();
    this.modelMap = new HashMap<>();
    this.metaTableProvider = new InMemoryMetaStore();
    BeamSqlEnvBuilder envBuilder = BeamSqlEnv.builder(this.metaTableProvider);
    envBuilder.autoLoadUserDefinedFunctions();
    ServiceLoader.load(TableProvider.class).forEach(metaTableProvider::registerProvider);
    // envBuilder.setCurrentSchema("spd");
    envBuilder.setQueryPlannerClassName(
        MoreObjects.firstNonNull(
            DEFAULT_PLANNER,
            this.pipeline.getOptions().as(BeamSqlPipelineOptions.class).getPlannerName()));
    envBuilder.setPipelineOptions(this.pipeline.getOptions());
    this.env = envBuilder.build();
  }

  public PCollection<Row> readFrom(String fullTableName, PBegin input) throws Exception {
    Table t = getTable(fullTableName);
    if (t == null) {
      throw new Exception("No table named " + fullTableName);
    }
    return tableMap.get(fullTableName).buildIOReader(input);
  }

  public Table getTable(String fullTableName) throws Exception {
    // If table already exists, return it
    Table t = metaTableProvider.getTable(fullTableName);
    if (t != null) {
      return t;
    }
    // If the table doesn't exist yet try to build it from models
    if (!tableMap.containsKey(fullTableName)) {
      StructuredModel model = modelMap.get(fullTableName);
      if (model == null) {
        throw new Exception("Model " + fullTableName + " doesn't exist.");
      }
      Map<String, Object> me = new HashMap<>();
      me.put("_spd", this);
      if (model instanceof SqlModel) {
        RenderResult result =
            JinjaFunctions.getDefault().renderForResult(((SqlModel) model).getRawQuery(), me);
        if (result.hasErrors()) {
          for (TemplateError error : result.getErrors()) {
            throw error.getException();
          }
        }

        PCollection<Row> pcollection =
            BeamSqlRelUtils.toPCollection(this.pipeline, env.parseQuery(result.getOutput()));
        BeamSqlTable table = new BeamPCollectionTable<>(pcollection);
        tableMap.put(fullTableName, table);
        return Table.builder()
            .name(fullTableName)
            .schema(pcollection.getSchema())
            .type("spd")
            .build();
      }
    }
    return null;
  }

  // Try both yml and yaml
  private Path yamlPath(Path basePath, String filename) {
    Path returnPath = basePath.resolve(filename + ".yml");
    if (Files.exists(returnPath)) {
      return returnPath;
    }
    returnPath = basePath.resolve(filename + ".yaml");
    if (Files.exists(returnPath)) {
      return returnPath;
    }
    return null;
  }

  private Schema columnsToSchema(Iterable<Column> columns) {
    Schema.Builder beamSchema = Schema.builder();
    for (Column c : columns) {
      HashSet<String> tests = new HashSet<>();
      tests.addAll(c.getTests());
      if (c.type != null && c.name != null) {
        Schema.FieldType type = Schema.FieldType.of(Schema.TypeName.valueOf(c.type));
        if (tests.contains("not_null")) {
          beamSchema.addField(c.getName(), type);
        } else {
          beamSchema.addNullableField(c.getName(), type);
        }
      }
    }
    return beamSchema.build();
  }

  // TODO: figure out the real way of doing this
  private String getFullName(Path basePath, Path subdir, String localName) {
    Path relativePath = subdir.toAbsolutePath().relativize(basePath.toAbsolutePath());
    LOG.info("Computing schema path for " + relativePath.toString());
    ArrayList<String> parts = new ArrayList<>();
    for (Path p : relativePath) {
      String part = p.toString().trim();
      if (!"..".equals(part) && !"".equals(part)) {
        parts.add(part);
      }
    }
    parts.add(localName);
    return String.join(".", parts);
  }

  private void readSchemaTables(Path basePath, Path schemaPath, ObjectMapper mapper)
      throws Exception {
    // TODO: Break the dependence between the YAML and a file to allow file configurations
    LOG.info("Reading seed tables in " + schemaPath.toAbsolutePath().toString());
    try (Stream<Path> files = Files.list(schemaPath)) {
      List<Path> fileList = files.collect(Collectors.toList());
      LOG.info("Found " + fileList.size() + " files");
      for (Path file : fileList) {
        String fileName = file.getFileName().toString();
        LOG.info("Examining file " + file.toAbsolutePath().toString());
        if (Files.isDirectory(file)) {
          readSchemaTables(basePath, file, mapper);
        } else if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
          LOG.info("Found schema file at " + file.toString());
          Schemas schemas = mapper.readValue(Files.newBufferedReader(file), Schemas.class);
          for (Seed seed : schemas.getSeeds()) {
            LOG.info("Checking for seed file in " + seed.getName());
            Path csvPath = schemaPath.resolve(seed.getName() + ".csv");
            if (Files.exists(csvPath)) {
              LOG.info("Found CSV at " + csvPath.toAbsolutePath().toString());
              Schema beamSchema = columnsToSchema(seed.getColumns());
              String fullName = getFullName(basePath, schemaPath, seed.getName());
              LOG.info(
                  "Creating csv external table "
                      + fullName
                      + " at "
                      + csvPath.toAbsolutePath().toString());
              metaTableProvider.createTable(
                  Table.builder()
                      .name(fullName)
                      .type("text")
                      .location(csvPath.toAbsolutePath().toString())
                      .schema(beamSchema)
                      .build());
            }
          }
          for (Model model : schemas.getModels()) {
            Path sqlPath = schemaPath.resolve(model.getName() + ".sql");
            String fullName = getFullName(basePath, schemaPath, model.getName());
            if (Files.exists(sqlPath)) {
              LOG.info("Found SQL at " + sqlPath.toAbsolutePath().toString());
              modelMap.put(
                  fullName,
                  new SqlModel(
                      fullName, model.getName(), String.join("\n", Files.readAllLines(sqlPath))));
            } else {
              // TODO: Allow for arbitrary SchemaTransforms as a model
            }
          }
        }
      }
    }
  }

  private void loadSchemas(Path basePath, Iterable<String> subdirs, ObjectMapper mapper)
      throws Exception {
    for (String schemaDir : subdirs) {
      Path schemaPath = basePath.resolve(schemaDir);
      if (Files.isDirectory(schemaPath)) {
        readSchemaTables(schemaPath, schemaPath, mapper);
      }
    }
  }

  private void resolveModels() throws Exception {
    Set<String> models = modelMap.keySet();
    for (String fullTableName : models) {
      LOG.info("Finding table " + fullTableName);
      getTable(fullTableName);
    }
    this.metaTableProvider.registerProvider(new ReadOnlyTableProvider("spd", tableMap));
  }

  public void loadProject(Path path) throws Exception {
    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    Path projectPath = yamlPath(path, "spd_project");
    if (projectPath == null) {
      throw new Exception("spd_project file not found at path " + path.toString());
    }
    LOG.info("Loading SPD project at " + path);
    Project project = mapper.readValue(Files.newBufferedReader(projectPath), Project.class);
    loadSchemas(path, project.seedPaths, mapper);
    loadSchemas(path, project.modelPaths, mapper);
    resolveModels();

    LOG.info("Dumping tables for " + env.toString());
    for (Map.Entry<String, Table> table : metaTableProvider.getTables().entrySet()) {
      LOG.info("Table: " + table.getKey() + ", " + table.getValue().toString());
    }
  }
}
