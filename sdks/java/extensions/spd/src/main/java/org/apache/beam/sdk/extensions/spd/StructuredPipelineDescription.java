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
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.extensions.spd.description.Column;
import org.apache.beam.sdk.extensions.spd.description.Model;
import org.apache.beam.sdk.extensions.spd.description.Project;
import org.apache.beam.sdk.extensions.spd.description.Schemas;
import org.apache.beam.sdk.extensions.spd.description.Seed;
import org.apache.beam.sdk.extensions.spd.models.PTransformModel;
import org.apache.beam.sdk.extensions.spd.models.PythonModel;
import org.apache.beam.sdk.extensions.spd.models.SqlModel;
import org.apache.beam.sdk.extensions.spd.models.StructuredModel;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv.BeamSqlEnvBuilder;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.util.PythonCallableSource;
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
  private PCollectionTableProvider tableMap;
  private Map<String, StructuredModel> modelMap;

  @Nullable Project project = null;
  private String target = "";

  public StructuredPipelineDescription(Pipeline pipeline) {
    this.pipeline = pipeline;
    this.tableMap = new PCollectionTableProvider("spd");
    this.modelMap = new HashMap<>();
    this.metaTableProvider = new InMemoryMetaStore();
    BeamSqlEnvBuilder envBuilder = BeamSqlEnv.builder(this.metaTableProvider);
    envBuilder.autoLoadUserDefinedFunctions();
    ServiceLoader.load(TableProvider.class).forEach(metaTableProvider::registerProvider);
    metaTableProvider.registerProvider(tableMap);
    // envBuilder.setCurrentSchema("spd");
    envBuilder.setQueryPlannerClassName(
        MoreObjects.firstNonNull(
            DEFAULT_PLANNER,
            this.pipeline.getOptions().as(BeamSqlPipelineOptions.class).getPlannerName()));
    envBuilder.setPipelineOptions(this.pipeline.getOptions());
    this.env = envBuilder.build();
  }

  public void setTarget(String target) {
    this.target = target;
  }

  public String getTarget() {
    return target;
  }

  public PCollection<Row> readFrom(String fullTableName, PBegin input) throws Exception {
    Table t = getTable(fullTableName);
    if (t == null) {
      throw new Exception("No table named " + fullTableName);
    }
    return tableMap.buildBeamSqlTable(t).buildIOReader(input);
  }

  public Relation getRelation(String name) throws Exception {
    return new Relation(getTable(name));
  }

  public Table getTable(String fullTableName) throws Exception {
    return getTable(null, fullTableName);
  }

  public Table getTable(@Nullable String packageName, String fullTableName) throws Exception {
    // If table already exists, return it
    Table t = null;

    // If a package name has been specified try to get it from a subprovider
    if (packageName != null && !"".equals(packageName)) {
      t = metaTableProvider.getSubProvider(packageName).getTable(fullTableName);
    }
    if (t != null) {
      return t;
    }

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
      metaTableProvider.createTable(tableMap.associatePCollection(fullTableName, pcollection));
    } else if (model instanceof PTransformModel) {
      PTransformModel ptm = (PTransformModel) model;

      RenderResult inputResult = JinjaFunctions.getDefault().renderForResult(ptm.getInput(), me);
      if (inputResult.hasErrors()) {
        for (TemplateError error : inputResult.getErrors()) {
          throw error.getException();
        }
      }
      PCollection<Row> pcollection =
          ptm.applyTo(readFrom(inputResult.getOutput(), pipeline.begin()));
      metaTableProvider.createTable(tableMap.associatePCollection(fullTableName, pcollection));
    } else if (model instanceof PythonModel) {
      PythonModel pymodel = (PythonModel) model;
      RenderResult result = JinjaFunctions.getDefault().renderForResult(pymodel.getRawPy(), me);
      if (result.hasErrors()) {
        for (TemplateError error : result.getErrors()) {
          throw error.getException();
        }
      }
      pipeline
          .begin()
          .apply(
              PythonExternalTransform.from("__callable__")
                  .withArgs(PythonCallableSource.of(result.getOutput())));
    }
    return metaTableProvider.getTable(fullTableName);
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

    // Split into different groups so we can process leftover Python and SQL files
    // when we are done with YAML files
    LOG.info("Reading " + schemaPath.toAbsolutePath().toString());
    List<Path> configFiles = new ArrayList<>();
    Set<Path> queryFiles = new HashSet<>();
    List<Path> directories = new ArrayList<>();
    Set<Path> seedFiles = new HashSet<>();

    try (Stream<Path> files = Files.list(schemaPath)) {
      // Split files into various types
      files.forEach(
          (file) -> {
            String fileName = file.getFileName().toString();
            if (Files.isDirectory(file)) {
              directories.add(file);
            } else if (fileName.endsWith(".yml") || fileName.endsWith(".yaml")) {
              configFiles.add(file);
            } else if (fileName.endsWith(".csv")) {
              seedFiles.add(file);
            } else {
              queryFiles.add(file);
            }
          });
    }

    LOG.info("Processing " + configFiles.size() + " schema files");
    for (Path file : configFiles) {
      LOG.info("Processing schemas in " + file.toString());
      Schemas schemas = mapper.readValue(Files.newBufferedReader(file), Schemas.class);
      for (Seed seed : schemas.getSeeds()) {
        Path csvPath = schemaPath.resolve(seed.getName() + ".csv");
        if (Files.exists(csvPath)) {
          seedFiles.remove(csvPath);
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
        } else {
          LOG.info("Skipping seed " + seed.getName() + ". Unable to find CSV file.");
        }
      }
      for (Model model : schemas.getModels()) {
        String fullName = getFullName(basePath, schemaPath, model.getName());
        Path sqlPath = schemaPath.resolve(model.getName() + ".sql");
        if (Files.exists(sqlPath)) {
          queryFiles.remove(sqlPath);
          LOG.info("Found SQL at " + sqlPath.toAbsolutePath().toString());
          // Note that models inside of a project live in a flat namespace and can't be
          // duplicated
          if (modelMap.containsKey(model.getName())) {
            throw new Exception("Duplicate model " + model.getName() + " found.");
          }
          modelMap.put(
              model.getName(),
              new SqlModel(
                  fullName, model.getName(), String.join("\n", Files.readAllLines(sqlPath))));
          continue;
        }
        Path pyPath = schemaPath.resolve(model.getName() + ".py");
        if (Files.exists(pyPath)) {
          queryFiles.remove(pyPath);
          LOG.info("Found Python at " + pyPath.toAbsolutePath().toString());
          continue;
        }
      }
    }

    // Handle the leftovers
    if (seedFiles.size() > 0) {
      LOG.warn("Unprocessed seed files.");
    }
    if (queryFiles.size() > 0) {
      for (Path file : queryFiles) {
        LOG.info("Loading model " + file.toAbsolutePath().toString());
      }
    }

    // Finally handle the subdirectories
    for (Path path : directories) {
      readSchemaTables(basePath, path, mapper);
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
    project = mapper.readValue(Files.newBufferedReader(projectPath), Project.class);
    loadSchemas(path, project.seedPaths, mapper);
    loadSchemas(path, project.modelPaths, mapper);
    resolveModels();

    LOG.info("Dumping tables for " + env.toString());
    for (Map.Entry<String, Table> table : metaTableProvider.getTables().entrySet()) {
      LOG.info("Table: " + table.getKey() + ", " + table.getValue().toString());
    }
  }

  public void applyProfiles(Path path) throws Exception {

    // Load up ye olde schema transforms.
    ServiceLoader<SchemaTransformProvider> loader =
        ServiceLoader.load(SchemaTransformProvider.class);
    loader.forEach(
        (provider) -> {
          LOG.info("Found provider " + provider.identifier());
        });

    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    if (!Files.exists(path)) {
      throw new Exception("Profiles not found.");
    }
    if (project == null || "".equals(project.profile) || project.profile == null) {
      throw new Exception("Project must be initialized with a profile.");
    }
    JsonNode allProfiles = mapper.readTree(Files.newBufferedReader(path));
    JsonNode profile = allProfiles.findPath(project.profile);
    // If target isn't set, try to set it from the profile object
    if (target == null || "".equals(target)) {
      this.target = profile.findPath("target").asText("");
    }
    if ("".equals(this.target)) {
      throw new Exception("Unable to apply profile as target is not set properly.");
    } else {
      LOG.info("Applying `" + target + "` profile target.");
    }
    JsonNode providers = profile.findPath("outputs").findPath(target);
    if (providers.isEmpty()) {
      throw new Exception("Unable to locate outputs ");
    }
  }
}
