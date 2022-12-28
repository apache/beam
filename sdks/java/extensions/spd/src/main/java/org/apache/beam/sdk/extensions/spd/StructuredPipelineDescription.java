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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hubspot.jinjava.interpret.RenderResult;
import com.hubspot.jinjava.interpret.TemplateError;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.python.transforms.DataframeTransform;
import org.apache.beam.sdk.extensions.spd.description.Column;
import org.apache.beam.sdk.extensions.spd.description.Model;
import org.apache.beam.sdk.extensions.spd.description.Profile;
import org.apache.beam.sdk.extensions.spd.description.Project;
import org.apache.beam.sdk.extensions.spd.description.Schemas;
import org.apache.beam.sdk.extensions.spd.description.Seed;
import org.apache.beam.sdk.extensions.spd.description.Source;
import org.apache.beam.sdk.extensions.spd.description.TableDesc;
import org.apache.beam.sdk.extensions.spd.macros.MacroContext;
import org.apache.beam.sdk.extensions.spd.models.ConfiguredModel;
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
  private Map<String, TableProvider> materializers;

  @Nullable Project project = null;
  @Nullable Profile profile = null;

  public StructuredPipelineDescription(Pipeline pipeline, TableProvider... providers) {
    this.pipeline = pipeline;
    this.tableMap = new PCollectionTableProvider("spd");
    this.modelMap = new HashMap<>();
    this.materializers = new HashMap<>();
    this.metaTableProvider = new InMemoryMetaStore();
    BeamSqlEnvBuilder envBuilder = BeamSqlEnv.builder(this.metaTableProvider);
    envBuilder.autoLoadUserDefinedFunctions();

    HashSet<String> overriddenProviders = new HashSet<>();
    for (TableProvider p : providers) {
      overriddenProviders.add(p.getTableType());
      metaTableProvider.registerProvider(p);
      materializers.put(p.getTableType(), p);
    }

    ServiceLoader.load(TableProvider.class)
        .forEach(
            (provider) -> {
              if (!overriddenProviders.contains(provider.getTableType())) {
                LOG.info("Registering table provider " + provider.getTableType() + ".");
                metaTableProvider.registerProvider(provider);
                materializers.put(provider.getTableType(), provider);
              } else {
                LOG.info("Skipping auto-registration of provider for " + provider.getTableType());
              }
            });
    metaTableProvider.registerProvider(tableMap);
    envBuilder.setQueryPlannerClassName(
        MoreObjects.firstNonNull(
            DEFAULT_PLANNER,
            this.pipeline.getOptions().as(BeamSqlPipelineOptions.class).getPlannerName()));
    envBuilder.setPipelineOptions(this.pipeline.getOptions());
    this.env = envBuilder.build();
  }

  public void materializeTo(String fullTableName, PCollection<Row> source) throws Exception {
    ObjectNode p = profile.getOutputs();
    if (p == null) {
      throw new Exception("No outputs defined in profile for target " + profile.getTarget() + "?");
    }
    String tableType = "" + p.path("type").asText();
    LOG.info("Materizializing " + fullTableName + " to " + tableType);
    TableProvider provider = materializers.get(tableType);
    if (provider == null) {
      throw new Exception("Unable to materialize to a destination of type `" + tableType + "`");
    }
    Table t =
        Table.builder().name(fullTableName).type(tableType).schema(source.getSchema()).build();
    provider.createTable(t);
    provider.buildBeamSqlTable(t).buildIOWriter(source);
  }

  public PCollection<Row> readFrom(String fullTableName, PBegin input) throws Exception {
    Table t = getTable(fullTableName);
    if (t == null) {
      throw new Exception("No table named " + fullTableName);
    }
    return metaTableProvider.buildBeamSqlTable(t).buildIOReader(input);
  }

  public Relation getRelation(String name) throws Exception {
    return new Relation(getTable(name));
  }

  public Relation getSourceRelation(String sourceName, String tableName) throws Exception {
    return new Relation(getTable(sourceName + "_" + tableName));
  }

  public Table getTable(String fullTableName) throws Exception {
    return getTable(null, fullTableName);
  }

  public Table getTable(@Nullable String packageName, String fullTableName) throws Exception {
    // If table already exists, return it
    Table t = null;

    // If a package name has been specified try to get it from a subprovider
    // otherwise get it from the root
    if (packageName != null && !"".equals(packageName)) {
      LOG.info("Searching for " + fullTableName + " in package " + packageName);
      t = metaTableProvider.getSubProvider(packageName).getTable(fullTableName);
    } else {
      t = metaTableProvider.getTable(fullTableName);
    }
    if (t != null) {
      return t;
    }
    MacroContext context = new MacroContext();
    StructuredModel model = modelMap.get(fullTableName);
    if (model == null) {
      throw new Exception("Model " + fullTableName + " doesn't exist.");
    }
    if (model instanceof SqlModel) {
      RenderResult result = context.eval(((SqlModel) model).getRawQuery(), this);
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

      RenderResult inputResult = context.eval(ptm.getInput(), this);
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
      ArrayList<Relation> rel = new ArrayList<>();
      RenderResult result = context.eval(pymodel.getRawPy(), this, rel);
      if (result.hasErrors()) {
        for (TemplateError error : result.getErrors()) {
          throw error.getException();
        }
      }
      if (rel.size() > 0) {
        Relation primary = rel.get(0);
        LOG.info("Python primary relation is "+primary.toString());
        PCollection<Row> pcollection =
            readFrom(primary.getTable().getName(), pipeline.begin())
                .apply(DataframeTransform.of(result.getOutput()));
        metaTableProvider.createTable(tableMap.associatePCollection(fullTableName, pcollection));
      } else {
        throw new Exception("Python model is not associated with an input table");
      }
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

  private void mergeConfiguration(String[] basePath, ObjectNode baseConfig, ConfiguredModel model) {
    // Apply configuration hierarchically from the models structure
    String[] path = Arrays.copyOf(basePath, basePath.length + 1);
    path[path.length - 1] = model.getName();

    if (baseConfig != null) {
      // TODO: Need to convert fields under "config"
      model.mergeConfiguration(baseConfig);
    }

    LOG.info("Attempting to merge configuration for " + String.join(".", path));
    JsonNode config = project.models;
    if (config != null) {
      for (String p : path) {
        model.mergeConfiguration(config);
        config = config.path(p);
      }
      model.mergeConfiguration(config);
    }
  }

  private void readSchemaTables(String[] path, Path schemaPath, ObjectMapper mapper)
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
      for (Source source : schemas.getSources()) {
        String tableType = "" + profile.getInputs().path("type").asText();
        LOG.info("Adding source " + source.name + " tables as " + tableType);
        for (TableDesc desc : source.getTables()) {
          LOG.info("Registering table " + desc.name);
          Schema beamSchema = columnsToSchema(desc.getColumns());
          Table existingTable = metaTableProvider.getTable(source.name + "_" + desc.name);
          if (existingTable != null) {
            if (!existingTable.getSchema().assignableTo(beamSchema)) {
              throw new Exception("Mismatch between source schema and defined schema");
            }
          } else {
            metaTableProvider.createTable(
                Table.builder()
                    .type(tableType)
                    .schema(beamSchema)
                    .name(source.name + "_" + desc.name)
                    .build());
          }
        }
      }

      for (Seed seed : schemas.getSeeds()) {
        Path csvPath = schemaPath.resolve(seed.getName() + ".csv");
        if (Files.exists(csvPath)) {
          seedFiles.remove(csvPath);
          LOG.info("Found CSV at " + csvPath.toAbsolutePath().toString());
          Schema beamSchema = columnsToSchema(seed.getColumns());
          String fullName = seed.getName();
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
        Path sqlPath = schemaPath.resolve(model.getName() + ".sql");
        if (Files.exists(sqlPath)) {
          queryFiles.remove(sqlPath);
          LOG.info("Found SQL at " + sqlPath.toAbsolutePath().toString());
          // Note that models inside of a project live in a flat namespace and can't be
          // duplicated
          if (modelMap.containsKey(model.getName())) {
            throw new Exception("Duplicate model " + model.getName() + " found.");
          }
          SqlModel sqlModel =
              new SqlModel(path, model.getName(), String.join("\n", Files.readAllLines(sqlPath)));
          mergeConfiguration(path, model.getConfig(), sqlModel);
          modelMap.put(model.getName(), sqlModel);
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
        if (file.getFileName().toString().endsWith(".sql")) {
          LOG.info("Found SQL at " + file.toAbsolutePath().toString());
          String name = file.getFileName().toString().replace(".sql", "");
          if (modelMap.containsKey(name)) {
            throw new Exception("Duplicate model " + name + " found.");
          }
          SqlModel sqlModel = new SqlModel(path, name, String.join("\n", Files.readAllLines(file)));
          mergeConfiguration(path, null, sqlModel);
          modelMap.put(name, sqlModel);
          continue;
        }
        if (file.getFileName().toString().endsWith(".py")) {
          LOG.info("Found Python at " + file.toAbsolutePath().toString());
          String name = file.getFileName().toString().replace(".py", "");
          if (modelMap.containsKey(name)) {
            throw new Exception("Duplicate model " + name + "found.");
          }
          PythonModel pyModel =
              new PythonModel(path, name, String.join("\n", Files.readAllLines(file)));
          mergeConfiguration(path, null, pyModel);
          modelMap.put(name, pyModel);
        }
        LOG.info("Unable to determine  model for " + file.toAbsolutePath().toString());
      }
    }

    // Finally handle the subdirectories
    for (Path subdir : directories) {
      String[] newPath = Arrays.copyOf(path, path.length + 1);
      newPath[newPath.length - 1] = subdir.getFileName().toString();
      readSchemaTables(newPath, subdir, mapper);
    }
  }

  private void loadSchemas(Path basePath, Iterable<String> subdirs, ObjectMapper mapper)
      throws Exception {
    for (String schemaDir : subdirs) {
      Path schemaPath = basePath.resolve(schemaDir);
      if (Files.isDirectory(schemaPath)) {
        readSchemaTables(new String[] {}, schemaPath, mapper);
      }
    }
  }

  private void resolveModels() throws Exception {
    MacroContext context = new MacroContext();
    PBegin begin = pipeline.begin();

    int materializeCount = 0;
    for (Entry<String, StructuredModel> model : modelMap.entrySet()) {
      if (model.getValue() instanceof ConfiguredModel) {
        ConfiguredModel m = (ConfiguredModel) model.getValue();
        String materialized = m.getMaterializedConfig(context, this);
        LOG.info("Attempting to materialize " + m.getName() + " as " + materialized);
        if ("table".equals(materialized)) {
          if (!m.getEnabledConfig(context, this)) {
            LOG.info(m.getName() + " has been disabled. No materialization.");
            continue;
          }
          materializeTo(m.getName(), readFrom(m.getName(), begin));
          materializeCount++;
        }
      }
    }
    if (materializeCount == 0) {
      throw new Exception("Project must materialize at least one table to be executable.");
    }
  }

  public void loadProject(Path profilePath, Path path) throws Exception {
    if (!Files.exists(profilePath)) {
      throw new Exception("Profile path not found: " + profilePath);
    }

    ObjectMapper mapper =
        new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    Path projectPath = yamlPath(path, "spd_project");
    if (projectPath == null) {
      throw new Exception("spd_project file not found at path " + path.toString());
    }
    LOG.info("Loading SPD project at " + path);
    project = mapper.readValue(Files.newBufferedReader(projectPath), Project.class);
    if (project.profile == null || "".equals(project.profile)) {
      throw new Exception("Project must define a profile.");
    }
    LOG.info("Loading profile " + project.profile + " from " + profilePath);
    JsonNode profileNode =
        mapper.readTree(Files.newBufferedReader(profilePath)).findPath(project.profile);
    if (profileNode == null || profileNode.isEmpty()) {
      throw new Exception("Profile " + project.profile + " not found.");
    }
    String target = profileNode.get("target").asText();
    if (target == null || "".equals(target)) {
      throw new Exception("No target specified for profile " + project.profile);
    }

    profile = new Profile();
    profile.setTarget(target);

    JsonNode outputs = profileNode.path("outputs").path(target);
    if (outputs == null || outputs.isEmpty() || !outputs.isObject()) {
      throw new Exception(
          "Could not find output configuration for " + target + "in profile " + project.profile);
    }
    profile.setOutputs((ObjectNode) outputs);
    JsonNode inputs = profileNode.path("inputs").path(target);
    if (inputs != null && !inputs.isEmpty() && inputs.isObject()) {
      profile.setInputs((ObjectNode) inputs);
    } else {
      LOG.info("No inputs found. Sources will be read from output configuration.");
      profile.setInputs(profile.getOutputs());
    }

    loadSchemas(path, project.seedPaths, mapper);
    loadSchemas(path, project.modelPaths, mapper);
    resolveModels();

    LOG.info("Dumping tables for " + env.toString());
    for (Map.Entry<String, Table> table : metaTableProvider.getTables().entrySet()) {
      LOG.info("Table: " + table.getKey() + ", " + table.getValue().toString());
    }
  }
}
