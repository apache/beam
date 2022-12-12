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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.RenderResult;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.spd.description.*;
import org.apache.beam.sdk.extensions.spd.models.SqlModel;
import org.apache.beam.sdk.extensions.spd.models.StructuredModel;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigquery.BigQueryTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.kafka.KafkaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite.PubsubLiteTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructuredPipelineDescription {
  private static final Logger LOG = LoggerFactory.getLogger(StructuredPipelineDescription.class);

  private ObjectMapper mapper;
  private Path path;
  private Jinjava jinjava;

  private Pipeline pipeline;

  private HashMap<String, StructuredModel> models = new HashMap<>();
  private HashMap<String, BeamSqlTable> modelTables = new HashMap<>();

  private InMemoryMetaStore metaStore;

  @Nullable private Project project = null;

  public @Nullable String getName() {
    return project == null ? "" : project.name;
  }

  public @Nullable String getVersion() {
    return project == null ? "" : project.version;
  }

  public @Nullable Table findTable(String name) {
    Table t = metaStore.getTable(name);

    // table already exists so we're good here.
    if (t != null) {
      return t;
    }

    // Otherwise we need to try to make the table from a model
    StructuredModel found = models.get(name);

    // Table by that name doesn't exist?
    if (found == null) {
      return null;
    }

    if (found instanceof SqlModel) {
      HashMap<String, Object> bindings = new HashMap<>();
      bindings.put("_name", found.getName());
      bindings.put("_path", found.getPath());
      bindings.put("_self", this);
      RenderResult result = jinjava.renderForResult(((SqlModel) found).getRawQuery(), bindings);
      if (result.hasErrors()) {
        return null;
      }

      BeamSqlEnv env =
          BeamSqlEnv.builder(metaStore).setPipelineOptions(pipeline.getOptions()).build();
      BeamSqlTable table =
          new BeamPCollectionTable<>(
              BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery(result.getOutput())));
      modelTables.put(found.getName(), table);
      return Table.builder().name(found.getName()).type("models").schema(table.getSchema()).build();
    }
    return null;
  }

  public static ObjectMapper defaultObjectMapper() {
    return new ObjectMapper(new YAMLFactory());
  }

  public StructuredPipelineDescription(
      Pipeline pipeline, ObjectMapper mapper, Jinjava jinjava, Path path) {
    this.pipeline = pipeline;
    this.mapper = mapper;
    this.jinjava = jinjava;
    this.path = path;
    this.metaStore = new InMemoryMetaStore();

    // TODO: I think we can do what SQLTransform does here?
    this.metaStore.registerProvider(new TextTableProvider());
    this.metaStore.registerProvider(new BigQueryTableProvider());
    this.metaStore.registerProvider(new BigtableTableProvider());
    this.metaStore.registerProvider(new KafkaTableProvider());
    this.metaStore.registerProvider(new PubsubTableProvider());
    this.metaStore.registerProvider(new PubsubLiteTableProvider());
    // Register our internal table name
    this.metaStore.registerProvider(new ReadOnlyTableProvider("models", modelTables));
  }

  public StructuredPipelineDescription(Pipeline pipeline, Path path) {
    this(pipeline, defaultObjectMapper(), JinjaFunctions.getDefault(), path);
  }

  private @Nullable Path yamlPath(Path base, String yamlName) throws Exception {
    Path newPath = base.resolve(yamlName + ".yml");
    if (!Files.exists(newPath)) {
      newPath = base.resolve(yamlName + ".yaml");
    }
    return Files.exists(newPath) ? newPath : null;
  }

  private void initialize() throws Exception {}

  private void importPackages(Path path, Project project) throws Exception {
    Path packagePath = yamlPath(path, "packages");
    if (packagePath == null) {
      return;
    }
    Packages packages = mapper.readValue(Files.newBufferedReader(packagePath), Packages.class);
    for (PackageImport toImport : packages.packages) {
      if (toImport.local != null && !"".equals(toImport.local)) {
        HashMap<String, Object> bindings = new HashMap<>();
        Path localPath = Paths.get(jinjava.render(toImport.local, bindings));
        if (Files.exists(localPath)) {
          loadPackage(localPath, project);
        } else {
          LOG.error("Package not found at '" + localPath.toAbsolutePath().toString() + "'");
        }
      } else if (toImport.git != null && !"".equals(toImport.git)) {
        HashMap<String, Object> bindings = new HashMap<>();
        String tmpDir = jinjava.render(project.packagesInstallPath, bindings);
        LOG.warn(
            "Git imports currently unsupported. Skipping import of '"
                + toImport.git
                + "' to "
                + tmpDir);
      }
    }
  }

  private String computeIdentifier(String prefix, String name) {
    return prefix + ("".equals(prefix) ? "" : "/") + name;
  }

  private void importSchemas(String prefix, Path modelPath, Project project) throws Exception {
    LOG.info("Loading models in " + modelPath.toString() + " into '" + prefix + "'");
    try (Stream<Path> files = Files.list(modelPath)) {
      for (Path file : files.collect(Collectors.toList())) {
        if (Files.isDirectory(file)) {
          Path filename = file.getFileName();
          if (filename != null) {
            importSchemas(computeIdentifier(prefix, filename.toString()), file, project);
          }
        } else if (Files.isReadable(file) && (file.endsWith("yml") || file.endsWith("yaml"))) {

          // Load up a yaml file containing models and sources (all schemas)
          Schemas schemas = mapper.readValue(Files.newBufferedReader(file), Schemas.class);

          // Create tables for our seeds
          for (Seed seed : schemas.seeds) {
            Path csvFile = path.resolve(seed.name + ".csv");
            // Path jsonFile = path.resolve(seed.name + ".json");

            if (Files.exists(csvFile)) {
              Schema beamSchema = columnsToSchema(seed.columns);
              metaStore.createTable(
                  Table.builder()
                      .type("text")
                      .location(csvFile.toAbsolutePath().toString())
                      .schema(beamSchema)
                      .build());
            }
          }

          // TODO: Create tables for our sources so they can be referenced

          // Create model objects for our models
          for (Model model : schemas.models) {

            if (model.name != null) {
              Path sqlFile = path.resolve(model.name + ".sql");
              Path pyFile = path.resolve(model.name + ".py");

              // If there's a sql file of the same name present we must be a SQL transform.
              if (Files.exists(sqlFile)) {
                this.models.put(
                    model.getName(),
                    new SqlModel(
                        prefix, model.getName(), String.join("\n", Files.readAllLines(sqlFile))));
              } else if (Files.exists(pyFile)) {
                // TODO: Handle python transofmrs
              } else {
                // Otherwise we're a "built-in" SchemaTransform (i.e. our definition comes from the
                // user's implementation
                // in an SDK.
              }
            }
          }
        }
      }
    }
  }

  private Schema columnsToSchema(List<Column> columns) {
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

  private void loadModels(Path path, Project project) throws Exception {
    ArrayList<String> allPaths = new ArrayList<>();
    allPaths.addAll(project.seedPaths);
    allPaths.addAll(project.modelPaths);

    for (String modelPathName : allPaths) {
      Path modelsPath = path.resolve(modelPathName);
      if (Files.exists(modelsPath)) {
        importSchemas("", modelsPath, project);
      }
    }
  }

  private Project loadPackage(Path path, Project parent) throws Exception {
    LOG.info("Loading package at path " + path.toString());
    Path projectPath = yamlPath(path, "spd_project");
    if (projectPath == null) {
      throw new Exception("spd_project file not found at path " + path.toString());
    }
    Project project = mapper.readValue(Files.newBufferedReader(projectPath), Project.class);
    importPackages(path, parent);
    loadModels(path, project);

    return project;
  }

  public void load() throws Exception {
    if (project != null) {
      return;
    }
    initialize();

    Project emptyBase = new Project();
    emptyBase.name = "_parent";
    emptyBase.version = "0";
    emptyBase.initializeEmpty(mapper);

    project = loadPackage(path, emptyBase);
  }

  public void compile() throws Exception {
    if (project == null) {
      return;
    }
    // Okay, super cheesy but here we go..
    for (Map.Entry<String, StructuredModel> e : models.entrySet()) {
      if (e.getValue() instanceof SqlModel) {
        HashMap<String, Object> bindings = new HashMap<String, Object>();
        jinjava.render(((SqlModel) e.getValue()).getRawQuery(), bindings);
      }
    }
  }

  public void run() throws Exception {
    load();
    compile();
    pipeline.run();
  }

  @Override
  public String toString() {
    return project == null ? "UNINITIALIZED_PROJECT" : "SPD:" + getName() + ":" + getVersion();
  }
}
