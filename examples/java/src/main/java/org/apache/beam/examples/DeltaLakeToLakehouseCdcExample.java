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
package org.apache.beam.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Apache Beam Java example pipeline that reads Change Data Feed (CDC) records from a Delta Lake
 * table on Google Cloud Storage (GCS) and applies those row-level changes (`INSERT`,
 * `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`) to a Google Cloud Platform (GCP) Lakehouse (BigLake
 * Metastore Iceberg REST Catalog) table using Beam's {@link Managed} I/O connectors.
 *
 * <h2>Overview</h2>
 *
 * <p>This pipeline performs the following steps:
 *
 * <ol>
 *   <li><b>Validates Delta Lake Change Data Feed (CDF):</b> Inspects the input Delta Lake table's
 *       transaction log ({@code _delta_log/*.json}) to verify that the table property {@code
 *       delta.enableChangeDataFeed = true} is enabled, failing fast with an error if it is not.
 *   <li><b>Reads Delta Lake CDC Data:</b> Uses {@code Managed.read(Managed.DELTA_LAKE_CDC)} to read
 *       change events over either a commit version range ({@code --startVersion} / {@code
 *       --endVersion}) or an ISO-8601 timestamp range ({@code --startTimestamp} / {@code
 *       --endTimestamp}). Standard GCS Hadoop filesystem properties and Delta CDC metadata columns
 *       are configured by default.
 *   <li><b>Writes CDC Data to GCP Lakehouse:</b> Uses {@code Managed.write(Managed.ICEBERG)} in
 *       {@code merge-on-read} mode against the BigLake Iceberg REST catalog to apply inserts,
 *       updates, and deletes ordered by {@code _commit_version}. Standard BigLake REST catalog and
 *       Iceberg CDC sink properties are configured by default.
 * </ol>
 *
 * <h2>Prerequisites</h2>
 *
 * <ul>
 *   <li><b>Java 17+:</b> Both the Delta Lake Kernel API and Iceberg require Java 17 or later. If
 *       using SDKMAN, switch to Java 17 before running:
 *       <pre>{@code
 * sdk use java 17.0.15-tem
 * }</pre>
 *   <li><b>Source Delta Lake Table on GCS:</b> Must have Change Data Feed enabled:
 *       <pre>{@code
 * ALTER TABLE delta.`gs://my-bucket/path/to/delta_table`
 * SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
 * }</pre>
 *   <li><b>Target GCP Lakehouse (Iceberg V2) Table:</b> Must be an Iceberg format-version 2 table
 *       registered in BigLake Metastore REST Catalog. Its schema must match the source Delta Lake
 *       table's columns. If the Iceberg table does not declare identifier (primary-key) fields in
 *       its table schema, pass {@code --equalityColumns=<col1,col2>} to specify the primary-key
 *       columns used for equality deletes.
 *   <li><b>GCP Authentication:</b> Set {@code GOOGLE_APPLICATION_CREDENTIALS} to a service account
 *       key JSON file (or configure Application Default Credentials) with permissions to access the
 *       GCS buckets, BigLake Metastore ({@code roles/biglake.admin}), and Dataflow.
 * </ul>
 *
 * <h2>Running the Example on Dataflow Runner v2</h2>
 *
 * <p>By default ({@code --useRunnerV2=true}), the pipeline runs on Dataflow Runner v2 ({@code
 * --experiments=use_runner_v2}). Because Dataflow Runner v2 does not yet propagate an element's
 * native Beam {@code ValueKind} metadata across the FnAPI boundary, the source includes {@code
 * _change_type} in {@code include_metadata_columns} and the Iceberg CDC sink is configured with
 * {@code change_type_column: "_change_type"} and {@code change_type_map}.
 *
 * <h3>1. Reading by Commit Version Range</h3>
 *
 * <pre>{@code
 * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
 *
 * ./gradlew :examples:java:execute \
 *   -PmainClass=org.apache.beam.examples.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --project=apache-beam-testing \
 *     --region=us-central1 \
 *     --tempLocation=gs://apache-beam-testing-chamikara/temp \
 *     --deltaTable=gs://apache-beam-testing-delta-lake/delta_lake/demo_employee_data/ \
 *     --lakehouseTable=apache-beam-testing.apache-beam-testing-chamikara.delta_lake_test.delta_lake_to_lakehouse_cdc \
 *     --equalityColumns=employee_id \
 *     --startVersion=2 \
 *     --endVersion=4"
 * }</pre>
 *
 * <h3>2. Reading by Timestamp Range</h3>
 *
 * <pre>{@code
 * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
 *
 * ./gradlew :examples:java:execute \
 *   -PmainClass=org.apache.beam.examples.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --project=apache-beam-testing \
 *     --region=us-central1 \
 *     --tempLocation=gs://apache-beam-testing-chamikara/temp \
 *     --deltaTable=gs://apache-beam-testing-delta-lake/delta_lake/demo_employee_data/ \
 *     --lakehouseTable=apache-beam-testing.apache-beam-testing-chamikara.delta_lake_test.delta_lake_to_lakehouse_cdc \
 *     --equalityColumns=employee_id \
 *     --startTimestamp=2026-10-01T00:00:00Z \
 *     --endTimestamp=2026-10-02T00:00:00Z"
 * }</pre>
 *
 * <h2>Running the Example on Dataflow Runner v1</h2>
 *
 * <p>When running on <b>Dataflow Runner v1</b> (the legacy worker, without {@code use_runner_v2}),
 * {@code change_type_column} (and {@code change_type_map}) <b>can be skipped</b> on the Iceberg CDC
 * sink, and {@code _change_type} does not need to be included in {@code include_metadata_columns}
 * on the Delta Lake CDC source. The Delta Lake CDC reader ({@code Managed.DELTA_LAKE_CDC})
 * automatically sets each emitted {@link Row}'s native Beam {@code ValueKind} ({@code INSERT},
 * {@code DELETE}, {@code UPDATE_BEFORE}, {@code UPDATE_AFTER}), which Dataflow Runner v1 preserves
 * and passes directly to the Iceberg CDC sink.
 *
 * <p>In custom pipelines running on Dataflow Runner v1, the {@code Managed} configurations only
 * need {@code _commit_version} for ordering:
 *
 * <pre>{@code
 * // Delta Lake CDC read config on Dataflow Runner v1 (no _change_type column needed):
 * Map<String, Object> readConfig = ImmutableMap.of(
 *     "table", deltaTable,
 *     "start_version", 2L,
 *     "end_version", 4L,
 *     "include_metadata_columns", ImmutableList.of("_commit_version"),
 *     "hadoop_config", hadoopConfig);
 *
 * // Iceberg CDC write config on Dataflow Runner v1 (change_type_column can be skipped):
 * Map<String, Object> writeConfig = ImmutableMap.of(
 *     "table", tableId,
 *     "catalog_name", "lakehouse",
 *     "catalog_properties", catalogProps,
 *     "mode", "merge-on-read",
 *     "sequence_number_column", "_commit_version",
 *     "equality_columns", ImmutableList.of("employee_id"));
 * }</pre>
 *
 * <p>To run this example on Dataflow Runner v1 (which omits {@code use_runner_v2} and skips {@code
 * change_type_column}), pass {@code --useRunnerV2=false}:
 *
 * <pre>{@code
 * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
 *
 * ./gradlew :examples:java:execute \
 *   -PmainClass=org.apache.beam.examples.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --useRunnerV2=false \
 *     --project=apache-beam-testing \
 *     --region=us-central1 \
 *     --tempLocation=gs://apache-beam-testing-chamikara/temp \
 *     --deltaTable=gs://apache-beam-testing-delta-lake/delta_lake/demo_employee_data/ \
 *     --lakehouseTable=apache-beam-testing.apache-beam-testing-chamikara.delta_lake_test.delta_lake_to_lakehouse_cdc \
 *     --equalityColumns=employee_id \
 *     --startVersion=2 \
 *     --endVersion=4"
 * }</pre>
 */
public class DeltaLakeToLakehouseCdcExample {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaLakeToLakehouseCdcExample.class);

  /** Delta Lake CDC metadata column names produced by {@code Managed.DELTA_LAKE_CDC}. */
  public static final String CHANGE_TYPE_COLUMN = "_change_type";

  public static final String COMMIT_VERSION_COLUMN = "_commit_version";

  /** Default BigLake Iceberg REST Catalog endpoint URI. */
  public static final String DEFAULT_BIGLAKE_CATALOG_URI =
      "https://biglake.googleapis.com/iceberg/v1/restcatalog";

  /**
   * Mapping from Delta Lake Change Data Feed {@code _change_type} values to the canonical change
   * types expected by the Iceberg CDC sink.
   */
  public static final Map<String, String> DELTA_TO_ICEBERG_CHANGE_TYPES =
      ImmutableMap.of(
          "insert", "INSERT",
          "delete", "DELETE",
          "update_preimage", "UPDATE_BEFORE",
          "update_postimage", "UPDATE_AFTER");

  /** Pipeline options for {@link DeltaLakeToLakehouseCdcExample}. */
  public interface Options extends GcpOptions {

    @Description(
        "GCS path of the source Delta Lake table repository to read CDC data from "
            + "(e.g. gs://my-bucket/delta_lake/my_table/).")
    @Validation.Required
    String getDeltaTable();

    void setDeltaTable(String value);

    @Description(
        "Target GCP Lakehouse Iceberg table identifier. Accepts either a 4-part BigLake table "
            + "identifier '<project>.<warehouse_bucket>.<namespace>.<table>' "
            + "(e.g. 'apache-beam-testing.apache-beam-testing-chamikara.delta_lake_test.delta_lake_to_lakehouse_cdc'), "
            + "a 3-part identifier '<warehouse_bucket>.<namespace>.<table>', or a 2-part Iceberg "
            + "table identifier '<namespace>.<table>' (when --warehouse is also specified).")
    @Validation.Required
    String getLakehouseTable();

    void setLakehouseTable(String value);

    @Description(
        "Starting Delta Lake commit version (inclusive) to read changes from. "
            + "Either --startVersion or --startTimestamp must be provided.")
    @Nullable Long getStartVersion();

    void setStartVersion(@Nullable Long value);

    @Description(
        "Ending Delta Lake commit version (inclusive) to read changes up to. Optional; defaults "
            + "to the latest commit version if omitted.")
    @Nullable Long getEndVersion();

    void setEndVersion(@Nullable Long value);

    @Description(
        "Starting timestamp in ISO-8601 format (e.g. '2026-10-01T00:00:00Z') to read Delta Lake "
            + "changes from. Either --startVersion or --startTimestamp must be provided.")
    @Nullable String getStartTimestamp();

    void setStartTimestamp(@Nullable String value);

    @Description(
        "Ending timestamp in ISO-8601 format (e.g. '2026-10-01T23:59:59Z') to read Delta Lake "
            + "changes up to. Optional.")
    @Nullable String getEndTimestamp();

    void setEndTimestamp(@Nullable String value);

    @Description(
        "GCS warehouse location for the GCP Lakehouse BigLake catalog (e.g. 'gs://my-warehouse-bucket'). "
            + "Optional when --lakehouseTable is specified as a 3-part or 4-part identifier containing "
            + "the warehouse bucket name.")
    @Nullable String getWarehouse();

    void setWarehouse(@Nullable String value);

    @Description(
        "Comma-separated list of primary-key (equality-delete) column names that uniquely identify "
            + "rows in the target Lakehouse Iceberg table (e.g. 'employee_id'). Required if the "
            + "target Iceberg table does not declare identifier-field-ids in its schema or if the "
            + "table does not exist yet.")
    @Nullable String getEqualityColumns();

    void setEqualityColumns(@Nullable String value);

    @Description(
        "If true, applies changes in upsert mode (UPDATE_BEFORE records are dropped and "
            + "INSERT/UPDATE_AFTER records are applied as upserts). Defaults to false.")
    @Default.Boolean(false)
    boolean getUpsert();

    void setUpsert(boolean value);

    @Description(
        "If true (default), runs on Dataflow Runner v2 ('use_runner_v2') and configures "
            + "'change_type_column' ('_change_type') on the Iceberg CDC sink. If false, runs on "
            + "Dataflow Runner v1 (legacy worker) and skips 'change_type_column', relying on the "
            + "native Beam ValueKind metadata attached to each Row by the Delta Lake CDC reader.")
    @Default.Boolean(true)
    boolean getUseRunnerV2();

    void setUseRunnerV2(boolean value);

    @Description("Name of the Iceberg catalog instance. Defaults to 'lakehouse'.")
    @Default.String("lakehouse")
    String getCatalogName();

    void setCatalogName(String value);

    @Description(
        "BigLake Iceberg REST Catalog endpoint URI. Defaults to "
            + DEFAULT_BIGLAKE_CATALOG_URI
            + ".")
    @Default.String(DEFAULT_BIGLAKE_CATALOG_URI)
    String getCatalogUri();

    void setCatalogUri(String value);
  }

  /** Parsed GCP Lakehouse table coordinates (`project`, `warehouse`, and `namespace.table`). */
  @VisibleForTesting
  static final class LakehouseTableSpec {
    final String project;
    final String warehouse;
    final String tableId;

    LakehouseTableSpec(String project, String warehouse, String tableId) {
      this.project = project;
      this.warehouse = warehouse;
      this.tableId = tableId;
    }
  }

  /**
   * Resolves the GCP project, GCS warehouse URI, and 2-part Iceberg {@code namespace.table}
   * identifier from {@link Options}.
   */
  @VisibleForTesting
  static LakehouseTableSpec resolveLakehouseTableSpec(Options options) {
    String rawTable = options.getLakehouseTable();
    if (rawTable == null || rawTable.trim().isEmpty()) {
      throw new IllegalArgumentException("--lakehouseTable must not be empty.");
    }
    List<String> parts = Arrays.asList(rawTable.trim().split("\\."));
    @Nullable String project = options.getProject();
    @Nullable String warehouse = options.getWarehouse();
    String tableId;

    if (parts.size() == 4) {
      if (project == null || project.trim().isEmpty()) {
        project = parts.get(0);
      }
      if (warehouse == null || warehouse.trim().isEmpty()) {
        warehouse = "gs://" + parts.get(1);
      }
      tableId = parts.get(2) + "." + parts.get(3);
    } else if (parts.size() == 3) {
      if (warehouse == null || warehouse.trim().isEmpty()) {
        warehouse = "gs://" + parts.get(0);
      }
      tableId = parts.get(1) + "." + parts.get(2);
    } else if (parts.size() == 2) {
      tableId = rawTable.trim();
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Invalid --lakehouseTable '%s'. Expected '<project>.<warehouse_bucket>.<namespace>.<table>', "
                  + "'<warehouse_bucket>.<namespace>.<table>', or '<namespace>.<table>'.",
              rawTable));
    }

    if (project == null || project.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "GCP project must be specified via --project or as the first component of a 4-part "
              + "--lakehouseTable identifier.");
    }
    if (warehouse == null || warehouse.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Lakehouse warehouse bucket must be specified via --warehouse or as part of a 3-part / "
              + "4-part --lakehouseTable identifier.");
    }
    String normalizedWarehouse =
        warehouse.startsWith("gs://") ? warehouse : "gs://" + warehouse.trim();

    return new LakehouseTableSpec(project.trim(), normalizedWarehouse, tableId);
  }

  /**
   * Validates that either a commit version range or a timestamp range (and not a mix of both) is
   * configured on {@link Options}.
   */
  @VisibleForTesting
  static void validateRangeOptions(Options options) {
    @Nullable Long startVersion = options.getStartVersion();
    @Nullable Long endVersion = options.getEndVersion();
    @Nullable String startTimestamp = options.getStartTimestamp();
    @Nullable String endTimestamp = options.getEndTimestamp();

    boolean hasStartVersion = startVersion != null;
    boolean hasEndVersion = endVersion != null;
    boolean hasStartTimestamp = startTimestamp != null && !startTimestamp.trim().isEmpty();
    boolean hasEndTimestamp = endTimestamp != null && !endTimestamp.trim().isEmpty();

    if (!hasStartVersion && !hasStartTimestamp) {
      throw new IllegalArgumentException(
          "Either --startVersion or --startTimestamp must be provided to read Delta Lake CDC data.");
    }
    if (hasStartVersion && hasStartTimestamp) {
      throw new IllegalArgumentException(
          "Cannot set both --startVersion and --startTimestamp; specify either a version range or "
              + "a timestamp range.");
    }
    if (hasEndVersion && hasEndTimestamp) {
      throw new IllegalArgumentException(
          "Cannot set both --endVersion and --endTimestamp; specify either a version range or "
              + "a timestamp range.");
    }
    if (hasStartVersion && hasEndTimestamp) {
      throw new IllegalArgumentException(
          "Cannot mix --startVersion with --endTimestamp; use --endVersion instead.");
    }
    if (hasStartTimestamp && hasEndVersion) {
      throw new IllegalArgumentException(
          "Cannot mix --startTimestamp with --endVersion; use --endTimestamp instead.");
    }
    if (startVersion != null && endVersion != null && startVersion > endVersion) {
      throw new IllegalArgumentException(
          String.format(
              "--startVersion (%d) must be less than or equal to --endVersion (%d).",
              startVersion, endVersion));
    }
  }

  /**
   * Verifies that the input Delta Lake table has {@code delta.enableChangeDataFeed = true} set in
   * its latest transaction log metadata.
   *
   * @throws IllegalArgumentException if no Delta Lake commit logs exist at {@code deltaTablePath}
   * @throws IllegalStateException if {@code delta.enableChangeDataFeed = true} is not enabled
   */
  @VisibleForTesting
  static void verifyChangeDataFeedEnabled(String deltaTablePath) throws IOException {
    String normalizedPath = deltaTablePath.trim().replaceAll("/+$", "");
    String logPattern = normalizedPath + "/_delta_log/*.json";

    MatchResult matchResult = FileSystems.match(logPattern);
    if (matchResult.status() != MatchResult.Status.OK || matchResult.metadata().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No Delta Lake transaction log files found matching '%s'. Verify that '%s' is a "
                  + "valid Delta Lake table.",
              logPattern, deltaTablePath));
    }

    List<MatchResult.Metadata> logFiles = new ArrayList<>(matchResult.metadata());
    logFiles.sort(
        Comparator.comparing((MatchResult.Metadata m) -> m.resourceId().toString()).reversed());

    ObjectMapper mapper = new ObjectMapper();
    for (MatchResult.Metadata metadata : logFiles) {
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(
                  Channels.newInputStream(FileSystems.open(metadata.resourceId())),
                  StandardCharsets.UTF_8))) {
        @Nullable String latestCdfPropertyInFile = null;
        boolean foundMetaDataInFile = false;
        @Nullable String line;
        while ((line = reader.readLine()) != null) {
          if (!line.contains("\"metaData\"")) {
            continue;
          }
          JsonNode root = mapper.readTree(line);
          JsonNode metaDataNode = root.get("metaData");
          if (metaDataNode != null && metaDataNode.isObject()) {
            foundMetaDataInFile = true;
            JsonNode configNode = metaDataNode.get("configuration");
            if (configNode != null && configNode.isObject()) {
              JsonNode cdfNode = configNode.get("delta.enableChangeDataFeed");
              latestCdfPropertyInFile = cdfNode != null ? cdfNode.asText() : null;
            } else {
              latestCdfPropertyInFile = null;
            }
          }
        }
        if (foundMetaDataInFile) {
          if ("true".equalsIgnoreCase(latestCdfPropertyInFile)) {
            LOG.info(
                "Verified 'delta.enableChangeDataFeed = true' on Delta Lake table '{}' (from {}).",
                deltaTablePath,
                metadata.resourceId());
            return;
          }
          throw new IllegalStateException(
              String.format(
                  "Delta Lake table '%s' does not have 'delta.enableChangeDataFeed = true' enabled "
                      + "(found '%s' in %s). Enable Change Data Feed on the table before running "
                      + "this pipeline.",
                  deltaTablePath, latestCdfPropertyInFile, metadata.resourceId()));
        }
      }
    }

    throw new IllegalStateException(
        String.format(
            "Could not find 'metaData' action in Delta Lake commit logs under '%s' to verify "
                + "'delta.enableChangeDataFeed = true'.",
            logPattern));
  }

  /** Builds the {@code Managed.read(Managed.DELTA_LAKE_CDC)} configuration map. */
  @VisibleForTesting
  static Map<String, Object> buildDeltaCdcReadConfig(Options options, String project) {
    Map<String, String> hadoopConfig =
        ImmutableMap.of(
            "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "fs.gs.auth.type", "APPLICATION_DEFAULT",
            "fs.gs.project.id", project);

    Map<String, Object> readConfig = new HashMap<>();
    readConfig.put("table", options.getDeltaTable());
    @Nullable Long startVersion = options.getStartVersion();
    if (startVersion != null) {
      readConfig.put("start_version", startVersion);
    }
    @Nullable Long endVersion = options.getEndVersion();
    if (endVersion != null) {
      readConfig.put("end_version", endVersion);
    }
    @Nullable String startTimestamp = options.getStartTimestamp();
    if (startTimestamp != null && !startTimestamp.trim().isEmpty()) {
      readConfig.put("start_timestamp", startTimestamp.trim());
    }
    @Nullable String endTimestamp = options.getEndTimestamp();
    if (endTimestamp != null && !endTimestamp.trim().isEmpty()) {
      readConfig.put("end_timestamp", endTimestamp.trim());
    }
    readConfig.put(
        "include_metadata_columns",
        options.getUseRunnerV2()
            ? ImmutableList.of(CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN)
            : ImmutableList.of(COMMIT_VERSION_COLUMN));
    readConfig.put("hadoop_config", hadoopConfig);
    return readConfig;
  }

  /** Builds the {@code Managed.write(Managed.ICEBERG)} CDC configuration map for GCP Lakehouse. */
  @VisibleForTesting
  static Map<String, Object> buildLakehouseCdcWriteConfig(
      Options options, LakehouseTableSpec tableSpec) {
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", options.getCatalogUri())
            .put("warehouse", tableSpec.warehouse)
            .put("header.x-goog-user-project", tableSpec.project)
            .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
            .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
            .build();

    Map<String, Object> writeConfig = new HashMap<>();
    writeConfig.put("table", tableSpec.tableId);
    writeConfig.put("catalog_name", options.getCatalogName());
    writeConfig.put("catalog_properties", catalogProps);
    writeConfig.put("mode", "merge-on-read");
    writeConfig.put("sequence_number_column", COMMIT_VERSION_COLUMN);
    if (options.getUseRunnerV2()) {
      writeConfig.put("change_type_column", CHANGE_TYPE_COLUMN);
      writeConfig.put("change_type_map", DELTA_TO_ICEBERG_CHANGE_TYPES);
    }
    writeConfig.put("upsert", options.getUpsert());

    @Nullable String equalityColsStr = options.getEqualityColumns();
    if (equalityColsStr != null && !equalityColsStr.trim().isEmpty()) {
      List<String> equalityCols =
          Splitter.on(',').trimResults().omitEmptyStrings().splitToList(equalityColsStr).stream()
              .collect(Collectors.toList());
      if (!equalityCols.isEmpty()) {
        writeConfig.put("equality_columns", equalityCols);
      }
    }
    return writeConfig;
  }

  public static void runDeltaLakeToLakehouseCdc(Options options) throws IOException {
    validateRangeOptions(options);
    LakehouseTableSpec tableSpec = resolveLakehouseTableSpec(options);
    if (options.getProject() == null || options.getProject().trim().isEmpty()) {
      options.setProject(tableSpec.project);
    }

    // Configure Dataflow Runner v2 by default unless --useRunnerV2=false (Runner v1) is specified.
    if (options.getUseRunnerV2()) {
      ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), "use_runner_v2");
    }

    // Initialize Beam FileSystems with pipeline options and verify Change Data Feed is enabled.
    FileSystems.setDefaultPipelineOptions(options);
    verifyChangeDataFeedEnabled(options.getDeltaTable());

    Map<String, Object> readConfig = buildDeltaCdcReadConfig(options, tableSpec.project);
    Map<String, Object> writeConfig = buildLakehouseCdcWriteConfig(options, tableSpec);

    LOG.info(
        "Starting Delta Lake to GCP Lakehouse CDC pipeline: source='{}', target='{}' (warehouse='{}', project='{}')",
        options.getDeltaTable(),
        tableSpec.tableId,
        tableSpec.warehouse,
        tableSpec.project);

    Pipeline p = Pipeline.create(options);

    PCollection<Row> changes =
        p.apply(
                "ReadDeltaLakeChangeFeed",
                Managed.read(Managed.DELTA_LAKE_CDC).withConfig(readConfig))
            .getSinglePCollection();

    changes.apply(
        "ApplyChangesToLakehouse", Managed.write(Managed.ICEBERG).withConfig(writeConfig));

    PipelineResult.State state = p.run().waitUntilFinish();
    if (state != PipelineResult.State.DONE) {
      throw new IllegalStateException(
          String.format(
              "Delta Lake to Lakehouse CDC pipeline failed with terminal state: %s", state));
    }
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    runDeltaLakeToLakehouseCdc(options);
  }
}
