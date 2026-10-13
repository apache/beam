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
package org.apache.beam.examples.iceberg;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcUtils.LakehouseTableSpec;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
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
 * `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`) to a Google Cloud Platform (GCP) Lakehouse table (via
 * the Lakehouse runtime catalog's Iceberg REST API) using Beam's {@link Managed} I/O connectors.
 *
 * <h2>Overview</h2>
 *
 * <p>This pipeline performs the following steps:
 *
 * <ol>
 *   <li><b>Validates Delta Lake Change Data Feed (CDF):</b> Uses the Delta Kernel API to read the
 *       latest snapshot of the input Delta Lake table and verify that the table property {@code
 *       delta.enableChangeDataFeed = true} is enabled, failing fast with an error if it is not.
 *   <li><b>Reads Delta Lake CDC Data:</b> Uses {@code Managed.read(Managed.DELTA_LAKE_CDC)} to read
 *       change events over either a commit version range ({@code --startVersion} / {@code
 *       --endVersion}) or an ISO-8601 timestamp range ({@code --startTimestamp} / {@code
 *       --endTimestamp}). Standard GCS Hadoop filesystem properties and Delta CDC metadata columns
 *       are configured by default.
 *   <li><b>Writes CDC Data to GCP Lakehouse:</b> Uses {@code Managed.write(Managed.ICEBERG)} in
 *       {@code merge-on-read} mode against the Lakehouse Iceberg REST catalog to apply inserts,
 *       updates, and deletes ordered by {@code _commit_version}. Standard Lakehouse REST catalog
 *       and Iceberg CDC sink properties are configured by default.
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
 *       registered in the Lakehouse runtime catalog. Its schema must match the source Delta Lake
 *       table's columns. If the Iceberg table does not declare identifier (primary-key) fields in
 *       its table schema, pass {@code --equalityColumns=<col1,col2>} to specify the primary-key
 *       columns used for equality deletes.
 *   <li><b>GCP Authentication:</b> Set {@code GOOGLE_APPLICATION_CREDENTIALS} to a service account
 *       key JSON file (or configure Application Default Credentials) with permissions to access the
 *       GCS buckets, Lakehouse runtime catalog ({@code roles/biglake.admin}), and Dataflow.
 * </ul>
 *
 * <h2>Running the Example on Dataflow Portable Runner (Example Default)</h2>
 *
 * <p>This example configures the <b>Dataflow Portable Runner</b> ({@code
 * --experiments=use_runner_v2}) by default ({@code --usePortableRunner=true}). Because the Dataflow
 * Portable Runner does not yet propagate an element's native Beam {@code ValueKind} metadata across
 * the FnAPI boundary, the source includes {@code _change_type} in {@code include_metadata_columns}
 * and the Iceberg CDC sink is configured with {@code change_type_column: "_change_type"} and {@code
 * change_type_map}.
 *
 * <h3>1. Reading by Commit Version Range</h3>
 *
 * <pre>{@code
 * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
 *
 * ./gradlew :examples:java:iceberg:execute \
 *   -PmainClass=org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --project=my-gcp-project \
 *     --region=us-central1 \
 *     --tempLocation=gs://my-temp-bucket/temp \
 *     --deltaTable=gs://my-delta-lake-bucket/delta_lake/employee_data/ \
 *     --lakehouseTable=my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table \
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
 * ./gradlew :examples:java:iceberg:execute \
 *   -PmainClass=org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --project=my-gcp-project \
 *     --region=us-central1 \
 *     --tempLocation=gs://my-temp-bucket/temp \
 *     --deltaTable=gs://my-delta-lake-bucket/delta_lake/employee_data/ \
 *     --lakehouseTable=my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table \
 *     --equalityColumns=employee_id \
 *     --startTimestamp=2026-10-01T00:00:00Z \
 *     --endTimestamp=2026-10-02T00:00:00Z"
 * }</pre>
 *
 * <h2>Running the Example on Dataflow Streaming Java Runner</h2>
 *
 * <p>When running on the <b>Dataflow Streaming Java Runner</b> (which is Dataflow's default runner
 * when {@code --experiments=use_runner_v2} is not enabled), {@code change_type_column} (and {@code
 * change_type_map}) <b>can be skipped</b> on the Iceberg CDC sink, and {@code _change_type} does
 * not need to be included in {@code include_metadata_columns} on the Delta Lake CDC source. The
 * Delta Lake CDC reader ({@code Managed.DELTA_LAKE_CDC}) automatically sets each emitted {@link
 * Row}'s native Beam {@code ValueKind} ({@code INSERT}, {@code DELETE}, {@code UPDATE_BEFORE},
 * {@code UPDATE_AFTER}), which the Dataflow Streaming Java Runner preserves and passes directly to
 * the Iceberg CDC sink.
 *
 * <p>In your own pipelines running on the Dataflow Streaming Java Runner, you do not need to set
 * any runner-version flag (since it is Dataflow's default when {@code use_runner_v2} is not added),
 * and the {@code Managed} configurations only need {@code _commit_version} for ordering:
 *
 * <pre>{@code
 * // Delta Lake CDC read config on Dataflow Streaming Java Runner (no _change_type column needed):
 * Map<String, Object> readConfig = ImmutableMap.of(
 *     "table", deltaTable,
 *     "start_version", 2L,
 *     "end_version", 4L,
 *     "include_metadata_columns", ImmutableList.of("_commit_version"),
 *     "hadoop_config", hadoopConfig);
 *
 * // Iceberg CDC write config on Dataflow Streaming Java Runner (change_type_column can be skipped):
 * Map<String, Object> writeConfig = ImmutableMap.of(
 *     "table", tableId,
 *     "catalog_name", "lakehouse",
 *     "catalog_properties", catalogProps,
 *     "mode", "merge-on-read",
 *     "sequence_number_column", "_commit_version",
 *     "equality_columns", ImmutableList.of("employee_id"));
 * }</pre>
 *
 * <p>Because this example enables the Dataflow Portable Runner by default ({@code
 * --usePortableRunner=true}), you can override it to run on the Dataflow Streaming Java Runner (and
 * skip {@code change_type_column}) by passing {@code --usePortableRunner=false}:
 *
 * <pre>{@code
 * export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
 *
 * ./gradlew :examples:java:iceberg:execute \
 *   -PmainClass=org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample \
 *   -Pexec.args="--runner=DataflowRunner \
 *     --usePortableRunner=false \
 *     --project=my-gcp-project \
 *     --region=us-central1 \
 *     --tempLocation=gs://my-temp-bucket/temp \
 *     --deltaTable=gs://my-delta-lake-bucket/delta_lake/employee_data/ \
 *     --lakehouseTable=my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table \
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

  /** Default Lakehouse Iceberg REST Catalog endpoint URI. */
  public static final String DEFAULT_LAKEHOUSE_CATALOG_URI =
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
        "Target GCP Lakehouse Iceberg table identifier. Accepts either a 4-part Lakehouse table "
            + "identifier '<project>.<warehouse_bucket>.<namespace>.<table>' "
            + "(e.g. 'my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table'), "
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
        "GCS warehouse location for the GCP Lakehouse catalog (e.g. 'gs://my-warehouse-bucket'). "
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
        "If true (default for this example), runs on the Dataflow Portable Runner ('use_runner_v2') "
            + "and configures 'change_type_column' ('_change_type') on the Iceberg CDC sink. "
            + "Set to false to override and run on the Dataflow Streaming Java Runner, which skips "
            + "'change_type_column' and relies on the native Beam ValueKind metadata attached to "
            + "each Row by the Delta Lake CDC reader.")
    @Default.Boolean(true)
    boolean getUsePortableRunner();

    void setUsePortableRunner(boolean value);

    @Description("Name of the Iceberg catalog instance. Defaults to 'lakehouse'.")
    @Default.String("lakehouse")
    String getCatalogName();

    void setCatalogName(String value);

    @Description(
        "Lakehouse Iceberg REST Catalog endpoint URI. Defaults to "
            + DEFAULT_LAKEHOUSE_CATALOG_URI
            + ".")
    @Default.String(DEFAULT_LAKEHOUSE_CATALOG_URI)
    String getCatalogUri();

    void setCatalogUri(String value);
  }

  /**
   * Builds the Hadoop configuration used to access the Delta Lake table on GCS, both by the Delta
   * Lake CDC source and when verifying that Change Data Feed is enabled on the table.
   */
  private static Map<String, String> buildGcsHadoopConfig(String project) {
    return ImmutableMap.of(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        "fs.gs.auth.type", "APPLICATION_DEFAULT",
        "fs.gs.project.id", project);
  }

  /** Builds the {@code Managed.read(Managed.DELTA_LAKE_CDC)} configuration map. */
  @VisibleForTesting
  static Map<String, Object> buildDeltaCdcReadConfig(Options options, String project) {
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
        options.getUsePortableRunner()
            ? ImmutableList.of(CHANGE_TYPE_COLUMN, COMMIT_VERSION_COLUMN)
            : ImmutableList.of(COMMIT_VERSION_COLUMN));
    readConfig.put("hadoop_config", buildGcsHadoopConfig(project));
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
    if (options.getUsePortableRunner()) {
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

  public static void runDeltaLakeToLakehouseCdc(Options options) {
    DeltaLakeToLakehouseCdcUtils.validateRangeOptions(options);
    LakehouseTableSpec tableSpec = DeltaLakeToLakehouseCdcUtils.resolveLakehouseTableSpec(options);
    if (options.getProject() == null || options.getProject().trim().isEmpty()) {
      options.setProject(tableSpec.project);
    }

    // Configure Dataflow Portable Runner by default unless --usePortableRunner=false (Dataflow
    // Streaming Java Runner) is specified.
    if (options.getUsePortableRunner()) {
      ExperimentalOptions.addExperiment(options.as(ExperimentalOptions.class), "use_runner_v2");
    }

    // Verify that Change Data Feed is enabled on the source Delta Lake table.
    DeltaLakeToLakehouseCdcUtils.verifyChangeDataFeedEnabled(
        options.getDeltaTable(), buildGcsHadoopConfig(tableSpec.project));

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

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    runDeltaLakeToLakehouseCdc(options);
  }
}
