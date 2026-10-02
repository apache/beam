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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.examples.DeltaLakeToLakehouseCdcExample.LakehouseTableSpec;
import org.apache.beam.examples.DeltaLakeToLakehouseCdcExample.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeltaLakeToLakehouseCdcExample}. */
@RunWith(JUnit4.class)
public class DeltaLakeToLakehouseCdcExampleTest {

  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testVerifyChangeDataFeedEnabledPassesWhenTrueInLatestMetaData() throws IOException {
    File tableDir = tmpFolder.newFolder("delta_table_enabled");
    File deltaLogDir = new File(tableDir, "_delta_log");
    deltaLogDir.mkdirs();

    // Commit 0: CDF not enabled initially
    Files.write(
        new File(deltaLogDir, "00000000000000000000.json").toPath(),
        Arrays.asList(
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"WRITE\"}}",
            "{\"metaData\":{\"id\":\"test-id\",\"configuration\":{}}}"),
        StandardCharsets.UTF_8);

    // Commit 1: ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    Files.write(
        new File(deltaLogDir, "00000000000000000001.json").toPath(),
        Arrays.asList(
            "{\"commitInfo\":{\"timestamp\":1700000001000,\"operation\":\"SET TBLPROPERTIES\"}}",
            "{\"metaData\":{\"id\":\"test-id\",\"configuration\":{\"delta.enableChangeDataFeed\":\"true\"}}}"),
        StandardCharsets.UTF_8);

    // Commit 2: Data commit with no metaData action
    Files.write(
        new File(deltaLogDir, "00000000000000000002.json").toPath(),
        Arrays.asList(
            "{\"commitInfo\":{\"timestamp\":1700000002000,\"operation\":\"WRITE\"}}",
            "{\"add\":{\"path\":\"part-00000.parquet\",\"size\":1024,\"modificationTime\":1700000002000,\"dataChange\":true}}"),
        StandardCharsets.UTF_8);

    DeltaLakeToLakehouseCdcExample.verifyChangeDataFeedEnabled(tableDir.getAbsolutePath());
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenFalse() throws IOException {
    File tableDir = tmpFolder.newFolder("delta_table_disabled");
    File deltaLogDir = new File(tableDir, "_delta_log");
    deltaLogDir.mkdirs();

    Files.write(
        new File(deltaLogDir, "00000000000000000000.json").toPath(),
        Arrays.asList(
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"WRITE\"}}",
            "{\"metaData\":{\"id\":\"test-id\",\"configuration\":{\"delta.enableChangeDataFeed\":\"false\"}}}"),
        StandardCharsets.UTF_8);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                DeltaLakeToLakehouseCdcExample.verifyChangeDataFeedEnabled(
                    tableDir.getAbsolutePath()));
    assertThat(ex.getMessage(), containsString("delta.enableChangeDataFeed = true"));
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenMissingFromConfiguration()
      throws IOException {
    File tableDir = tmpFolder.newFolder("delta_table_missing_prop");
    File deltaLogDir = new File(tableDir, "_delta_log");
    deltaLogDir.mkdirs();

    Files.write(
        new File(deltaLogDir, "00000000000000000000.json").toPath(),
        Arrays.asList(
            "{\"commitInfo\":{\"timestamp\":1700000000000,\"operation\":\"WRITE\"}}",
            "{\"metaData\":{\"id\":\"test-id\",\"configuration\":{}}}"),
        StandardCharsets.UTF_8);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                DeltaLakeToLakehouseCdcExample.verifyChangeDataFeedEnabled(
                    tableDir.getAbsolutePath()));
    assertThat(ex.getMessage(), containsString("delta.enableChangeDataFeed = true"));
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenNoLogsExist() throws IOException {
    File emptyDir = tmpFolder.newFolder("not_a_delta_table");

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaLakeToLakehouseCdcExample.verifyChangeDataFeedEnabled(
                    emptyDir.getAbsolutePath()));
    assertThat(ex.getMessage(), containsString("No Delta Lake transaction log files found"));
  }

  @Test
  public void testResolveLakehouseTableSpecFourPart() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcExample.resolveLakehouseTableSpec(options);
    assertEquals("my-gcp-project", spec.project);
    assertEquals("gs://my-lakehouse-warehouse-bucket", spec.warehouse);
    assertEquals("my_namespace.my_iceberg_table", spec.tableId);
  }

  @Test
  public void testResolveLakehouseTableSpecTwoPartWithExplicitWarehouseAndProject() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setProject("my-gcp-project");
    options.setWarehouse("gs://my-lakehouse-warehouse-bucket");
    options.setLakehouseTable("my_namespace.my_iceberg_table");

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcExample.resolveLakehouseTableSpec(options);
    assertEquals("my-gcp-project", spec.project);
    assertEquals("gs://my-lakehouse-warehouse-bucket", spec.warehouse);
    assertEquals("my_namespace.my_iceberg_table", spec.tableId);
  }

  @Test
  public void testValidateRangeOptionsVersionAndTimestampRanges() {
    Options versionOpts = PipelineOptionsFactory.as(Options.class);
    versionOpts.setStartVersion(1L);
    versionOpts.setEndVersion(3L);
    DeltaLakeToLakehouseCdcExample.validateRangeOptions(versionOpts);

    Options tsOpts = PipelineOptionsFactory.as(Options.class);
    tsOpts.setStartTimestamp("2026-10-01T00:00:00Z");
    tsOpts.setEndTimestamp("2026-10-02T00:00:00Z");
    DeltaLakeToLakehouseCdcExample.validateRangeOptions(tsOpts);

    Options missingOpts = PipelineOptionsFactory.as(Options.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcExample.validateRangeOptions(missingOpts));

    Options mixedOpts = PipelineOptionsFactory.as(Options.class);
    mixedOpts.setStartVersion(1L);
    mixedOpts.setStartTimestamp("2026-10-01T00:00:00Z");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcExample.validateRangeOptions(mixedOpts));

    Options invertedOpts = PipelineOptionsFactory.as(Options.class);
    invertedOpts.setStartVersion(5L);
    invertedOpts.setEndVersion(2L);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcExample.validateRangeOptions(invertedOpts));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildManagedConfigs() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setDeltaTable("gs://my-delta-lake-bucket/delta_lake/employee_data/");
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");
    options.setEqualityColumns("employee_id");
    options.setStartVersion(2L);
    options.setEndVersion(4L);

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcExample.resolveLakehouseTableSpec(options);
    Map<String, Object> readConfig =
        DeltaLakeToLakehouseCdcExample.buildDeltaCdcReadConfig(options, spec.project);
    assertEquals("gs://my-delta-lake-bucket/delta_lake/employee_data/", readConfig.get("table"));
    assertEquals(2L, readConfig.get("start_version"));
    assertEquals(4L, readConfig.get("end_version"));
    assertEquals(
        Arrays.asList("_change_type", "_commit_version"),
        readConfig.get("include_metadata_columns"));
    Map<String, String> hadoopConfig = (Map<String, String>) readConfig.get("hadoop_config");
    assertEquals("my-gcp-project", hadoopConfig.get("fs.gs.project.id"));

    Map<String, Object> writeConfig =
        DeltaLakeToLakehouseCdcExample.buildLakehouseCdcWriteConfig(options, spec);
    assertEquals("my_namespace.my_iceberg_table", writeConfig.get("table"));
    assertEquals("merge-on-read", writeConfig.get("mode"));
    assertEquals("_commit_version", writeConfig.get("sequence_number_column"));
    assertEquals("_change_type", writeConfig.get("change_type_column"));
    assertEquals(Arrays.asList("employee_id"), writeConfig.get("equality_columns"));
    Map<String, String> catalogProps = (Map<String, String>) writeConfig.get("catalog_properties");
    assertEquals("rest", catalogProps.get("type"));
    assertEquals("gs://my-lakehouse-warehouse-bucket", catalogProps.get("warehouse"));
    assertEquals("my-gcp-project", catalogProps.get("header.x-goog-user-project"));
  }

  @Test
  public void testBuildManagedConfigsStreamingJavaRunnerSkipsChangeTypeColumn() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setUsePortableRunner(false);
    options.setDeltaTable("gs://my-delta-lake-bucket/delta_lake/employee_data/");
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");
    options.setEqualityColumns("employee_id");
    options.setStartVersion(2L);
    options.setEndVersion(4L);

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcExample.resolveLakehouseTableSpec(options);
    Map<String, Object> readConfig =
        DeltaLakeToLakehouseCdcExample.buildDeltaCdcReadConfig(options, spec.project);
    assertEquals(
        Collections.singletonList("_commit_version"), readConfig.get("include_metadata_columns"));

    Map<String, Object> writeConfig =
        DeltaLakeToLakehouseCdcExample.buildLakehouseCdcWriteConfig(options, spec);
    assertEquals("_commit_version", writeConfig.get("sequence_number_column"));
    assertFalse(writeConfig.containsKey("change_type_column"));
    assertFalse(writeConfig.containsKey("change_type_map"));
  }
}
