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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample.Options;
import org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcUtils.LakehouseTableSpec;
import org.apache.beam.sdk.io.delta.DeltaWriteTestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeltaLakeToLakehouseCdcUtils}. */
@RunWith(JUnit4.class)
public class DeltaLakeToLakehouseCdcUtilsTest {

  private static final String ENGINE_INFO = "DeltaLakeToLakehouseCdcUtilsTest";

  private static final String ENABLE_CDF_PROPERTY = "delta.enableChangeDataFeed";

  private static final StructType DELTA_SCHEMA =
      new StructType().add("employee_id", LongType.LONG).add("name", StringType.STRING);

  private static final Schema BEAM_SCHEMA =
      Schema.builder().addInt64Field("employee_id").addStringField("name").build();

  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private final Engine engine = DefaultEngine.create(new Configuration());

  private static Row employee(long id, String name) {
    return Row.withSchema(BEAM_SCHEMA).addValues(id, name).build();
  }

  /**
   * Appends rows to a Delta Lake table as commit {@code version} using {@link DeltaWriteTestUtils}.
   * Version 0 creates the table with Change Data Feed enabled.
   */
  private void appendRows(String tablePath, long version, Row... rows) throws Exception {
    DeltaWriteTestUtils.writeAppendCommit(
        engine, tablePath, version, System.currentTimeMillis(), DELTA_SCHEMA, Arrays.asList(rows));
  }

  /** Creates an empty Delta Lake table (commit version 0) with the given table properties. */
  private String createDeltaTable(Map<String, String> tableProperties) throws IOException {
    String tablePath = tmpFolder.newFolder().getAbsolutePath();
    Table.forPath(engine, tablePath)
        .createTransactionBuilder(engine, ENGINE_INFO, Operation.CREATE_TABLE)
        .withSchema(engine, DELTA_SCHEMA)
        .withTableProperties(engine, tableProperties)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());
    return tablePath;
  }

  /**
   * Commits a new table version that updates the given table properties, like {@code ALTER TABLE
   * ... SET TBLPROPERTIES}.
   */
  private void setTableProperties(String tablePath, Map<String, String> tableProperties) {
    Table.forPath(engine, tablePath)
        .createTransactionBuilder(engine, ENGINE_INFO, Operation.MANUAL_UPDATE)
        .withTableProperties(engine, tableProperties)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());
  }

  /** Runs the Change Data Feed check on a local table, which needs no Hadoop configuration. */
  private static void verifyChangeDataFeedEnabled(String tablePath) {
    DeltaLakeToLakehouseCdcUtils.verifyChangeDataFeedEnabled(tablePath, Collections.emptyMap());
  }

  @Test
  public void testVerifyChangeDataFeedEnabledPassesWhenEnabled() throws Exception {
    String tablePath = tmpFolder.newFolder().getAbsolutePath();
    appendRows(tablePath, 0L, employee(1L, "Alice"), employee(2L, "Bob"));
    appendRows(tablePath, 1L, employee(3L, "Carol"));

    verifyChangeDataFeedEnabled(tablePath);
  }

  @Test
  public void testVerifyChangeDataFeedEnabledPassesWhenEnabledInLatestSnapshot() throws Exception {
    // Version 0: the table is created without Change Data Feed.
    String tablePath = createDeltaTable(Collections.emptyMap());
    // Version 1: ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    setTableProperties(tablePath, ImmutableMap.of(ENABLE_CDF_PROPERTY, "true"));
    // Version 2: a data write, which doesn't change the table metadata.
    appendRows(tablePath, 2L, employee(1L, "Alice"));

    verifyChangeDataFeedEnabled(tablePath);
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenFalse() throws Exception {
    String tablePath = createDeltaTable(ImmutableMap.of(ENABLE_CDF_PROPERTY, "false"));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> verifyChangeDataFeedEnabled(tablePath));
    assertThat(ex.getMessage(), containsString("delta.enableChangeDataFeed = true"));
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenMissingFromTableProperties()
      throws Exception {
    String tablePath = createDeltaTable(Collections.emptyMap());

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> verifyChangeDataFeedEnabled(tablePath));
    assertThat(ex.getMessage(), containsString("delta.enableChangeDataFeed = true"));
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenDisabledInLatestSnapshot() throws Exception {
    // Version 0: the table is created with Change Data Feed enabled.
    String tablePath = tmpFolder.newFolder().getAbsolutePath();
    appendRows(tablePath, 0L, employee(1L, "Alice"));
    // Version 1: ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')
    setTableProperties(tablePath, ImmutableMap.of(ENABLE_CDF_PROPERTY, "false"));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> verifyChangeDataFeedEnabled(tablePath));
    assertThat(ex.getMessage(), containsString("delta.enableChangeDataFeed = true"));
  }

  @Test
  public void testVerifyChangeDataFeedEnabledFailsWhenNotADeltaTable() throws Exception {
    String emptyDir = tmpFolder.newFolder("not_a_delta_table").getAbsolutePath();

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> verifyChangeDataFeedEnabled(emptyDir));
    assertThat(ex.getMessage(), containsString("No Delta Lake table found"));
  }

  @Test
  public void testResolveLakehouseTableSpecFourPart() {
    Options options = PipelineOptionsFactory.as(Options.class);
    options.setLakehouseTable(
        "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table");

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcUtils.resolveLakehouseTableSpec(options);
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

    LakehouseTableSpec spec = DeltaLakeToLakehouseCdcUtils.resolveLakehouseTableSpec(options);
    assertEquals("my-gcp-project", spec.project);
    assertEquals("gs://my-lakehouse-warehouse-bucket", spec.warehouse);
    assertEquals("my_namespace.my_iceberg_table", spec.tableId);
  }

  @Test
  public void testValidateRangeOptionsVersionAndTimestampRanges() {
    Options versionOpts = PipelineOptionsFactory.as(Options.class);
    versionOpts.setStartVersion(1L);
    versionOpts.setEndVersion(3L);
    DeltaLakeToLakehouseCdcUtils.validateRangeOptions(versionOpts);

    Options tsOpts = PipelineOptionsFactory.as(Options.class);
    tsOpts.setStartTimestamp("2026-10-01T00:00:00Z");
    tsOpts.setEndTimestamp("2026-10-02T00:00:00Z");
    DeltaLakeToLakehouseCdcUtils.validateRangeOptions(tsOpts);

    Options missingOpts = PipelineOptionsFactory.as(Options.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcUtils.validateRangeOptions(missingOpts));

    Options mixedOpts = PipelineOptionsFactory.as(Options.class);
    mixedOpts.setStartVersion(1L);
    mixedOpts.setStartTimestamp("2026-10-01T00:00:00Z");
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcUtils.validateRangeOptions(mixedOpts));

    Options invertedOpts = PipelineOptionsFactory.as(Options.class);
    invertedOpts.setStartVersion(5L);
    invertedOpts.setEndVersion(2L);
    assertThrows(
        IllegalArgumentException.class,
        () -> DeltaLakeToLakehouseCdcUtils.validateRangeOptions(invertedOpts));
  }
}
