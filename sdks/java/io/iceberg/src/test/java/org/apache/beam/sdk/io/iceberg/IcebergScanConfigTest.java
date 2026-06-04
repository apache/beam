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
package org.apache.beam.sdk.io.iceberg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link IcebergScanConfig}. */
public class IcebergScanConfigTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "category", Types.StringType.get()),
              Types.NestedField.required(4, "event_time", Types.TimestampType.withoutZone()),
              Types.NestedField.required(5, "event_micros", Types.LongType.get()),
              Types.NestedField.optional(6, "optional_time", Types.TimestampType.withoutZone()),
              Types.NestedField.required(7, "required_text", Types.StringType.get()),
              Types.NestedField.required(
                  8,
                  "nested",
                  Types.StructType.of(
                      Types.NestedField.required(
                          9, "nested_time", Types.TimestampType.withoutZone())))),
          ImmutableSet.of(1));

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void cdcValidationRequiresIdentifierFields() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    IcebergScanConfig scanConfig =
        scanConfigBuilder(tableId, TestFixtures.SCHEMA).setUseCdc(true).build();

    IllegalStateException thrown =
        assertThrows(IllegalStateException.class, () -> scanConfig.validate(table));
    assertThat(thrown.getMessage(), containsString("Cannot read CDC records"));
    assertThat(thrown.getMessage(), containsString("primary key fields"));
  }

  @Test
  public void cdcValidationRejectsProjectionDroppingIdentifierFields() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);

    IcebergScanConfig keepWithoutPk =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setKeepFields(ImmutableList.of("data"))
            .build();
    IllegalArgumentException keepException =
        assertThrows(IllegalArgumentException.class, () -> keepWithoutPk.validate(table));
    assertThat(
        keepException.getMessage(),
        containsString("projected schema must not drop primary key fields"));

    IcebergScanConfig dropPk =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setDropFields(ImmutableList.of("id"))
            .build();
    IllegalArgumentException dropException =
        assertThrows(IllegalArgumentException.class, () -> dropPk.validate(table));
    assertThat(
        dropException.getMessage(),
        containsString("projected schema must not drop primary key fields"));
  }

  @Test
  public void requiredSchemaIncludesFilterOnlyFieldsWithoutChangingProjection() {
    TableIdentifier tableId = uniqueTableId();
    warehouse.createTable(tableId, CDC_SCHEMA);
    IcebergScanConfig scanConfig =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setKeepFields(ImmutableList.of("id"))
            .setFilterString("\"data\" = 'keep' AND \"category\" = 'include'")
            .build();

    assertEquals(ImmutableList.of("id"), fieldNames(scanConfig.getProjectedSchema()));
    assertEquals(
        ImmutableSet.of("id", "data", "category"),
        ImmutableSet.copyOf(fieldNames(scanConfig.getRequiredSchema())));
  }

  @Test
  public void metadataColumnsRequireCdcMode() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    IcebergScanConfig scanConfig =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setMetadataColumns(ImmutableList.of(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID))
            .build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> scanConfig.validate(table));
    assertThat(thrown.getMessage(), containsString("metadata_columns"));
  }

  @Test
  public void metadataColumnsRejectUnsupportedAndDuplicateNames() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);

    IcebergScanConfig unsupported =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setMetadataColumns(ImmutableList.of("_missing_metadata"))
            .build();
    IllegalArgumentException unsupportedThrown =
        assertThrows(IllegalArgumentException.class, () -> unsupported.validate(table));
    assertThat(unsupportedThrown.getMessage(), containsString("unsupported metadata_columns"));

    IcebergScanConfig duplicate =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setMetadataColumns(
                ImmutableList.of(
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID,
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID))
            .build();
    IllegalArgumentException duplicateThrown =
        assertThrows(IllegalArgumentException.class, () -> duplicate.validate(table));
    assertThat(duplicateThrown.getMessage(), containsString("duplicate"));
  }

  @Test
  public void rowLineageMetadataRequiresFormatV3Table() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);
    IcebergScanConfig scanConfig =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(true)
            .setMetadataColumns(ImmutableList.of(IcebergCdcMetadataColumns.ROW_ID))
            .build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> scanConfig.validate(table));
    assertThat(thrown.getMessage(), containsString("format v3+"));
  }

  @Test
  public void watermarkColumnAcceptsRequiredTimestampAndLongColumns() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);

    scanConfigBuilder(tableId, CDC_SCHEMA)
        .setUseCdc(true)
        .setKeepFields(ImmutableList.of("id", "event_time"))
        .setWatermarkColumn("event_time")
        .build()
        .validate(table);

    scanConfigBuilder(tableId, CDC_SCHEMA)
        .setUseCdc(true)
        .setKeepFields(ImmutableList.of("id", "event_micros"))
        .setWatermarkColumn("event_micros")
        .build()
        .validate(table);
  }

  @Test
  public void watermarkColumnRejectsInvalidConfigurations() {
    TableIdentifier tableId = uniqueTableId();
    Table table = warehouse.createTable(tableId, CDC_SCHEMA);

    assertInvalidWatermark(
        tableId, table, "event_time", false, ImmutableList.of("id", "event_time"), "CDC mode");
    assertInvalidWatermark(
        tableId, table, "missing", true, ImmutableList.of("id"), "unknown column");
    assertInvalidWatermark(
        tableId,
        table,
        "optional_time",
        true,
        ImmutableList.of("id", "optional_time"),
        "non-nullable");
    assertInvalidWatermark(
        tableId,
        table,
        "required_text",
        true,
        ImmutableList.of("id", "required_text"),
        "must be a timestamp-typed column");
    assertInvalidWatermark(
        tableId, table, "event_time", true, ImmutableList.of("id"), "should not be dropped");
  }

  private void assertInvalidWatermark(
      TableIdentifier tableId,
      Table table,
      String watermarkColumn,
      boolean useCdc,
      List<String> keepFields,
      String expectedMessage) {
    IcebergScanConfig scanConfig =
        scanConfigBuilder(tableId, CDC_SCHEMA)
            .setUseCdc(useCdc)
            .setKeepFields(keepFields)
            .setWatermarkColumn(watermarkColumn)
            .build();

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> scanConfig.validate(table));
    assertThat(thrown.getMessage(), containsString(expectedMessage));
  }

  private IcebergScanConfig.Builder scanConfigBuilder(
      TableIdentifier tableId, org.apache.iceberg.Schema schema) {
    return IcebergScanConfig.builder()
        .setCatalogConfig(
            IcebergCatalogConfig.builder()
                .setCatalogName("name")
                .setCatalogProperties(
                    ImmutableMap.of(
                        "type",
                        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
                        "warehouse",
                        warehouse.location))
                .build())
        .setTableIdentifier(tableId)
        .setSchema(IcebergUtils.icebergSchemaToBeamSchema(schema));
  }

  private static List<String> fieldNames(org.apache.iceberg.Schema schema) {
    return schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
  }

  private static TableIdentifier uniqueTableId() {
    return TableIdentifier.of(
        "default", "table_" + Long.toString(UUID.randomUUID().hashCode(), 16));
  }
}
