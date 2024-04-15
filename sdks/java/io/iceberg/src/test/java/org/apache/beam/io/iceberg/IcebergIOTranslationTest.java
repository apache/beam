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
package org.apache.beam.io.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergIOTranslationTest {
  // A mapping from WriteRows transform builder methods to the corresponding schema fields in
  // IcebergIOTranslation.
  static final Map<String, String> WRITE_TRANSFORM_SCHEMA_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("getCatalogConfig", "catalog_config")
          .put("getTableIdentifier", "table_identifier")
          .put("getDynamicDestinations", "dynamic_destinations")
          .build();

  // A mapping from ReadRows transform builder methods to the corresponding schema fields in
  // IcebergIOTranslation.
  static final Map<String, String> READ_TRANSFORM_SCHEMA_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("getCatalogConfig", "catalog_config")
          .put("getTableIdentifier", "table_identifier")
          .build();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void testReCreateWriteTransformFromRowTable() {
    // setting a subset of fields here.
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    IcebergIO.WriteRows writeTransform =
        IcebergIO.writeRows(config).to(TableIdentifier.of("test_namespace", "test_table"));

    IcebergIOTranslation.IcebergIOWriteTranslator translator =
        new IcebergIOTranslation.IcebergIOWriteTranslator();
    Row row = translator.toConfigRow(writeTransform);

    IcebergIO.WriteRows writeTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(writeTransformFromRow.getTableIdentifier());
    assertEquals(
        "test_namespace", writeTransformFromRow.getTableIdentifier().namespace().levels()[0]);
    assertEquals("test_table", writeTransformFromRow.getTableIdentifier().name());
    assertEquals("test_catalog", writeTransformFromRow.getCatalogConfig().getName());
    assertEquals(
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
        writeTransformFromRow.getCatalogConfig().getIcebergCatalogType());
    assertEquals(
        warehouse.location, writeTransformFromRow.getCatalogConfig().getWarehouseLocation());
  }

  @Test
  public void testWriteTransformRowIncludesAllFields() {
    List<String> getMethodNames =
        Arrays.stream(IcebergIO.WriteRows.class.getDeclaredMethods())
            .map(method -> method.getName())
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'IcebergIO.WriteRows' transform. Please update"
              + "'IcebergIOTranslation.IcebergIOWriteTranslator' to track the new method "
              + "and update this test.",
          WRITE_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    WRITE_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "IcebergIOTranslation.IcebergIOWriteTranslator.",
                  IcebergIOTranslation.IcebergIOWriteTranslator.schema
                      .getFieldNames()
                      .contains(fieldName));
            });
  }

  @Test
  public void testReCreateReadTransformFromRowTable() {
    // setting a subset of fields here.
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    IcebergIO.ReadRows readTransform =
        IcebergIO.readRows(config).from(TableIdentifier.of("test_namespace", "test_table"));

    IcebergIOTranslation.IcebergIOReadTranslator translator =
        new IcebergIOTranslation.IcebergIOReadTranslator();
    Row row = translator.toConfigRow(readTransform);

    IcebergIO.ReadRows readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(readTransformFromRow.getTableIdentifier());
    assertEquals(
        "test_namespace", readTransformFromRow.getTableIdentifier().namespace().levels()[0]);
    assertEquals("test_table", readTransformFromRow.getTableIdentifier().name());
    assertEquals("test_catalog", readTransformFromRow.getCatalogConfig().getName());
    assertEquals(
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
        readTransformFromRow.getCatalogConfig().getIcebergCatalogType());
    assertEquals(
        warehouse.location, readTransformFromRow.getCatalogConfig().getWarehouseLocation());
  }

  @Test
  public void testReadTransformRowIncludesAllFields() {
    List<String> getMethodNames =
        Arrays.stream(IcebergIO.ReadRows.class.getDeclaredMethods())
            .map(method -> method.getName())
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'IcebergIO.ReadRows' transform. Please update"
              + "'IcebergIOTranslation.IcebergIOReadTranslator' to track the new method "
              + "and update this test.",
          READ_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    READ_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "IcebergIOTranslation.IcebergIOReadTranslator.",
                  IcebergIOTranslation.IcebergIOReadTranslator.schema
                      .getFieldNames()
                      .contains(fieldName));
            });
  }
}
