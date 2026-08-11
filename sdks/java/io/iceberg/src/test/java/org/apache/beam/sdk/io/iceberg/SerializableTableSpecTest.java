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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SerializableTableSpecTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Catalog catalog;
  private String warehouseLocation;

  private static final Schema COMPLEX_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "timestamp_val", Types.TimestampType.withZone()),
          optional(4, "amount", Types.DecimalType.of(10, 2)),
          optional(
              5,
              "nested_struct",
              Types.StructType.of(
                  required(6, "nested_id", Types.IntegerType.get()),
                  optional(7, "nested_desc", Types.StringType.get()))),
          optional(8, "string_list", Types.ListType.ofOptional(9, Types.StringType.get())),
          optional(
              10,
              "str_int_map",
              Types.MapType.ofOptional(11, 12, Types.StringType.get(), Types.IntegerType.get())));

  @Before
  public void setUp() throws Exception {
    warehouseLocation = "file:" + tempFolder.newFolder().getAbsolutePath();
    catalog =
        CatalogUtil.loadCatalog(
            CatalogUtil.ICEBERG_CATALOG_HADOOP,
            "hadoop",
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation),
            new Configuration());
  }

  @Test
  public void testFromTableAndGettersUnpartitioned() {
    TableIdentifier tableId = TableIdentifier.of("default", "unpartitioned_table");
    Table table = catalog.createTable(tableId, TestFixtures.SCHEMA);

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, table);

    assertEquals(IcebergUtils.tableIdentifierToString(tableId), spec.getTableIdentifierString());
    assertEquals(table.name(), spec.getName());
    assertEquals(table.location(), spec.getLocation());
    assertEquals(table.spec().specId(), spec.getSpecId());
    assertEquals(table.schema().asStruct(), spec.getSchema().asStruct());
    assertEquals(table.spec(), spec.getPartitionSpec());
    assertTrue(spec.getPartitionSpec().isUnpartitioned());
    assertEquals(table.sortOrder(), spec.getSortOrder());
    assertEquals(tableId, spec.getTableIdentifier());
  }

  @Test
  public void testFromTableAndGettersPartitionedWithSortOrder() {
    TableIdentifier tableId = TableIdentifier.of("default", "partitioned_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(COMPLEX_SCHEMA).day("timestamp_val").identity("name").build();
    SortOrder sortOrder =
        SortOrder.builderFor(COMPLEX_SCHEMA)
            .sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST)
            .sortBy("name", SortDirection.DESC, NullOrder.NULLS_LAST)
            .build();
    Map<String, String> properties =
        ImmutableMap.of("write.format.default", "parquet", "custom.property", "test-val");

    Table table =
        catalog
            .buildTable(tableId, COMPLEX_SCHEMA)
            .withPartitionSpec(partitionSpec)
            .withSortOrder(sortOrder)
            .withProperties(properties)
            .create();

    SerializableTableSpec spec = SerializableTableSpec.fromTable(table);

    assertEquals(table.name(), spec.getTableIdentifierString());
    assertEquals(table.name(), spec.getName());
    assertEquals(table.location(), spec.getLocation());
    assertEquals(partitionSpec.specId(), spec.getSpecId());
    assertEquals(table.schema().asStruct(), spec.getSchema().asStruct());
    assertEquals(partitionSpec, spec.getPartitionSpec());
    assertEquals(sortOrder, spec.getSortOrder());
    assertEquals(
        properties.get("write.format.default"), spec.getProperties().get("write.format.default"));
    assertEquals(properties.get("custom.property"), spec.getProperties().get("custom.property"));
  }

  @Test
  public void testDottedNestedNamespaceIdentifier() {
    TableIdentifier tableId = TableIdentifier.of("my", "nested", "catalog", "deep_table");
    Table table = catalog.createTable(tableId, TestFixtures.SCHEMA);

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, table);

    assertEquals("my.nested.catalog.deep_table", spec.getTableIdentifierString());
    assertEquals(tableId, spec.getTableIdentifier());
  }

  @Test
  public void testBuilderAndToBuilderWithEmptyProperties() {
    TableIdentifier tableId = TableIdentifier.of("default", "empty_prop_table");
    Table table = catalog.createTable(tableId, TestFixtures.SCHEMA);

    SerializableTableSpec spec =
        SerializableTableSpec.fromTable(tableId, table)
            .toBuilder()
            .setProperties(Collections.emptyMap())
            .build();

    assertTrue(spec.getProperties().isEmpty());
    assertEquals(tableId, spec.getTableIdentifier());
  }

  @Test
  public void testJavaSerializationRoundtrip() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "ser_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(COMPLEX_SCHEMA).bucket("name", 16).build();
    Table table =
        catalog.buildTable(tableId, COMPLEX_SCHEMA).withPartitionSpec(partitionSpec).create();

    SerializableTableSpec original = SerializableTableSpec.fromTable(tableId, table);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(original);
    }

    SerializableTableSpec deserialized;
    try (ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      deserialized = (SerializableTableSpec) ois.readObject();
    }

    assertNotNull(deserialized);
    assertEquals(original, deserialized);
    assertEquals(original.getTableIdentifierString(), deserialized.getTableIdentifierString());
    assertEquals(original.getSchema().asStruct(), deserialized.getSchema().asStruct());
    assertEquals(original.getPartitionSpec(), deserialized.getPartitionSpec());
    assertEquals(original.getSortOrder(), deserialized.getSortOrder());
  }

  @Test
  public void testBeamSchemaCoderRoundtrip() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "coder_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(COMPLEX_SCHEMA).hour("timestamp_val").build();
    SortOrder sortOrder =
        SortOrder.builderFor(COMPLEX_SCHEMA)
            .sortBy("id", SortDirection.DESC, NullOrder.NULLS_LAST)
            .build();
    Table table =
        catalog
            .buildTable(tableId, COMPLEX_SCHEMA)
            .withPartitionSpec(partitionSpec)
            .withSortOrder(sortOrder)
            .withProperties(ImmutableMap.of("k1", "v1"))
            .create();

    SerializableTableSpec original = SerializableTableSpec.fromTable(tableId, table);
    SchemaCoder<SerializableTableSpec> coder = SerializableTableSpec.getCoder();

    SerializableTableSpec decoded = CoderUtils.clone(coder, original);

    assertNotNull(decoded);
    assertEquals(original, decoded);
    assertEquals(original.getTableIdentifierString(), decoded.getTableIdentifierString());
    assertEquals(original.getName(), decoded.getName());
    assertEquals(original.getLocation(), decoded.getLocation());
    assertEquals(original.getSpecId(), decoded.getSpecId());
    assertEquals(original.getSchema().asStruct(), decoded.getSchema().asStruct());
    assertEquals(original.getPartitionSpec(), decoded.getPartitionSpec());
    assertEquals(original.getSortOrder(), decoded.getSortOrder());
    assertEquals(original.getProperties(), decoded.getProperties());
  }

  @Test
  @SuppressWarnings("ReferenceEquality")
  public void testConcurrentGetterInitializationThreadSafety() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "concurrent_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(COMPLEX_SCHEMA).bucket("name", 8).build();
    Table table =
        catalog.buildTable(tableId, COMPLEX_SCHEMA).withPartitionSpec(partitionSpec).create();

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, table);

    int numThreads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Void>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < numThreads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  startLatch.await();
                  Schema schema = spec.getSchema();
                  PartitionSpec ps = spec.getPartitionSpec();
                  SortOrder so = spec.getSortOrder();
                  TableIdentifier ti = spec.getTableIdentifier();

                  if (schema == null || ps == null || so == null || ti == null) {
                    throw new IllegalStateException("Getter returned null");
                  }
                  if (schema != spec.getSchema() || ps != spec.getPartitionSpec()) {
                    throw new IllegalStateException("Getter returned non-identical instance");
                  }
                  return null;
                }));
      }

      startLatch.countDown();
      for (Future<Void> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdown();
    }
  }
}
