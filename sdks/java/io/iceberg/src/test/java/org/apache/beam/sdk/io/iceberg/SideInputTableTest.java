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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SideInputTableTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private Catalog catalog;
  private String warehouseLocation;

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
  public void testConstructorNullChecks() {
    assertThrows(NullPointerException.class, () -> new SideInputTable(null));
    TableIdentifier tableId = TableIdentifier.of("default", "null_check_table");
    Table realTable = catalog.createTable(tableId, TestFixtures.SCHEMA);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    assertThrows(
        NullPointerException.class, () -> new SideInputTable(spec, (Map<String, String>) null));
  }

  @Test
  public void testMetadataDelegation() {
    TableIdentifier tableId = TableIdentifier.of("default", "side_input_test_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(TestFixtures.SCHEMA).identity("data").build();
    SortOrder sortOrder =
        SortOrder.builderFor(TestFixtures.SCHEMA)
            .sortBy("id", SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build();
    Map<String, String> properties =
        ImmutableMap.of("write.format.default", "parquet", "user.key", "user.val");

    Table realTable =
        catalog
            .buildTable(tableId, TestFixtures.SCHEMA)
            .withPartitionSpec(partitionSpec)
            .withSortOrder(sortOrder)
            .withProperties(properties)
            .create();

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    SideInputTable sideInputTable = new SideInputTable(spec, ImmutableMap.of());

    assertEquals(realTable.name(), sideInputTable.name());
    assertEquals(realTable.location(), sideInputTable.location());
    assertEquals(realTable.schema().asStruct(), sideInputTable.schema().asStruct());
    assertEquals(realTable.schemas().keySet(), sideInputTable.schemas().keySet());
    assertEquals(realTable.spec(), sideInputTable.spec());
    assertEquals(realTable.specs().keySet(), sideInputTable.specs().keySet());
    assertEquals(realTable.sortOrder(), sideInputTable.sortOrder());
    assertEquals(realTable.sortOrders().keySet(), sideInputTable.sortOrders().keySet());
    assertEquals(
        realTable.properties().get("user.key"), sideInputTable.properties().get("user.key"));
    assertNotNull(sideInputTable.io());
    assertEquals(realTable.io().getClass().getName(), sideInputTable.io().getClass().getName());
    assertNotNull(sideInputTable.locationProvider());
    assertNotNull(sideInputTable.encryption());
    assertTrue(sideInputTable.encryption() instanceof PlaintextEncryptionManager);
    assertEquals(spec, sideInputTable.getTableSpec());
    assertTrue(sideInputTable.specs().containsKey(spec.getSpecId()));
  }

  @Test
  public void testWritingPartitionedWithRecordWriter() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "partitioned_writer_table");
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(TestFixtures.SCHEMA).identity("data").build();
    Table realTable =
        catalog.buildTable(tableId, TestFixtures.SCHEMA).withPartitionSpec(partitionSpec).create();

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    SideInputTable sideInputTable = new SideInputTable(spec);

    PartitionKey partitionKey = new PartitionKey(sideInputTable.spec(), sideInputTable.schema());
    Record record = GenericRecord.create(sideInputTable.schema());
    record.setField("id", 42L);
    record.setField("data", "test_partition_value");
    partitionKey.partition(record);

    RecordWriter writer =
        new RecordWriter(
            sideInputTable, FileFormat.PARQUET, "test_file_001", partitionKey, ImmutableMap.of());

    writer.write(record);
    writer.close();

    assertNotNull(writer.getDataFile());
    assertNotNull(writer.getDataFile().path());
    assertEquals(1, writer.getDataFile().recordCount());
    assertEquals(FileFormat.PARQUET, writer.getDataFile().format());
  }

  @Test
  public void testWritingUnpartitionedWithRecordWriter() throws Exception {
    TableIdentifier tableId = TableIdentifier.of("default", "unpartitioned_writer_table");
    Table realTable = catalog.createTable(tableId, TestFixtures.SCHEMA);

    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    SideInputTable sideInputTable = new SideInputTable(spec);

    PartitionKey partitionKey = new PartitionKey(sideInputTable.spec(), sideInputTable.schema());
    Record record = GenericRecord.create(sideInputTable.schema());
    record.setField("id", 99L);
    record.setField("data", "unpartitioned_data");
    partitionKey.partition(record);

    RecordWriter writer =
        new RecordWriter(
            sideInputTable,
            FileFormat.PARQUET,
            "test_unpartitioned_file_001",
            partitionKey,
            ImmutableMap.of());

    writer.write(record);
    writer.close();

    assertNotNull(writer.getDataFile());
    assertNotNull(writer.getDataFile().path());
    assertEquals(1, writer.getDataFile().recordCount());
    assertEquals(FileFormat.PARQUET, writer.getDataFile().format());
  }

  @Test
  public void testUnsupportedAndNoOpOperationsThrowExceptions() {
    TableIdentifier tableId = TableIdentifier.of("default", "mutations_test_table");
    Table realTable = catalog.createTable(tableId, TestFixtures.SCHEMA);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    SideInputTable sideInputTable = new SideInputTable(spec);

    // Refresh & Snapshot operations must throw UnsupportedOperationException
    assertThrows(UnsupportedOperationException.class, sideInputTable::refresh);
    assertThrows(UnsupportedOperationException.class, sideInputTable::currentSnapshot);
    assertThrows(UnsupportedOperationException.class, () -> sideInputTable.snapshot(12345L));
    assertThrows(UnsupportedOperationException.class, sideInputTable::snapshots);
    assertThrows(UnsupportedOperationException.class, sideInputTable::history);
    assertThrows(UnsupportedOperationException.class, sideInputTable::refs);
    assertThrows(UnsupportedOperationException.class, sideInputTable::statisticsFiles);
    assertThrows(UnsupportedOperationException.class, sideInputTable::partitionStatisticsFiles);

    // Scans & Mutations
    assertThrows(UnsupportedOperationException.class, sideInputTable::newScan);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newIncrementalAppendScan);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newIncrementalChangelogScan);
    assertThrows(UnsupportedOperationException.class, sideInputTable::updateSchema);
    assertThrows(UnsupportedOperationException.class, sideInputTable::updateSpec);
    assertThrows(UnsupportedOperationException.class, sideInputTable::updateProperties);
    assertThrows(UnsupportedOperationException.class, sideInputTable::replaceSortOrder);
    assertThrows(UnsupportedOperationException.class, sideInputTable::updateLocation);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newAppend);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newFastAppend);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newRewrite);
    assertThrows(UnsupportedOperationException.class, sideInputTable::rewriteManifests);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newOverwrite);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newRowDelta);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newReplacePartitions);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newDelete);
    assertThrows(UnsupportedOperationException.class, sideInputTable::updateStatistics);
    assertThrows(UnsupportedOperationException.class, sideInputTable::expireSnapshots);
    assertThrows(UnsupportedOperationException.class, sideInputTable::manageSnapshots);
    assertThrows(UnsupportedOperationException.class, sideInputTable::newTransaction);
  }

  @Test
  public void testEqualsHashCodeAndToString() {
    TableIdentifier tableId1 = TableIdentifier.of("default", "t1");
    TableIdentifier tableId2 = TableIdentifier.of("default", "t2");
    Table realTable1 = catalog.createTable(tableId1, TestFixtures.SCHEMA);
    Table realTable2 = catalog.createTable(tableId2, TestFixtures.SCHEMA);

    SerializableTableSpec spec1 = SerializableTableSpec.fromTable(tableId1, realTable1);
    SerializableTableSpec spec2 = SerializableTableSpec.fromTable(tableId2, realTable2);

    SideInputTable table1a = new SideInputTable(spec1);
    SideInputTable table1b = new SideInputTable(spec1);
    SideInputTable table2 = new SideInputTable(spec2);

    assertEquals(table1a, table1b);
    assertEquals(table1a.hashCode(), table1b.hashCode());
    assertNotEquals(table1a, table2);
    assertNotNull(table1a.toString());
    assertTrue(table1a.toString().contains("SideInputTable"));
  }
}
