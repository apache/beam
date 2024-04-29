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

import static org.apache.beam.sdk.io.iceberg.SchemaAndRowConversions.rowToRecord;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class IcebergIOWriteTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOWriteTest.class);

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testSimpleAppend() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    // Create a table and add records to it.
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setName("hadoop")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();

    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .setRowSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", IcebergIO.writeRows(catalog).to(tableId));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());

    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
  }

  /** Tests that a small write to three different tables with dynamic destinations works. */
  @Test
  public void testDynamicDestinationsWithoutSpillover() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);
    final TableIdentifier table1Id = TableIdentifier.of("default", "table1-" + salt);
    final TableIdentifier table2Id = TableIdentifier.of("default", "table2-" + salt);
    final TableIdentifier table3Id = TableIdentifier.of("default", "table3-" + salt);

    // Create a table and add records to it.
    Table table1 = warehouse.createTable(table1Id, TestFixtures.SCHEMA);
    Table table2 = warehouse.createTable(table2Id, TestFixtures.SCHEMA);
    Table table3 = warehouse.createTable(table3Id, TestFixtures.SCHEMA);

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setName("hadoop")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          private final Schema schema = Schema.builder().addInt64Field("tableNumber").build();

          @Override
          public Schema getMetadataSchema() {
            return schema;
          }

          @Override
          public Row assignDestinationMetadata(Row data) {
            long rowId = data.getInt64("id");
            return Row.withSchema(schema).addValues((rowId / 3) + 1).build();
          }

          @Override
          public IcebergDestination instantiateDestination(Row dest) {
            return IcebergDestination.builder()
                .setTableIdentifier(
                    TableIdentifier.of(
                        "default", "table" + dest.getInt64("tableNumber") + "-" + salt))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }
        };

    testPipeline
        .apply(
            "Records To Add",
            Create.of(
                TestFixtures.asRows(
                    Iterables.concat(
                        TestFixtures.FILE1SNAPSHOT1,
                        TestFixtures.FILE1SNAPSHOT2,
                        TestFixtures.FILE1SNAPSHOT3))))
        .setRowSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", IcebergIO.writeRows(catalog).to(dynamicDestinations));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    List<Record> writtenRecords1 = ImmutableList.copyOf(IcebergGenerics.read(table1).build());
    List<Record> writtenRecords2 = ImmutableList.copyOf(IcebergGenerics.read(table2).build());
    List<Record> writtenRecords3 = ImmutableList.copyOf(IcebergGenerics.read(table3).build());

    assertThat(writtenRecords1, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
    assertThat(writtenRecords2, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT2.toArray()));
    assertThat(writtenRecords3, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT3.toArray()));
  }

  /**
   * Tests writing to enough destinations to spill over to the "slow" write path.
   *
   * <p>Note that we could have added a config to lower the spill number but exceeding the default
   * is fine
   */
  @Test
  public void testDynamicDestinationsWithSpillover() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);

    // Create far more tables than the max writers per bundle
    int numDestinations = 5 * WriteUngroupedRowsToFiles.DEFAULT_MAX_WRITERS_PER_BUNDLE;
    List<TableIdentifier> tableIdentifiers = Lists.newArrayList();
    List<Table> tables = Lists.newArrayList();
    for (int i = 0; i < numDestinations; ++i) {
      TableIdentifier id = TableIdentifier.of("default", "table" + i + "-" + salt);
      tableIdentifiers.add(id);
      tables.add(warehouse.createTable(id, TestFixtures.SCHEMA));
    }

    // Create plenty of data to hit each table
    int numElements = 10 * numDestinations;
    List<Record> elements = Lists.newArrayList();
    final Record genericRecord = GenericRecord.create(TestFixtures.SCHEMA);
    Map<TableIdentifier, List<Record>> elementsPerTable = Maps.newHashMap();
    for (int i = 0; i < numElements; ++i) {
      Record element = genericRecord.copy(ImmutableMap.of("id", (long) i, "data", "data for " + i));
      TableIdentifier tableId = tableIdentifiers.get(i % numDestinations);
      elements.add(element);
      elementsPerTable.computeIfAbsent(tableId, ignored -> Lists.newArrayList()).add(element);
    }

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setName("hadoop")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          private final Schema schema = Schema.builder().addInt64Field("tableNumber").build();

          @Override
          public Schema getMetadataSchema() {
            return schema;
          }

          @Override
          public Row assignDestinationMetadata(Row data) {
            long rowId = data.getInt64("id");
            return Row.withSchema(schema).addValues(rowId % numDestinations).build();
          }

          @Override
          public IcebergDestination instantiateDestination(Row dest) {
            return IcebergDestination.builder()
                .setTableIdentifier(
                    TableIdentifier.of(
                        "default", "table" + dest.getInt64("tableNumber") + "-" + salt))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }
        };

    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(elements)))
        .setRowSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", IcebergIO.writeRows(catalog).to(dynamicDestinations));

    LOG.info("Executing pipeline");
    testPipeline.run().waitUntilFinish();
    LOG.info("Done running pipeline");

    for (int i = 0; i < numDestinations; ++i) {
      TableIdentifier tableId = tableIdentifiers.get(i);
      Table table = tables.get(i);
      List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
      assertThat(
          writtenRecords, Matchers.containsInAnyOrder(elementsPerTable.get(tableId).toArray()));
    }
  }

  /**
   * A test of our assumptions about how two commits of the same file work in iceberg's Java API.
   */
  @Test
  public void testIdempotentCommit() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    // Create a table and add records to it.
    Table table = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    Record record =
        rowToRecord(
            table.schema(),
            Row.withSchema(SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
                .addValues(42L, "bizzle")
                .build());

    OutputFile outputFile = table.io().newOutputFile(TEMPORARY_FOLDER.newFile().toString());
    DataWriter<Record> icebergDataWriter =
        Parquet.writeData(outputFile)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .schema(table.schema())
            .withSpec(table.spec())
            .overwrite()
            .build();

    icebergDataWriter.write(record);
    icebergDataWriter.close();
    DataFile dataFile = icebergDataWriter.toDataFile();

    AppendFiles update = table.newAppend();
    update.appendFile(dataFile);
    update.commit();

    AppendFiles secondUpdate = table.newAppend();
    secondUpdate.appendFile(dataFile);
    secondUpdate.commit();
  }
}
