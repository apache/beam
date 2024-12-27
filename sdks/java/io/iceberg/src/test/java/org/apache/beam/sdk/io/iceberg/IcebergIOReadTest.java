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

import static org.apache.beam.sdk.io.iceberg.TestFixtures.createRecord;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class IcebergIOReadTest {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOReadTest.class);

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  static class PrintRow extends DoFn<Row, Row> {

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> output) throws Exception {
      LOG.info("Got row {}", row);
      output.output(row);
    }
  }

  @Test
  public void testSimpleScan() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file2s1.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file3s1.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT1))
        .commit();

    final List<Row> expectedRows =
        Stream.of(
                TestFixtures.FILE1SNAPSHOT1,
                TestFixtures.FILE2SNAPSHOT1,
                TestFixtures.FILE3SNAPSHOT1)
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    PCollection<Row> output =
        testPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA)));

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testIdentityColumnScan() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);

    String identityColumnName = "identity";
    String identityColumnValue = "some-value";
    simpleTable.updateSchema().addColumn(identityColumnName, Types.StringType.get()).commit();
    simpleTable.updateSpec().addField(identityColumnName).commit();

    PartitionSpec spec = simpleTable.spec();
    PartitionKey partitionKey = new PartitionKey(simpleTable.spec(), simpleTable.schema());
    partitionKey.set(0, identityColumnValue);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet",
                TestFixtures.SCHEMA,
                spec,
                partitionKey,
                TestFixtures.FILE1SNAPSHOT1))
        .commit();

    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema());
    final List<Row> expectedRows =
        Stream.of(TestFixtures.FILE1SNAPSHOT1_DATA)
            .flatMap(List::stream)
            .map(
                d ->
                    ImmutableMap.<String, Object>builder()
                        .putAll(d)
                        .put(identityColumnName, identityColumnValue)
                        .build())
            .map(r -> createRecord(simpleTable.schema(), r))
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    PCollection<Row> output =
        testPipeline
            .apply(IcebergIO.readRows(catalogConfig).from(tableId))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema())));

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }
}
