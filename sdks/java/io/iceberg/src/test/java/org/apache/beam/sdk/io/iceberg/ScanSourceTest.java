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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ScanSourceTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void testUnstartedReaderReadsSamesItsSource() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
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

    PipelineOptions options = PipelineOptionsFactory.create();

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    BoundedSource<Row> source =
        new ScanSource(
            IcebergScanConfig.builder()
                .setCatalogConfig(
                    IcebergCatalogConfig.builder()
                        .setCatalogName("name")
                        .setCatalogProperties(catalogProps)
                        .build())
                .setScanType(IcebergScanConfig.ScanType.TABLE)
                .setTableIdentifier(simpleTable.name().replace("hadoop.", "").split("\\."))
                .setSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
                .build());

    BoundedSource.BoundedReader<Row> reader = source.createReader(options);

    SourceTestUtils.assertUnstartedReaderReadsSameAsItsSource(reader, options);
  }

  @Test
  public void testInitialSplitting() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
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

    PipelineOptions options = PipelineOptionsFactory.create();

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    BoundedSource<Row> source =
        new ScanSource(
            IcebergScanConfig.builder()
                .setCatalogConfig(
                    IcebergCatalogConfig.builder()
                        .setCatalogName("name")
                        .setCatalogProperties(catalogProps)
                        .build())
                .setScanType(IcebergScanConfig.ScanType.TABLE)
                .setTableIdentifier(simpleTable.name().replace("hadoop.", "").split("\\."))
                .setSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
                .build());

    // Input data for this test is tiny so try a number of very small split sizes
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(1, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(2, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(5, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(10, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(100, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(source, source.split(1000, options), options);
  }

  @Test
  public void testDoubleInitialSplitting() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));
    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
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

    PipelineOptions options = PipelineOptionsFactory.create();

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    BoundedSource<Row> source =
        new ScanSource(
            IcebergScanConfig.builder()
                .setCatalogConfig(
                    IcebergCatalogConfig.builder()
                        .setCatalogName("name")
                        .setCatalogProperties(catalogProps)
                        .build())
                .setScanType(IcebergScanConfig.ScanType.TABLE)
                .setTableIdentifier(simpleTable.name().replace("hadoop.", "").split("\\."))
                .setSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
                .build());

    // Input data for this test is tiny so make sure to split and get a few, but so they can be
    // split more
    List<? extends BoundedSource<Row>> splits = source.split(100, options);
    assertThat(splits.size(), Matchers.greaterThan(2));

    // We are going to re-split this one
    BoundedSource<Row> arbitrarySplit = splits.get(0);

    SourceTestUtils.assertSourcesEqualReferenceSource(
        arbitrarySplit, arbitrarySplit.split(1, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(
        arbitrarySplit, arbitrarySplit.split(10, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(
        arbitrarySplit, arbitrarySplit.split(100, options), options);
    SourceTestUtils.assertSourcesEqualReferenceSource(
        arbitrarySplit, arbitrarySplit.split(1000, options), options);
  }
}
