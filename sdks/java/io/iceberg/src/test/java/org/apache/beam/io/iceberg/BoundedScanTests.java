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

import org.apache.beam.io.iceberg.util.SchemaHelper;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class BoundedScanTests {

  private static Logger LOG = LoggerFactory.getLogger(BoundedScanTests.class);
  @ClassRule public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(temporaryFolder, "default");

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
    Table simpleTable = warehouse.createTable(TestFixtures.SCHEMA);
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

    PCollection<Row> output =
        testPipeline
            .apply(
                Read.from(
                    new IcebergBoundedSource(
                        Iceberg.Scan.builder()
                            .catalog(
                                Iceberg.Catalog.builder()
                                    .name("hadoop")
                                    .icebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                                    .warehouseLocation(warehouse.location)
                                    .build())
                            .type(Iceberg.ScanType.TABLE)
                            .table(simpleTable.name().replace("hadoop.", "").split("\\."))
                            .schema(SchemaHelper.convert(TestFixtures.SCHEMA))
                            .build())))
            .apply(ParDo.of(new PrintRow()))
            .setCoder(RowCoder.of(SchemaHelper.convert(TestFixtures.SCHEMA)));
    PAssert.that(output);
    testPipeline.run();
  }
}
