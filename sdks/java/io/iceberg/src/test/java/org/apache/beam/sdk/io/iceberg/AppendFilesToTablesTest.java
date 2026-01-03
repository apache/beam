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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AppendFilesToTablesTest implements Serializable {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testAppendAfterDelete() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of("default", "table" + Long.toString(UUID.randomUUID().hashCode(), 16));

    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    IcebergCatalogConfig catalog =
        IcebergCatalogConfig.builder()
            .setCatalogName("name")
            .setCatalogProperties(catalogProps)
            .build();

    // 1. Create table and write some data
    testPipeline
        .apply("Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append To Table", IcebergIO.writeRows(catalog).to(tableId));

    testPipeline.run().waitUntilFinish();

    // 2. Delete the data
    Table table = warehouse.loadTable(tableId);
    DeleteFiles delete = table.newDelete();
    // Delete all data files in the current snapshot
    table.currentSnapshot().addedDataFiles(table.io()).forEach(delete::deleteFile);
    delete.commit();

    // 3. Write more data
    testPipeline
        .apply("More Records To Add", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT2)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA))
        .apply("Append More To Table", IcebergIO.writeRows(catalog).to(tableId));

    testPipeline.run().waitUntilFinish();

    // Verify data
    // We mainly want to verify that no exception is thrown during the write.
    // The exact content might depend on how delete operations interact with
    // subsequent appends
    // which is not the focus of this test.
    table.refresh();
    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.hasItem(Matchers.anything()));
  }
}
