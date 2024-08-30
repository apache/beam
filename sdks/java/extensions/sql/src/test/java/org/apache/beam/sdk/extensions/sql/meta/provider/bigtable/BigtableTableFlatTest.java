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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.KEY1;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.KEY2;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.TEST_FLAT_SCHEMA;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.bigTableRow;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.bigTableSegmentedRows;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.columnsMappingString;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createFlatTableString;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createReadTable;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.flatRow;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.setFixedTimestamp;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.COLUMNS_MAPPING;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class BigtableTableFlatTest {

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR = BigtableEmulatorRule.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TestPipeline writePipeline = TestPipeline.create();

  private static BigtableClientWrapper emulatorWrapper;

  private static final String PROJECT = "fakeProject";
  private static final String INSTANCE = "fakeInstance";

  @BeforeClass
  public static void setUp() throws Exception {
    emulatorWrapper =
        new BigtableClientWrapper(PROJECT, INSTANCE, BIGTABLE_EMULATOR.getPort(), null);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    emulatorWrapper.closeSession();
  }

  @Test
  public void testCreatesFlatSchemaCorrectly() {
    final String tableId = "flatTableSchema";
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new BigtableTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(createFlatTableString(tableId, location(tableId)));

    Table table = metaStore.getTables().get(tableId);
    assertNotNull(table);
    assertEquals(TEST_FLAT_SCHEMA, table.getSchema());

    ObjectNode properties = table.getProperties();
    assertTrue(properties.has(COLUMNS_MAPPING));
    assertEquals(columnsMappingString(), properties.get(COLUMNS_MAPPING).asText());
  }

  @Test
  public void testSimpleSelectFlat() {
    final String tableId = "flatTable";
    createReadTable(tableId, emulatorWrapper);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(tableId, location(tableId)));

    String query = "SELECT key, boolColumn, longColumn, stringColumn, doubleColumn FROM flatTable";

    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(TEST_FLAT_SCHEMA));

    PAssert.that(queryOutput).containsInAnyOrder(flatRow(KEY1), flatRow(KEY2));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectFlatKeyRegexQuery() {
    final String tableId = "regexTable";
    createReadTable(tableId, emulatorWrapper);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(tableId, location(tableId)));

    String query = "SELECT key FROM regexTable WHERE key LIKE '^key[0134]{1}'";

    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(filterSchema()));

    PAssert.that(queryOutput).containsInAnyOrder(filterRow(KEY1));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSegmentedInsert() {
    final String tableId = "beamWriteTableWithSegmentedRead";
    emulatorWrapper.createTable(tableId, FAMILY_TEST);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(tableId, location(tableId)));

    String query =
        "INSERT INTO beamWriteTableWithSegmentedRead(key, boolColumn, longColumn, stringColumn, doubleColumn) "
            + "VALUES ('key0', TRUE, CAST(10 AS bigint), 'stringValue', 5.5), "
            + "('key1', TRUE, CAST(10 AS bigint), 'stringValue', 5.5), "
            + "('key2', TRUE, CAST(10 AS bigint), 'stringValue', 5.5), "
            + "('key3', TRUE, CAST(10 AS bigint), 'stringValue', 5.5), "
            + "('key4', TRUE, CAST(10 AS bigint), 'stringValue', 5.5)";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(query));
    writePipeline.run().waitUntilFinish();

    PCollection<com.google.bigtable.v2.Row> bigTableRows =
        readPipeline
            .apply(readTransformWithSegment(tableId))
            .apply(MapElements.via(new ReplaceCellTimestamp()));

    PAssert.that(bigTableRows).containsInAnyOrder(bigTableSegmentedRows());
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSimpleInsert() {
    final String tableId = "beamWriteTable";
    emulatorWrapper.createTable(tableId, FAMILY_TEST);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(tableId, location(tableId)));

    String query =
        "INSERT INTO beamWriteTable(key, boolColumn, longColumn, stringColumn, doubleColumn) "
            + "VALUES ('key', TRUE, CAST(10 AS bigint), 'stringValue', 5.5)";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(query));
    writePipeline.run().waitUntilFinish();

    PCollection<com.google.bigtable.v2.Row> bigTableRows =
        readPipeline
            .apply(readTransform(tableId))
            .apply(MapElements.via(new ReplaceCellTimestamp()));

    PAssert.that(bigTableRows).containsInAnyOrder(bigTableRow());
    readPipeline.run().waitUntilFinish();
  }

  private String location(String tableId) {
    return BigtableTableTestUtils.location(PROJECT, INSTANCE, tableId, BIGTABLE_EMULATOR.getPort());
  }

  private Schema filterSchema() {
    return Schema.builder().addStringField(KEY).build();
  }

  private Row filterRow(String key) {
    return Row.withSchema(filterSchema()).attachValues(key);
  }

  private static class ReplaceCellTimestamp
      extends SimpleFunction<com.google.bigtable.v2.Row, com.google.bigtable.v2.Row> {
    @Override
    public com.google.bigtable.v2.Row apply(com.google.bigtable.v2.Row input) {
      return setFixedTimestamp(input);
    }
  }

  private BigtableIO.Read readTransform(String table) {
    return BigtableIO.read()
        .withProjectId("fakeProject")
        .withInstanceId("fakeInstance")
        .withTableId(table)
        .withEmulator("localhost:" + BIGTABLE_EMULATOR.getPort());
  }

  private BigtableIO.Read readTransformWithSegment(String table) {
    return BigtableIO.read()
        .withProjectId("fakeProject")
        .withInstanceId("fakeInstance")
        .withTableId(table)
        .withMaxBufferElementCount(2)
        .withEmulator("localhost:" + BIGTABLE_EMULATOR.getPort());
  }
}
