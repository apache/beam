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

import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.COLUMNS_MAPPING;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY1;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY2;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.TEST_FLAT_SCHEMA;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.bigTableRow;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.setFixedTimestamp;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSONObject;
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
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigtableTableFlatTest extends BigtableTableTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  private String createFlatTableString(String table) {
    return "CREATE EXTERNAL TABLE "
        + table
        + "( \n"
        + "  key VARCHAR NOT NULL, \n"
        + "  boolColumn BOOLEAN NOT NULL, \n"
        + "  longColumn BIGINT NOT NULL, \n"
        + "  stringColumn VARCHAR NOT NULL, \n"
        + "  doubleColumn DOUBLE NOT NULL \n"
        + ") \n"
        + "TYPE bigtable \n"
        + "LOCATION '"
        + getLocation(table)
        + "' \n"
        + "TBLPROPERTIES '{ \n"
        + "  \"columnsMapping\": \""
        + columnsMappingString()
        + "\"}'";
  }

  @Test
  public void testCreatesFlatSchemaCorrectly() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new BigtableTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(createFlatTableString("flatTable"));

    Table table = metaStore.getTables().get("flatTable");
    assertNotNull(table);
    assertEquals(TEST_FLAT_SCHEMA, table.getSchema());

    JSONObject properties = table.getProperties();
    assertTrue(properties.containsKey(COLUMNS_MAPPING));
    assertEquals(columnsMappingString(), properties.getString(COLUMNS_MAPPING));
  }

  @Test
  public void testSimpleSelectFlat() throws Exception {
    createReadTable("flatTable");
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString("flatTable"));

    String query =
        "SELECT \n"
            + "  key, \n"
            + "  boolColumn, \n"
            + "  longColumn, \n"
            + "  stringColumn, \n"
            + "  doubleColumn \n"
            + "FROM flatTable";

    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(TEST_FLAT_SCHEMA));

    PAssert.that(queryOutput).containsInAnyOrder(row(KEY1), row(KEY2));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectFlatKeyRegexQuery() throws Exception {
    createReadTable("regexTable");
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString("regexTable"));

    String query = "SELECT key FROM regexTable WHERE key LIKE '^key[0134]{1}'";

    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(filterSchema()));

    PAssert.that(queryOutput).containsInAnyOrder(filterRow(KEY1));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSimpleInsert() {
    createTable("beamWriteTable");
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString("beamWriteTable"));

    String query =
        "INSERT INTO beamWriteTable(key, boolColumn, longColumn, stringColumn, doubleColumn) "
            + "VALUES ('key', TRUE, 10, 'stringValue', 5.5)";

    BeamSqlRelUtils.toPCollection(writePipeline, sqlEnv.parseQuery(query));
    writePipeline.run().waitUntilFinish();

    PCollection<com.google.bigtable.v2.Row> bigTableRows =
        readPipeline
            .apply(readTransform("beamWriteTable"))
            .apply(MapElements.via(new ReplaceCellTimestamp()));

    PAssert.that(bigTableRows).containsInAnyOrder(bigTableRow());
    readPipeline.run().waitUntilFinish();
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

  private String columnsMappingString() {
    return "familyTest:boolColumn,familyTest:longColumn,familyTest:doubleColumn,"
        + "familyTest:stringColumn";
  }

  private static Row row(String key) {
    return Row.withSchema(TEST_FLAT_SCHEMA).attachValues(key, false, 2L, "string2", 2.20);
  }

  private static BigtableIO.Read readTransform(String table) {
    return BigtableIO.read()
        .withProjectId("fakeProject")
        .withInstanceId("fakeInstance")
        .withTableId(table)
        .withEmulator("localhost:" + BIGTABLE_EMULATOR.getPort());
  }
}
