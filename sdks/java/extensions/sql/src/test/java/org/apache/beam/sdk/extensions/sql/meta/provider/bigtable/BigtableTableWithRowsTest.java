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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.LABELS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.BINARY_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.BOOL_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.DOUBLE_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY1;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.KEY2;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.LATER;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.io.gcp.testing.BigtableTestUtils.TEST_SCHEMA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BigtableTableWithRowsTest extends BigtableTableTest {

  private String createTableString() {
    return "CREATE EXTERNAL TABLE beamTable( \n"
        + "  key VARCHAR NOT NULL, \n"
        + "  familyTest ROW< \n"
        + "    boolColumn BOOLEAN NOT NULL, \n"
        + "    longColumn ROW< \n"
        + "      val BIGINT NOT NULL, \n"
        + "      timestampMicros BIGINT NOT NULL, \n"
        + "      labels ARRAY<VARCHAR> NOT NULL \n"
        + "    > NOT NULL, \n"
        + "    stringColumn ARRAY<VARCHAR> NOT NULL, \n"
        + "    doubleColumn DOUBLE NOT NULL, \n"
        + "    binaryColumn BINARY NOT NULL \n"
        + "  > NOT NULL \n"
        + ") \n"
        + "TYPE bigtable \n"
        + "LOCATION '"
        + getLocation()
        + "'";
  }

  @Test
  public void testCreatesSchemaCorrectly() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new BigtableTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(createTableString());

    Table table = metaStore.getTables().get("beamTable");
    assertNotNull(table);
    assertEquals(TEST_SCHEMA, table.getSchema());
  }

  @Test
  public void testSimpleSelect() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createTableString());
    String query =
        ""
            + "SELECT key, \n"
            + "  bt.familyTest.boolColumn, \n"
            + "  bt.familyTest.longColumn.val AS longValue, \n"
            + "  bt.familyTest.longColumn.timestampMicros, \n"
            + "  bt.familyTest.longColumn.labels, \n"
            + "  bt.familyTest.stringColumn, \n"
            + "  bt.familyTest.doubleColumn, \n"
            + "  bt.familyTest.binaryColumn \n"
            + "FROM beamTable bt";
    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(expectedSchema()));

    PCollection<Row> sorted =
        queryOutput.apply(MapElements.via(new SortByTimestamp())).setRowSchema(expectedSchema());

    PAssert.that(sorted)
        .containsInAnyOrder(row(expectedSchema(), KEY1), row(expectedSchema(), KEY2));
    pipeline.run().waitUntilFinish();
  }

  private static Schema expectedSchema() {
    return Schema.builder()
        .addStringField(KEY)
        .addBooleanField(BOOL_COLUMN)
        .addInt64Field("longValue")
        .addInt64Field(TIMESTAMP_MICROS)
        .addArrayField(LABELS, Schema.FieldType.STRING)
        .addArrayField(STRING_COLUMN, Schema.FieldType.STRING)
        .addDoubleField(DOUBLE_COLUMN)
        .addByteArrayField(BINARY_COLUMN)
        .build();
  }

  private static Row row(Schema schema, String key) {
    return Row.withSchema(schema)
        .attachValues(
            key,
            false,
            2L,
            LATER,
            ImmutableList.of(),
            ImmutableList.of("string1", "string2"),
            2.20,
            "blob2".getBytes(UTF_8));
  }

  private static class SortByTimestamp extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      return Row.fromRow(input)
          .withFieldValue(
              STRING_COLUMN,
              ImmutableList.copyOf(
                  input.getArray(STRING_COLUMN).stream().sorted().collect(toList())))
          .build();
    }
  }
}
