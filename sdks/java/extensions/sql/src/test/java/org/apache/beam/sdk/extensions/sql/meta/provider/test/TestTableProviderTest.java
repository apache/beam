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
package org.apache.beam.sdk.extensions.sql.meta.provider.test;

import static org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PUSH_DOWN_OPTION;

import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider.PushDownOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TestTableProviderTest {
  private static final Schema BASIC_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();
  private BeamSqlTable beamSqlTable;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Before
  public void buildUp() {
    TestTableProvider tableProvider = new TestTableProvider();
    Table table = getTable("tableName");
    tableProvider.createTable(table);
    tableProvider.addRows(
        table.getName(), row(BASIC_SCHEMA, 1, "one"), row(BASIC_SCHEMA, 2, "two"));

    beamSqlTable = tableProvider.buildBeamSqlTable(table);
  }

  @Test
  public void testInMemoryTableProvider_returnsSelectedColumns() {
    PCollection<Row> result =
        beamSqlTable.buildIOReader(
            pipeline.begin(),
            beamSqlTable.constructFilter(ImmutableList.of()),
            ImmutableList.of("name"));

    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), "one"), row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testInMemoryTableProvider_withEmptySelectedColumns_returnsAllColumns() {
    PCollection<Row> result =
        beamSqlTable.buildIOReader(
            pipeline.begin(), beamSqlTable.constructFilter(ImmutableList.of()), ImmutableList.of());

    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), 1, "one"), row(result.getSchema(), 2, "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testInMemoryTableProvider_withAllSelectedColumns_returnsAllColumns() {
    PCollection<Row> result =
        beamSqlTable.buildIOReader(
            pipeline.begin(),
            beamSqlTable.constructFilter(ImmutableList.of()),
            ImmutableList.of("name", "id"));

    // Selected columns are outputted in the same order they are listed in the schema.
    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), "one", 1), row(result.getSchema(), "two", 2));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testInMemoryTableProvider_withDuplicateSelectedColumns_returnsSelectedColumnsOnce() {
    PCollection<Row> result =
        beamSqlTable.buildIOReader(
            pipeline.begin(),
            beamSqlTable.constructFilter(ImmutableList.of()),
            ImmutableList.of("name", "name"));

    PAssert.that(result)
        .containsInAnyOrder(row(result.getSchema(), "one"), row(result.getSchema(), "two"));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  private static Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }

  private static Table getTable(String name) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .schema(BASIC_SCHEMA)
        .properties(
            JSON.parseObject(
                "{ " + PUSH_DOWN_OPTION + ": " + "\"" + PushDownOptions.BOTH.toString() + "\" }"))
        .type("test")
        .build();
  }
}
