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
package org.apache.beam.sdk.extensions.sql.impl.rel;

import static org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest.compilePipeline;
import static org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest.registerTable;

import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

public class BeamMatchRelTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void matchLogicalPlanTest() {
    Schema schemaType =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addInt32Field("proctime")
            .build();

    registerTable(
        "TestTable", TestBoundedTable.of(schemaType).addRows(1, "a", 1, 1, "b", 2, 1, "c", 3));

    String sql =
        "SELECT * "
            + "FROM TestTable "
            + "MATCH_RECOGNIZE ("
            + "PARTITION BY id "
            + "ORDER BY proctime "
            + "PATTERN (A B C) "
            + "DEFINE "
            + "A AS name = 'a', "
            + "B AS name = 'b', "
            + "C AS name = 'c' "
            + ") AS T";

    PCollection<Row> result = compilePipeline(sql, pipeline);

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "id",
                    Schema.FieldType.STRING, "name",
                    Schema.FieldType.INT32, "proctime")
                .addRows(1, "a", 1, 1, "b", 2, 1, "c", 3)
                .getRows());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void matchQuantifierTest() {
    Schema schemaType =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addInt32Field("proctime")
            .build();

    registerTable(
        "TestTable", TestBoundedTable.of(schemaType).addRows(1, "a", 1, 1, "a", 2, 1, "b", 3, 1, "c", 4));

    String sql =
        "SELECT * "
            + "FROM TestTable "
            + "MATCH_RECOGNIZE ("
            + "PARTITION BY id "
            + "ORDER BY proctime "
            + "PATTERN (A+ B C) "
            + "DEFINE "
            + "A AS name = 'a', "
            + "B AS name = 'b', "
            + "C AS name = 'c' "
            + ") AS T";

    PCollection<Row> result = compilePipeline(sql, pipeline);

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Schema.FieldType.INT32, "id",
                Schema.FieldType.STRING, "name",
                Schema.FieldType.INT32, "proctime")
                .addRows(1, "a", 1, 1, "a", 2, 1, "b", 3, 1, "c", 4)
                .getRows());

    pipeline.run().waitUntilFinish();
  }
}
