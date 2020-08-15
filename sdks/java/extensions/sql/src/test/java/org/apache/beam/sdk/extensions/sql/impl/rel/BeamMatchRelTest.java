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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@code BeamMatchRel}. */
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
            + "ALL ROWS PER MATCH "
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
        "TestTable",
        TestBoundedTable.of(schemaType).addRows(1, "a", 1, 1, "a", 2, 1, "b", 3, 1, "c", 4));

    String sql =
        "SELECT * "
            + "FROM TestTable "
            + "MATCH_RECOGNIZE ("
            + "PARTITION BY id "
            + "ORDER BY proctime "
            + "ALL ROWS PER MATCH "
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

  @Test
  public void matchMeasuresTest() {
    Schema schemaType =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addInt32Field("proctime")
            .build();

    registerTable(
        "TestTable",
        TestBoundedTable.of(schemaType)
            .addRows(
                1, "a", 1, 1, "a", 2, 1, "b", 3, 1, "c", 4, 1, "b", 8, 1, "a", 7, 1, "c", 9, 2, "a",
                6, 2, "b", 10, 2, "c", 11, 5, "a", 0));

    String sql =
        "SELECT * "
            + "FROM TestTable "
            + "MATCH_RECOGNIZE ("
            + "PARTITION BY id "
            + "ORDER BY proctime "
            + "MEASURES "
            + "LAST (A.proctime) AS atime, "
            + "B.proctime AS btime, "
            + "C.proctime AS ctime "
            + "PATTERN (A+ B C) "
            + "DEFINE "
            + "A AS id + 1 > 1, "
            + "B AS name = 'b', "
            + "C AS name = 'c' "
            + ") AS T " +
        "WHERE T.id > 0";

    PCollection<Row> result = compilePipeline(sql, pipeline);

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT32, "id",
                    Schema.FieldType.INT32, "T.atime",
                    Schema.FieldType.INT32, "T.btime",
                    Schema.FieldType.INT32, "T.ctime")
                .addRows(1, 2, 3, 4, 1, 7, 8, 9, 2, 6, 10, 11)
                .getRows());

    pipeline.run().waitUntilFinish();
  }

  @Ignore("NFA has not been implemented for now.")
  @Test
  public void matchNFATest() {
    Schema schemaType =
        Schema.builder()
            .addStringField("Symbol")
            .addDateTimeField("TradeDay")
            .addInt32Field("Price")
            .build();

    registerTable(
        "Ticker",
        TestBoundedTable.of(schemaType)
            .addRows(
                "a",
                "2020-07-01",
                32, // 1st A
                "a",
                "2020-06-01",
                34,
                "a",
                "2020-07-02",
                31, // B
                "a",
                "2020-08-30",
                30, // B
                "a",
                "2020-08-31",
                35, // C
                "a",
                "2020-10-01",
                28,
                "a",
                "2020-10-15",
                30, // 2nd A
                "a",
                "2020-11-01",
                22, // B
                "a",
                "2020-11-08",
                29, // C
                "a",
                "2020-12-10",
                30, // C
                "b",
                "2020-12-01",
                22,
                "c",
                "2020-05-16",
                27, // A
                "c",
                "2020-09-14",
                26, // B
                "c",
                "2020-10-13",
                30)); // C

    // match `V` shapes in prices
    String sql =
        "SELECT M.Symbol,"
            + " M.Matchno,"
            + " M.Startp,"
            + " M.Bottomp,"
            + " M.Endp,"
            + " M.Avgp"
            + "FROM Ticker "
            + "MATCH_RECOGNIZE ("
            + "PARTITION BY Symbol "
            + "ORDER BY Tradeday "
            + "MEASURES "
            + "MATCH_NUMBER() AS Matchno, "
            + "A.price AS Startp, "
            + "LAST (B.Price) AS Bottomp, "
            + "LAST (C.Price) AS ENDp, "
            + "AVG (U.Price) AS Avgp "
            + "AFTER MATCH SKIP PAST LAST ROW "
            + "PATTERN (A B+ C+) "
            + "SUBSET U = (A, B, C) "
            + "DEFINE "
            + "B AS B.Price < PREV (B.Price), "
            + "C AS C.Price > PREV (C.Price) "
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
}
