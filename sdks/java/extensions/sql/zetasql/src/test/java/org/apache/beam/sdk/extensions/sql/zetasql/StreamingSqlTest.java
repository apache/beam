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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimestampWithUTCTimeZone;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.chrono.ISOChronology;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ZetaSQL windowing functions (TUMBLE, HOP, and SESSION). */
@RunWith(JUnit4.class)
public class StreamingSqlTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testZetaSQLBasicSlidingWindowing() {
    String sql =
        "SELECT "
            + "COUNT(*) as field_count, "
            + "HOP_START(\"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") as window_start, "
            + "HOP_END(\"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") as window_end "
            + "FROM window_test_table "
            + "GROUP BY HOP(ts, \"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_star")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 9, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 5, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 10, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 9, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 11, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLBasicSessionWindowing() {
    String sql =
        "SELECT "
            + "COUNT(*) as field_count, "
            + "SESSION_START(\"INTERVAL 3 SECOND\") as window_start, "
            + "SESSION_END(\"INTERVAL 3 SECOND\") as window_end "
            + "FROM window_test_table_two "
            + "GROUP BY SESSION(ts, \"INTERVAL 3 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_star")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 12, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 12, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testZetaSQLNestedQueryFour() {
    String sql =
        "SELECT t1.Value, TUMBLE_START('INTERVAL 1 SECOND') AS period_start, MIN(t2.Value) as"
            + " min_v FROM KeyValue AS t1 INNER JOIN BigTable AS t2 on t1.Key = t2.RowKey GROUP BY"
            + " t1.Value, TUMBLE(t2.ts, 'INTERVAL 1 SECOND')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addStringField("value")
                        .addDateTimeField("min_v")
                        .addStringField("period_start")
                        .build())
                .addValues(
                    "KeyValue235",
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    "BigTable235")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testWithQuerySeven() {
    String sql =
        "WITH T1 AS (SELECT * FROM KeyValue) SELECT "
            + "COUNT(*) as field_count, "
            + "TUMBLE_START(\"INTERVAL 1 SECOND\") as window_start, "
            + "TUMBLE_END(\"INTERVAL 1 SECOND\") as window_end "
            + "FROM T1 "
            + "GROUP BY TUMBLE(ts, \"INTERVAL 1 SECOND\");";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("count_start")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTVFTumbleAggregation() {
    String sql =
        "SELECT COUNT(*) as field_count, "
            + "window_start "
            + "FROM TUMBLE((select * from KeyValue), descriptor(ts), 'INTERVAL 1 SECOND') "
            + "GROUP BY window_start";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder().addInt64Field("field_count").addDateTimeField("window_start").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(1L, new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(1L, new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testHopAsTVFAggregation() {
    String sql =
        "SELECT COUNT(*) as field_count, window_start, window_end "
            + "FROM HOP((select * from window_test_table), "
            + "descriptor(ts), \"INTERVAL 1 SECOND\", \"INTERVAL 2 SECOND\") "
            + "GROUP BY window_start, window_end;";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);

    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("field_count")
            .addDateTimeField("field1")
            .addDateTimeField("field2")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 9, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 5, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 7, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 6, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    2L,
                    new DateTime(2018, 7, 1, 21, 26, 8, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 10, ISOChronology.getInstanceUTC()))
                .build(),
            Row.withSchema(schema)
                .addValues(
                    1L,
                    new DateTime(2018, 7, 1, 21, 26, 9, ISOChronology.getInstanceUTC()),
                    new DateTime(2018, 7, 1, 21, 26, 11, ISOChronology.getInstanceUTC()))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void runTumbleWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS getFieldCount,"
            + " window_start, "
            + " window_end "
            + " FROM TUMBLE((select * from streaming_sql_test_table_a), descriptor(f_timestamp), \"INTERVAL 1 HOUR\") "
            + " GROUP BY window_start, window_end, f_long";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:00:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 02:00:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 02:00:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 03:00:00"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runTumbleWindowFor31Days() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS getFieldCount,"
            + " window_start, "
            + " window_end "
            + " FROM TUMBLE((select * from streaming_sql_test_table_b), descriptor(f_timestamp), \"INTERVAL 31 DAY\") "
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2016-12-08 00:00:00"),
                    parseTimestampWithUTCTimeZone("2017-01-08 00:00:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    2000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-08 00:00:00"),
                    parseTimestampWithUTCTimeZone("2017-02-08 00:00:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    3000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-02-08 00:00:00"),
                    parseTimestampWithUTCTimeZone("2017-03-11 00:00:00"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runHopWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS `getFieldCount`,"
            + "  `window_start`, "
            + "  `window_end` "
            + " FROM HOP((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " \"INTERVAL 30 MINUTE\", \"INTERVAL 1 HOUR\")"
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimestampWithUTCTimeZone("2017-01-01 00:30:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 01:30:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:00:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 02:00:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:30:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 02:30:00"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 02:00:00"),
                    parseTimestampWithUTCTimeZone("2017-01-01 03:00:00"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runSessionWindow() throws Exception {
    String sql =
        "SELECT f_long, COUNT(*) AS `getFieldCount`,"
            + " `window_start`, "
            + " `window_end` "
            + " FROM SESSION((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " descriptor(f_long), \"INTERVAL 5 MINUTE\")"
            + " GROUP BY f_long, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    3L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:01:03"),
                    parseTimestampWithUTCTimeZone("2017-01-01 01:11:03"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 02:04:03"),
                    parseTimestampWithUTCTimeZone("2017-01-01 02:09:03"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void runSessionWindow2() throws Exception {
    String sql =
        "SELECT f_long, f_string, COUNT(*) AS `getFieldCount`,"
            + " `window_start`, `window_end` "
            + " FROM SESSION((select * from streaming_sql_test_table_a), descriptor(f_timestamp), "
            + " descriptor(f_long, f_string), \"INTERVAL 5 MINUTE\")"
            + " GROUP BY f_long, f_string, window_start, window_end";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema resultType =
        Schema.builder()
            .addInt64Field("f_long")
            .addStringField("f_string")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        Arrays.asList(
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    "string_row1",
                    2L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:01:03"),
                    parseTimestampWithUTCTimeZone("2017-01-01 01:07:03"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    1000L,
                    "string_row3",
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 01:06:03"),
                    parseTimestampWithUTCTimeZone("2017-01-01 01:11:03"))
                .build(),
            Row.withSchema(resultType)
                .addValues(
                    4000L,
                    "第四行",
                    1L,
                    parseTimestampWithUTCTimeZone("2017-01-01 02:04:03"),
                    parseTimestampWithUTCTimeZone("2017-01-01 02:09:03"))
                .build());

    PAssert.that(stream).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  @Ignore("[BEAM-9191] CAST operator does not work fully due to bugs in unparsing")
  public void testZetaSQLStructFieldAccessInTumble() {
    String sql =
        "SELECT TUMBLE_START('INTERVAL 1 MINUTE') FROM table_with_struct_ts_string AS A GROUP BY "
            + "TUMBLE(CAST(A.struct_col.struct_col_str AS TIMESTAMP), 'INTERVAL 1 MINUTE')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    final Schema schema = Schema.builder().addDateTimeField("field").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValue(parseTimestampWithUTCTimeZone("2019-01-15 13:21:00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
