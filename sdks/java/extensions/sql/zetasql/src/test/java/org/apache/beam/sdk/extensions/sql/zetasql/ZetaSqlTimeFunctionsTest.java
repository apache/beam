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

import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseDateToValue;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimeToValue;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimestampWithTZToValue;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimestampWithTimeZone;
import static org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils.parseTimestampWithUTCTimeZone;

import com.google.zetasql.CivilTimeEncoder;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ZetaSQL time functions (DATE, TIME, DATETIME, and TIMESTAMP functions). */
@RunWith(JUnit4.class)
public class ZetaSqlTimeFunctionsTest extends ZetaSqlTestBase {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  /////////////////////////////////////////////////////////////////////////////
  // DATE type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testDateLiteral() {
    String sql = "SELECT DATE '2020-3-30'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2020, 3, 30))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateColumn() {
    String sql = "SELECT FORMAT_DATE('%b-%d-%Y', date_field) FROM table_with_date";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_date_str").build())
                .addValues("Dec-25-2008")
                .build(),
            Row.withSchema(Schema.builder().addStringField("f_date_str").build())
                .addValues("Apr-07-2020")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testGroupByDate() {
    String sql = "SELECT date_field, COUNT(*) FROM table_with_date GROUP BY date_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addLogicalTypeField("date_field", SqlTypes.DATE)
            .addInt64Field("count")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(LocalDate.of(2008, 12, 25), 1L).build(),
            Row.withSchema(schema).addValues(LocalDate.of(2020, 4, 7), 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAggregateOnDate() {
    String sql = "SELECT MAX(date_field) FROM table_with_date GROUP BY str_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("date_field", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2020, 4, 7))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // TODO[BEAM-9166]: Add a test for CURRENT_DATE function ("SELECT CURRENT_DATE()")

  @Test
  public void testExtractFromDate() {
    String sql =
        "WITH Dates AS (\n"
            + "  SELECT DATE '2015-12-31' AS date UNION ALL\n"
            + "  SELECT DATE '2016-01-01'\n"
            + ")\n"
            + "SELECT\n"
            + "  EXTRACT(ISOYEAR FROM date) AS isoyear,\n"
            + "  EXTRACT(YEAR FROM date) AS year,\n"
            + "  EXTRACT(ISOWEEK FROM date) AS isoweek,\n"
            // TODO[BEAM-9178]: Add tests for DATE_TRUNC and EXTRACT with "week with weekday" date
            //  parts once they are supported
            // + "  EXTRACT(WEEK FROM date) AS week,\n"
            + "  EXTRACT(MONTH FROM date) AS month,\n"
            + "  EXTRACT(QUARTER FROM date) AS quarter,\n"
            + "  EXTRACT(DAY FROM date) AS day,\n"
            + "  EXTRACT(DAYOFYEAR FROM date) AS dayofyear,\n"
            + "  EXTRACT(DAYOFWEEK FROM date) AS dayofweek\n"
            + "FROM Dates";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("isoyear")
            .addInt64Field("year")
            .addInt64Field("isoweek")
            // .addInt64Field("week")
            .addInt64Field("month")
            .addInt64Field("quarter")
            .addInt64Field("day")
            .addInt64Field("dayofyear")
            .addInt64Field("dayofweek")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(2015L, 2015L, 53L /* , 52L */, 12L, 4L, 31L, 365L, 5L)
                .build(),
            Row.withSchema(schema)
                .addValues(2015L, 2016L, 53L /* , 0L */, 1L, 1L, 1L, 1L, 6L)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateFromYearMonthDay() {
    String sql = "SELECT DATE(2008, 12, 25)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2008, 12, 25))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateFromTimestamp() {
    String sql = "SELECT DATE(TIMESTAMP '2016-12-25 05:30:00+07', 'America/Los_Angeles')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2016, 12, 24))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateAdd() {
    String sql =
        "SELECT "
            + "DATE_ADD(DATE '2008-12-25', INTERVAL 5 DAY), "
            + "DATE_ADD(DATE '2008-12-25', INTERVAL 1 MONTH), "
            + "DATE_ADD(DATE '2008-12-25', INTERVAL 1 YEAR), ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_date1", SqlTypes.DATE)
                        .addLogicalTypeField("f_date2", SqlTypes.DATE)
                        .addLogicalTypeField("f_date3", SqlTypes.DATE)
                        .build())
                .addValues(
                    LocalDate.of(2008, 12, 30),
                    LocalDate.of(2009, 1, 25),
                    LocalDate.of(2009, 12, 25))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateAddWithParameter() {
    String sql =
        "SELECT "
            + "DATE_ADD(@p0, INTERVAL @p1 DAY), "
            + "DATE_ADD(@p2, INTERVAL @p3 DAY), "
            + "DATE_ADD(@p4, INTERVAL @p5 YEAR), "
            + "DATE_ADD(@p6, INTERVAL @p7 DAY), "
            + "DATE_ADD(@p8, INTERVAL @p9 MONTH)";

    ImmutableMap<String, Value> params =
        ImmutableMap.<String, Value>builder()
            .put("p0", Value.createDateValue(0)) // 1970-01-01
            .put("p1", Value.createInt64Value(2L))
            .put("p2", parseDateToValue("2019-01-01"))
            .put("p3", Value.createInt64Value(2L))
            .put("p4", Value.createSimpleNullValue(TypeKind.TYPE_DATE))
            .put("p5", Value.createInt64Value(1L))
            .put("p6", parseDateToValue("2000-02-29"))
            .put("p7", Value.createInt64Value(-365L))
            .put("p8", parseDateToValue("1999-03-31"))
            .put("p9", Value.createInt64Value(-1L))
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addLogicalTypeField("f_date1", SqlTypes.DATE)
            .addLogicalTypeField("f_date2", SqlTypes.DATE)
            .addNullableField("f_date3", FieldType.logicalType(SqlTypes.DATE))
            .addLogicalTypeField("f_date4", SqlTypes.DATE)
            .addLogicalTypeField("f_date5", SqlTypes.DATE)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    LocalDate.of(1970, 1, 3),
                    LocalDate.of(2019, 1, 3),
                    null,
                    LocalDate.of(1999, 3, 1),
                    LocalDate.of(1999, 2, 28))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateSub() {
    String sql =
        "SELECT "
            + "DATE_SUB(DATE '2008-12-25', INTERVAL 5 DAY), "
            + "DATE_SUB(DATE '2008-12-25', INTERVAL 1 MONTH), "
            + "DATE_SUB(DATE '2008-12-25', INTERVAL 1 YEAR), ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_date1", SqlTypes.DATE)
                        .addLogicalTypeField("f_date2", SqlTypes.DATE)
                        .addLogicalTypeField("f_date3", SqlTypes.DATE)
                        .build())
                .addValues(
                    LocalDate.of(2008, 12, 20),
                    LocalDate.of(2008, 11, 25),
                    LocalDate.of(2007, 12, 25))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateDiff() {
    String sql = "SELECT DATE_DIFF(DATE '2010-07-07', DATE '2008-12-25', DAY)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_date_diff").build())
                .addValues(559L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateDiffNegativeResult() {
    String sql = "SELECT DATE_DIFF(DATE '2017-12-17', DATE '2017-12-18', ISOWEEK)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_date_diff").build())
                .addValues(-1L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTrunc() {
    String sql = "SELECT DATE_TRUNC(DATE '2015-06-15', ISOYEAR)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_date_trunc", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2014, 12, 29))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFormatDate() {
    String sql = "SELECT FORMAT_DATE('%b-%d-%Y', DATE '2008-12-25')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_date_str").build())
                .addValues("Dec-25-2008")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParseDate() {
    String sql = "SELECT PARSE_DATE('%m %d %y', '10 14 18')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2018, 10, 14))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateToUnixInt64() {
    String sql = "SELECT UNIX_DATE(DATE '2008-12-25')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_unix_date").build())
                .addValues(14238L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateFromUnixInt64() {
    String sql = "SELECT DATE_FROM_UNIX_DATE(14238)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2008, 12, 25))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // TIME type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testTimeLiteral() {
    String sql = "SELECT TIME '15:30:00', TIME '15:30:00.135246' ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_time1", SqlTypes.TIME)
                        .addLogicalTypeField("f_time2", SqlTypes.TIME)
                        .build())
                .addValues(LocalTime.of(15, 30, 0))
                .addValues(LocalTime.of(15, 30, 0, 135246000))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeColumn() {
    String sql = "SELECT FORMAT_TIME('%T', time_field) FROM table_with_time";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_time_str").build())
                .addValues("15:30:00")
                .build(),
            Row.withSchema(Schema.builder().addStringField("f_time_str").build())
                .addValues("23:35:59")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testGroupByTime() {
    String sql = "SELECT time_field, COUNT(*) FROM table_with_time GROUP BY time_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addLogicalTypeField("time_field", SqlTypes.TIME)
            .addInt64Field("count")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(LocalTime.of(15, 30, 0), 1L).build(),
            Row.withSchema(schema).addValues(LocalTime.of(23, 35, 59), 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAggregateOnTime() {
    String sql = "SELECT MAX(time_field) FROM table_with_time GROUP BY str_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("time_field", SqlTypes.TIME).build())
                .addValues(LocalTime.of(23, 35, 59))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // TODO[BEAM-9166]: Add a test for CURRENT_TIME function ("SELECT CURRENT_TIME()")

  @Test
  public void testExtractFromTime() {
    String sql =
        "SELECT "
            + "EXTRACT(HOUR FROM TIME '15:30:35.123456') as hour, "
            + "EXTRACT(MINUTE FROM TIME '15:30:35.123456') as minute, "
            + "EXTRACT(SECOND FROM TIME '15:30:35.123456') as second, "
            + "EXTRACT(MILLISECOND FROM TIME '15:30:35.123456') as millisecond, "
            + "EXTRACT(MICROSECOND FROM TIME '15:30:35.123456') as microsecond ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("hour")
            .addInt64Field("minute")
            .addInt64Field("second")
            .addInt64Field("millisecond")
            .addInt64Field("microsecond")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues(15L, 30L, 35L, 123L, 123456L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeFromHourMinuteSecond() {
    String sql = "SELECT TIME(15, 30, 0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(15, 30, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeFromTimestamp() {
    String sql = "SELECT TIME(TIMESTAMP '2008-12-25 15:30:00+08', 'America/Los_Angeles')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(23, 30, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeAdd() {
    String sql =
        "SELECT "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MICROSECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MILLISECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 SECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MINUTE), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 HOUR) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_time1", SqlTypes.TIME)
                        .addLogicalTypeField("f_time2", SqlTypes.TIME)
                        .addLogicalTypeField("f_time3", SqlTypes.TIME)
                        .addLogicalTypeField("f_time4", SqlTypes.TIME)
                        .addLogicalTypeField("f_time5", SqlTypes.TIME)
                        .build())
                .addValues(
                    LocalTime.of(15, 30, 0, 10000),
                    LocalTime.of(15, 30, 0, 10000000),
                    LocalTime.of(15, 30, 10, 0),
                    LocalTime.of(15, 40, 0, 0),
                    LocalTime.of(1, 30, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeAddWithParameter() {
    String sql = "SELECT TIME_ADD(@p0, INTERVAL @p1 SECOND)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", parseTimeToValue("12:13:14.123"),
            "p1", Value.createInt64Value(1L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(12, 13, 15, 123000000))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeSub() {
    String sql =
        "SELECT "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MICROSECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MILLISECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 SECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MINUTE), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 HOUR) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_time1", SqlTypes.TIME)
                        .addLogicalTypeField("f_time2", SqlTypes.TIME)
                        .addLogicalTypeField("f_time3", SqlTypes.TIME)
                        .addLogicalTypeField("f_time4", SqlTypes.TIME)
                        .addLogicalTypeField("f_time5", SqlTypes.TIME)
                        .build())
                .addValues(
                    LocalTime.of(15, 29, 59, 999990000),
                    LocalTime.of(15, 29, 59, 990000000),
                    LocalTime.of(15, 29, 50, 0),
                    LocalTime.of(15, 20, 0, 0),
                    LocalTime.of(5, 30, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeDiff() {
    String sql = "SELECT TIME_DIFF(TIME '15:30:00', TIME '14:35:00', MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_time_diff").build())
                .addValues(55L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeDiffNegativeResult() {
    String sql = "SELECT TIME_DIFF(TIME '14:35:00', TIME '15:30:00', MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_time_diff").build())
                .addValues(-55L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimeTrunc() {
    String sql = "SELECT TIME_TRUNC(TIME '15:30:35', HOUR)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_time_trunc", SqlTypes.TIME).build())
                .addValues(LocalTime.of(15, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFormatTime() {
    String sql = "SELECT FORMAT_TIME('%R', TIME '15:30:00')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_time_str").build())
                .addValues("15:30")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParseTime() {
    String sql = "SELECT PARSE_TIME('%H', '15')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(15, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // DATETIME type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testDateTimeLiteral() {
    String sql = "SELECT DATETIME '2008-12-25 15:30:00'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 25, 15, 30, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeColumn() {
    String sql = "SELECT FORMAT_DATETIME('%b-%d-%Y', datetime_field) FROM table_with_datetime";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_datetime_str").build())
                .addValues("Dec-25-2008")
                .build(),
            Row.withSchema(Schema.builder().addStringField("f_datetime_str").build())
                .addValues("Oct-06-2012")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testGroupByDateTime() {
    String sql = "SELECT datetime_field, COUNT(*) FROM table_with_datetime GROUP BY datetime_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addLogicalTypeField("datetime_field", SqlTypes.DATETIME)
            .addInt64Field("count")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(LocalDateTime.of(2008, 12, 25, 15, 30, 0), 1L).build(),
            Row.withSchema(schema).addValues(LocalDateTime.of(2012, 10, 6, 11, 45, 0), 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAggregateOnDateTime() {
    String sql = "SELECT MAX(datetime_field) FROM table_with_datetime GROUP BY str_field";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("datetime_field", SqlTypes.DATETIME)
                        .build())
                .addValues(LocalDateTime.of(2012, 10, 6, 11, 45, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // TODO[BEAM-9166]: Add a test for CURRENT_DATETIME function ("SELECT CURRENT_DATETIME()")

  @Test
  public void testExtractFromDateTime() {
    String sql =
        "SELECT "
            + "EXTRACT(YEAR FROM DATETIME '2008-12-25 15:30:00') as year, "
            + "EXTRACT(MONTH FROM DATETIME '2008-12-25 15:30:00') as month, "
            + "EXTRACT(WEEK FROM DATETIME '2008-12-25 15:30:00') as week, "
            + "EXTRACT(DAY FROM DATETIME '2008-12-25 15:30:00') as day, "
            + "EXTRACT(DAYOFWEEK FROM DATETIME '2008-12-25 15:30:00') as dayofweek, "
            + "EXTRACT(DATE FROM DATETIME '2008-12-25 15:30:00') as date, "
            + "EXTRACT(TIME FROM DATETIME '2008-12-25 15:30:00') as time, "
            + "EXTRACT(HOUR FROM DATETIME '2008-12-25 15:30:00.123456') as hour, "
            + "EXTRACT(MINUTE FROM DATETIME '2008-12-25 15:30:00.123456') as minute, "
            + "EXTRACT(SECOND FROM DATETIME '2008-12-25 15:30:00.123456') as second, "
            + "EXTRACT(MILLISECOND FROM DATETIME '2008-12-25 15:30:00.123456') as millisecond, "
            + "EXTRACT(MICROSECOND FROM DATETIME '2008-12-25 15:30:00.123456') as microsecond ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("hour")
            .addInt64Field("minute")
            .addInt64Field("second")
            .addInt64Field("millisecond")
            .addInt64Field("microsecond")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(schema).addValues(15L, 30L, 35L, 123L, 123456L).build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeFromDateAndTime() {
    String sql = "SELECT DATETIME(DATE '2008-12-25', TIME '15:30:00.123456')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 25, 15, 30, 0).withNano(123456000))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeFromDate() {
    String sql = "SELECT DATETIME(DATE '2008-12-25')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 25, 0, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeFromYearMonthDayHourMinuteSecond() {
    String sql = "SELECT DATETIME(2008, 12, 25, 15, 30, 0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 25, 15, 30, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeFromTimestamp() {
    String sql = "SELECT DATETIME(TIMESTAMP '2008-12-25 15:30:00+08', 'America/Los_Angeles')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 24, 23, 30, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeAdd() {
    String sql =
        "SELECT "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MICROSECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MILLISECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 SECOND), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 MINUTE), "
            + "TIME_ADD(TIME '15:30:00', INTERVAL 10 HOUR) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_time1", SqlTypes.TIME)
                        .addLogicalTypeField("f_time2", SqlTypes.TIME)
                        .addLogicalTypeField("f_time3", SqlTypes.TIME)
                        .addLogicalTypeField("f_time4", SqlTypes.TIME)
                        .addLogicalTypeField("f_time5", SqlTypes.TIME)
                        .build())
                .addValues(
                    LocalTime.of(15, 30, 0, 10000),
                    LocalTime.of(15, 30, 0, 10000000),
                    LocalTime.of(15, 30, 10, 0),
                    LocalTime.of(15, 40, 0, 0),
                    LocalTime.of(1, 30, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeAddWithParameter() {
    String sql = "SELECT DATETIME_ADD(@p0, INTERVAL @p1 HOUR)";

    LocalDateTime datetime = LocalDateTime.of(2008, 12, 25, 15, 30, 00).withNano(123456000);
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0",
                Value.createDatetimeValue(
                    CivilTimeEncoder.encodePacked64DatetimeSeconds(datetime), datetime.getNano()),
            "p1", Value.createInt64Value(3L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_datetime", SqlTypes.DATETIME).build())
                .addValues(LocalDateTime.of(2008, 12, 25, 18, 30, 00).withNano(123456000))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeSub() {
    String sql =
        "SELECT "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MICROSECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MILLISECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 SECOND), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 MINUTE), "
            + "TIME_SUB(TIME '15:30:00', INTERVAL 10 HOUR) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addLogicalTypeField("f_time1", SqlTypes.TIME)
                        .addLogicalTypeField("f_time2", SqlTypes.TIME)
                        .addLogicalTypeField("f_time3", SqlTypes.TIME)
                        .addLogicalTypeField("f_time4", SqlTypes.TIME)
                        .addLogicalTypeField("f_time5", SqlTypes.TIME)
                        .build())
                .addValues(
                    LocalTime.of(15, 29, 59, 999990000),
                    LocalTime.of(15, 29, 59, 990000000),
                    LocalTime.of(15, 29, 50, 0),
                    LocalTime.of(15, 20, 0, 0),
                    LocalTime.of(5, 30, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeDiff() {
    String sql = "SELECT TIME_DIFF(TIME '15:30:00', TIME '14:35:00', MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_time_diff").build())
                .addValues(55L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeDiffNegativeResult() {
    String sql = "SELECT TIME_DIFF(TIME '14:35:00', TIME '15:30:00', MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_time_diff").build())
                .addValues(-55L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateTimeTrunc() {
    String sql = "SELECT TIME_TRUNC(TIME '15:30:35', HOUR)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addLogicalTypeField("f_time_trunc", SqlTypes.TIME).build())
                .addValues(LocalTime.of(15, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFormatDateTime() {
    String sql = "SELECT FORMAT_TIME('%R', TIME '15:30:00')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_time_str").build())
                .addValues("15:30")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParseDateTime() {
    String sql = "SELECT PARSE_TIME('%H', '15')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("f_time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(15, 0, 0))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // TIMESTAMP type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testTimestampMicrosecondUnsupported() {
    String sql =
        "WITH Timestamps AS (\n"
            + "  SELECT TIMESTAMP '2000-01-01 00:11:22.345678+00' as timestamp\n"
            + ")\n"
            + "SELECT\n"
            + "  timestamp,\n"
            + "  EXTRACT(ISOYEAR FROM timestamp) AS isoyear,\n"
            + "  EXTRACT(YEAR FROM timestamp) AS year,\n"
            + "  EXTRACT(ISOWEEK FROM timestamp) AS week,\n"
            + "  EXTRACT(MINUTE FROM timestamp) AS minute\n"
            + "FROM Timestamps\n";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(UnsupportedOperationException.class);
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testTimestampLiteralWithoutTimeZone() {
    String sql = "SELECT TIMESTAMP '2016-12-25 05:30:00'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("field1").build())
                .addValues(parseTimestampWithUTCTimeZone("2016-12-25 05:30:00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampLiteralWithUTCTimeZone() {
    String sql = "SELECT TIMESTAMP '2016-12-25 05:30:00+00'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("field1").build())
                .addValues(parseTimestampWithUTCTimeZone("2016-12-25 05:30:00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampLiteralWithNonUTCTimeZone() {
    String sql = "SELECT TIMESTAMP '2018-12-10 10:38:59-10:00'";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp_with_time_zone").build())
                .addValues(parseTimestampWithTimeZone("2018-12-10 10:38:59-1000"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // TODO[BEAM-9166]: Add a test for CURRENT_TIMESTAMP function ("SELECT CURRENT_TIMESTAMP()")

  @Test
  public void testExtractFromTimestamp() {
    String sql =
        "WITH Timestamps AS (\n"
            + "  SELECT TIMESTAMP '2007-12-31 12:34:56.789' AS timestamp UNION ALL\n"
            + "  SELECT TIMESTAMP '2009-12-31'\n"
            + ")\n"
            + "SELECT\n"
            + "  EXTRACT(ISOYEAR FROM timestamp) AS isoyear,\n"
            + "  EXTRACT(YEAR FROM timestamp) AS year,\n"
            + "  EXTRACT(ISOWEEK FROM timestamp) AS isoweek,\n"
            // TODO[BEAM-9178]: Add tests for TIMESTAMP_TRUNC and EXTRACT with "week with weekday"
            //  date parts once they are supported
            // + "  EXTRACT(WEEK FROM timestamp) AS week,\n"
            + "  EXTRACT(MONTH FROM timestamp) AS month,\n"
            + "  EXTRACT(QUARTER FROM timestamp) AS quarter,\n"
            + "  EXTRACT(DAY FROM timestamp) AS day,\n"
            + "  EXTRACT(DAYOFYEAR FROM timestamp) AS dayofyear,\n"
            + "  EXTRACT(DAYOFWEEK FROM timestamp) AS dayofweek,\n"
            + "  EXTRACT(HOUR FROM timestamp) AS hour,\n"
            + "  EXTRACT(MINUTE FROM timestamp) AS minute,\n"
            + "  EXTRACT(SECOND FROM timestamp) AS second,\n"
            + "  EXTRACT(MILLISECOND FROM timestamp) AS millisecond\n"
            + "FROM Timestamps";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("isoyear")
            .addInt64Field("year")
            .addInt64Field("isoweek")
            // .addInt64Field("week")
            .addInt64Field("month")
            .addInt64Field("quarter")
            .addInt64Field("day")
            .addInt64Field("dayofyear")
            .addInt64Field("dayofweek")
            .addInt64Field("hour")
            .addInt64Field("minute")
            .addInt64Field("second")
            .addInt64Field("millisecond")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(
                    2008L, 2007L, 1L /* , 53L */, 12L, 4L, 31L, 365L, 2L, 12L, 34L, 56L, 789L)
                .build(),
            Row.withSchema(schema)
                .addValues(2009L, 2009L, 53L /* , 52L */, 12L, 4L, 31L, 365L, 5L, 0L, 0L, 0L, 0L)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExtractDateFromTimestamp() {
    String sql = "SELECT EXTRACT(DATE FROM TIMESTAMP '2017-05-26 12:34:56')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("date", SqlTypes.DATE).build())
                .addValues(LocalDate.of(2017, 5, 26))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExtractTimeFromTimestamp() {
    String sql = "SELECT EXTRACT(TIME FROM TIMESTAMP '2017-05-26 12:34:56')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addLogicalTypeField("time", SqlTypes.TIME).build())
                .addValues(LocalTime.of(12, 34, 56))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExtractFromTimestampAtTimeZone() {
    String sql =
        "WITH Timestamps AS (\n"
            + "  SELECT TIMESTAMP '2007-12-31 12:34:56.789' AS timestamp\n"
            + ")\n"
            + "SELECT\n"
            + "  EXTRACT(DAY FROM timestamp AT TIME ZONE 'America/Vancouver') AS day,\n"
            + "  EXTRACT(DATE FROM timestamp AT TIME ZONE 'UTC') AS date,\n"
            + "  EXTRACT(TIME FROM timestamp AT TIME ZONE 'Asia/Shanghai') AS time\n"
            + "FROM Timestamps";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema =
        Schema.builder()
            .addInt64Field("day")
            .addLogicalTypeField("date", SqlTypes.DATE)
            .addLogicalTypeField("time", SqlTypes.TIME)
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(31L, LocalDate.of(2007, 12, 31), LocalTime.of(20, 34, 56, 789000000))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testStringFromTimestamp() {
    String sql = "SELECT STRING(TIMESTAMP '2008-12-25 15:30:00', 'America/Los_Angeles')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_timestamp_string").build())
                .addValues("2008-12-25 07:30:00-08")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampFromString() {
    String sql = "SELECT TIMESTAMP('2008-12-25 15:30:00', 'America/Los_Angeles')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp").build())
                .addValues(parseTimestampWithTimeZone("2008-12-25 15:30:00-08"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampFromDate() {
    String sql = "SELECT TIMESTAMP(DATE '2014-01-31')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp").build())
                .addValues(parseTimestampWithTimeZone("2014-01-31 00:00:00+00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  // test default timezone works properly in query execution stage
  public void testTimestampFromDateWithDefaultTimezoneSet() {
    String sql = "SELECT TIMESTAMP(DATE '2014-01-31')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    zetaSQLQueryPlanner.setDefaultTimezone("Asia/Shanghai");
    pipeline
        .getOptions()
        .as(BeamSqlPipelineOptions.class)
        .setZetaSqlDefaultTimezone("Asia/Shanghai");

    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp").build())
                .addValues(parseTimestampWithTimeZone("2014-01-31 00:00:00+08"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampAdd() {
    String sql =
        "SELECT "
            + "TIMESTAMP_ADD(TIMESTAMP '2008-12-25 15:30:00 UTC', INTERVAL 5+5 MINUTE), "
            + "TIMESTAMP_ADD(TIMESTAMP '2008-12-25 15:30:00+07:30', INTERVAL 10 MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDateTimeField("f_timestamp_add")
                        .addDateTimeField("f_timestamp_with_time_zone_add")
                        .build())
                .addValues(
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:40:00"),
                    parseTimestampWithTimeZone("2008-12-25 15:40:00+0730"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampAddWithParameter1() {
    String sql = "SELECT TIMESTAMP_ADD(@p0, INTERVAL @p1 MILLISECOND)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", parseTimestampWithTZToValue("2001-01-01 00:00:00+00"),
            "p1", Value.createInt64Value(1L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addDateTimeField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(parseTimestampWithTimeZone("2001-01-01 00:00:00.001+00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampAddWithParameter2() {
    String sql = "SELECT TIMESTAMP_ADD(@p0, INTERVAL @p1 MINUTE)";
    ImmutableMap<String, Value> params =
        ImmutableMap.of(
            "p0", parseTimestampWithTZToValue("2008-12-25 15:30:00+07:30"),
            "p1", Value.createInt64Value(10L));

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql, params);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addDateTimeField("field1").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(parseTimestampWithTimeZone("2008-12-25 15:40:00+07:30"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampSub() {
    String sql =
        "SELECT "
            + "TIMESTAMP_SUB(TIMESTAMP '2008-12-25 15:30:00 UTC', INTERVAL 5+5 MINUTE), "
            + "TIMESTAMP_SUB(TIMESTAMP '2008-12-25 15:30:00+07:30', INTERVAL 10 MINUTE)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDateTimeField("f_timestamp_sub")
                        .addDateTimeField("f_timestamp_with_time_zone_sub")
                        .build())
                .addValues(
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:20:00"),
                    parseTimestampWithTimeZone("2008-12-25 15:20:00+0730"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampDiff() {
    String sql =
        "SELECT TIMESTAMP_DIFF("
            + "TIMESTAMP '2018-10-14 15:30:00.000 UTC', "
            + "TIMESTAMP '2018-08-14 15:05:00.001 UTC', "
            + "MILLISECOND)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_timestamp_diff").build())
                .addValues((61L * 24 * 60 + 25) * 60 * 1000 - 1)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampDiffNegativeResult() {
    String sql = "SELECT TIMESTAMP_DIFF(TIMESTAMP '2018-08-14', TIMESTAMP '2018-10-14', DAY)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_timestamp_diff").build())
                .addValues(-61L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampTrunc() {
    String sql = "SELECT TIMESTAMP_TRUNC(TIMESTAMP '2017-11-06 00:00:00+12', ISOWEEK, 'UTC')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp_trunc").build())
                .addValues(DateTimeUtils.parseTimestampWithUTCTimeZone("2017-10-30 00:00:00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFormatTimestamp() {
    String sql = "SELECT FORMAT_TIMESTAMP('%D %T', TIMESTAMP '2018-10-14 15:30:00.123+00', 'UTC')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addStringField("f_timestamp_str").build())
                .addValues("10/14/18 15:30:00")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testParseTimestamp() {
    String sql = "SELECT PARSE_TIMESTAMP('%m-%d-%y %T', '10-14-18 15:30:00', 'UTC')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDateTimeField("f_timestamp").build())
                .addValues(DateTimeUtils.parseTimestampWithUTCTimeZone("2018-10-14 15:30:00"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampFromInt64() {
    String sql = "SELECT TIMESTAMP_SECONDS(1230219000), TIMESTAMP_MILLIS(1230219000123) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDateTimeField("f_timestamp_seconds")
                        .addDateTimeField("f_timestamp_millis")
                        .build())
                .addValues(
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:30:00"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:30:00.123"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampToUnixInt64() {
    String sql =
        "SELECT "
            + "UNIX_SECONDS(TIMESTAMP '2008-12-25 15:30:00 UTC'), "
            + "UNIX_MILLIS(TIMESTAMP '2008-12-25 15:30:00.123 UTC')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt64Field("f_unix_seconds")
                        .addInt64Field("f_unix_millis")
                        .build())
                .addValues(1230219000L, 1230219000123L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTimestampFromUnixInt64() {
    String sql =
        "SELECT "
            + "TIMESTAMP_FROM_UNIX_SECONDS(1230219000), "
            + "TIMESTAMP_FROM_UNIX_MILLIS(1230219000123) ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDateTimeField("f_timestamp_seconds")
                        .addDateTimeField("f_timestamp_millis")
                        .build())
                .addValues(
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:30:00"),
                    DateTimeUtils.parseTimestampWithUTCTimeZone("2008-12-25 15:30:00.123"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
