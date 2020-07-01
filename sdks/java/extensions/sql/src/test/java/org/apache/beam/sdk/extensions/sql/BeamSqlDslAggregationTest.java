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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithoutTimeZone;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for GROUP-BY/aggregation, with global_window/fix_time_window/sliding_window/session_window
 * with BOUNDED PCollection.
 */
public class BeamSqlDslAggregationTest extends BeamSqlDslBase {
  public PCollection<Row> boundedInput3;

  @Before
  public void setUp() {
    Schema schemaInTableB =
        Schema.builder()
            .addInt32Field("f_int")
            .addDoubleField("f_double")
            .addInt32Field("f_int2")
            .addDecimalField("f_decimal")
            .build();

    List<Row> rowsInTableB =
        TestUtils.RowsBuilder.of(schemaInTableB)
            .addRows(
                1,
                1.0,
                0,
                new BigDecimal(1),
                4,
                4.0,
                0,
                new BigDecimal(4),
                7,
                7.0,
                0,
                new BigDecimal(7),
                13,
                13.0,
                0,
                new BigDecimal(13),
                5,
                5.0,
                0,
                new BigDecimal(5),
                10,
                10.0,
                0,
                new BigDecimal(10),
                17,
                17.0,
                0,
                new BigDecimal(17))
            .getRows();

    boundedInput3 =
        pipeline.apply("boundedInput3", Create.of(rowsInTableB).withRowSchema(schemaInTableB));
  }

  /** GROUP-BY with single aggregation function with bounded PCollection. */
  @Test
  public void testAggregationWithoutWindowWithBounded() throws Exception {
    runAggregationWithoutWindow(boundedInput1);
  }

  /** GROUP-BY with single aggregation function with unbounded PCollection. */
  @Test
  public void testAggregationWithoutWindowWithUnbounded() throws Exception {
    runAggregationWithoutWindow(unboundedInput1);
  }

  private void runAggregationWithoutWindow(PCollection<Row> input) throws Exception {
    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount` FROM PCOLLECTION GROUP BY f_int2";

    PCollection<Row> result = input.apply("testAggregationWithoutWindow", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("f_int2").addInt64Field("size").build();

    Row row = Row.withSchema(resultType).addValues(0, 4L).build();

    PAssert.that(result).containsInAnyOrder(row);

    pipeline.run().waitUntilFinish();
  }

  /** GROUP-BY with multiple aggregation functions with bounded PCollection. */
  @Test
  public void testAggregationFunctionsWithBounded() throws Exception {
    runAggregationFunctions(boundedInput1);
  }

  /** GROUP-BY with multiple aggregation functions with unbounded PCollection. */
  @Test
  public void testAggregationFunctionsWithUnbounded() throws Exception {
    runAggregationFunctions(unboundedInput1);
  }

  private void runAggregationFunctions(PCollection<Row> input) throws Exception {
    String sql =
        "select f_int2, count(*) as getFieldCount, "
            + "sum(f_long) as sum1, avg(f_long) as avg1, "
            + "max(f_long) as max1, min(f_long) as min1, "
            + "sum(f_short) as sum2, avg(f_short) as avg2, "
            + "max(f_short) as max2, min(f_short) as min2, "
            + "sum(f_byte) as sum3, avg(f_byte) as avg3, "
            + "max(f_byte) as max3, min(f_byte) as min3, "
            + "sum(f_float) as sum4, avg(f_float) as avg4, "
            + "max(f_float) as max4, min(f_float) as min4, "
            + "sum(f_double) as sum5, avg(f_double) as avg5, "
            + "max(f_double) as max5, min(f_double) as min5, "
            + "max(f_timestamp) as max6, min(f_timestamp) as min6, "
            + "max(f_string) as max7, min(f_string) as min7, "
            + "var_pop(f_double) as varpop1, var_samp(f_double) as varsamp1, "
            + "var_pop(f_int) as varpop2, var_samp(f_int) as varsamp2 "
            + "FROM TABLE_A group by f_int2";

    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testAggregationFunctions", SqlTransform.query(sql));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int2")
            .addInt64Field("size")
            .addInt64Field("sum1")
            .addInt64Field("avg1")
            .addInt64Field("max1")
            .addInt64Field("min1")
            .addInt16Field("sum2")
            .addInt16Field("avg2")
            .addInt16Field("max2")
            .addInt16Field("min2")
            .addByteField("sum3")
            .addByteField("avg3")
            .addByteField("max3")
            .addByteField("min3")
            .addFloatField("sum4")
            .addFloatField("avg4")
            .addFloatField("max4")
            .addFloatField("min4")
            .addDoubleField("sum5")
            .addDoubleField("avg5")
            .addDoubleField("max5")
            .addDoubleField("min5")
            .addDateTimeField("max6")
            .addDateTimeField("min6")
            .addStringField("max7")
            .addStringField("min7")
            .addDoubleField("varpop1")
            .addDoubleField("varsamp1")
            .addInt32Field("varpop2")
            .addInt32Field("varsamp2")
            .build();

    Row row =
        Row.withSchema(resultType)
            .addValues(
                0,
                4L,
                10000L,
                2500L,
                4000L,
                1000L,
                (short) 10,
                (short) 2,
                (short) 4,
                (short) 1,
                (byte) 10,
                (byte) 2,
                (byte) 4,
                (byte) 1,
                10.0F,
                2.5F,
                4.0F,
                1.0F,
                10.0,
                2.5,
                4.0,
                1.0,
                parseTimestampWithoutTimeZone("2017-01-01 02:04:03"),
                parseTimestampWithoutTimeZone("2017-01-01 01:01:03"),
                "第四行",
                "string_row1",
                1.25,
                1.666666667,
                1,
                1)
            .build();

    PAssert.that(result).containsInAnyOrder(row);

    pipeline.run().waitUntilFinish();
  }

  /** GROUP-BY with the any_value aggregation function. */
  @Test
  public void testAnyValueFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    Schema schema = Schema.builder().addInt32Field("key").addInt32Field("col").build();

    PCollection<Row> inputRows =
        pipeline
            .apply(
                Create.of(
                    TestUtils.rowsBuilderOf(schema)
                        .addRows(
                            0, 1,
                            0, 2,
                            1, 3,
                            2, 4,
                            2, 5)
                        .getRows()))
            .setRowSchema(schema);

    String sql = "SELECT key, any_value(col) as any_value FROM PCOLLECTION GROUP BY key";

    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));

    Map<Integer, List<Integer>> allowedTuples = new HashMap<>();
    allowedTuples.put(0, Arrays.asList(1, 2));
    allowedTuples.put(1, Arrays.asList(3));
    allowedTuples.put(2, Arrays.asList(4, 5));

    PAssert.that(result)
        .satisfies(
            input -> {
              Iterator<Row> iter = input.iterator();
              while (iter.hasNext()) {
                Row row = iter.next();
                List<Integer> values = allowedTuples.remove(row.getInt32("key"));
                assertTrue(values != null);
                assertTrue(values.contains(row.getInt32("any_value")));
              }
              assertTrue(allowedTuples.isEmpty());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testBitOrFunction() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    Schema schemaInTableA =
        Schema.builder().addInt64Field("f_long").addInt32Field("f_int2").build();

    Schema resultType = Schema.builder().addInt64Field("finalAnswer").build();

    List<Row> rowsInTableA =
        TestUtils.RowsBuilder.of(schemaInTableA)
            .addRows(
                0xF001L, 0,
                0x00A1L, 0,
                44L, 0)
            .getRows();

    String sql = "SELECT bit_or(f_long) as bitor " + "FROM PCOLLECTION GROUP BY f_int2";

    Row rowResult = Row.withSchema(resultType).addValues(61613L).build();

    PCollection<Row> inputRows =
        pipeline.apply("longVals", Create.of(rowsInTableA).withRowSchema(schemaInTableA));
    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));

    PAssert.that(result).containsInAnyOrder(rowResult);

    pipeline.run().waitUntilFinish();
  }

  /**
   * NULL values don't work correctly. (https://issues.apache.org/jira/browse/BEAM-10379)
   *
   * <p>Comment the following tests for BitAnd for now
   */
  //  @Test
  //  public void testBitAndFunction() throws Exception {
  //    pipeline.enableAbandonedNodeEnforcement(false);
  //
  //    Schema schemaInTableA =
  //        Schema.builder().addInt64Field("f_long").addInt32Field("f_int2").build();
  //
  //    Schema resultType = Schema.builder().addInt64Field("finalAnswer").build();
  //
  //    List<Row> rowsInTableA =
  //        TestUtils.RowsBuilder.of(schemaInTableA)
  //            .addRows(
  //                0xF001L, 0,
  //                0x00A1L, 0)
  //            .getRows();
  //
  //    String sql = "SELECT bit_and(f_long) as bitand " + "FROM PCOLLECTION GROUP BY f_int2";
  //
  //    Row rowResult = Row.withSchema(resultType).addValues(1L).build();
  //
  //    PCollection<Row> inputRows =
  //        pipeline.apply("longVals", Create.of(rowsInTableA).withRowSchema(schemaInTableA));
  //    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));
  //
  //    PAssert.that(result).containsInAnyOrder(rowResult);
  //
  //    pipeline.run().waitUntilFinish();
  //  }
  //
  //  @Test
  //  public void testBitAndFunctionWithEmptyTable() throws Exception {
  //    pipeline.enableAbandonedNodeEnforcement(false);
  //
  //    Schema schemaInTableA =
  //        Schema.builder()
  //            .addNullableField("f_long", Schema.FieldType.INT64)
  //            .addNullableField("f_int2", Schema.FieldType.INT32)
  //            .build();
  //
  //    List<Row> rowsInTableA = TestUtils.RowsBuilder.of(schemaInTableA).getRows();
  //
  //    String sql = "SELECT bit_and(f_long) as bitand " + "FROM PCOLLECTION GROUP BY f_int2";
  //
  //    PCollection<Row> inputRows =
  //        pipeline.apply("longVals", Create.of(rowsInTableA).withRowSchema(schemaInTableA));
  //    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));
  //
  //    PAssert.that(result).empty();
  //
  //    pipeline.run().waitUntilFinish();
  //  }
  //
  //  @Test
  //  public void testBitAndFunctionWithOnlyNullInput() throws Exception {
  //    pipeline.enableAbandonedNodeEnforcement(false);
  //
  //    Schema schemaInTableA =
  //        Schema.builder()
  //            .addNullableField("f_long", Schema.FieldType.INT64)
  //            .addNullableField("f_int2", Schema.FieldType.INT32)
  //            .build();
  //
  //    Schema resultType =
  //        Schema.builder().addNullableField("finalAnswer", Schema.FieldType.INT64).build();
  //
  //    List<Row> rowsInTableA = TestUtils.RowsBuilder.of(schemaInTableA).addRows(null,
  // 0).getRows();
  //
  //    String sql = "SELECT bit_and(f_long) as bitand " + "FROM PCOLLECTION GROUP BY f_int2";
  //
  //    Row rowResult = Row.withSchema(resultType).addValues((Long) null).build();
  //
  //    PCollection<Row> inputRows =
  //        pipeline.apply("longVals", Create.of(rowsInTableA).withRowSchema(schemaInTableA));
  //    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));
  //
  //    PAssert.that(result).containsInAnyOrder(rowResult);
  //
  //    //    The following attempts which try printing the rows in PCollection failed even if using
  //    // setCoder() or setRowSchema() at the end.
  //    //    To try running them, uncomment the code and import required data structures such as
  // ParDo
  //    // and DoFn.
  //
  //    //    PCollection<Row> a = result.apply(ParDo.of(new DoFn<Row, Row>() {    // a DoFn as an
  //    // anonymous inner class instance
  //    //      @ProcessElement
  //    //      public void processElement(@Element Row row, OutputReceiver<Row> out) {
  //    //        System.out.println(row);
  //    //        out.output(row);
  //    //      }
  //    //    })).setCoder(result.getCoder());
  //    //    PAssert.that(a).containsInAnyOrder(rowResult);
  //    //
  //    //    PCollection<Row> b = result.apply(ParDo.of(new DoFn<Row, Row>() {    // a DoFn as an
  //    // anonymous inner class instance
  //    //      @ProcessElement
  //    //      public void processElement(@Element Row row, OutputReceiver<Row> out) {
  //    //        System.out.println(row);
  //    //        out.output(row);
  //    //      }
  //    //    })).setRowSchema(result.getSchema());
  //    //    PAssert.that(b).containsInAnyOrder(rowResult);
  //
  //    pipeline.run().waitUntilFinish();
  //  }
  //
  //  /**
  //   * The following commented test checks the condition when inputs contain both null value and
  // valid
  //   * INT64 values.
  //   *
  //   * <p>It never passes, when null value and valid value both exist, null values seemed to be
  //   * directly ignored.
  //   */
  //  //  @Test
  //  //  public void testBitAndFunctionWithNullInputAndValueInput() throws Exception {
  //  //    pipeline.enableAbandonedNodeEnforcement(false);
  //  //
  //  //    Schema schemaInTableA =
  //  //            Schema.builder()
  //  //                    .addNullableField("f_long", Schema.FieldType.INT64)
  //  //                    .addNullableField("f_int2", Schema.FieldType.INT32)
  //  //                    .build();
  //  //
  //  //    Schema resultType = Schema.builder().addNullableField("finalAnswer",
  //  // Schema.FieldType.INT64).build();
  //  //
  //  //    List<Row> rowsInTableA =
  //  //            TestUtils.RowsBuilder.of(schemaInTableA)
  //  //                    .addRows(null, 0)
  //  //                    .addRows(1L, 0)
  //  //                    .getRows();
  //  //
  //  //    String sql = "SELECT bit_and(f_long) as bitand " + "FROM PCOLLECTION GROUP BY f_int2";
  //  //
  //  //    Row rowResult = Row.withSchema(resultType).addValues((Long) null).build();
  //  //
  //  //    PCollection<Row> inputRows =
  //  //            pipeline.apply("longVals",
  // Create.of(rowsInTableA).withRowSchema(schemaInTableA));
  //  //    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));
  //  //
  //  //    PAssert.that(result).containsInAnyOrder(rowResult);
  //  //
  //  //    pipeline.run().waitUntilFinish();
  //  //  }

  private static class CheckerBigDecimalDivide
      implements SerializableFunction<Iterable<Row>, Void> {

    @Override
    public Void apply(Iterable<Row> input) {
      Iterator<Row> iter = input.iterator();
      assertTrue(iter.hasNext());
      Row row = iter.next();
      assertEquals(row.getDouble("avg1"), 8.142857143, 1e-7);
      assertTrue(row.getInt32("avg2") == 8);
      assertEquals(row.getDouble("varpop1"), 26.40816326, 1e-7);
      assertTrue(row.getInt32("varpop2") == 26);
      assertEquals(row.getDouble("varsamp1"), 30.80952381, 1e-7);
      assertTrue(row.getInt32("varsamp2") == 30);
      assertFalse(iter.hasNext());
      return null;
    }
  }

  /** GROUP-BY with aggregation functions with BigDeciaml Calculation (Avg, Var_Pop, etc). */
  @Test
  public void testAggregationFunctionsWithBoundedOnBigDecimalDivide() throws Exception {
    String sql =
        "SELECT AVG(f_double) as avg1, AVG(f_int) as avg2, "
            + "VAR_POP(f_double) as varpop1, VAR_POP(f_int) as varpop2, "
            + "VAR_SAMP(f_double) as varsamp1, VAR_SAMP(f_int) as varsamp2 "
            + "FROM PCOLLECTION GROUP BY f_int2";

    PCollection<Row> result =
        boundedInput3.apply("testAggregationWithDecimalValue", SqlTransform.query(sql));

    PAssert.that(result).satisfies(new CheckerBigDecimalDivide());

    pipeline.run().waitUntilFinish();
  }

  /** Implicit GROUP-BY with DISTINCT with bounded PCollection. */
  @Test
  public void testDistinctWithBounded() throws Exception {
    runDistinct(boundedInput1);
  }

  /** Implicit GROUP-BY with DISTINCT with unbounded PCollection. */
  @Test
  public void testDistinctWithUnbounded() throws Exception {
    runDistinct(unboundedInput1);
  }

  private void runDistinct(PCollection<Row> input) throws Exception {
    String sql = "SELECT distinct f_int, f_long FROM PCOLLECTION ";

    PCollection<Row> result = input.apply("testDistinct", SqlTransform.query(sql));

    Schema resultType = Schema.builder().addInt32Field("f_int").addInt64Field("f_long").build();

    List<Row> expectedRows =
        TestUtils.RowsBuilder.of(resultType)
            .addRows(
                1, 1000L,
                2, 2000L,
                3, 3000L,
                4, 4000L)
            .getRows();

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  /** GROUP-BY with TUMBLE window(aka fix_time_window) with bounded PCollection. */
  @Test
  public void testTumbleWindowWithBounded() throws Exception {
    runTumbleWindow(boundedInput1);
  }

  /** GROUP-BY with TUMBLE window(aka fix_time_window) with unbounded PCollection. */
  @Test
  public void testTumbleWindowWithUnbounded() throws Exception {
    runTumbleWindow(unboundedInput1);
  }

  @Test
  public void testTumbleWindowWith31DaysBounded() throws Exception {
    runTumbleWindowFor31Days(boundedInputMonthly);
  }

  private void runTumbleWindowFor31Days(PCollection<Row> input) throws Exception {
    String sql =
        "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
            + " TUMBLE_START(f_timestamp, INTERVAL '31' DAY) AS `window_start`, "
            + " TUMBLE_END(f_timestamp, INTERVAL '31' DAY) AS `window_end` "
            + " FROM TABLE_A"
            + " GROUP BY f_int2, TUMBLE(f_timestamp, INTERVAL '31' DAY)";
    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testTumbleWindow", SqlTransform.query(sql));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int2")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        TestUtils.RowsBuilder.of(resultType)
            .addRows(
                0,
                1L,
                parseTimestampWithUTCTimeZone("2016-12-08 00:00:00"),
                parseTimestampWithUTCTimeZone("2017-01-08 00:00:00"),
                0,
                1L,
                parseTimestampWithUTCTimeZone("2017-01-08 00:00:00"),
                parseTimestampWithUTCTimeZone("2017-02-08 00:00:00"),
                0,
                1L,
                parseTimestampWithUTCTimeZone("2017-02-08 00:00:00"),
                parseTimestampWithUTCTimeZone("2017-03-11 00:00:00"))
            .getRows();

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  private void runTumbleWindow(PCollection<Row> input) throws Exception {
    String sql =
        "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
            + " TUMBLE_START(f_timestamp, INTERVAL '1' HOUR) AS `window_start`, "
            + " TUMBLE_END(f_timestamp, INTERVAL '1' HOUR) AS `window_end` "
            + " FROM TABLE_A"
            + " GROUP BY f_int2, TUMBLE(f_timestamp, INTERVAL '1' HOUR)";
    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testTumbleWindow", SqlTransform.query(sql));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int2")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        TestUtils.RowsBuilder.of(resultType)
            .addRows(
                0,
                3L,
                parseTimestampWithoutTimeZone("2017-01-01 01:00:00"),
                parseTimestampWithoutTimeZone("2017-01-01 02:00:00"),
                0,
                1L,
                parseTimestampWithoutTimeZone("2017-01-01 02:00:00"),
                parseTimestampWithoutTimeZone("2017-01-01 03:00:00"))
            .getRows();

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Tests that a trigger set up prior to a SQL statement still is effective within the SQL
   * statement.
   */
  @Test
  @Category(UsesTestStream.class)
  public void testTriggeredTumble() throws Exception {
    Schema inputSchema =
        Schema.builder().addInt32Field("f_int").addDateTimeField("f_timestamp").build();

    PCollection<Row> input =
        pipeline.apply(
            TestStream.create(inputSchema)
                .addElements(
                    Row.withSchema(inputSchema)
                        .addValues(1, parseTimestampWithoutTimeZone("2017-01-01 01:01:01"))
                        .build(),
                    Row.withSchema(inputSchema)
                        .addValues(2, parseTimestampWithoutTimeZone("2017-01-01 01:01:01"))
                        .build())
                .addElements(
                    Row.withSchema(inputSchema)
                        .addValues(3, parseTimestampWithoutTimeZone("2017-01-01 01:01:01"))
                        .build())
                .addElements(
                    Row.withSchema(inputSchema)
                        .addValues(4, parseTimestampWithoutTimeZone("2017-01-01 01:01:01"))
                        .build())
                .advanceWatermarkToInfinity());

    String sql =
        "SELECT SUM(f_int) AS f_int_sum FROM PCOLLECTION"
            + " GROUP BY TUMBLE(f_timestamp, INTERVAL '1' HOUR)";

    Schema outputSchema = Schema.builder().addInt32Field("fn_int_sum").build();

    PCollection<Row> result =
        input
            .apply(
                "Triggering",
                Window.<Row>configure()
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                    .accumulatingFiredPanes())
            .apply("Windowed Query", SqlTransform.query(sql));

    PAssert.that(result)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(outputSchema)
                .addRows(3) // first bundle 1+2
                .addRows(6) // next bundle 1+2+3
                .addRows(10) // next bundle 1+2+3+4)
                .getRows());

    pipeline.run().waitUntilFinish();
  }

  /** GROUP-BY with HOP window(aka sliding_window) with bounded PCollection. */
  @Test
  public void testHopWindowWithBounded() throws Exception {
    runHopWindow(boundedInput1);
  }

  /** GROUP-BY with HOP window(aka sliding_window) with unbounded PCollection. */
  @Test
  public void testHopWindowWithUnbounded() throws Exception {
    runHopWindow(unboundedInput1);
  }

  private void runHopWindow(PCollection<Row> input) throws Exception {
    String sql =
        "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
            + " HOP_START(f_timestamp, INTERVAL '30' MINUTE, INTERVAL '1' HOUR) AS `window_start`, "
            + " HOP_END(f_timestamp, INTERVAL '30' MINUTE, INTERVAL '1' HOUR) AS `window_end` "
            + " FROM PCOLLECTION"
            + " GROUP BY f_int2, HOP(f_timestamp, INTERVAL '30' MINUTE, INTERVAL '1' HOUR)";
    PCollection<Row> result = input.apply("testHopWindow", SqlTransform.query(sql));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int2")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        TestUtils.RowsBuilder.of(resultType)
            .addRows(
                0,
                3L,
                parseTimestampWithoutTimeZone("2017-01-01 00:30:00"),
                parseTimestampWithoutTimeZone("2017-01-01 01:30:00"),
                0,
                3L,
                parseTimestampWithoutTimeZone("2017-01-01 01:00:00"),
                parseTimestampWithoutTimeZone("2017-01-01 02:00:00"),
                0,
                1L,
                parseTimestampWithoutTimeZone("2017-01-01 01:30:00"),
                parseTimestampWithoutTimeZone("2017-01-01 02:30:00"),
                0,
                1L,
                parseTimestampWithoutTimeZone("2017-01-01 02:00:00"),
                parseTimestampWithoutTimeZone("2017-01-01 03:00:00"))
            .getRows();

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  /** GROUP-BY with SESSION window with bounded PCollection. */
  @Test
  public void testSessionWindowWithBounded() throws Exception {
    runSessionWindow(boundedInput1);
  }

  /** GROUP-BY with SESSION window with unbounded PCollection. */
  @Test
  public void testSessionWindowWithUnbounded() throws Exception {
    runSessionWindow(unboundedInput1);
  }

  private void runSessionWindow(PCollection<Row> input) throws Exception {
    String sql =
        "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
            + " SESSION_START(f_timestamp, INTERVAL '5' MINUTE) AS `window_start`, "
            + " SESSION_END(f_timestamp, INTERVAL '5' MINUTE) AS `window_end` "
            + " FROM TABLE_A"
            + " GROUP BY f_int2, SESSION(f_timestamp, INTERVAL '5' MINUTE)";
    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), input)
            .apply("testSessionWindow", SqlTransform.query(sql));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int2")
            .addInt64Field("size")
            .addDateTimeField("window_start")
            .addDateTimeField("window_end")
            .build();

    List<Row> expectedRows =
        TestUtils.RowsBuilder.of(resultType)
            .addRows(
                0,
                3L,
                parseTimestampWithoutTimeZone("2017-01-01 01:01:03"),
                parseTimestampWithoutTimeZone("2017-01-01 01:01:03"),
                0,
                1L,
                parseTimestampWithoutTimeZone("2017-01-01 02:04:03"),
                parseTimestampWithoutTimeZone("2017-01-01 02:04:03"))
            .getRows();

    PAssert.that(result).containsInAnyOrder(expectedRows);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWindowOnNonTimestampField() throws Exception {
    exceptions.expect(ParseException.class);
    exceptions.expectCause(
        hasMessage(
            containsString(
                "Cannot apply 'TUMBLE' to arguments of type 'TUMBLE(<BIGINT>, <INTERVAL HOUR>)'")));
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql =
        "SELECT f_int2, COUNT(*) AS `getFieldCount` FROM TABLE_A "
            + "GROUP BY f_int2, TUMBLE(f_long, INTERVAL '1' HOUR)";
    PCollection<Row> result =
        PCollectionTuple.of(new TupleTag<>("TABLE_A"), boundedInput1)
            .apply("testWindowOnNonTimestampField", SqlTransform.query(sql));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedDistinct() throws Exception {
    exceptions.expect(ParseException.class);
    exceptions.expectCause(hasMessage(containsString("Encountered \"*\"")));
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int2, COUNT(DISTINCT *) AS `size` " + "FROM PCOLLECTION GROUP BY f_int2";

    PCollection<Row> result =
        boundedInput1.apply("testUnsupportedDistinct", SqlTransform.query(sql));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedDistinct2() throws Exception {
    exceptions.expect(UnsupportedOperationException.class);
    exceptions.expectMessage(containsString("COUNT DISTINCT"));
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int2, COUNT(DISTINCT f_long)" + "FROM PCOLLECTION GROUP BY f_int2";
    boundedInput1.apply("testUnsupportedDistinct", SqlTransform.query(sql));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedGlobalWindowWithDefaultTrigger() {
    exceptions.expect(UnsupportedOperationException.class);

    pipeline.enableAbandonedNodeEnforcement(false);

    PCollection<Row> input =
        unboundedInput1.apply(
            "unboundedInput1.globalWindow",
            Window.<Row>into(new GlobalWindows()).triggering(DefaultTrigger.of()));

    String sql = "SELECT f_int2, COUNT(*) AS `size` FROM PCOLLECTION GROUP BY f_int2";

    input.apply("testUnsupportedGlobalWindows", SqlTransform.query(sql));
  }

  @Test
  public void testSupportsGlobalWindowWithCustomTrigger() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    DateTime startTime = parseTimestampWithoutTimeZone("2017-1-1 0:0:0");

    Schema type =
        Schema.builder()
            .addInt32Field("f_intGroupingKey")
            .addInt32Field("f_intValue")
            .addDateTimeField("f_timestamp")
            .build();

    Object[] rows =
        new Object[] {
          0, 1, startTime.plusSeconds(0),
          0, 2, startTime.plusSeconds(1),
          0, 3, startTime.plusSeconds(2),
          0, 4, startTime.plusSeconds(3),
          0, 5, startTime.plusSeconds(4),
          0, 6, startTime.plusSeconds(6)
        };

    PCollection<Row> input =
        createTestPCollection(type, rows, "f_timestamp")
            .apply(
                Window.<Row>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .discardingFiredPanes()
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    String sql = "SELECT SUM(f_intValue) AS `sum` FROM PCOLLECTION GROUP BY f_intGroupingKey";

    PCollection<Row> result = input.apply("sql", SqlTransform.query(sql));

    assertEquals(new GlobalWindows(), result.getWindowingStrategy().getWindowFn());
    PAssert.that(result).containsInAnyOrder(rowsWithSingleIntField("sum", Arrays.asList(3, 7, 11)));

    pipeline.run();
  }

  /** Query has all the input fields, so no projection is added. */
  @Test
  public void testSupportsAggregationWithoutProjection() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    Schema schema =
        Schema.builder().addInt32Field("f_intGroupingKey").addInt32Field("f_intValue").build();

    PCollection<Row> inputRows =
        pipeline
            .apply(
                Create.of(
                    TestUtils.rowsBuilderOf(schema)
                        .addRows(
                            0, 1,
                            0, 2,
                            1, 3,
                            2, 4,
                            2, 5)
                        .getRows()))
            .setRowSchema(schema);

    String sql = "SELECT SUM(f_intValue) FROM PCOLLECTION GROUP BY f_intGroupingKey";

    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));

    PAssert.that(result).containsInAnyOrder(rowsWithSingleIntField("sum", Arrays.asList(3, 3, 9)));

    pipeline.run();
  }

  @Test
  public void testSupportsAggregationWithFilterWithoutProjection() throws Exception {
    pipeline.enableAbandonedNodeEnforcement(false);

    Schema schema =
        Schema.builder().addInt32Field("f_intGroupingKey").addInt32Field("f_intValue").build();

    PCollection<Row> inputRows =
        pipeline
            .apply(
                Create.of(
                    TestUtils.rowsBuilderOf(schema)
                        .addRows(
                            0, 1,
                            0, 2,
                            1, 3,
                            2, 4,
                            2, 5)
                        .getRows()))
            .setRowSchema(schema);

    String sql =
        "SELECT SUM(f_intValue) FROM PCOLLECTION WHERE f_intValue < 5 GROUP BY f_intGroupingKey";

    PCollection<Row> result = inputRows.apply("sql", SqlTransform.query(sql));

    PAssert.that(result).containsInAnyOrder(rowsWithSingleIntField("sum", Arrays.asList(3, 3, 4)));

    pipeline.run();
  }

  @Test
  public void testSupportsNonGlobalWindowWithCustomTrigger() {
    DateTime startTime = parseTimestampWithoutTimeZone("2017-1-1 0:0:0");

    Schema type =
        Schema.builder()
            .addInt32Field("f_intGroupingKey")
            .addInt32Field("f_intValue")
            .addDateTimeField("f_timestamp")
            .build();

    Object[] rows =
        new Object[] {
          0, 1, startTime.plusSeconds(0),
          0, 2, startTime.plusSeconds(1),
          0, 3, startTime.plusSeconds(2),
          0, 4, startTime.plusSeconds(3),
          0, 5, startTime.plusSeconds(4),
          0, 6, startTime.plusSeconds(6)
        };

    PCollection<Row> input =
        createTestPCollection(type, rows, "f_timestamp")
            .apply(
                Window.<Row>into(FixedWindows.of(Duration.standardSeconds(3)))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO)
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    String sql = "SELECT SUM(f_intValue) AS `sum` FROM PCOLLECTION GROUP BY f_intGroupingKey";

    PCollection<Row> result = input.apply("sql", SqlTransform.query(sql));

    assertEquals(
        FixedWindows.of(Duration.standardSeconds(3)), result.getWindowingStrategy().getWindowFn());

    PAssert.that(result)
        .containsInAnyOrder(rowsWithSingleIntField("sum", Arrays.asList(3, 3, 9, 6)));

    pipeline.run();
  }

  private List<Row> rowsWithSingleIntField(String fieldName, List<Integer> values) {
    return TestUtils.rowsBuilderOf(Schema.builder().addInt32Field(fieldName).build())
        .addRows(values)
        .getRows();
  }

  private PCollection<Row> createTestPCollection(
      Schema type, Object[] rows, String timestampField) {
    return TestUtils.rowsBuilderOf(type)
        .addRows(rows)
        .getPCollectionBuilder()
        .inPipeline(pipeline)
        .withTimestampField(timestampField)
        .buildUnbounded();
  }
}
