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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for GROUP-BY/aggregation, with global_window/fix_time_window/sliding_window/session_window
 * with BOUNDED PCollection.
 */
public class BeamSqlDslAggregationTest extends BeamSqlDslBase {
  public PCollection<BeamRecord> boundedInput3;

  @Before
  public void setUp(){
    BeamRecordSqlType rowTypeInTableB = BeamRecordSqlType.create(
            Arrays.asList("f_int", "f_double", "f_int2", "f_decimal"),
            Arrays.asList(Types.INTEGER, Types.DOUBLE, Types.INTEGER, Types.DECIMAL));

    List<BeamRecord> recordsInTableB = new ArrayList<>();
    BeamRecord row1 = new BeamRecord(rowTypeInTableB
            , 1, 1.0, 0, new BigDecimal(1));
    recordsInTableB.add(row1);

    BeamRecord row2 = new BeamRecord(rowTypeInTableB
            , 4, 4.0, 0, new BigDecimal(4));
    recordsInTableB.add(row2);

    BeamRecord row3 = new BeamRecord(rowTypeInTableB
            , 7, 7.0, 0, new BigDecimal(7));
    recordsInTableB.add(row3);

    BeamRecord row4 = new BeamRecord(rowTypeInTableB
            , 13, 13.0, 0, new BigDecimal(13));
    recordsInTableB.add(row4);

    BeamRecord row5 = new BeamRecord(rowTypeInTableB
            , 5, 5.0, 0, new BigDecimal(5));
    recordsInTableB.add(row5);

    BeamRecord row6 = new BeamRecord(rowTypeInTableB
            , 10, 10.0, 0, new BigDecimal(10));
    recordsInTableB.add(row6);

    BeamRecord row7 = new BeamRecord(rowTypeInTableB
            , 17, 17.0, 0, new BigDecimal(17));
    recordsInTableB.add(row7);

    boundedInput3 = PBegin.in(pipeline).apply("boundedInput3",
            Create.of(recordsInTableB).withCoder(rowTypeInTableB.getRecordCoder()));
  }

  /**
   * GROUP-BY with single aggregation function with bounded PCollection.
   */
  @Test
  public void testAggregationWithoutWindowWithBounded() throws Exception {
    runAggregationWithoutWindow(boundedInput1);
  }

  /**
   * GROUP-BY with single aggregation function with unbounded PCollection.
   */
  @Test
  public void testAggregationWithoutWindowWithUnbounded() throws Exception {
    runAggregationWithoutWindow(unboundedInput1);
  }

  private void runAggregationWithoutWindow(PCollection<BeamRecord> input) throws Exception {
    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount` FROM PCOLLECTION GROUP BY f_int2";

    PCollection<BeamRecord> result =
        input.apply("testAggregationWithoutWindow", BeamSql.query(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(Arrays.asList("f_int2", "size"),
        Arrays.asList(Types.INTEGER, Types.BIGINT));


    BeamRecord record = new BeamRecord(resultType, 0, 4L);

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }

  /**
   * GROUP-BY with multiple aggregation functions with bounded PCollection.
   */
  @Test
  public void testAggregationFunctionsWithBounded() throws Exception{
    runAggregationFunctions(boundedInput1);
  }

  /**
   * GROUP-BY with multiple aggregation functions with unbounded PCollection.
   */
  @Test
  public void testAggregationFunctionsWithUnbounded() throws Exception{
    runAggregationFunctions(unboundedInput1);
  }

  private void runAggregationFunctions(PCollection<BeamRecord> input) throws Exception{
    String sql = "select f_int2, count(*) as getFieldCount, "
        + "sum(f_long) as sum1, avg(f_long) as avg1, max(f_long) as max1, min(f_long) as min1, "
        + "sum(f_short) as sum2, avg(f_short) as avg2, max(f_short) as max2, min(f_short) as min2, "
        + "sum(f_byte) as sum3, avg(f_byte) as avg3, max(f_byte) as max3, min(f_byte) as min3, "
        + "sum(f_float) as sum4, avg(f_float) as avg4, max(f_float) as max4, min(f_float) as min4, "
        + "sum(f_double) as sum5, avg(f_double) as avg5, "
        + "max(f_double) as max5, min(f_double) as min5, "
        + "max(f_timestamp) as max6, min(f_timestamp) as min6, "
        + "var_pop(f_double) as varpop1, var_samp(f_double) as varsamp1, "
        + "var_pop(f_int) as varpop2, var_samp(f_int) as varsamp2 "
        + "FROM TABLE_A group by f_int2";

    PCollection<BeamRecord> result =
        PCollectionTuple.of(new TupleTag<BeamRecord>("TABLE_A"), input)
        .apply("testAggregationFunctions", BeamSql.queryMulti(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(
        Arrays.asList("f_int2", "size", "sum1", "avg1", "max1", "min1", "sum2", "avg2", "max2",
            "min2", "sum3", "avg3", "max3", "min3", "sum4", "avg4", "max4", "min4", "sum5", "avg5",
            "max5", "min5", "max6", "min6",
            "varpop1", "varsamp1", "varpop2", "varsamp2"),
        Arrays.asList(Types.INTEGER, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
            Types.BIGINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
            Types.TINYINT, Types.TINYINT, Types.TINYINT, Types.TINYINT, Types.FLOAT, Types.FLOAT,
            Types.FLOAT, Types.FLOAT, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE, Types.DOUBLE,
            Types.TIMESTAMP, Types.TIMESTAMP,
            Types.DOUBLE, Types.DOUBLE, Types.INTEGER, Types.INTEGER));

    BeamRecord record = new BeamRecord(resultType
        , 0, 4L
        , 10000L, 2500L, 4000L, 1000L
        , (short) 10, (short) 2, (short) 4, (short) 1
        , (byte) 10, (byte) 2, (byte) 4, (byte) 1
        , 10.0F, 2.5F, 4.0F, 1.0F
        , 10.0, 2.5, 4.0, 1.0
        , FORMAT.parse("2017-01-01 02:04:03"), FORMAT.parse("2017-01-01 01:01:03")
        , 1.25, 1.666666667, 1, 1);

    PAssert.that(result).containsInAnyOrder(record);

    pipeline.run().waitUntilFinish();
  }

  private static class CheckerBigDecimalDivide
          implements SerializableFunction<Iterable<BeamRecord>, Void> {
    @Override public Void apply(Iterable<BeamRecord> input) {
      Iterator<BeamRecord> iter = input.iterator();
      assertTrue(iter.hasNext());
      BeamRecord row = iter.next();
      assertEquals(row.getDouble("avg1"), 8.142857143, 1e-7);
      assertTrue(row.getInteger("avg2") == 8);
      assertEquals(row.getDouble("varpop1"), 26.40816326, 1e-7);
      assertTrue(row.getInteger("varpop2") == 26);
      assertEquals(row.getDouble("varsamp1"), 30.80952381, 1e-7);
      assertTrue(row.getInteger("varsamp2") == 30);
      assertFalse(iter.hasNext());
      return null;
    }
  }

  /**
   * GROUP-BY with aggregation functions with BigDeciaml Calculation (Avg, Var_Pop, etc).
   */
  @Test
  public void testAggregationFunctionsWithBoundedOnBigDecimalDivide() throws Exception {
    String sql = "SELECT AVG(f_double) as avg1, AVG(f_int) as avg2, "
            + "VAR_POP(f_double) as varpop1, VAR_POP(f_int) as varpop2, "
            + "VAR_SAMP(f_double) as varsamp1, VAR_SAMP(f_int) as varsamp2 "
            + "FROM PCOLLECTION GROUP BY f_int2";

    PCollection<BeamRecord> result =
            boundedInput3.apply("testAggregationWithDecimalValue", BeamSql.query(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(
            Arrays.asList("avg1", "avg2", "avg3",
                    "varpop1", "varpop2",
                    "varsamp1", "varsamp2"),
            Arrays.asList(Types.DOUBLE, Types.INTEGER, Types.DECIMAL,
                    Types.DOUBLE, Types.INTEGER,
                    Types.DOUBLE, Types.INTEGER));

    PAssert.that(result).satisfies(new CheckerBigDecimalDivide());

    pipeline.run().waitUntilFinish();
  }

  /**
   * Implicit GROUP-BY with DISTINCT with bounded PCollection.
   */
  @Test
  public void testDistinctWithBounded() throws Exception {
    runDistinct(boundedInput1);
  }

  /**
   * Implicit GROUP-BY with DISTINCT with unbounded PCollection.
   */
  @Test
  public void testDistinctWithUnbounded() throws Exception {
    runDistinct(unboundedInput1);
  }

  private void runDistinct(PCollection<BeamRecord> input) throws Exception {
    String sql = "SELECT distinct f_int, f_long FROM PCOLLECTION ";

    PCollection<BeamRecord> result =
        input.apply("testDistinct", BeamSql.query(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(Arrays.asList("f_int", "f_long"),
        Arrays.asList(Types.INTEGER, Types.BIGINT));

    BeamRecord record1 = new BeamRecord(resultType, 1, 1000L);
    BeamRecord record2 = new BeamRecord(resultType, 2, 2000L);
    BeamRecord record3 = new BeamRecord(resultType, 3, 3000L);
    BeamRecord record4 = new BeamRecord(resultType, 4, 4000L);

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * GROUP-BY with TUMBLE window(aka fix_time_window) with bounded PCollection.
   */
  @Test
  public void testTumbleWindowWithBounded() throws Exception {
    runTumbleWindow(boundedInput1);
  }

  /**
   * GROUP-BY with TUMBLE window(aka fix_time_window) with unbounded PCollection.
   */
  @Test
  public void testTumbleWindowWithUnbounded() throws Exception {
    runTumbleWindow(unboundedInput1);
  }

  private void runTumbleWindow(PCollection<BeamRecord> input) throws Exception {
    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
        + " TUMBLE_START(f_timestamp, INTERVAL '1' HOUR) AS `window_start`"
        + " FROM TABLE_A"
        + " GROUP BY f_int2, TUMBLE(f_timestamp, INTERVAL '1' HOUR)";
    PCollection<BeamRecord> result =
        PCollectionTuple.of(new TupleTag<BeamRecord>("TABLE_A"), input)
        .apply("testTumbleWindow", BeamSql.queryMulti(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(
        Arrays.asList("f_int2", "size", "window_start"),
        Arrays.asList(Types.INTEGER, Types.BIGINT, Types.TIMESTAMP));

    BeamRecord record1 = new BeamRecord(resultType, 0, 3L, FORMAT.parse("2017-01-01 01:00:00"));
    BeamRecord record2 = new BeamRecord(resultType, 0, 1L, FORMAT.parse("2017-01-01 02:00:00"));

    PAssert.that(result).containsInAnyOrder(record1, record2);

    pipeline.run().waitUntilFinish();
  }

  /**
   * GROUP-BY with HOP window(aka sliding_window) with bounded PCollection.
   */
  @Test
  public void testHopWindowWithBounded() throws Exception {
    runHopWindow(boundedInput1);
  }

  /**
   * GROUP-BY with HOP window(aka sliding_window) with unbounded PCollection.
   */
  @Test
  public void testHopWindowWithUnbounded() throws Exception {
    runHopWindow(unboundedInput1);
  }

  private void runHopWindow(PCollection<BeamRecord> input) throws Exception {
    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
        + " HOP_START(f_timestamp, INTERVAL '1' HOUR, INTERVAL '30' MINUTE) AS `window_start`"
        + " FROM PCOLLECTION"
        + " GROUP BY f_int2, HOP(f_timestamp, INTERVAL '1' HOUR, INTERVAL '30' MINUTE)";
    PCollection<BeamRecord> result =
        input.apply("testHopWindow", BeamSql.query(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(
        Arrays.asList("f_int2", "size", "window_start"),
        Arrays.asList(Types.INTEGER, Types.BIGINT, Types.TIMESTAMP));

    BeamRecord record1 = new BeamRecord(resultType, 0, 3L, FORMAT.parse("2017-01-01 00:30:00"));
    BeamRecord record2 = new BeamRecord(resultType, 0, 3L, FORMAT.parse("2017-01-01 01:00:00"));
    BeamRecord record3 = new BeamRecord(resultType, 0, 1L, FORMAT.parse("2017-01-01 01:30:00"));
    BeamRecord record4 = new BeamRecord(resultType, 0, 1L, FORMAT.parse("2017-01-01 02:00:00"));

    PAssert.that(result).containsInAnyOrder(record1, record2, record3, record4);

    pipeline.run().waitUntilFinish();
  }

  /**
   * GROUP-BY with SESSION window with bounded PCollection.
   */
  @Test
  public void testSessionWindowWithBounded() throws Exception {
    runSessionWindow(boundedInput1);
  }

  /**
   * GROUP-BY with SESSION window with unbounded PCollection.
   */
  @Test
  public void testSessionWindowWithUnbounded() throws Exception {
    runSessionWindow(unboundedInput1);
  }

  private void runSessionWindow(PCollection<BeamRecord> input) throws Exception {
    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount`,"
        + " SESSION_START(f_timestamp, INTERVAL '5' MINUTE) AS `window_start`"
        + " FROM TABLE_A"
        + " GROUP BY f_int2, SESSION(f_timestamp, INTERVAL '5' MINUTE)";
    PCollection<BeamRecord> result =
        PCollectionTuple.of(new TupleTag<BeamRecord>("TABLE_A"), input)
        .apply("testSessionWindow", BeamSql.queryMulti(sql));

    BeamRecordSqlType resultType = BeamRecordSqlType.create(
        Arrays.asList("f_int2", "size", "window_start"),
        Arrays.asList(Types.INTEGER, Types.BIGINT, Types.TIMESTAMP));

    BeamRecord record1 = new BeamRecord(resultType, 0, 3L, FORMAT.parse("2017-01-01 01:01:03"));
    BeamRecord record2 = new BeamRecord(resultType, 0, 1L, FORMAT.parse("2017-01-01 02:04:03"));

    PAssert.that(result).containsInAnyOrder(record1, record2);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWindowOnNonTimestampField() throws Exception {
    exceptions.expect(IllegalStateException.class);
    exceptions.expectMessage(
        "Cannot apply 'TUMBLE' to arguments of type 'TUMBLE(<BIGINT>, <INTERVAL HOUR>)'");
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int2, COUNT(*) AS `getFieldCount` FROM TABLE_A "
        + "GROUP BY f_int2, TUMBLE(f_long, INTERVAL '1' HOUR)";
    PCollection<BeamRecord> result =
        PCollectionTuple.of(new TupleTag<BeamRecord>("TABLE_A"), boundedInput1)
        .apply("testWindowOnNonTimestampField", BeamSql.queryMulti(sql));

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testUnsupportedDistinct() throws Exception {
    exceptions.expect(IllegalStateException.class);
    exceptions.expectMessage("Encountered \"*\"");
    pipeline.enableAbandonedNodeEnforcement(false);

    String sql = "SELECT f_int2, COUNT(DISTINCT *) AS `size` "
        + "FROM PCOLLECTION GROUP BY f_int2";

    PCollection<BeamRecord> result =
        boundedInput1.apply("testUnsupportedDistinct", BeamSql.query(sql));

    pipeline.run().waitUntilFinish();
  }
}
