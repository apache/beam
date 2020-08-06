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

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithoutTimeZone;
import static org.hamcrest.Matchers.containsString;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

import com.google.auto.service.AutoService;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteTable;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.function.Parameter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.TranslatableTable;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for UDF/UDAF. */
public class BeamSqlDslUdfUdafTest extends BeamSqlDslBase {

  /** GROUP-BY with UDAF. */
  @Test
  public void testUdaf() throws Exception {
    Schema resultType = Schema.builder().addInt32Field("f_int2").addInt32Field("squaresum").build();

    Row row = Row.withSchema(resultType).addValues(0, 30).build();

    String sql1 =
        "SELECT f_int2, squaresum1(f_int) AS `squaresum`" + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testUdaf1", SqlTransform.query(sql1).registerUdaf("squaresum1", new SquareSum()));
    PAssert.that(result1).containsInAnyOrder(row);

    String sql2 =
        "SELECT f_int2, squaresum2(f_int) AS `squaresum`" + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result2 =
        PCollectionTuple.of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply(
                "testUdaf2", SqlTransform.query(sql2).registerUdaf("squaresum2", new SquareSum()));
    PAssert.that(result2).containsInAnyOrder(row);

    pipeline.run().waitUntilFinish();
  }

  /** Test Joda time UDF/UDAF. */
  @Test
  public void testJodaTimeUdfUdaf() throws Exception {
    Schema resultType = Schema.builder().addDateTimeField("jodatime").build();

    Row row1 =
        Row.withSchema(resultType)
            .addValues(parseTimestampWithoutTimeZone("2017-01-01 02:04:03"))
            .build();

    String sql1 = "SELECT MAX_JODA(f_timestamp) as jodatime FROM PCOLLECTION";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testJodaUdaf", SqlTransform.query(sql1).registerUdaf("MAX_JODA", new JodaMax()));
    PAssert.that(result1).containsInAnyOrder(row1);

    Row row2 =
        Row.withSchema(resultType)
            .addValues(parseTimestampWithoutTimeZone("2016-12-31 01:01:03"))
            .build();

    String sql2 = "SELECT PRE_DAY(f_timestamp) as jodatime FROM PCOLLECTION WHERE f_int=1";
    PCollection<Row> result2 =
        boundedInput1.apply(
            "testTimeUdf", SqlTransform.query(sql2).registerUdf("PRE_DAY", PreviousDay.class));
    PAssert.that(result2).containsInAnyOrder(row2);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testListUdf() throws Exception {
    Schema resultType1 = Schema.builder().addArrayField("array_field", FieldType.INT64).build();
    Row row1 = Row.withSchema(resultType1).addValue(Arrays.asList(1L)).build();
    String sql1 = "SELECT test_array(1)";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testArrayUdf",
            SqlTransform.query(sql1).registerUdf("test_array", TestReturnTypeList.class));
    PAssert.that(result1).containsInAnyOrder(row1);

    Schema resultType2 = Schema.builder().addInt32Field("int_field").build();
    Row row2 = Row.withSchema(resultType2).addValue(3).build();
    String sql2 = "select array_length(ARRAY[1, 2, 3])";
    PCollection<Row> result2 =
        boundedInput1.apply(
            "testArrayUdf2",
            SqlTransform.query(sql2).registerUdf("array_length", TestListLength.class));
    PAssert.that(result2).containsInAnyOrder(row2);

    pipeline.run().waitUntilFinish();
  }

  /** Test that an indirect subclass of a {@link CombineFn} works as a UDAF. BEAM-3777 */
  @Test
  public void testUdafMultiLevelDescendent() {
    Schema resultType = Schema.builder().addInt32Field("f_int2").addInt32Field("squaresum").build();

    Row row = Row.withSchema(resultType).addValues(0, 354).build();

    String sql1 =
        "SELECT f_int2, double_square_sum(f_int) AS `squaresum`"
            + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testUdaf",
            SqlTransform.query(sql1).registerUdaf("double_square_sum", new SquareSquareSum()));
    PAssert.that(result1).containsInAnyOrder(row);

    pipeline.run().waitUntilFinish();
  }

  /**
   * Test that correct exception is thrown when subclass of {@link CombineFn} is not parameterized.
   * BEAM-3777
   */
  @Test
  public void testRawCombineFnSubclass() {
    exceptions.expect(ParseException.class);
    exceptions.expectCause(hasMessage(containsString("CombineFn must be parameterized")));
    pipeline.enableAbandonedNodeEnforcement(false);

    Schema resultType = Schema.builder().addInt32Field("f_int2").addInt32Field("squaresum").build();

    Row row = Row.withSchema(resultType).addValues(0, 354).build();

    String sql1 =
        "SELECT f_int2, squaresum(f_int) AS `squaresum`" + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testUdaf", SqlTransform.query(sql1).registerUdaf("squaresum", new RawCombineFn()));
  }

  /** test UDF. */
  @Test
  public void testUdf() throws Exception {
    Schema resultType = Schema.builder().addInt32Field("f_int").addInt32Field("cubicvalue").build();
    Row row = Row.withSchema(resultType).addValues(2, 8).build();

    String sql1 = "SELECT f_int, cubic1(f_int) as cubicvalue FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testUdf1", SqlTransform.query(sql1).registerUdf("cubic1", CubicInteger.class));
    PAssert.that(result1).containsInAnyOrder(row);

    String sql2 = "SELECT f_int, cubic2(f_int) as cubicvalue FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result2 =
        PCollectionTuple.of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply(
                "testUdf2", SqlTransform.query(sql2).registerUdf("cubic2", new CubicIntegerFn()));
    PAssert.that(result2).containsInAnyOrder(row);

    String sql3 = "SELECT f_int, substr(f_string) as sub_string FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result3 =
        PCollectionTuple.of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply(
                "testUdf3", SqlTransform.query(sql3).registerUdf("substr", UdfFnWithDefault.class));

    Schema subStrSchema =
        Schema.builder().addInt32Field("f_int").addStringField("sub_string").build();
    Row subStrRow = Row.withSchema(subStrSchema).addValues(2, "s").build();
    PAssert.that(result3).containsInAnyOrder(subStrRow);

    pipeline.run().waitUntilFinish();
  }

  /**
   * test {@link org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.TableMacro} UDF.
   */
  @Test
  public void testTableMacroUdf() throws Exception {
    String sql1 = "SELECT * FROM table(range_udf(0, 3))";

    Schema schema = Schema.of(Schema.Field.of("f0", Schema.FieldType.INT32));

    PCollection<Row> rows =
        pipeline.apply(SqlTransform.query(sql1).registerUdf("range_udf", RangeUdf.class));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue(0).build(),
            Row.withSchema(schema).addValue(1).build(),
            Row.withSchema(schema).addValue(2).build());

    pipeline.run();
  }

  /** test auto-provider UDF/UDAF. */
  @Test
  public void testAutoUdfUdaf() throws Exception {
    Schema resultType =
        Schema.builder().addInt32Field("f_int2").addInt32Field("autoload_squarecubicsum").build();

    Row row = Row.withSchema(resultType).addValues(0, 4890).build();

    String sql =
        "SELECT f_int2, autoload_squaresum(autoload_cubic(f_int)) AS `autoload_squarecubicsum`"
            + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result =
        boundedInput1.apply("testUdaf", SqlTransform.query(sql).withAutoUdfUdafLoad(true));

    PAssert.that(result).containsInAnyOrder(row);
    pipeline.run().waitUntilFinish();
  }

  /** Auto provider for test. */
  @AutoService(UdfUdafProvider.class)
  public static class UdfUdafProviderTest implements UdfUdafProvider {

    @Override
    public Map<String, Class<? extends BeamSqlUdf>> getBeamSqlUdfs() {
      return ImmutableMap.of("autoload_cubic", CubicInteger.class);
    }

    @Override
    public Map<String, CombineFn> getUdafs() {
      return ImmutableMap.of("autoload_squaresum", new SquareSum());
    }
  }

  /** UDAF(CombineFn) for test, which returns the sum of square. */
  public static class SquareSum extends CombineFn<Integer, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
      return 0;
    }

    @Override
    public Integer addInput(Integer accumulator, Integer input) {
      return accumulator + input * input;
    }

    @Override
    public Integer mergeAccumulators(Iterable<Integer> accumulators) {
      int v = 0;
      for (Integer accumulator : accumulators) {
        v += accumulator;
      }
      return v;
    }

    @Override
    public Integer extractOutput(Integer accumulator) {
      return accumulator;
    }
  }

  /** UDAF(CombineFn) to test support of Joda time. */
  public static class JodaMax extends CombineFn<Instant, Instant, Instant> {
    @Override
    public Instant createAccumulator() {
      return new Instant(0L);
    }

    @Override
    public Instant addInput(Instant accumulator, Instant input) {
      return accumulator.isBefore(input) ? input : accumulator;
    }

    @Override
    public Instant mergeAccumulators(Iterable<Instant> accumulators) {
      Instant v = new Instant(0L);
      for (Instant accumulator : accumulators) {
        v = accumulator.isBefore(v) ? v : accumulator;
      }
      return v;
    }

    @Override
    public Instant extractOutput(Instant accumulator) {
      return accumulator;
    }
  }

  /**
   * Non-parameterized CombineFn. Intended to test that non-parameterized CombineFns are correctly
   * rejected. The methods just return null, as they should never be called.
   */
  public static class RawCombineFn extends CombineFn {

    @Override
    public Object createAccumulator() {
      return null;
    }

    @Override
    public Object addInput(Object accumulator, Object input) {
      return null;
    }

    @Override
    public Object mergeAccumulators(Iterable accumulators) {
      return null;
    }

    @Override
    public Object extractOutput(Object accumulator) {
      return null;
    }
  }

  /** An example UDAF with two levels of descendancy from CombineFn. */
  public static class SquareSquareSum extends SquareSum {
    @Override
    public Integer addInput(Integer accumulator, Integer input) {
      return super.addInput(accumulator, input * input);
    }
  }

  /** An example UDF for test. */
  public static class CubicInteger implements BeamSqlUdf {
    public static Integer eval(Integer input) {
      return input * input * input;
    }
  }

  /** An example UDF with {@link SerializableFunction}. */
  public static class CubicIntegerFn implements SerializableFunction<Integer, Integer> {
    @Override
    public Integer apply(Integer input) {
      return input * input * input;
    }
  }

  /** A UDF with default parameters. */
  public static final class UdfFnWithDefault implements BeamSqlUdf {
    public static String eval(
        @Parameter(name = "s") String s, @Parameter(name = "n", optional = true) Integer n) {
      return s.substring(0, n == null ? 1 : n);
    }
  }

  /** A UDF to test support of time. */
  public static final class PreviousDay implements BeamSqlUdf {
    public static Timestamp eval(Timestamp time) {
      return new Timestamp(time.getTime() - 24 * 3600 * 1000L);
    }
  }

  /** A UDF to test support of array as return type. */
  public static final class TestReturnTypeList implements BeamSqlUdf {
    public static java.util.List<Long> eval(Long i) {
      return Arrays.asList(i);
    }
  }

  /** A UDF to test support of array as argument type. */
  public static final class TestListLength implements BeamSqlUdf {
    public static Integer eval(java.util.List<Long> i) {
      return i.size();
    }
  }

  /**
   * UDF to test support for {@link
   * org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.TableMacro}.
   */
  public static final class RangeUdf implements BeamSqlUdf {
    public static TranslatableTable eval(int startInclusive, int endExclusive) {
      Schema schema = Schema.of(Schema.Field.of("f0", Schema.FieldType.INT32));
      Object[] values = IntStream.range(startInclusive, endExclusive).boxed().toArray();
      return BeamCalciteTable.of(new TestBoundedTable(schema).addRows(values));
    }
  }
}
