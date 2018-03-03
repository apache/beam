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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.calcite.linq4j.function.Parameter;
import org.junit.Test;

/**
 * Tests for UDF/UDAF.
 */
public class BeamSqlDslUdfUdafTest extends BeamSqlDslBase {
  /**
   * GROUP-BY with UDAF.
   */
  @Test
  public void testUdaf() throws Exception {
    RowType resultType = RowSqlType.builder()
        .withIntegerField("f_int2")
        .withIntegerField("squaresum")
        .build();

    Row row = Row.withRowType(resultType).addValues(0, 30).build();

    String sql1 = "SELECT f_int2, squaresum1(f_int) AS `squaresum`"
        + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result1 =
        boundedInput1.apply(
            "testUdaf1",
            BeamSql.query(sql1).registerUdaf("squaresum1", new SquareSum()));
    PAssert.that(result1).containsInAnyOrder(row);

    String sql2 = "SELECT f_int2, squaresum2(f_int) AS `squaresum`"
        + " FROM PCOLLECTION GROUP BY f_int2";
    PCollection<Row> result2 =
        PCollectionTuple
            .of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply("testUdaf2",
                   BeamSql
                       .query(sql2)
                       .registerUdaf("squaresum2", new SquareSum()));
    PAssert.that(result2).containsInAnyOrder(row);

    pipeline.run().waitUntilFinish();
  }

  /**
   * test UDF.
   */
  @Test
  public void testUdf() throws Exception{
    RowType resultType = RowSqlType.builder()
        .withIntegerField("f_int")
        .withIntegerField("cubicvalue")
        .build();
    Row row = Row.withRowType(resultType).addValues(2, 8).build();

    String sql1 = "SELECT f_int, cubic1(f_int) as cubicvalue FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result1 =
        boundedInput1.apply("testUdf1",
            BeamSql.query(sql1).registerUdf("cubic1", CubicInteger.class));
    PAssert.that(result1).containsInAnyOrder(row);

    String sql2 = "SELECT f_int, cubic2(f_int) as cubicvalue FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result2 =
        PCollectionTuple.of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply("testUdf2",
                   BeamSql.query(sql2).registerUdf("cubic2", new CubicIntegerFn()));
    PAssert.that(result2).containsInAnyOrder(row);

    String sql3 = "SELECT f_int, substr(f_string) as sub_string FROM PCOLLECTION WHERE f_int = 2";
    PCollection<Row> result3 =
        PCollectionTuple.of(new TupleTag<>("PCOLLECTION"), boundedInput1)
            .apply("testUdf3",
                   BeamSql.query(sql3).registerUdf("substr", UdfFnWithDefault.class));

    RowType subStrRowType = RowSqlType.builder()
        .withIntegerField("f_int")
        .withVarcharField("sub_string")
        .build();
    Row subStrRow = Row.withRowType(subStrRowType).addValues(2, "s").build();
    PAssert.that(result3).containsInAnyOrder(subStrRow);

    pipeline.run().waitUntilFinish();
  }

  /**
   * UDAF(CombineFn) for test, which returns the sum of square.
   */
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

  /**
   * A example UDF for test.
   */
  public static class CubicInteger implements BeamSqlUdf {
    public static Integer eval(Integer input) {
      return input * input * input;
    }
  }

  /**
   * A example UDF with {@link SerializableFunction}.
   */
  public static class CubicIntegerFn implements SerializableFunction<Integer, Integer> {
    @Override
    public Integer apply(Integer input) {
      return input * input * input;
    }
  }

  /**
   * A UDF with default parameters.
   *
   */
  public static final class UdfFnWithDefault implements BeamSqlUdf {
    public static String eval(@Parameter(name = "s") String s,
        @Parameter(name = "n", optional = true) Integer n) {
      return s.substring(0, n == null ? 1 : n);
    }
  }
}
