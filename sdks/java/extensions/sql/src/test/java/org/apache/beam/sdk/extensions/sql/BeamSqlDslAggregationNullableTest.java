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

import static org.apache.beam.sdk.extensions.sql.utils.RowAsserts.matchesNull;
import static org.apache.beam.sdk.extensions.sql.utils.RowAsserts.matchesScalar;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Integration tests for aggregation nullable columns. */
public class BeamSqlDslAggregationNullableTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private PCollection<Row> boundedInput;

  @Before
  public void setUp() {
    Schema schema =
        Schema.builder()
            .addNullableField("f_int1", Schema.FieldType.INT32)
            .addNullableField("f_int2", Schema.FieldType.INT32)
            .addInt32Field("f_int3")
            .build();

    List<Row> rows =
        TestUtils.RowsBuilder.of(schema)
            .addRows(1, 5, 1)
            .addRows(null, 1, 1)
            .addRows(2, 1, 1)
            .addRows(null, 1, 1)
            .addRows(null, null, 1)
            .addRows(null, null, 1)
            .addRows(3, 2, 1)
            .getRows();

    boundedInput = PBegin.in(pipeline).apply(Create.of(rows).withRowSchema(schema));
  }

  @Test
  public void testCount() {
    String sql = "SELECT COUNT(f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(3L));

    pipeline.run();
  }

  @Test
  public void testCountNull() {
    String sql = "SELECT COUNT(f_int1) FROM PCOLLECTION WHERE f_int2 IS NULL GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(0L));

    pipeline.run();
  }

  @Test
  public void testCountStar() {
    String sql = "SELECT COUNT(*) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(7L));

    pipeline.run();
  }

  @Test
  public void testCountThroughSum() {
    String sql =
        "SELECT SUM(CASE f_int1 IS NOT NULL WHEN TRUE THEN 1 ELSE 0 END) "
            + "FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(3));

    pipeline.run();
  }

  @Test
  public void testCountNulls() {
    String sql =
        "SELECT SUM(CASE f_int1 IS NULL WHEN TRUE THEN 1 ELSE 0 END) "
            + "FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(4));

    pipeline.run();
  }

  @Test
  public void testSum() {
    String sql = "SELECT SUM(f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(6));

    pipeline.run();
  }

  @Test
  public void testSumNull() {
    String sql = "SELECT SUM(f_int1) FROM PCOLLECTION WHERE f_int2 IS NULL GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesNull());

    pipeline.run();
  }

  @Test
  public void testMin() {
    String sql = "SELECT MIN(f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(1));

    pipeline.run();
  }

  @Test
  public void testMinNull() {
    String sql = "SELECT MIN(f_int1) FROM PCOLLECTION WHERE f_int2 IS NULL GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesNull());

    pipeline.run();
  }

  @Test
  public void testMax() {
    String sql = "SELECT MAX(f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(3));

    pipeline.run();
  }

  @Test
  public void testMaxNull() {
    String sql = "SELECT MAX(f_int1) FROM PCOLLECTION WHERE f_int2 IS NULL GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesNull());

    pipeline.run();
  }

  @Test
  public void testAvg() {
    String sql = "SELECT AVG(f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(2));

    pipeline.run();
  }

  @Test
  public void testAvgNull() {
    String sql = "SELECT AVG(f_int1) FROM PCOLLECTION WHERE f_int2 IS NULL GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesNull());

    pipeline.run();
  }

  @Test
  public void testAvgGroupByNullable() {
    String sql = "SELECT AVG(f_int1), f_int2 FROM PCOLLECTION GROUP BY f_int2";

    PCollection<Row> out = boundedInput.apply(SqlTransform.query(sql));
    Schema schema = out.getSchema();

    PAssert.that(out)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(null, null).build(),
            Row.withSchema(schema).addValues(2, 1).build(),
            Row.withSchema(schema).addValues(1, 5).build(),
            Row.withSchema(schema).addValues(3, 2).build());

    pipeline.run();
  }

  @Test
  public void testCountGroupByNullable() {
    String sql = "SELECT COUNT(f_int1) as c, f_int2 FROM PCOLLECTION GROUP BY f_int2";

    PCollection<Row> out = boundedInput.apply(SqlTransform.query(sql));
    Schema schema = out.getSchema();

    PAssert.that(out)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues(0L, null).build(),
            Row.withSchema(schema).addValues(1L, 1).build(),
            Row.withSchema(schema).addValues(1L, 5).build(),
            Row.withSchema(schema).addValues(1L, 2).build());

    assertEquals(
        Schema.builder()
            // COUNT() is never nullable, and calcite knows it
            .addInt64Field("c")
            .addNullableField("f_int2", Schema.FieldType.INT32)
            .build(),
        schema);

    pipeline.run();
  }

  @Test
  public void testSampleVariance() {
    // a special case of aggregator with two parameters
    String sql = "SELECT COVAR_SAMP(f_int1, f_int2) FROM PCOLLECTION GROUP BY f_int3";

    // COVAR_SAMP(f_int1, f_int2) =
    //   (SUM(f_int1 * f_int2) - SUM(f_int1) * SUM(f_int2) / n) / (n-1) =
    //   (SUM([1 * 5, 2 * 1, 3 * 2]) - SUM([1, 2, 3]) * SUM([5, 1, 2]) / 3) / 2 =
    //   -1.5

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(-1));

    pipeline.run();
  }

  @Test
  public void testSampleVarianceReverse() {
    String sql = "SELECT COVAR_SAMP(f_int2, f_int1) FROM PCOLLECTION GROUP BY f_int3";

    PAssert.that(boundedInput.apply(SqlTransform.query(sql))).satisfies(matchesScalar(-1));

    pipeline.run();
  }
}
