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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ZetaSQL Math functions (on INT64, DOUBLE, NUMERIC types). */
@RunWith(JUnit4.class)
public class ZetaSqlMathFunctionsTest extends ZetaSqlTestBase {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  /////////////////////////////////////////////////////////////////////////////
  // INT64 type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testArithmeticOperatorsInt64() {
    String sql = "SELECT -1, 1 + 2, 1 - 2, 1 * 2, 1 / 2";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt64Field("f_int64_1")
                        .addInt64Field("f_int64_2")
                        .addInt64Field("f_int64_3")
                        .addInt64Field("f_int64_4")
                        .addDoubleField("f_double")
                        .build())
                .addValues(-1L, 3L, -1L, 2L, 0.5)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAbsInt64() {
    String sql = "SELECT ABS(1), ABS(-1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addInt64Field("f_int64_1").addInt64Field("f_int64_2").build())
                .addValues(1L, 1L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSignInt64() {
    String sql = "SELECT SIGN(0), SIGN(5), SIGN(-5)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt64Field("f_int64_1")
                        .addInt64Field("f_int64_2")
                        .addInt64Field("f_int64_3")
                        .build())
                .addValues(0L, 1L, -1L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testModInt64() {
    String sql = "SELECT MOD(4, 2)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("f_int64").build())
                .addValues(0L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDivInt64() {
    String sql = "SELECT DIV(1, 2), DIV(2, 1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addInt64Field("f_int64_1").addInt64Field("f_int64_2").build())
                .addValues(0L, 2L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSafeArithmeticFunctionsInt64() {
    String sql =
        "SELECT SAFE_ADD(9223372036854775807, 1), "
            + "SAFE_SUBTRACT(-9223372036854775808, 1), "
            + "SAFE_MULTIPLY(9223372036854775807, 2), "
            + "SAFE_DIVIDE(1, 0), "
            + "SAFE_NEGATE(-9223372036854775808)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addNullableField("f_int64_1", Schema.FieldType.INT64)
                        .addNullableField("f_int64_2", Schema.FieldType.INT64)
                        .addNullableField("f_int64_3", Schema.FieldType.INT64)
                        .addNullableField("f_int64_4", Schema.FieldType.INT64)
                        .addNullableField("f_int64_5", Schema.FieldType.INT64)
                        .build())
                .addValues(null, null, null, null, null)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // DOUBLE (FLOAT64) type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testDoubleLiteral() {
    String sql =
        "SELECT 3.0, CAST('+inf' AS FLOAT64), CAST('-inf' AS FLOAT64), CAST('NaN' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .addDoubleField("f_double3")
                        .addDoubleField("f_double4")
                        .build())
                .addValues(3.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testArithmeticOperatorsDouble() {
    String sql = "SELECT -1.5, 1.5 + 2.5, 1.5 - 2.5, 1.5 * 2.5, 1.5 / 2.5";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .addDoubleField("f_double3")
                        .addDoubleField("f_double4")
                        .addDoubleField("f_double5")
                        .build())
                .addValues(-1.5, 4.0, -1.0, 3.75, 0.6)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEqualsInf() {
    String sql =
        "SELECT CAST('+inf' AS FLOAT64) = CAST('+inf' AS FLOAT64), "
            + "CAST('+inf' AS FLOAT64) = CAST('-inf' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .build())
                .addValues(true, false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testEqualsNaN() {
    String sql = "SELECT CAST('NaN' AS FLOAT64) = CAST('NaN' AS FLOAT64)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addBooleanField("f_boolean").build())
                .addValues(false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAbsDouble() {
    String sql = "SELECT ABS(1.5), ABS(-1.0), ABS(CAST('NaN' AS FLOAT64))";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .addDoubleField("f_double3")
                        .build())
                .addValues(1.5, 1.0, Double.NaN)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSignDouble() {
    String sql = "SELECT SIGN(-0.0), SIGN(1.5), SIGN(-1.5), SIGN(CAST('NaN' AS FLOAT64))";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .addDoubleField("f_double3")
                        .addDoubleField("f_double4")
                        .build())
                .addValues(0.0, 1.0, -1.0, Double.NaN)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testRoundDouble() {
    String sql = "SELECT ROUND(1.23), ROUND(-1.27, 1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .build())
                .addValues(1.0, -1.3)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTruncDouble() {
    String sql = "SELECT TRUNC(1.23), TRUNC(-1.27, 1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .build())
                .addValues(1.0, -1.2)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCeilDouble() {
    String sql = "SELECT CEIL(1.2), CEIL(-1.2)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .build())
                .addValues(2.0, -1.0)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFloorDouble() {
    String sql = "SELECT FLOOR(1.2), FLOOR(-1.2)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .build())
                .addValues(1.0, -2.0)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsInf() {
    String sql =
        "SELECT IS_INF(CAST('+inf' AS FLOAT64)), IS_INF(CAST('-inf' AS FLOAT64)), IS_INF(3.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .addBooleanField("f_boolean3")
                        .build())
                .addValues(true, true, false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIsNaN() {
    String sql = "SELECT IS_NAN(CAST('NaN' AS FLOAT64)), IS_NAN(3.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addBooleanField("f_boolean1")
                        .addBooleanField("f_boolean2")
                        .build())
                .addValues(true, false)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testIeeeDivide() {
    String sql = "SELECT IEEE_DIVIDE(1.0, 0.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(Double.POSITIVE_INFINITY)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSafeDivide() {
    String sql = "SELECT SAFE_DIVIDE(1.0, 0.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addNullableField("f_double", Schema.FieldType.DOUBLE).build())
                .addValue(null)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSqrtDouble() {
    String sql = "SELECT SQRT(4.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(2.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testPowDouble() {
    String sql = "SELECT POW(2.0, 3.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(8.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExpDouble() {
    String sql = "SELECT EXP(2.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(7.38905609893065)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLnDouble() {
    String sql = "SELECT LN(7.38905609893065)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(2.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLog10Double() {
    String sql = "SELECT LOG10(100.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(2.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLogDouble() {
    String sql = "SELECT LOG(2.25, 1.5)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDoubleField("f_double").build())
                .addValues(2.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrigonometricFunctions() {
    String sql =
        "SELECT COS(0.0), COSH(0.0), ACOS(1.0), ACOSH(1.0), "
            + "SIN(0.0), SINH(0.0), ASIN(0.0), ASINH(0.0), "
            + "TAN(0.0), TANH(0.0), ATAN(0.0), ATANH(0.0), ATAN2(0.0, 0.0)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDoubleField("f_double1")
                        .addDoubleField("f_double2")
                        .addDoubleField("f_double3")
                        .addDoubleField("f_double4")
                        .addDoubleField("f_double5")
                        .addDoubleField("f_double6")
                        .addDoubleField("f_double7")
                        .addDoubleField("f_double8")
                        .addDoubleField("f_double9")
                        .addDoubleField("f_double10")
                        .addDoubleField("f_double11")
                        .addDoubleField("f_double12")
                        .addDoubleField("f_double13")
                        .build())
                .addValues(1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /////////////////////////////////////////////////////////////////////////////
  // NUMERIC type tests
  /////////////////////////////////////////////////////////////////////////////

  @Test
  public void testNumericLiteral() {
    String sql =
        "SELECT NUMERIC '0', "
            + "NUMERIC '123456', "
            + "NUMERIC '-3.14', "
            + "NUMERIC '-0.54321', "
            + "NUMERIC '1.23456e05', "
            + "NUMERIC '-9.876e-3', "
            // min value for ZetaSQL NUMERIC type
            + "NUMERIC '-99999999999999999999999999999.999999999', "
            // max value for ZetaSQL NUMERIC type
            + "NUMERIC '99999999999999999999999999999.999999999'";
    ;

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .addDecimalField("f_numeric3")
                        .addDecimalField("f_numeric4")
                        .addDecimalField("f_numeric5")
                        .addDecimalField("f_numeric6")
                        .addDecimalField("f_numeric7")
                        .addDecimalField("f_numeric8")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("0"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("123456"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-3.14"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.54321"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("123456"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.009876"),
                    ZetaSqlCalciteTranslationUtils.ZETASQL_NUMERIC_MIN_VALUE,
                    ZetaSqlCalciteTranslationUtils.ZETASQL_NUMERIC_MAX_VALUE)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNumericColumn() {
    String sql = "SELECT numeric_field FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addDecimalField("f_numeric").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(schema)
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("123.4567"))
                .build(),
            Row.withSchema(schema)
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("765.4321"))
                .build(),
            Row.withSchema(schema)
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("-555.5555"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testArithmeticOperatorsNumeric() {
    String sql =
        "SELECT - NUMERIC '1.23456e05', "
            + "NUMERIC '1.23456e05' + NUMERIC '9.876e-3', "
            + "NUMERIC '1.23456e05' - NUMERIC '-9.876e-3', "
            + "NUMERIC '1.23e02' * NUMERIC '-1.001e-3', "
            + "NUMERIC '-1.23123e-1' / NUMERIC '-1.001e-3', ";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .addDecimalField("f_numeric3")
                        .addDecimalField("f_numeric4")
                        .addDecimalField("f_numeric5")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-123456"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("123456.009876"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("123456.009876"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-0.123123"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("123"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAbsNumeric() {
    String sql = "SELECT ABS(NUMERIC '1.23456e04'), ABS(NUMERIC '-1.23456e04')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12345.6"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12345.6"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSignNumeric() {
    String sql = "SELECT SIGN(NUMERIC '0'), SIGN(NUMERIC '1.23e01'), SIGN(NUMERIC '-1.23e01')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .addDecimalField("f_numeric3")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("0"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("1"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-1"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testRoundNumeric() {
    String sql = "SELECT ROUND(NUMERIC '1.23456e04'), ROUND(NUMERIC '-1.234567e04', 1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12346"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-12345.7"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTruncNumeric() {
    String sql = "SELECT TRUNC(NUMERIC '1.23456e04'), TRUNC(NUMERIC '-1.234567e04', 1)";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12345"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-12345.6"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCeilNumeric() {
    String sql = "SELECT CEIL(NUMERIC '1.23456e04'), CEIL(NUMERIC '-1.23456e04')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12346"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-12345"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testFloorNumeric() {
    String sql = "SELECT FLOOR(NUMERIC '1.23456e04'), FLOOR(NUMERIC '-1.23456e04')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addDecimalField("f_numeric1")
                        .addDecimalField("f_numeric2")
                        .build())
                .addValues(
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("12345"),
                    ZetaSqlTypesUtils.bigDecimalAsNumeric("-12346"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testModNumeric() {
    String sql = "SELECT MOD(NUMERIC '1.23456e05', NUMERIC '5')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("1"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDivNumeric() {
    String sql = "SELECT DIV(NUMERIC '1.23456e05', NUMERIC '5')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("24691"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSafeArithmeticFunctionsNumeric() {
    String sql =
        "SELECT SAFE_ADD(NUMERIC '99999999999999999999999999999.999999999', NUMERIC '1'), "
            + "SAFE_SUBTRACT(NUMERIC '-99999999999999999999999999999.999999999', NUMERIC '1'), "
            + "SAFE_MULTIPLY(NUMERIC '99999999999999999999999999999.999999999', NUMERIC '2'), "
            + "SAFE_DIVIDE(NUMERIC '1.23456e05', NUMERIC '0'), "
            + "SAFE_NEGATE(NUMERIC '99999999999999999999999999999.999999999')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addNullableField("f_numeric1", Schema.FieldType.DECIMAL)
                        .addNullableField("f_numeric2", Schema.FieldType.DECIMAL)
                        .addNullableField("f_numeric3", Schema.FieldType.DECIMAL)
                        .addNullableField("f_numeric4", Schema.FieldType.DECIMAL)
                        .addNullableField("f_numeric5", Schema.FieldType.DECIMAL)
                        .build())
                .addValues(
                    null,
                    null,
                    null,
                    null,
                    ZetaSqlCalciteTranslationUtils.ZETASQL_NUMERIC_MIN_VALUE)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSqrtNumeric() {
    String sql = "SELECT SQRT(NUMERIC '4')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("2"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testPowNumeric() {
    String sql = "SELECT POW(NUMERIC '2', NUMERIC '3')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("8"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExpNumeric() {
    String sql = "SELECT EXP(NUMERIC '2')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("7.389056099"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLnNumeric() {
    String sql = "SELECT LN(NUMERIC '7.389056099')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("2"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLog10Numeric() {
    String sql = "SELECT LOG10(NUMERIC '100')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("2"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testLogNumeric() {
    String sql = "SELECT LOG(NUMERIC '2.25', NUMERIC '1.5')";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("2"))
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testSumNumeric() {
    String sql = "SELECT SUM(numeric_field) FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("333.3333"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testAvgNumeric() {
    String sql = "SELECT AVG(numeric_field) FROM table_with_numeric";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addDecimalField("f_numeric").build())
                .addValues(ZetaSqlTypesUtils.bigDecimalAsNumeric("111.1111"))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
