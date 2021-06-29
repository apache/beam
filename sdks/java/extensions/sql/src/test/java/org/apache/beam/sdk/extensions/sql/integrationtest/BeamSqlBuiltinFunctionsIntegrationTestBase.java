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
package org.apache.beam.sdk.extensions.sql.integrationtest;

import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.extensions.sql.utils.RowAsserts.matchesScalar;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertTrue;

import com.google.auto.value.AutoValue;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.junit.Rule;

/** Base class for all built-in functions integration tests. */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final double PRECISION_DOUBLE = 1e-7;
  private static final float PRECISION_FLOAT = 1e-7f;

  private static final Map<Class, TypeName> JAVA_CLASS_TO_TYPENAME =
      ImmutableMap.<Class, TypeName>builder()
          .put(Byte.class, TypeName.BYTE)
          .put(Short.class, TypeName.INT16)
          .put(Integer.class, TypeName.INT32)
          .put(Long.class, TypeName.INT64)
          .put(Float.class, TypeName.FLOAT)
          .put(Double.class, TypeName.DOUBLE)
          .put(BigDecimal.class, TypeName.DECIMAL)
          .put(String.class, TypeName.STRING)
          .put(DateTime.class, TypeName.DATETIME)
          .put(Boolean.class, TypeName.BOOLEAN)
          .put(byte[].class, TypeName.BYTES)
          .build();

  private static final Schema ROW_TYPE =
      Schema.builder()
          .addDateTimeField("ts")
          .addByteField("c_tinyint")
          .addInt16Field("c_smallint")
          .addInt32Field("c_integer")
          .addInt64Field("c_bigint")
          .addFloatField("c_float")
          .addDoubleField("c_double")
          .addDecimalField("c_decimal")
          .addByteField("c_tinyint_max")
          .addInt16Field("c_smallint_max")
          .addInt32Field("c_integer_max")
          .addInt64Field("c_bigint_max")
          .build();

  private static final Schema ROW_TYPE_TWO =
      Schema.builder()
          .addNullableField("ts", FieldType.DATETIME)
          .addNullableField("c_tinyint", FieldType.BYTE)
          .addNullableField("c_smallint", FieldType.INT16)
          .addNullableField("c_integer", FieldType.INT32)
          .addNullableField("c_integer_two", FieldType.INT32)
          .addNullableField("c_bigint", FieldType.INT64)
          .addNullableField("c_float", FieldType.FLOAT)
          .addNullableField("c_double", FieldType.DOUBLE)
          .addNullableField("c_double_two", FieldType.DOUBLE)
          .addNullableField("c_decimal", FieldType.DECIMAL)
          .build();

  private static final Schema ROW_TYPE_THREE =
      Schema.builder()
          .addField("ts", FieldType.DATETIME)
          .addField("c_double", FieldType.DOUBLE)
          .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<Row> getTestPCollection() {
    try {
      return TestBoundedTable.of(ROW_TYPE)
          .addRows(
              parseTimestampWithUTCTimeZone("1986-02-15 11:35:26"),
              (byte) 1,
              (short) 1,
              1,
              1L,
              1.0f,
              1.0,
              BigDecimal.ONE,
              (byte) 127,
              (short) 32767,
              2147483647,
              9223372036854775807L)
          .buildIOReader(pipeline.begin())
          .setRowSchema(ROW_TYPE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected PCollection<Row> getFloorCeilingTestPCollection() {
    try {
      return TestBoundedTable.of(ROW_TYPE_THREE)
          .addRows(parseTimestampWithUTCTimeZone("1986-02-15 11:35:26"), 1.4)
          .buildIOReader(pipeline.begin())
          .setRowSchema(ROW_TYPE_THREE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected PCollection<Row> getAggregationTestPCollection() {
    try {
      return TestBoundedTable.of(ROW_TYPE_TWO)
          .addRows(
              parseTimestampWithUTCTimeZone("1986-02-15 11:35:26"),
              (byte) 1,
              (short) 1,
              1,
              5,
              1L,
              1.0f,
              1.0,
              7.0,
              BigDecimal.valueOf(1.0))
          .addRows(
              parseTimestampWithUTCTimeZone("1986-03-15 11:35:26"),
              (byte) 2,
              (short) 2,
              2,
              6,
              2L,
              2.0f,
              2.0,
              8.0,
              BigDecimal.valueOf(2.0))
          .addRows(
              parseTimestampWithUTCTimeZone("1986-04-15 11:35:26"),
              (byte) 3,
              (short) 3,
              3,
              7,
              3L,
              3.0f,
              3.0,
              9.0,
              BigDecimal.valueOf(3.0))
          .addRows(null, null, null, null, null, null, null, null, null, null)
          .buildIOReader(pipeline.begin())
          .setRowSchema(ROW_TYPE_TWO);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @AutoValue
  abstract static class ExpressionTestCase {

    private static ExpressionTestCase of(
        String sqlExpr, Object expectedResult, FieldType resultFieldType) {
      return new AutoValue_BeamSqlBuiltinFunctionsIntegrationTestBase_ExpressionTestCase(
          sqlExpr, expectedResult, resultFieldType);
    }

    abstract String sqlExpr();

    abstract @Nullable Object expectedResult();

    abstract FieldType resultFieldType();
  }

  /**
   * Helper class to write tests for built-in functions.
   *
   * <p>example usage:
   *
   * <pre>{@code
   * ExpressionChecker checker = new ExpressionChecker()
   *   .addExpr("1 + 1", 2)
   *   .addExpr("1.0 + 1", 2.0)
   *   .addExpr("1 + 1.0", 2.0)
   *   .addExpr("1.0 + 1.0", 2.0)
   *   .addExpr("c_tinyint + c_tinyint", (byte) 2);
   * checker.buildRunAndCheck(inputCollections);
   * }</pre>
   */
  public class ExpressionChecker {
    private transient List<ExpressionTestCase> exps = new ArrayList<>();

    public ExpressionChecker addExpr(String expression, Object expectedValue) {
      TypeName resultTypeName = JAVA_CLASS_TO_TYPENAME.get(expectedValue.getClass());
      checkArgument(
          resultTypeName != null,
          String.format(
              "The type of the expected value '%s' is unknown in 'addExpr(String expression, "
                  + "Object expectedValue)'. Please use 'addExpr(String expr, Object expected, "
                  + "FieldType type)' instead and provide the type of the expected object",
              expectedValue));
      addExpr(expression, expectedValue, FieldType.of(resultTypeName));
      return this;
    }

    public ExpressionChecker addExprWithNullExpectedValue(
        String expression, TypeName resultTypeName) {
      addExpr(expression, null, FieldType.of(resultTypeName));
      return this;
    }

    public ExpressionChecker addExpr(
        String expression, Object expectedValue, FieldType resultFieldType) {
      exps.add(ExpressionTestCase.of(expression, expectedValue, resultFieldType));
      return this;
    }

    public void buildRunAndCheck() {
      buildRunAndCheck(getTestPCollection());
    }

    /** Build the corresponding SQL, compile to Beam Pipeline, run it, and check the result. */
    public void buildRunAndCheck(PCollection<Row> inputCollection) {

      for (ExpressionTestCase testCase : exps) {
        String expression = testCase.sqlExpr();
        Object expectedValue = testCase.expectedResult();
        String sql = String.format("SELECT %s FROM PCOLLECTION", expression);
        Schema schema;
        if (expectedValue == null) {
          schema =
              Schema.builder().addNullableField(expression, testCase.resultFieldType()).build();
        } else {
          schema = Schema.builder().addField(expression, testCase.resultFieldType()).build();
        }

        PCollection<Row> output =
            inputCollection.apply(testCase.toString(), SqlTransform.query(sql));

        // For floating point number(Double and Float), it's allowed to have some precision delta,
        // other types can use regular equality check.
        if (expectedValue instanceof Double) {
          PAssert.that(output).satisfies(matchesScalar((double) expectedValue, PRECISION_DOUBLE));
        } else if (expectedValue instanceof Float) {
          PAssert.that(output).satisfies(matchesScalar((float) expectedValue, PRECISION_FLOAT));
        } else {
          PAssert.that(output)
              .containsInAnyOrder(
                  TestUtils.RowsBuilder.of(schema).addRows(expectedValue).getRows());
        }
      }

      inputCollection.getPipeline().run();
    }
  }

  /**
   * Helper class to write tests for SQL Expressions.
   *
   * <p>Differs from {@link ExpressionChecker}:
   *
   * <ul>
   *   <li>Tests a SQL expression is SQL true, not against a Java return type. Correctness relies on
   *       bootstrapped testing of literals and whatever operators, like {@code =} and {@code <} are
   *       used in the expression.
   *   <li>There is no implicit table to reference columns from, just literals.
   *   <li>Runs tests both via QueryTransform and also via JDBC driver.
   *   <li>Requires a pipeline to be provided where it will attach transforms.
   * </ul>
   *
   * <p>example usage:
   *
   * <pre>{@code
   * SqlExpressionChecker checker = new ExpressionChecker()
   *   .addExpr("1 + 1 = 2")
   *   .addExpr("1.0 + 1 = 2.0")
   *   .addExpr("1 + 1.0 = 2.0")
   *   .addExpr("1.0 + 1.0 = 2.0")
   *   .addExpr("CAST(1 AS TINYINT) + CAST(1 AS TINYINT) = CAST(1 AS TINYINT)");
   * checker.check(pipeline);
   * }</pre>
   */
  public static class SqlExpressionChecker {

    private transient List<String> exprs = new ArrayList<>();

    public SqlExpressionChecker addExpr(String expr) {
      exprs.add(expr);
      return this;
    }

    /**
     * Tests the cases set up via a PTransform in the given pipeline as well as via the JDBC driver.
     */
    public void check(Pipeline pipeline) throws Exception {
      checkPTransform(pipeline);
      checkJdbc(pipeline.getOptions());
    }

    private static final Schema DUMMY_SCHEMA = Schema.builder().addBooleanField("dummy").build();
    private static final Row DUMMY_ROW = Row.withSchema(DUMMY_SCHEMA).addValue(true).build();

    private void checkPTransform(Pipeline pipeline) {
      for (String expr : exprs) {
        pipeline.apply(expr, new CheckPTransform(expr));
      }
    }

    private static final ReadOnlyTableProvider BOUNDED_TABLE =
        new ReadOnlyTableProvider(
            "test",
            ImmutableMap.of(
                "test",
                TestBoundedTable.of(
                        Schema.FieldType.INT32, "id",
                        Schema.FieldType.STRING, "name")
                    .addRows(1, "first")));

    private void checkJdbc(PipelineOptions pipelineOptions) throws Exception {
      // Beam SQL code is only invoked when the calling convention insists on it, so we
      // have to express this as selecting from a Beam table, even though the contents are
      // irrelevant.
      //
      // Sometimes this means the results are incorrect, because other calling conventions
      // are incorrect: https://issues.apache.org/jira/browse/BEAM-4704
      //
      // Here we create a Beam table just to force the calling convention.
      TestTableProvider tableProvider = new TestTableProvider();
      Connection connection = JdbcDriver.connect(tableProvider, pipelineOptions);
      connection
          .createStatement()
          .executeUpdate("CREATE EXTERNAL TABLE dummy (dummy BOOLEAN) TYPE 'test'");
      tableProvider.addRows("dummy", DUMMY_ROW);

      for (String expr : exprs) {
        ResultSet exprResult =
            connection.createStatement().executeQuery(String.format("SELECT %s FROM dummy", expr));
        exprResult.next();
        exprResult.getBoolean(1);
        assertTrue("Test expression is false: " + expr, exprResult.getBoolean(1));
      }
    }

    private static class CheckPTransform extends PTransform<PBegin, PDone> {

      private final String expr;

      private CheckPTransform(String expr) {
        this.expr = expr;
      }

      @Override
      public PDone expand(PBegin begin) {
        PCollection<Boolean> result =
            begin
                .apply(Create.of(DUMMY_ROW).withRowSchema(DUMMY_SCHEMA))
                .apply(SqlTransform.query("SELECT " + expr))
                .apply(MapElements.into(TypeDescriptors.booleans()).via(row -> row.getBoolean(0)));

        PAssert.that(result)
            .satisfies(
                input -> {
                  assertTrue("Test expression is false: " + expr, Iterables.getOnlyElement(input));
                  return null;
                });
        return PDone.in(begin.getPipeline());
      }
    }
  }
}
