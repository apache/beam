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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.fail;

import com.google.zetasql.SqlException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.codehaus.commons.compiler.CompileException;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for ZetaSQL UDFs written in Java.
 *
 * <p>System properties <code>beam.sql.udf.test.jarpath</code> and <code>
 * beam.sql.udf.test.empty_jar_path</code> must be set.
 */
@RunWith(JUnit4.class)
public class ZetaSqlJavaUdfTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String jarPathProperty = "beam.sql.udf.test.jar_path";
  private final String emptyJarPathProperty = "beam.sql.udf.test.empty_jar_path";

  private final @Nullable String jarPath = System.getProperty(jarPathProperty);
  private final @Nullable String emptyJarPath = System.getProperty(emptyJarPathProperty);

  @Before
  public void setUp() {
    if (jarPath == null) {
      fail(
          String.format(
              "System property %s must be set to run %s.",
              jarPathProperty, ZetaSqlJavaUdfTest.class.getSimpleName()));
    }
    if (emptyJarPath == null) {
      fail(
          String.format(
              "System property %s must be set to run %s.",
              emptyJarPathProperty, ZetaSqlJavaUdfTest.class.getSimpleName()));
    }
    initialize();
  }

  @Test
  public void testNullaryJavaUdf() {
    String sql =
        String.format(
            "CREATE FUNCTION helloWorld() RETURNS STRING LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT helloWorld();",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addStringField("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(singleField).addValues("Hello world!").build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnaryJavaUdf() {
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT increment(1);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(2L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testJavaUdfColumnReference() {
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT increment(int64_col) FROM table_all_types;",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(singleField).addValues(0L).build(),
            Row.withSchema(singleField).addValues(-1L).build(),
            Row.withSchema(singleField).addValues(-2L).build(),
            Row.withSchema(singleField).addValues(-3L).build(),
            Row.withSchema(singleField).addValues(-4L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNestedJavaUdf() {
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT increment(increment(1));",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnexpectedNullArgumentThrowsRuntimeException() {
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT increment(NULL);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectMessage("CalcFn failed to evaluate");
    thrown.expectCause(
        allOf(isA(RuntimeException.class), hasProperty("cause", isA(NullPointerException.class))));
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExpectedNullArgument() {
    String sql =
        String.format(
            "CREATE FUNCTION isNull(s STRING) RETURNS BOOLEAN LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT isNull(NULL);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addBooleanField("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(true).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  public static class IncrementFn implements BeamSqlUdf {
    public Long eval(Long i) {
      return i + 1;
    }
  }

  @Test
  public void testSqlTransformRegisterUdf() {
    String sql = "SELECT increment(0);";
    PCollection<Row> stream =
        pipeline.apply(
            SqlTransform.query(sql)
                .withQueryPlannerClass(ZetaSQLQueryPlanner.class)
                .registerUdf("increment", IncrementFn.class));
    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  /** This tests a subset of the code path used by {@link #testSqlTransformRegisterUdf()}. */
  @Test
  public void testUdfFromCatalog() throws NoSuchMethodException {
    // Add IncrementFn to Calcite schema.
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(
            new ReadOnlyTableProvider("empty_table_provider", ImmutableMap.of()),
            PipelineOptionsFactory.create());
    Method method = IncrementFn.class.getMethod("eval", Long.class);
    jdbcConnection.getCurrentSchemaPlus().add("increment", ScalarFunctionImpl.create(method));
    this.config =
        Frameworks.newConfigBuilder(config)
            .defaultSchema(jdbcConnection.getCurrentSchemaPlus())
            .build();

    String sql = "SELECT increment(0);";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    final Schema schema = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testNullArgumentIsTypeChecked() {
    // The Java definition for isNull takes a String, but here we declare it in SQL with INT64.
    String sql =
        String.format(
            "CREATE FUNCTION isNull(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT isNull(NULL);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    // TODO(https://github.com/apache/beam/issues/20614) This should fail earlier, before compiling
    // the CalcFn.
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Could not compile CalcFn");
    thrown.expectCause(
        allOf(
            isA(CompileException.class),
            hasProperty(
                "message",
                containsString(
                    "No applicable constructor/method found for actual parameters \"java.lang.Long\""))));
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Test
  public void testFunctionSignatureTypeMismatchFailsPipelineConstruction() {
    // The Java definition for isNull takes a String, but here we pass it a Long.
    String sql =
        String.format(
            "CREATE FUNCTION isNull(i INT64) RETURNS BOOLEAN LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT isNull(0);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    // TODO(https://github.com/apache/beam/issues/20614) This should fail earlier, before compiling
    // the CalcFn.
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Could not compile CalcFn");
    thrown.expectCause(
        allOf(
            isA(CompileException.class),
            hasProperty(
                "message",
                containsString(
                    "No applicable constructor/method found for actual parameters \"long\""))));
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Test
  public void testJavaUdfWithNoReturnTypeIsRejected() {
    String sql =
        String.format(
            "CREATE FUNCTION helloWorld() LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT helloWorld();",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(SqlException.class);
    thrown.expectMessage("Non-SQL functions must specify a return type");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testProjectUdfAndBuiltin() {
    String sql =
        String.format(
            "CREATE FUNCTION matches(str STRING, regStr STRING) RETURNS BOOLEAN LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT matches(\"a\", \"a\"), 'apple'='beta'",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addBooleanField("field1").addBooleanField("field2").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(true, false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testProjectNestedUdfAndBuiltin() {
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT increment(increment(0) + 1);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema schema = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(schema).addValues(3L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testJavaUdfEmptyPath() {
    String sql =
        "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java OPTIONS (path=''); SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to define function 'foo'");
    thrown.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty("message", containsString("No jar was provided to define function foo."))));
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testJavaUdfNoJarProvided() {
    String sql = "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java; SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to define function 'foo'");
    thrown.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty("message", containsString("No jar was provided to define function foo."))));
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testPathOptionNotString() {
    String sql =
        "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java OPTIONS (path=23); SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to define function 'foo'");
    thrown.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString("Option 'path' has type TYPE_INT64 (expected TYPE_STRING)."))));
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testUdaf() {
    String sql =
        String.format(
            "CREATE AGGREGATE FUNCTION my_sum(f INT64) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT my_sum(f_int_1) from aggregate_test_table",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(28L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUdafNotFoundFailsToParse() {
    String sql =
        String.format(
            "CREATE AGGREGATE FUNCTION nonexistent(f INT64) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT nonexistent(f_int_1) from aggregate_test_table",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Failed to define function 'nonexistent'");
    thrown.expectCause(
        allOf(
            isA(IllegalArgumentException.class),
            hasProperty(
                "message",
                containsString("No implementation of aggregate function nonexistent found"))));

    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testRegisterUdaf() {
    String sql = "SELECT my_sum(k) FROM UNNEST([1, 2, 3]) k;";
    PCollection<Row> stream =
        pipeline.apply(
            SqlTransform.query(sql)
                .withQueryPlannerClass(ZetaSQLQueryPlanner.class)
                .registerUdaf("my_sum", Sum.ofLongs()));
    Schema singleField = Schema.builder().addInt64Field("field1").build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(6L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testDateUdf() {
    String sql =
        String.format(
            "CREATE FUNCTION dateIncrementAll(d DATE) RETURNS DATE LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT dateIncrementAll('2020-04-04');",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema singleField = Schema.builder().addLogicalTypeField("field1", SqlTypes.DATE).build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(singleField).addValues(LocalDate.of(2021, 5, 5)).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
