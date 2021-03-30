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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptPlanner.CannotPlanException;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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

    // Add BeamJavaUdfCalcRule to planner to enable UDFs.
    this.config =
        Frameworks.newConfigBuilder(config)
            .ruleSets(
                ZetaSQLQueryPlanner.getZetaSqlRuleSets(
                        ImmutableList.of(
                            BeamZetaSqlCalcRule.INSTANCE, BeamJavaUdfCalcRule.INSTANCE))
                    .toArray(new RuleSet[0]))
            .build();
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
                + "SELECT increment(increment(0));",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValues(2L).build());
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

  /**
   * This is a loophole in type checking. The SQL function signature does not need to match the Java
   * function signature; only the generated code is typechecked.
   */
  // TODO(BEAM-11171): fix this and adjust test accordingly.
  @Test
  public void testNullArgumentIsNotTypeChecked() {
    String sql =
        String.format(
            "CREATE FUNCTION isNull(i INT64) RETURNS INT64 LANGUAGE java "
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
    // TODO(BEAM-11171) This should fail earlier, before compiling the CalcFn.
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
  public void testBinaryJavaUdf() {
    String sql =
        String.format(
            "CREATE FUNCTION matches(str STRING, regStr STRING) RETURNS BOOLEAN LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT matches(\"a\", \"a\"), 'apple'='beta'",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(CannotPlanException.class);
    thrown.expectMessage(
        "There are not enough rules to produce a node with desired properties: convention=BEAM_LOGICAL.");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  // TODO(BEAM-11747) Add tests that mix UDFs and builtin functions that rely on the ZetaSQL C++
  // implementation.

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
}
