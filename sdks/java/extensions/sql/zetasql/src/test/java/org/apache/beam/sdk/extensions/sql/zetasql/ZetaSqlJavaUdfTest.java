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

import static org.junit.Assume.assumeNotNull;

import java.nio.file.ProviderNotFoundException;
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

/**
 * Tests for ZetaSQL UDFs written in Java.
 *
 * <p>Some tests will be ignored unless the system property <code>beam.sql.udf.test.jarpath</code>
 * is set.
 */
@RunWith(JUnit4.class)
public class ZetaSqlJavaUdfTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private final String jarPath = System.getProperty("beam.sql.udf.test.jar_path");
  private final String emptyJarPath = System.getProperty("beam.sql.udf.test.empty_jar_path");

  @Before
  public void setUp() {
    initialize();
  }

  @Test
  public void testNullaryJavaUdf() {
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT foo();",
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
  public void testNestedJavaUdf() {
    assumeNotNull(jarPath);
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
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE FUNCTION increment(i INT64) RETURNS INT64 LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT increment(NULL);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("CalcFn failed to evaluate");
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testExpectedNullArgument() {
    assumeNotNull(jarPath);
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
  // TODO(ibzib): fix this and adjust test accordingly.
  @Test
  public void testNullArgumentIsNotTypeChecked() {
    assumeNotNull(jarPath);
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
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE FUNCTION isNull(i INT64) RETURNS BOOLEAN LANGUAGE java "
                + "OPTIONS (path='%s'); "
                + "SELECT isNull(0);",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Could not compile CalcFn");
    BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
  }

  @Test
  public void testBinaryJavaUdf() {
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE FUNCTION fun(str STRING, regStr STRING) RETURNS BOOLEAN LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT fun(\"a\", \"a\"), 'apple'='beta'",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField =
        Schema.builder().addBooleanField("field1").addBooleanField("field2").build();

    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(singleField).addValues(true, false).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testBuiltinAggregateUdf() {
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE AGGREGATE FUNCTION agg_fun(str STRING) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT agg_fun(Value) from KeyValue",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValue(2L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testCustomAggregateUdf() {
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE AGGREGATE FUNCTION custom_agg(f INT64) RETURNS INT64 LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT custom_agg(f_int_1) from aggregate_test_table",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema singleField = Schema.builder().addInt64Field("field1").build();

    PAssert.that(stream).containsInAnyOrder(Row.withSchema(singleField).addValue(0L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testUnregisteredFunction() {
    assumeNotNull(jarPath);
    String sql =
        String.format(
            "CREATE FUNCTION notRegistered() RETURNS STRING LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT notRegistered();",
            jarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(
        String.format("No implementation of scalar function notRegistered found in %s.", jarPath));
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testJarContainsNoUdfProviders() {
    assumeNotNull(jarPath);
    assumeNotNull(emptyJarPath);
    String sql =
        String.format(
            // Load an inhabited jar first so we can make sure jars load UdfProviders in isolation
            // from other jars.
            "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java OPTIONS (path='%s'); "
                + "CREATE FUNCTION bar() RETURNS STRING LANGUAGE java OPTIONS (path='%s'); "
                + "SELECT bar();",
            jarPath, emptyJarPath);
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(ProviderNotFoundException.class);
    thrown.expectMessage(String.format("No UdfProvider implementation found in %s.", emptyJarPath));
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testJavaUdfNoJarProvided() {
    String sql = "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java; SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No jar was provided to define function foo.");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }

  @Test
  public void testPathOptionNotString() {
    String sql =
        "CREATE FUNCTION foo() RETURNS STRING LANGUAGE java OPTIONS (path=23); SELECT foo();";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Option 'path' has type TYPE_INT64 (expected TYPE_STRING).");
    zetaSQLQueryPlanner.convertToBeamRel(sql);
  }
}
