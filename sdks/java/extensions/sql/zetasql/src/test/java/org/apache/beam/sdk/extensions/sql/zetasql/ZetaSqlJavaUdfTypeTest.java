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

import java.lang.reflect.Method;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.Frameworks;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests verifying that various data types can be passed through Java UDFs without data loss. */
@RunWith(JUnit4.class)
public class ZetaSqlJavaUdfTypeTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private Method boolUdf;
  private Method longUdf;
  private Method stringUdf;
  private Method bytesUdf;
  private Method doubleUdf;

  @Before
  public void setUp() throws NoSuchMethodException {
    initialize();

    // Add BeamJavaUdfCalcRule to planner to enable UDFs.
    this.config =
        Frameworks.newConfigBuilder(config)
            .ruleSets(
                ZetaSQLQueryPlanner.getZetaSqlRuleSets(
                        ImmutableList.of(BeamJavaUdfCalcRule.INSTANCE))
                    .toArray(new RuleSet[0]))
            .build();

    // Look up UDF methods.
    this.boolUdf = BooleanIdentityFn.class.getMethod("eval", Boolean.class);
    this.longUdf = Int64IdentityFn.class.getMethod("eval", Long.class);
    this.stringUdf = StringIdentityFn.class.getMethod("eval", String.class);
    this.bytesUdf = BytesIdentityFn.class.getMethod("eval", byte[].class);
    this.doubleUdf = DoubleIdentityFn.class.getMethod("eval", Double.class);
  }

  public static class BooleanIdentityFn implements BeamSqlUdf {
    public Boolean eval(Boolean input) {
      return input;
    }
  }

  public static class Int64IdentityFn implements BeamSqlUdf {
    public Long eval(Long input) {
      return input;
    }
  }

  public static class StringIdentityFn implements BeamSqlUdf {
    public String eval(String input) {
      return input;
    }
  }

  public static class BytesIdentityFn implements BeamSqlUdf {
    public byte[] eval(byte[] input) {
      return input;
    }
  }

  public static class DoubleIdentityFn implements BeamSqlUdf {
    public Double eval(Double input) {
      return input;
    }
  }

  private void runUdfTypeTest(String query, Object result, Schema.TypeName typeName, Method udf)
      throws NoSuchMethodException {
    // Add UDF to Calcite schema.
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(
            new ReadOnlyTableProvider("empty_table_provider", ImmutableMap.of()),
            PipelineOptionsFactory.create());
    jdbcConnection.getCurrentSchemaPlus().add("test", ScalarFunctionImpl.create(udf));
    config =
        Frameworks.newConfigBuilder(config)
            .defaultSchema(jdbcConnection.getCurrentSchemaPlus())
            .build();

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(query);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema outputSchema = Schema.builder().addField("res", Schema.FieldType.of(typeName)).build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(outputSchema).addValues(result).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrueLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(true);", true, Schema.TypeName.BOOLEAN, boolUdf);
  }

  @Test
  public void testFalseLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(false);", false, Schema.TypeName.BOOLEAN, boolUdf);
  }

  @Test
  public void testZeroInt64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(0);", 0L, Schema.TypeName.INT64, longUdf);
  }

  @Test
  public void testPosInt64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(123);", 123L, Schema.TypeName.INT64, longUdf);
  }

  @Test
  public void testNegInt64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(-123);", -123L, Schema.TypeName.INT64, longUdf);
  }

  @Test
  public void testMaxInt64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(9223372036854775807);", 9223372036854775807L, Schema.TypeName.INT64, longUdf);
  }

  @Test
  public void testMinInt64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(-9223372036854775808);",
        -9223372036854775808L,
        Schema.TypeName.INT64,
        longUdf);
  }

  @Test
  public void testEmptyStringLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test('');", "", Schema.TypeName.STRING, stringUdf);
  }

  @Test
  public void testAsciiStringLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test('abc');", "abc", Schema.TypeName.STRING, stringUdf);
  }

  @Test
  public void testUnicodeStringLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test('スタリング');", "スタリング", Schema.TypeName.STRING, stringUdf);
  }

  @Test
  public void testEmptyBytesLiteral() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(b'');", new byte[] {}, Schema.TypeName.BYTES, bytesUdf);
  }

  @Test
  public void testAsciiBytesLiteral() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(b'abc');", new byte[] {'a', 'b', 'c'}, Schema.TypeName.BYTES, bytesUdf);
  }

  @Test
  public void testUnicodeBytesLiteral() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(b'ス');", new byte[] {-29, -126, -71}, Schema.TypeName.BYTES, bytesUdf);
  }

  @Test
  public void testZeroFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(0.0);", 0.0, Schema.TypeName.DOUBLE, doubleUdf);
  }

  @Test
  public void testNonIntegerFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(0.123);", 0.123, Schema.TypeName.DOUBLE, doubleUdf);
  }

  @Test
  public void testPosFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(123.0);", 123.0, Schema.TypeName.DOUBLE, doubleUdf);
  }

  @Test
  public void testNegFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest("SELECT test(-123.0);", -123.0, Schema.TypeName.DOUBLE, doubleUdf);
  }

  @Test
  public void testMaxFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(1.7976931348623157e+308);",
        1.7976931348623157e+308,
        Schema.TypeName.DOUBLE,
        doubleUdf);
  }

  @Test
  public void testMinPosFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(2.2250738585072014e-308);",
        2.2250738585072014e-308,
        Schema.TypeName.DOUBLE,
        doubleUdf);
  }

  @Test
  @Ignore(
      "+inf is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testPosInfFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(CAST('+inf' AS FLOAT64));",
        Double.POSITIVE_INFINITY,
        Schema.TypeName.DOUBLE,
        doubleUdf);
  }

  @Test
  @Ignore(
      "-inf is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testNegInfFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(CAST('-inf' AS FLOAT64));",
        Double.NEGATIVE_INFINITY,
        Schema.TypeName.DOUBLE,
        doubleUdf);
  }

  @Test
  @Ignore(
      "NaN is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testNaNFloat64Literal() throws NoSuchMethodException {
    runUdfTypeTest(
        "SELECT test(CAST('NaN' AS FLOAT64));", Double.NaN, Schema.TypeName.DOUBLE, doubleUdf);
  }

  // TODO(ibzib) Test that dates and times are rejected.
  // TODO(ibzib) Test arrays.
  // TODO(ibzib) Test structs.
  // TODO(ibzib) Test input refs.
}
