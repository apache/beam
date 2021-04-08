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

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.impl.JdbcConnection;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.ScalarFunctionImpl;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
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

  private static final TestBoundedTable table =
      TestBoundedTable.of(
              Schema.builder()
                  .addBooleanField("boolean_true")
                  .addBooleanField("boolean_false")
                  .addInt64Field("int64_0")
                  .addInt64Field("int64_pos")
                  .addInt64Field("int64_neg")
                  .addInt64Field("int64_max")
                  .addInt64Field("int64_min")
                  .addStringField("string_empty")
                  .addStringField("string_ascii")
                  .addStringField("string_unicode")
                  .addByteArrayField("bytes_empty")
                  .addByteArrayField("bytes_ascii")
                  .addByteArrayField("bytes_unicode")
                  .addDoubleField("float64_0")
                  .addDoubleField("float64_noninteger")
                  .addDoubleField("float64_pos")
                  .addDoubleField("float64_neg")
                  .addDoubleField("float64_max")
                  .addDoubleField("float64_min_pos")
                  .addDoubleField("float64_inf")
                  .addDoubleField("float64_neg_inf")
                  .addDoubleField("float64_nan")
                  .build())
          .addRows(
              true /* boolean_true */,
              false /* boolean_false */,
              0L /* int64_0 */,
              123L /* int64_pos */,
              -123L /* int64_neg */,
              9223372036854775807L /* int64_max */,
              -9223372036854775808L /* int64_min */,
              "" /* string_empty */,
              "abc" /* string_ascii */,
              "スタリング" /* string_unicode */,
              new byte[] {} /* bytes_empty */,
              new byte[] {'a', 'b', 'c'} /* bytes_ascii */,
              new byte[] {-29, -126, -71} /* bytes_unicode */,
              0.0 /* float64_0 */,
              0.123 /* float64_noninteger */,
              123.0 /* float64_pos */,
              -123.0 /* float64_neg */,
              1.7976931348623157e+308 /* float64_max */,
              2.2250738585072014e-308 /* float64_min_pos */,
              Double.POSITIVE_INFINITY /* float64_inf */,
              Double.NEGATIVE_INFINITY /* float64_neg_inf */,
              Double.NaN /* float64_nan */);

  @Before
  public void setUp() throws NoSuchMethodException {
    initialize();

    // Register test table.
    JdbcConnection jdbcConnection =
        JdbcDriver.connect(
            new ReadOnlyTableProvider("table_provider", ImmutableMap.of("table", table)),
            PipelineOptionsFactory.create());

    // Register UDFs.
    SchemaPlus schema = jdbcConnection.getCurrentSchemaPlus();
    schema.add(
        "test_boolean",
        ScalarFunctionImpl.create(BooleanIdentityFn.class.getMethod("eval", Boolean.class)));
    schema.add(
        "test_int64",
        ScalarFunctionImpl.create(Int64IdentityFn.class.getMethod("eval", Long.class)));
    schema.add(
        "test_string",
        ScalarFunctionImpl.create(StringIdentityFn.class.getMethod("eval", String.class)));
    schema.add(
        "test_bytes",
        ScalarFunctionImpl.create(BytesIdentityFn.class.getMethod("eval", byte[].class)));
    schema.add(
        "test_float64",
        ScalarFunctionImpl.create(DoubleIdentityFn.class.getMethod("eval", Double.class)));

    this.config =
        Frameworks.newConfigBuilder(config)
            .defaultSchema(schema)
            // Add BeamJavaUdfCalcRule to planner to enable UDFs.
            .ruleSets(
                ZetaSQLQueryPlanner.getZetaSqlRuleSets(
                        ImmutableList.of(BeamJavaUdfCalcRule.INSTANCE))
                    .toArray(new RuleSet[0]))
            .build();
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

  private void runUdfTypeTest(String query, Object result, Schema.TypeName typeName) {
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(query);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);

    Schema outputSchema = Schema.builder().addField("res", Schema.FieldType.of(typeName)).build();
    PAssert.that(stream).containsInAnyOrder(Row.withSchema(outputSchema).addValues(result).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void testTrueLiteral() {
    runUdfTypeTest("SELECT test_boolean(true);", true, Schema.TypeName.BOOLEAN);
  }

  @Test
  public void testTrueInput() {
    runUdfTypeTest("SELECT test_boolean(boolean_true) FROM table;", true, Schema.TypeName.BOOLEAN);
  }

  @Test
  public void testFalseLiteral() {
    runUdfTypeTest("SELECT test_boolean(false);", false, Schema.TypeName.BOOLEAN);
  }

  @Test
  public void testFalseInput() {
    runUdfTypeTest(
        "SELECT test_boolean(boolean_false) FROM table;", false, Schema.TypeName.BOOLEAN);
  }

  @Test
  public void testZeroInt64Literal() {
    runUdfTypeTest("SELECT test_int64(0);", 0L, Schema.TypeName.INT64);
  }

  @Test
  public void testZeroInt64Input() {
    runUdfTypeTest("SELECT test_int64(int64_0) FROM table;", 0L, Schema.TypeName.INT64);
  }

  @Test
  public void testPosInt64Literal() {
    runUdfTypeTest("SELECT test_int64(123);", 123L, Schema.TypeName.INT64);
  }

  @Test
  public void testPosInt64Input() {
    runUdfTypeTest("SELECT test_int64(int64_pos) FROM table;", 123L, Schema.TypeName.INT64);
  }

  @Test
  public void testNegInt64Literal() {
    runUdfTypeTest("SELECT test_int64(-123);", -123L, Schema.TypeName.INT64);
  }

  @Test
  public void testNegInt64Input() {
    runUdfTypeTest("SELECT test_int64(int64_neg) FROM table;", -123L, Schema.TypeName.INT64);
  }

  @Test
  public void testMaxInt64Literal() {
    runUdfTypeTest(
        "SELECT test_int64(9223372036854775807);", 9223372036854775807L, Schema.TypeName.INT64);
  }

  @Test
  public void testMaxInt64Input() {
    runUdfTypeTest(
        "SELECT test_int64(int64_max) FROM table;", 9223372036854775807L, Schema.TypeName.INT64);
  }

  @Test
  public void testMinInt64Literal() {
    runUdfTypeTest(
        "SELECT test_int64(-9223372036854775808);", -9223372036854775808L, Schema.TypeName.INT64);
  }

  @Test
  public void testMinInt64Input() {
    runUdfTypeTest(
        "SELECT test_int64(int64_min) FROM table;", -9223372036854775808L, Schema.TypeName.INT64);
  }

  @Test
  public void testEmptyStringLiteral() {
    runUdfTypeTest("SELECT test_string('');", "", Schema.TypeName.STRING);
  }

  @Test
  public void testEmptyStringInput() {
    runUdfTypeTest("SELECT test_string(string_empty) FROM table;", "", Schema.TypeName.STRING);
  }

  @Test
  public void testAsciiStringLiteral() {
    runUdfTypeTest("SELECT test_string('abc');", "abc", Schema.TypeName.STRING);
  }

  @Test
  public void testAsciiStringInput() {
    runUdfTypeTest("SELECT test_string(string_ascii) FROM table;", "abc", Schema.TypeName.STRING);
  }

  @Test
  public void testUnicodeStringLiteral() {
    runUdfTypeTest("SELECT test_string('スタリング');", "スタリング", Schema.TypeName.STRING);
  }

  @Test
  public void testUnicodeStringInput() {
    runUdfTypeTest(
        "SELECT test_string(string_unicode) FROM table;", "スタリング", Schema.TypeName.STRING);
  }

  @Test
  public void testEmptyBytesLiteral() {
    runUdfTypeTest("SELECT test_bytes(b'');", new byte[] {}, Schema.TypeName.BYTES);
  }

  @Test
  public void testEmptyBytesInput() {
    runUdfTypeTest(
        "SELECT test_bytes(bytes_empty) FROM table;", new byte[] {}, Schema.TypeName.BYTES);
  }

  @Test
  public void testAsciiBytesLiteral() {
    runUdfTypeTest("SELECT test_bytes(b'abc');", new byte[] {'a', 'b', 'c'}, Schema.TypeName.BYTES);
  }

  @Test
  public void testAsciiBytesInput() {
    runUdfTypeTest(
        "SELECT test_bytes(bytes_ascii) FROM table;",
        new byte[] {'a', 'b', 'c'},
        Schema.TypeName.BYTES);
  }

  @Test
  public void testUnicodeBytesLiteral() {
    runUdfTypeTest("SELECT test_bytes(b'ス');", new byte[] {-29, -126, -71}, Schema.TypeName.BYTES);
  }

  @Test
  public void testUnicodeBytesInput() {
    runUdfTypeTest(
        "SELECT test_bytes(bytes_unicode) FROM table;",
        new byte[] {-29, -126, -71},
        Schema.TypeName.BYTES);
  }

  @Test
  public void testZeroFloat64Literal() {
    runUdfTypeTest("SELECT test_float64(0.0);", 0.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testZeroFloat64Input() {
    runUdfTypeTest("SELECT test_float64(float64_0) FROM table;", 0.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNonIntegerFloat64Literal() {
    runUdfTypeTest("SELECT test_float64(0.123);", 0.123, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNonIntegerFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_noninteger) FROM table;", 0.123, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testPosFloat64Literal() {
    runUdfTypeTest("SELECT test_float64(123.0);", 123.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testPosFloat64Input() {
    runUdfTypeTest("SELECT test_float64(float64_pos) FROM table;", 123.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNegFloat64Literal() {
    runUdfTypeTest("SELECT test_float64(-123.0);", -123.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNegFloat64Input() {
    runUdfTypeTest("SELECT test_float64(float64_neg) FROM table;", -123.0, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testMaxFloat64Literal() {
    runUdfTypeTest(
        "SELECT test_float64(1.7976931348623157e+308);",
        1.7976931348623157e+308,
        Schema.TypeName.DOUBLE);
  }

  @Test
  public void testMaxFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_max) FROM table;",
        1.7976931348623157e+308,
        Schema.TypeName.DOUBLE);
  }

  @Test
  public void testMinPosFloat64Literal() {
    runUdfTypeTest(
        "SELECT test_float64(2.2250738585072014e-308);",
        2.2250738585072014e-308,
        Schema.TypeName.DOUBLE);
  }

  @Test
  public void testMinPosFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_min_pos) FROM table;",
        2.2250738585072014e-308,
        Schema.TypeName.DOUBLE);
  }

  @Test
  @Ignore(
      "+inf is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testPosInfFloat64Literal() {
    runUdfTypeTest(
        "SELECT test_float64(CAST('+inf' AS FLOAT64));",
        Double.POSITIVE_INFINITY,
        Schema.TypeName.DOUBLE);
  }

  @Test
  public void testPosInfFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_inf) FROM table;",
        Double.POSITIVE_INFINITY,
        Schema.TypeName.DOUBLE);
  }

  @Test
  @Ignore(
      "-inf is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testNegInfFloat64Literal() {
    runUdfTypeTest(
        "SELECT test_float64(CAST('-inf' AS FLOAT64));",
        Double.NEGATIVE_INFINITY,
        Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNegInfFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_neg_inf) FROM table;",
        Double.NEGATIVE_INFINITY,
        Schema.TypeName.DOUBLE);
  }

  @Test
  @Ignore(
      "NaN is implemented as a ZetaSQL builtin function, so combining it with a UDF requires Calc splitting (BEAM-12009).")
  public void testNaNFloat64Literal() {
    runUdfTypeTest(
        "SELECT test_float64(CAST('NaN' AS FLOAT64));", Double.NaN, Schema.TypeName.DOUBLE);
  }

  @Test
  public void testNaNFloat64Input() {
    runUdfTypeTest(
        "SELECT test_float64(float64_nan) FROM table;", Double.NaN, Schema.TypeName.DOUBLE);
  }
}
