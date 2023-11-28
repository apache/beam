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
package org.apache.beam.sdk.schemas.transforms.providers;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.util.function.Function;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

public class JavaRowUdfTest {

  public static final Schema TEST_SCHEMA =
      Schema.of(
          Schema.Field.of("anInt32", Schema.FieldType.INT32).withNullable(true),
          Schema.Field.of("anInt64", Schema.FieldType.INT64).withNullable(true),
          Schema.Field.of("aDouble", Schema.FieldType.DOUBLE).withNullable(true));

  @Test
  public void testExpressionUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder().setExpression("anInt32 + anInt64").build(),
            TEST_SCHEMA);
    assertEquals(Schema.FieldType.INT64, udf.getOutputType());
    assertEquals(
        5L,
        udf.getFunction()
            .apply(
                Row.withSchema(TEST_SCHEMA)
                    .withFieldValue("anInt32", 2)
                    .withFieldValue("anInt64", 3L)
                    .build()));
  }

  @Test
  public void testFieldNameExpressionUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder().setExpression("anInt32").build(), TEST_SCHEMA);
    assertEquals(Schema.FieldType.INT32.withNullable(true), udf.getOutputType());
    assertEquals(
        2,
        udf.getFunction()
            .apply(
                Row.withSchema(TEST_SCHEMA)
                    .withFieldValue("anInt32", 2)
                    .withFieldValue("anInt64", 3L)
                    .build()));
  }

  @Test
  public void testCallableUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder()
                .setCallable(
                    String.join(
                        "\n",
                        "import org.apache.beam.sdk.values.Row;",
                        "import java.util.function.Function;",
                        "public class MyFunction implements Function<Row, Double> {",
                        "  public Double apply(Row row) { return 1.0 / row.getDouble(\"aDouble\"); }",
                        "}"))
                .build(),
            TEST_SCHEMA);
    assertEquals(Schema.FieldType.DOUBLE, udf.getOutputType());
    assertEquals(
        0.25,
        udf.getFunction()
            .apply(Row.withSchema(TEST_SCHEMA).withFieldValue("aDouble", 4.0).build()));
  }

  public static class TestFunction implements Function<Row, Double> {
    @Override
    public Double apply(Row row) {
      return 1.0 / row.getDouble("aDouble");
    }
  }

  public static double staticTestMethod(Row row) {
    return 1.0 / row.getDouble("aDouble");
  }

  public static class TestClassWithMethod {
    public double testMethod(Row row) {
      return 1.0 / row.getDouble("aDouble");
    }
  }

  @Test
  public void testNamedFunctionUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder()
                .setName(getClass().getTypeName() + "$TestFunction")
                .build(),
            TEST_SCHEMA);
    assertEquals(Schema.FieldType.DOUBLE, udf.getOutputType());
    assertEquals(
        0.25,
        udf.getFunction()
            .apply(Row.withSchema(TEST_SCHEMA).withFieldValue("aDouble", 4.0).build()));
  }

  @Test
  public void testClassMethodUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder()
                .setName(getClass().getTypeName() + "$TestClassWithMethod::testMethod")
                .build(),
            TEST_SCHEMA);
    assertEquals(Schema.FieldType.DOUBLE, udf.getOutputType());
    assertEquals(
        0.25,
        udf.getFunction()
            .apply(Row.withSchema(TEST_SCHEMA).withFieldValue("aDouble", 4.0).build()));
  }

  @Test
  public void testStaticMethodUdf()
      throws MalformedURLException, ReflectiveOperationException, StringCompiler.CompileException {
    JavaRowUdf udf =
        new JavaRowUdf(
            JavaRowUdf.Configuration.builder()
                .setName(getClass().getTypeName() + "::staticTestMethod")
                .build(),
            TEST_SCHEMA);
    assertEquals(Schema.FieldType.DOUBLE, udf.getOutputType());
    assertEquals(
        0.25,
        udf.getFunction()
            .apply(Row.withSchema(TEST_SCHEMA).withFieldValue("aDouble", 4.0).build()));
  }
}
