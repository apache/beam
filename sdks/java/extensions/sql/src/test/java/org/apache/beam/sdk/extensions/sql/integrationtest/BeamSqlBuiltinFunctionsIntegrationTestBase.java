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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Rule;

/** Base class for all built-in functions integration tests. */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {

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

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<Row> getTestPCollection() {
    try {
      return MockedBoundedTable.of(ROW_TYPE)
          .addRows(
              parseDate("1986-02-15 11:35:26"),
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
          .setCoder(ROW_TYPE.getRowCoder());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static DateTime parseDate(String str) {
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC().parseDateTime(str);
  }

  @AutoValue
  abstract static class ExpressionTestCase {

    private static ExpressionTestCase of(
        String sqlExpr, Object expectedResult, FieldType resultFieldType) {
      return new AutoValue_BeamSqlBuiltinFunctionsIntegrationTestBase_ExpressionTestCase(
          sqlExpr, expectedResult, resultFieldType);
    }

    abstract String sqlExpr();

    abstract Object expectedResult();

    abstract FieldType resultFieldType();
  }

  /**
   * Helper class to make write integration test for built-in functions easier.
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
      // Because of erasure, we can only automatically infer non-parameterized types
      TypeName resultTypeName = JAVA_CLASS_TO_TYPENAME.get(expectedValue.getClass());
      checkArgument(
          resultTypeName != null,
          "Could not infer a Beam type for %s."
              + " Parameterized types must be provided explicitly.");
      addExpr(expression, expectedValue, FieldType.of(resultTypeName));
      return this;
    }

    public ExpressionChecker addExpr(
        String expression, Object expectedValue, FieldType resultFieldType) {
      exps.add(ExpressionTestCase.of(expression, expectedValue, resultFieldType));
      return this;
    }

    /** Build the corresponding SQL, compile to Beam Pipeline, run it, and check the result. */
    public void buildRunAndCheck() {
      PCollection<Row> inputCollection = getTestPCollection();

      for (ExpressionTestCase testCase : exps) {
        String expression = testCase.sqlExpr();
        Object expectedValue = testCase.expectedResult();
        String sql = String.format("SELECT %s FROM PCOLLECTION", expression);
        Schema schema = Schema.builder().addField(expression, testCase.resultFieldType()).build();

        PCollection<Row> output =
            inputCollection.apply(testCase.toString(), SqlTransform.query(sql));

        PAssert.that(output)
            .containsInAnyOrder(TestUtils.RowsBuilder.of(schema).addRows(expectedValue).getRows());
      }

      inputCollection.getPipeline().run();
    }
  }
}
