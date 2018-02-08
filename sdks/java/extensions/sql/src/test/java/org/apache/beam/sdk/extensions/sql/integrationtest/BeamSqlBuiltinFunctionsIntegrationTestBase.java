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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.values.RowType.toRowType;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.RowSqlType;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoder;
import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.apache.calcite.util.Pair;
import org.junit.Rule;

/**
 * Base class for all built-in functions integration tests.
 */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final Map<Class, SqlTypeCoder> JAVA_CLASS_TO_CODER = ImmutableMap
      .<Class, SqlTypeCoder>builder()
      .put(Byte.class, SqlTypeCoders.TINYINT)
      .put(Short.class, SqlTypeCoders.SMALLINT)
      .put(Integer.class, SqlTypeCoders.INTEGER)
      .put(Long.class, SqlTypeCoders.BIGINT)
      .put(Float.class, SqlTypeCoders.FLOAT)
      .put(Double.class, SqlTypeCoders.DOUBLE)
      .put(BigDecimal.class, SqlTypeCoders.DECIMAL)
      .put(String.class, SqlTypeCoders.VARCHAR)
      .put(Date.class, SqlTypeCoders.DATE)
      .put(Boolean.class, SqlTypeCoders.BOOLEAN)
      .build();

  private static final RowType ROW_TYPE = RowSqlType.builder()
      .withDateField("ts")
      .withTinyIntField("c_tinyint")
      .withSmallIntField("c_smallint")
      .withIntegerField("c_integer")
      .withBigIntField("c_bigint")
      .withFloatField("c_float")
      .withDoubleField("c_double")
      .withDecimalField("c_decimal")
      .withTinyIntField("c_tinyint_max")
      .withSmallIntField("c_smallint_max")
      .withIntegerField("c_integer_max")
      .withBigIntField("c_bigint_max")
      .build();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<Row> getTestPCollection() {
    try {
      return MockedBoundedTable
          .of(ROW_TYPE)
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
              9223372036854775807L
          )
          .buildIOReader(pipeline)
          .setCoder(ROW_TYPE.getRowCoder());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected static Date parseDate(String str) {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
      return sdf.parse(str);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Helper class to make write integration test for built-in functions easier.
   *
   * <p>example usage:
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
    private transient List<Pair<String, Object>> exps = new ArrayList<>();

    public ExpressionChecker addExpr(String expression, Object expectedValue) {
      exps.add(Pair.of(expression, expectedValue));
      return this;
    }

    private String getSql() {
      List<String> expStrs = new ArrayList<>();
      for (Pair<String, Object> pair : exps) {
        expStrs.add(pair.getKey());
      }
      return "SELECT " + Joiner.on(",\n  ").join(expStrs) + " FROM PCOLLECTION";
    }

    /**
     * Build the corresponding SQL, compile to Beam Pipeline, run it, and check the result.
     */
    public void buildRunAndCheck() {
      PCollection<Row> inputCollection = getTestPCollection();
      System.out.println("SQL:>\n" + getSql());
      try {
        RowType rowType =
            exps.stream()
                .map(exp -> RowType.newField(
                    exp.getKey(),
                    JAVA_CLASS_TO_CODER.get(exp.getValue().getClass())))
                .collect(toRowType());

        List<Object> values = exps.stream().map(Pair::getValue).collect(toList());

        PCollection<Row> rows = inputCollection.apply(BeamSql.query(getSql()));
        PAssert.that(rows).containsInAnyOrder(
            TestUtils.RowsBuilder
                .of(rowType)
                .addRows(values)
                .getRows()
        );
        inputCollection.getPipeline().run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
