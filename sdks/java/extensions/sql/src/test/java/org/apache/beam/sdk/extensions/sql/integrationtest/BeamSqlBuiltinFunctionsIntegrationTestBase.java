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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.BeamSql;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.util.Pair;
import org.junit.Rule;

/**
 * Base class for all built-in functions integration tests.
 */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final Map<Class, Integer> JAVA_CLASS_TO_SQL_TYPE = ImmutableMap
      .<Class, Integer> builder()
      .put(Byte.class, Types.TINYINT)
      .put(Short.class, Types.SMALLINT)
      .put(Integer.class, Types.INTEGER)
      .put(Long.class, Types.BIGINT)
      .put(Float.class, Types.FLOAT)
      .put(Double.class, Types.DOUBLE)
      .put(BigDecimal.class, Types.DECIMAL)
      .put(String.class, Types.VARCHAR)
      .put(Date.class, Types.DATE)
      .put(Boolean.class, Types.BOOLEAN)
      .build();

  private static final BeamRecordSqlType RECORD_SQL_TYPE = BeamRecordSqlType.builder()
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

  protected PCollection<BeamRecord> getTestPCollection() {
    try {
      return MockedBoundedTable
          .of(RECORD_SQL_TYPE)
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
          .setCoder(RECORD_SQL_TYPE.getRecordCoder());
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
      PCollection<BeamRecord> inputCollection = getTestPCollection();
      System.out.println("SQL:>\n" + getSql());
      try {
        List<String> names = new ArrayList<>();
        List<Integer> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();

        for (Pair<String, Object> pair : exps) {
          names.add(pair.getKey());
          types.add(JAVA_CLASS_TO_SQL_TYPE.get(pair.getValue().getClass()));
          values.add(pair.getValue());
        }

        PCollection<BeamRecord> rows = inputCollection.apply(BeamSql.query(getSql()));
        PAssert.that(rows).containsInAnyOrder(
            TestUtils.RowsBuilder
                .of(BeamRecordSqlType.create(names, types))
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
