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

package org.apache.beam.dsls.sql.integrationtest;

import com.google.common.base.Joiner;
import java.math.BigDecimal;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.dsls.sql.BeamSql;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRecordType;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.util.Pair;
import org.junit.Rule;

/**
 * Base class for all built-in functions integration tests.
 */
public class BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final Map<Class, Integer> JAVA_CLASS_TO_SQL_TYPE = new HashMap<>();
  static {
    JAVA_CLASS_TO_SQL_TYPE.put(Byte.class, Types.TINYINT);
    JAVA_CLASS_TO_SQL_TYPE.put(Short.class, Types.SMALLINT);
    JAVA_CLASS_TO_SQL_TYPE.put(Integer.class, Types.INTEGER);
    JAVA_CLASS_TO_SQL_TYPE.put(Long.class, Types.BIGINT);
    JAVA_CLASS_TO_SQL_TYPE.put(Float.class, Types.FLOAT);
    JAVA_CLASS_TO_SQL_TYPE.put(Double.class, Types.DOUBLE);
    JAVA_CLASS_TO_SQL_TYPE.put(BigDecimal.class, Types.DECIMAL);
    JAVA_CLASS_TO_SQL_TYPE.put(String.class, Types.VARCHAR);
    JAVA_CLASS_TO_SQL_TYPE.put(Date.class, Types.DATE);
  }

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  protected PCollection<BeamSqlRow> getTestPCollection() {
    BeamSqlRecordType type = BeamSqlRecordType.create(
        Arrays.asList("ts", "c_tinyint", "c_smallint",
            "c_integer", "c_bigint", "c_float", "c_double", "c_decimal",
            "c_tinyint_max", "c_smallint_max", "c_integer_max", "c_bigint_max"),
        Arrays.asList(Types.DATE, Types.TINYINT, Types.SMALLINT,
            Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE, Types.DECIMAL,
            Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT)
    );
    try {
      return MockedBoundedTable
          .of(type)
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
          .setCoder(new BeamSqlRowCoder(type));
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
      PCollection<BeamSqlRow> inputCollection = getTestPCollection();
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

        PCollection<BeamSqlRow> rows = inputCollection.apply(BeamSql.simpleQuery(getSql()));
        PAssert.that(rows).containsInAnyOrder(
            TestUtils.RowsBuilder
                .of(BeamSqlRecordType.create(names, types))
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
