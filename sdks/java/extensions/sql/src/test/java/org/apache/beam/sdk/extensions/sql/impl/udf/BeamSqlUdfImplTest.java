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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlDslBase;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for UDFs. */
@RunWith(JUnit4.class)
public class BeamSqlUdfImplTest extends BeamSqlDslBase {

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testIsInf() throws Exception {
    Schema resultType =
        Schema.builder()
            .addBooleanField("field_1")
            .addBooleanField("field_2")
            .addBooleanField("field_3")
            .addBooleanField("field_4")
            .build();
    Row resultRow = Row.withSchema(resultType).addValues(true, true, true, true).build();

    String sql =
        "SELECT IS_INF(f_float_1), IS_INF(f_double_1), IS_INF(f_float_2), IS_INF(f_double_2) FROM PCOLLECTION";
    PCollection<Row> result = boundedInputFloatDouble.apply("testUdf", SqlTransform.query(sql));
    PAssert.that(result).containsInAnyOrder(resultRow);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testIsNan() throws Exception {
    Schema resultType =
        Schema.builder()
            .addBooleanField("field_1")
            .addBooleanField("field_2")
            .addBooleanField("field_3")
            .addBooleanField("field_4")
            .build();
    Row resultRow = Row.withSchema(resultType).addValues(false, false, true, true).build();

    String sql =
        "SELECT IS_NAN(f_float_2), IS_NAN(f_double_2), IS_NAN(f_float_3), IS_NAN(f_double_3) FROM PCOLLECTION";
    PCollection<Row> result = boundedInputFloatDouble.apply("testUdf", SqlTransform.query(sql));
    PAssert.that(result).containsInAnyOrder(resultRow);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCOSH() throws Exception {
    Schema resultType = Schema.builder().addNullableField("field", Schema.FieldType.DOUBLE).build();

    Row resultRow1 = Row.withSchema(resultType).addValues(Math.cosh(1.0)).build();
    String sql1 = "SELECT COSH(CAST(1.0 as DOUBLE))";
    PCollection<Row> result1 = boundedInput1.apply("testUdf1", SqlTransform.query(sql1));
    PAssert.that(result1).containsInAnyOrder(resultRow1);

    Row resultRow2 = Row.withSchema(resultType).addValues(Math.cosh(710.0)).build();
    String sql2 = "SELECT COSH(CAST(710.0 as DOUBLE))";
    PCollection<Row> result2 = boundedInput1.apply("testUdf2", SqlTransform.query(sql2));
    PAssert.that(result2).containsInAnyOrder(resultRow2);

    Row resultRow3 = Row.withSchema(resultType).addValues(Math.cosh(-1.0)).build();
    String sql3 = "SELECT COSH(CAST(-1.0 as DOUBLE))";
    PCollection<Row> result3 = boundedInput1.apply("testUdf3", SqlTransform.query(sql3));
    PAssert.that(result3).containsInAnyOrder(resultRow3);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testSINH() throws Exception {
    Schema resultType = Schema.builder().addNullableField("field", Schema.FieldType.DOUBLE).build();

    Row resultRow1 = Row.withSchema(resultType).addValues(Math.sinh(1.0)).build();
    String sql1 = "SELECT SINH(CAST(1.0 as DOUBLE))";
    PCollection<Row> result1 = boundedInput1.apply("testUdf1", SqlTransform.query(sql1));
    PAssert.that(result1).containsInAnyOrder(resultRow1);

    Row resultRow2 = Row.withSchema(resultType).addValues(Math.sinh(710.0)).build();
    String sql2 = "SELECT SINH(CAST(710.0 as DOUBLE))";
    PCollection<Row> result2 = boundedInput1.apply("testUdf2", SqlTransform.query(sql2));
    PAssert.that(result2).containsInAnyOrder(resultRow2);

    Row resultRow3 = Row.withSchema(resultType).addValues(Math.sinh(-1.0)).build();
    String sql3 = "SELECT SINH(CAST(-1.0 as DOUBLE))";
    PCollection<Row> result3 = boundedInput1.apply("testUdf3", SqlTransform.query(sql3));
    PAssert.that(result3).containsInAnyOrder(resultRow3);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testTANH() throws Exception {
    Schema resultType = Schema.builder().addNullableField("field", Schema.FieldType.DOUBLE).build();

    Row resultRow1 = Row.withSchema(resultType).addValues(Math.tanh(1.0)).build();
    String sql1 = "SELECT TANH(CAST(1.0 as DOUBLE))";
    PCollection<Row> result1 = boundedInput1.apply("testUdf1", SqlTransform.query(sql1));
    PAssert.that(result1).containsInAnyOrder(resultRow1);

    Row resultRow2 = Row.withSchema(resultType).addValues(Math.tanh(0.0)).build();
    String sql2 = "SELECT TANH(CAST(0.0 as DOUBLE))";
    PCollection<Row> result2 = boundedInput1.apply("testUdf2", SqlTransform.query(sql2));
    PAssert.that(result2).containsInAnyOrder(resultRow2);

    Row resultRow3 = Row.withSchema(resultType).addValues(Math.tanh(-1.0)).build();
    String sql3 = "SELECT TANH(CAST(-1.0 as DOUBLE))";
    PCollection<Row> result3 = boundedInput1.apply("testUdf3", SqlTransform.query(sql3));
    PAssert.that(result3).containsInAnyOrder(resultRow3);

    pipeline.run().waitUntilFinish();
  }
}
