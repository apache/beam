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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for UDFs. */
@RunWith(JUnit4.class)
public class BeamSqlUdfImplTest extends BeamSqlDslBase {

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
  public void testLength() throws Exception {
    Schema resultType = Schema.builder().addInt64Field("field").build();
    Row resultRow = Row.withSchema(resultType).addValues(10L).build();
    Row resultRow2 = Row.withSchema(resultType).addValues(0L).build();
    Row resultRow3 = Row.withSchema(resultType).addValues(2L).build();
    String sql = "SELECT LENGTH(f_bytes) FROM PCOLLECTION";
    PCollection<Row> result = boundedInputBytes.apply("testUdf", SqlTransform.query(sql));
    PAssert.that(result).containsInAnyOrder(resultRow, resultRow2, resultRow3);
    pipeline.run().waitUntilFinish();
  }
}
