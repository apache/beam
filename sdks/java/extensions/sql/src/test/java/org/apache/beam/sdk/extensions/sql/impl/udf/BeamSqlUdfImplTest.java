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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlDslBase;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.UdfUdafProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
    PCollection<Row> result =
        boundedInputFloatDouble.apply("testUdf", SqlTransform.query(sql).withAutoUdfUdafLoad(true));
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
    PCollection<Row> result =
        boundedInputFloatDouble.apply("testUdf", SqlTransform.query(sql).withAutoUdfUdafLoad(true));
    PAssert.that(result).containsInAnyOrder(resultRow);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testGreatest() throws Exception {
    Schema resultType = Schema.builder().addDoubleField("field").build();

    Row resultRow1 = Row.withSchema(resultType).addValues(5.0).build();

    String sql1 = "SELECT GREATEST(arr) FROM PCOLLECTION WHERE id = 1";
    PCollection<Row> result1 =
        boundedInputArray.apply("testUdf1", SqlTransform.query(sql1).withAutoUdfUdafLoad(true));
    PAssert.that(result1).containsInAnyOrder(resultRow1);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testCOSH() throws Exception {
    Schema resultType = Schema.builder().addNullableField("field", Schema.FieldType.DOUBLE).build();

    Row resultRow1 = Row.withSchema(resultType).addValues(Math.cosh(1.0)).build();
    String sql1 = "SELECT COSH(CAST(1.0 as DOUBLE))";
    PCollection<Row> result1 =
        boundedInputDouble.apply("testUdf1", SqlTransform.query(sql1).withAutoUdfUdafLoad(true));
    PAssert.that(result1).containsInAnyOrder(resultRow1);


    Row resultRow2 = Row.withSchema(resultType).addValues(Math.cosh(710.0)).build();
    String sql2 = "SELECT COSH(CAST(710.0 as DOUBLE))";
    PCollection<Row> result2 =
            boundedInputDouble.apply("testUdf2", SqlTransform.query(sql2).withAutoUdfUdafLoad(true));
    PAssert.that(result2).containsInAnyOrder(resultRow2);

    Row resultRow3 = Row.withSchema(resultType).addValues(Math.cosh(Double.parseDouble(null))).build();
    String sql3 = "SELECT COSH(CAST(null as DOUBLE))";
    PCollection<Row> result3 =
            boundedInputDouble.apply("testUdf2", SqlTransform.query(sql3).withAutoUdfUdafLoad(true));
    PAssert.that(result3).containsInAnyOrder(resultRow3);

    pipeline.run().waitUntilFinish();
  }

  @AutoService(UdfUdafProvider.class)
  public static class UdfProvider implements UdfUdafProvider {
    @Override
    public Map<String, Class<? extends BeamSqlUdf>> getBeamSqlUdfs() {
      ImmutableMap.Builder<String, Class<? extends BeamSqlUdf>> builder = ImmutableMap.builder();
      builder.put(IsInf.FUNCTION_NAME, IsInf.class);
      builder.put(IsNan.FUNCTION_NAME, IsNan.class);
      builder.put(Greatest.FUNCTION_NAME, Greatest.class);
      builder.put(HyperbolicCosine.FUNCTION_NAME, HyperbolicCosine.class);
      return builder.build();
    }

    @Override
    public Map<String, Combine.CombineFn> getUdafs() {
      return Collections.EMPTY_MAP;
    }
  }
}
