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
package org.apache.beam.sdk.extensions.sql;

import java.time.LocalDate;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Ad-hoc tests for CAST. */
public class BeamSqlCastTest {

  private static final Schema INPUT_ROW_SCHEMA =
      Schema.builder().addInt32Field("f_int").addStringField("f_string").build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testCastToDate() {
    PCollection<Row> input =
        pipeline.apply(
            Create.of(Row.withSchema(INPUT_ROW_SCHEMA).addValues(1).addValue("20181018").build())
                .withRowSchema(INPUT_ROW_SCHEMA));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addNullableField("f_date", CalciteUtils.DATE)
            .build();

    PCollection<Row> result =
        input.apply(
            SqlTransform.query(
                "SELECT f_int, \n"
                    + "   CAST( \n"
                    + "     SUBSTRING(TRIM(f_string) FROM 1 FOR 4) \n"
                    + "      ||'-' \n"
                    + "      ||SUBSTRING(TRIM(f_string) FROM 5 FOR 2) \n"
                    + "      ||'-' \n"
                    + "      ||SUBSTRING(TRIM(f_string) FROM 7 FOR 2) as DATE) \n"
                    + "FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1, LocalDate.of(2018, 10, 18)).build());

    pipeline.run();
  }

  @Test
  public void testCastToDateWithCase() {
    PCollection<Row> input =
        pipeline.apply(
            Create.of(Row.withSchema(INPUT_ROW_SCHEMA).addValues(1).addValue("20181018").build())
                .withRowSchema(INPUT_ROW_SCHEMA));

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addLogicalTypeField("f_date", SqlTypes.DATE)
            .build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery",
            SqlTransform.query(
                "SELECT f_int, \n"
                    + "CASE WHEN CHAR_LENGTH(TRIM(f_string)) = 8 \n"
                    + "    THEN CAST (\n"
                    + "       SUBSTRING(TRIM(f_string) FROM 1 FOR 4) \n"
                    + "        ||'-' \n"
                    + "        ||SUBSTRING(TRIM(f_string) FROM 5 FOR 2) \n"
                    + "        ||'-' \n"
                    + "        ||SUBSTRING(TRIM(f_string) FROM 7 FOR 2) AS DATE)\n"
                    + "    ELSE DATE '2001-01-01'\n"
                    + "END \n"
                    + "FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1, LocalDate.of(2018, 10, 18)).build());

    pipeline.run();
  }
}
