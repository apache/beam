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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for SQL arrays.
 */
public class BeamSqlArrayTest {

  public static final DateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final RowType INPUT_ROW_TYPE =
      RowSqlType
        .builder()
        .withIntegerField("f_int")
        .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectArrayValue() {
    PCollection<Row> input = createPCollection();

    RowType resultType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withArrayField("f_arr", SqlTypeCoders.CHAR)
            .build();

    PCollection<Row> result =
        input
            .apply(
                "sqlQuery",
                BeamSql.query("SELECT 42, ARRAY ['aa', 'bb'] as `f_arr` FROM PCOLLECTION"));

    PAssert.that(result)
           .containsInAnyOrder(
               Row
                   .withRowType(resultType)
                   .addValues(42, Arrays.asList("aa", "bb"))
                   .build());

    pipeline.run();
  }

  private PCollection<Row> createPCollection() {
    return
        PBegin
            .in(pipeline)
            .apply("boundedInput1",
                   Create
                       .of(Row.withRowType(INPUT_ROW_TYPE).addValues(1).build())
                       .withCoder(INPUT_ROW_TYPE.getRowCoder()));
  }

}
