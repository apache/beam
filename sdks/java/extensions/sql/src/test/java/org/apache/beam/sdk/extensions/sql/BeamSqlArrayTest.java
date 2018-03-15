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

import java.util.Arrays;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for SQL arrays.
 */
public class BeamSqlArrayTest {

  private static final Schema INPUT_ROW_TYPE =
      RowSqlType
        .builder()
        .withIntegerField("f_int")
        .withArrayField("f_stringArr", SqlTypeCoders.VARCHAR)
        .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectArrayValue() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
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
                   .build(),

               Row
                   .withRowType(resultType)
                   .addValues(42, Arrays.asList("aa", "bb"))
                   .build());

    pipeline.run();
  }

  @Test
  public void testProjectArrayField() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withArrayField("f_stringArr", SqlTypeCoders.VARCHAR)
            .build();

    PCollection<Row> result =
        input
            .apply(
                "sqlQuery",
                BeamSql.query("SELECT f_int, f_stringArr FROM PCOLLECTION"));

    PAssert.that(result)
           .containsInAnyOrder(
               Row
                   .withRowType(resultType)
                   .addValues(1)
                   .addArray(Arrays.asList("111", "222"))
                   .build(),
               Row
                   .withRowType(resultType)
                   .addValues(2)
                   .addArray(Arrays.asList("33", "44", "55"))
                   .build());

    pipeline.run();
  }

  @Test
  public void testAccessArrayElement() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        RowSqlType
            .builder()
            .withArrayField("f_arrElem", SqlTypeCoders.VARCHAR)
            .build();

    PCollection<Row> result =
        input
            .apply(
                "sqlQuery",
                BeamSql.query("SELECT f_stringArr[0] FROM PCOLLECTION"));

    PAssert.that(result)
           .containsInAnyOrder(
               Row
                   .withRowType(resultType)
                   .addValues("111")
                   .build(),
               Row
                   .withRowType(resultType)
                   .addValues("33")
                   .build());

    pipeline.run();
  }

  @Test
  public void testSingleElement() {
    PCollection<Row> input =
        PBegin
            .in(pipeline)
            .apply(
                "boundedInput1",
                Create
                    .of(
                        Row.withRowType(INPUT_ROW_TYPE)
                            .addValues(1)
                            .addArray(Arrays.asList("111"))
                            .build())
                    .withCoder(INPUT_ROW_TYPE.getRowCoder()));

    Schema resultType =
        RowSqlType
            .builder()
            .withArrayField("f_arrElem", SqlTypeCoders.VARCHAR)
            .build();

    PCollection<Row> result =
        input
            .apply(
                "sqlQuery",
                BeamSql.query("SELECT ELEMENT(f_stringArr) FROM PCOLLECTION"));

    PAssert.that(result)
           .containsInAnyOrder(
               Row
                   .withRowType(resultType)
                   .addValues("111")
                   .build());

    pipeline.run();
  }

  @Test
  public void testCardinality() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        RowSqlType
            .builder()
            .withIntegerField("f_size")
            .build();

    PCollection<Row> result =
        input
            .apply(
                "sqlQuery",
                BeamSql.query("SELECT CARDINALITY(f_stringArr) FROM PCOLLECTION"));

    PAssert.that(result)
           .containsInAnyOrder(
               Row
                   .withRowType(resultType)
                   .addValues(2)
                   .build(),
               Row
                   .withRowType(resultType)
                   .addValues(3)
                   .build());

    pipeline.run();
  }

  @Test
  public void testSelectRowsFromArrayOfRows() {
    RowType elementRowType =
        RowSqlType
            .builder()
            .withVarcharField("f_rowString")
            .withIntegerField("f_rowInt")
            .build();

    RowType resultRowType =
        RowSqlType
            .builder()
            .withArrayField("f_resultArray", elementRowType)
            .build();

    RowType inputType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withArrayField("f_arrayOfRows", elementRowType)
            .build();

    PCollection<Row> input =
        PBegin.in(pipeline)
              .apply(
                  Create.of(
                      Row.withRowType(inputType)
                         .addValues(
                             1,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("AA", 11).build(),
                                 Row.withRowType(elementRowType).addValues("BB", 22).build()))
                         .build(),
                      Row.withRowType(inputType)
                         .addValues(
                             2,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("CC", 33).build(),
                                 Row.withRowType(elementRowType).addValues("DD", 44).build()))
                         .build())
                        .withCoder(inputType.getRowCoder()));

    PCollection<Row> result =
        input
            .apply(
                BeamSql.query(
                    "SELECT f_arrayOfRows FROM PCOLLECTION"))
            .setCoder(resultRowType.getRowCoder());

    PAssert.that(result)
           .containsInAnyOrder(
               Row.withRowType(resultRowType)
                  .addArray(
                      Arrays.asList(
                          Row.withRowType(elementRowType).addValues("AA", 11).build(),
                          Row.withRowType(elementRowType).addValues("BB", 22).build()))
                  .build(),
               Row.withRowType(resultRowType)
                  .addArray(
                      Arrays.asList(
                          Row.withRowType(elementRowType).addValues("CC", 33).build(),
                          Row.withRowType(elementRowType).addValues("DD", 44).build()))
                  .build()
           );

    pipeline.run();
  }

  @Test
  public void testSelectSingleRowFromArrayOfRows() {
    RowType elementRowType =
        RowSqlType
            .builder()
            .withVarcharField("f_rowString")
            .withIntegerField("f_rowInt")
            .build();

    RowType resultRowType = elementRowType;

    RowType inputType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withArrayField("f_arrayOfRows", elementRowType)
            .build();

    PCollection<Row> input =
        PBegin.in(pipeline)
              .apply(
                  Create.of(
                      Row.withRowType(inputType)
                         .addValues(
                             1,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("AA", 11).build(),
                                 Row.withRowType(elementRowType).addValues("BB", 22).build()))
                         .build(),
                      Row.withRowType(inputType)
                         .addValues(
                             2,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("CC", 33).build(),
                                 Row.withRowType(elementRowType).addValues("DD", 44).build()))
                         .build())
                        .withCoder(inputType.getRowCoder()));

    PCollection<Row> result =
        input
            .apply(
                BeamSql.query(
                    "SELECT f_arrayOfRows[1] FROM PCOLLECTION"))
            .setCoder(resultRowType.getRowCoder());

    PAssert.that(result)
           .containsInAnyOrder(
               Row.withRowType(elementRowType).addValues("BB", 22).build(),
               Row.withRowType(elementRowType).addValues("DD", 44).build());

    pipeline.run();
  }

  @Test
  public void testSelectRowFieldFromArrayOfRows() {
    RowType elementRowType =
        RowSqlType
            .builder()
            .withVarcharField("f_rowString")
            .withIntegerField("f_rowInt")
            .build();

    RowType resultRowType =
        RowSqlType
            .builder()
            .withVarcharField("f_stringField")
            .build();

    RowType inputType =
        RowSqlType
            .builder()
            .withIntegerField("f_int")
            .withArrayField("f_arrayOfRows", elementRowType)
            .build();

    PCollection<Row> input =
        PBegin.in(pipeline)
              .apply(
                  Create.of(
                      Row.withRowType(inputType)
                         .addValues(
                             1,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("AA", 11).build(),
                                 Row.withRowType(elementRowType).addValues("BB", 22).build()))
                         .build(),
                      Row.withRowType(inputType)
                         .addValues(
                             2,
                             Arrays.asList(
                                 Row.withRowType(elementRowType).addValues("CC", 33).build(),
                                 Row.withRowType(elementRowType).addValues("DD", 44).build()))
                         .build())
                        .withCoder(inputType.getRowCoder()));

    PCollection<Row> result =
        input
            .apply(
                BeamSql.query(
                    "SELECT f_arrayOfRows[1].f_rowString FROM PCOLLECTION"))
            .setCoder(resultRowType.getRowCoder());

    PAssert.that(result)
           .containsInAnyOrder(
               Row.withRowType(resultRowType).addValues("BB").build(),
               Row.withRowType(resultRowType).addValues("DD").build());

    pipeline.run();
  }

  private PCollection<Row> pCollectionOf2Elements() {
    return
        PBegin
            .in(pipeline)
            .apply("boundedInput1",
                   Create
                       .of(
                           Row
                               .withRowType(INPUT_ROW_TYPE)
                               .addValues(1)
                               .addArray(Arrays.asList("111", "222"))
                               .build(),
                           Row
                               .withRowType(INPUT_ROW_TYPE)
                               .addValues(2)
                               .addArray(Arrays.asList("33", "44", "55"))
                               .build())
                       .withCoder(INPUT_ROW_TYPE.getRowCoder()));
  }
}
