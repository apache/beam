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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for SQL arrays. */
public class BeamSqlDslArrayTest {

  private static final Schema INPUT_SCHEMA =
      Schema.builder()
          .addInt32Field("f_int")
          .addArrayField("f_stringArr", Schema.FieldType.STRING)
          .build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectArrayValue() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addArrayField("f_arr", Schema.FieldType.STRING)
            .build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery",
            SqlTransform.query("SELECT 42, ARRAY ['aa', 'bb'] as `f_arr` FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(42, Arrays.asList("aa", "bb")).build(),
            Row.withSchema(resultType).addValues(42, Arrays.asList("aa", "bb")).build());

    pipeline.run();
  }

  @Test
  public void testProjectArrayField() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType =
        Schema.builder()
            .addInt32Field("f_int")
            .addArrayField("f_stringArr", Schema.FieldType.STRING)
            .build();

    PCollection<Row> result =
        input.apply("sqlQuery", SqlTransform.query("SELECT f_int, f_stringArr FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(1).addArray(Arrays.asList("111", "222")).build(),
            Row.withSchema(resultType)
                .addValues(2)
                .addArray(Arrays.asList("33", "44", "55"))
                .build());

    pipeline.run();
  }

  @Test
  public void testAccessArrayElement() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType = Schema.builder().addStringField("f_arrElem").build();

    PCollection<Row> result =
        input.apply("sqlQuery", SqlTransform.query("SELECT f_stringArr[1] FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues("111").build(),
            Row.withSchema(resultType).addValues("33").build());

    pipeline.run();
  }

  @Test
  public void testSingleElement() throws Exception {
    Row inputRow = Row.withSchema(INPUT_SCHEMA).addValues(1).addArray(Arrays.asList("111")).build();

    PCollection<Row> input =
        pipeline.apply("boundedInput1", Create.of(inputRow).withRowSchema(INPUT_SCHEMA));

    Schema resultType = Schema.builder().addStringField("f_arrElem").build();

    PCollection<Row> result =
        input.apply("sqlQuery", SqlTransform.query("SELECT ELEMENT(f_stringArr) FROM PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(Row.withSchema(resultType).addValues("111").build());

    pipeline.run();
  }

  @Test
  public void testCardinality() {
    PCollection<Row> input = pCollectionOf2Elements();

    Schema resultType = Schema.builder().addInt32Field("f_size").build();

    PCollection<Row> result =
        input.apply(
            "sqlQuery", SqlTransform.query("SELECT CARDINALITY(f_stringArr) FROM PCOLLECTION"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(2).build(),
            Row.withSchema(resultType).addValues(3).build());

    pipeline.run();
  }

  @Test
  public void testUnnestLiteral() {
    PCollection<Row> input =
        pipeline.apply(
            "boundedInput1",
            Create.empty(TypeDescriptor.of(Row.class)).withRowSchema(INPUT_SCHEMA));

    // Because we have a multi-part FROM the DSL considers it multi-input
    TupleTag<Row> mainTag = new TupleTag<Row>("main") {};
    PCollectionTuple inputTuple = PCollectionTuple.of(mainTag, input);

    Schema resultType = Schema.builder().addStringField("f_string").build();

    PCollection<Row> result =
        inputTuple.apply(
            "sqlQuery", SqlTransform.query("SELECT * FROM UNNEST (ARRAY ['a', 'b', 'c'])"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues("a").build(),
            Row.withSchema(resultType).addValues("b").build(),
            Row.withSchema(resultType).addValues("c").build());

    pipeline.run();
  }

  @Test
  public void testUnnestNamedLiteral() {
    PCollection<Row> input =
        pipeline.apply(
            "boundedInput1",
            Create.empty(TypeDescriptor.of(Row.class)).withRowSchema(INPUT_SCHEMA));

    // Because we have a multi-part FROM the DSL considers it multi-input
    TupleTag<Row> mainTag = new TupleTag<Row>("main") {};
    PCollectionTuple inputTuple = PCollectionTuple.of(mainTag, input);

    Schema resultType = Schema.builder().addStringField("f_string").build();

    PCollection<Row> result =
        inputTuple.apply(
            "sqlQuery",
            SqlTransform.query("SELECT * FROM UNNEST (ARRAY ['a', 'b', 'c']) AS t(f_string)"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues("a").build(),
            Row.withSchema(resultType).addValues("b").build(),
            Row.withSchema(resultType).addValues("c").build());

    pipeline.run();
  }

  @Test
  public void testUnnestCrossJoin() {
    Row row1 =
        Row.withSchema(INPUT_SCHEMA)
            .addValues(42)
            .addArray(Arrays.asList("111", "222", "333"))
            .build();

    Row row2 =
        Row.withSchema(INPUT_SCHEMA).addValues(13).addArray(Arrays.asList("444", "555")).build();

    PCollection<Row> input =
        pipeline.apply("boundedInput1", Create.of(row1, row2).withRowSchema(INPUT_SCHEMA));

    // Because we have a multi-part FROM the DSL considers it multi-input
    TupleTag<Row> mainTag = new TupleTag<Row>("main") {};
    PCollectionTuple inputTuple = PCollectionTuple.of(mainTag, input);

    Schema resultType = Schema.builder().addInt32Field("f_int").addStringField("f_string").build();

    PCollection<Row> result =
        inputTuple.apply(
            "sqlQuery",
            SqlTransform.query(
                "SELECT f_int, arrElems.f_string FROM main "
                    + " CROSS JOIN UNNEST (main.f_stringArr) AS arrElems(f_string)"));

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultType).addValues(42, "111").build(),
            Row.withSchema(resultType).addValues(42, "222").build(),
            Row.withSchema(resultType).addValues(42, "333").build(),
            Row.withSchema(resultType).addValues(13, "444").build(),
            Row.withSchema(resultType).addValues(13, "555").build());

    pipeline.run();
  }

  @Test
  public void testSelectRowsFromArrayOfRows() {
    Schema elementSchema =
        Schema.builder().addStringField("f_rowString").addInt32Field("f_rowInt").build();

    Schema resultSchema =
        Schema.builder()
            .addArrayField("f_resultArray", Schema.FieldType.row(elementSchema))
            .build();

    Schema inputType =
        Schema.builder()
            .addInt32Field("f_int")
            .addArrayField("f_arrayOfRows", Schema.FieldType.row(elementSchema))
            .build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("AA", 11).build(),
                                Row.withSchema(elementSchema).addValues("BB", 22).build()))
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("CC", 33).build(),
                                Row.withSchema(elementSchema).addValues("DD", 44).build()))
                        .build())
                .withRowSchema(inputType));

    PCollection<Row> result =
        input
            .apply(SqlTransform.query("SELECT f_arrayOfRows FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema)
                .addArray(
                    Arrays.asList(
                        Row.withSchema(elementSchema).addValues("AA", 11).build(),
                        Row.withSchema(elementSchema).addValues("BB", 22).build()))
                .build(),
            Row.withSchema(resultSchema)
                .addArray(
                    Arrays.asList(
                        Row.withSchema(elementSchema).addValues("CC", 33).build(),
                        Row.withSchema(elementSchema).addValues("DD", 44).build()))
                .build());

    pipeline.run();
  }

  @Test
  public void testSelectSingleRowFromArrayOfRows() {
    Schema elementSchema =
        Schema.builder().addStringField("f_rowString").addInt32Field("f_rowInt").build();

    Schema resultSchema = elementSchema;

    Schema inputType =
        Schema.builder()
            .addInt32Field("f_int")
            .addArrayField("f_arrayOfRows", Schema.FieldType.row(elementSchema))
            .build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("AA", 11).build(),
                                Row.withSchema(elementSchema).addValues("BB", 22).build()))
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("CC", 33).build(),
                                Row.withSchema(elementSchema).addValues("DD", 44).build()))
                        .build())
                .withRowSchema(inputType));

    PCollection<Row> result =
        input
            .apply(SqlTransform.query("SELECT f_arrayOfRows[2] FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(elementSchema).addValues("BB", 22).build(),
            Row.withSchema(elementSchema).addValues("DD", 44).build());

    pipeline.run();
  }

  @Test
  public void testSelectRowFieldFromArrayOfRows() {
    Schema elementSchema =
        Schema.builder().addStringField("f_rowString").addInt32Field("f_rowInt").build();

    Schema resultSchema = Schema.builder().addStringField("f_stringField").build();

    Schema inputType =
        Schema.builder()
            .addInt32Field("f_int")
            .addArrayField("f_arrayOfRows", Schema.FieldType.row(elementSchema))
            .build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("AA", 11).build(),
                                Row.withSchema(elementSchema).addValues("BB", 22).build()))
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2,
                            Arrays.asList(
                                Row.withSchema(elementSchema).addValues("CC", 33).build(),
                                Row.withSchema(elementSchema).addValues("DD", 44).build()))
                        .build())
                .withRowSchema(inputType));

    PCollection<Row> result =
        input
            .apply(SqlTransform.query("SELECT f_arrayOfRows[2].f_rowString FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema).addValues("BB").build(),
            Row.withSchema(resultSchema).addValues("DD").build());

    pipeline.run();
  }

  private PCollection<Row> pCollectionOf2Elements() {
    return pipeline.apply(
        "boundedInput1",
        Create.of(
                Row.withSchema(INPUT_SCHEMA)
                    .addValues(1)
                    .addArray(Arrays.asList("111", "222"))
                    .build(),
                Row.withSchema(INPUT_SCHEMA)
                    .addValues(2)
                    .addArray(Arrays.asList("33", "44", "55"))
                    .build())
            .withRowSchema(INPUT_SCHEMA));
  }
}
