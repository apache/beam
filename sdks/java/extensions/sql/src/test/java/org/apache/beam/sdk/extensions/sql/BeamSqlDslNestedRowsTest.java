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
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for nested rows handling. */
public class BeamSqlDslNestedRowsTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testRowConstructorKeyword() {
    Schema nestedSchema =
        Schema.builder()
            .addInt32Field("f_nestedInt")
            .addStringField("f_nestedString")
            .addInt32Field("f_nestedIntPlusOne")
            .build();

    Schema resultSchema =
        Schema.builder()
            .addInt32Field("f_int")
            .addInt32Field("f_int2")
            .addStringField("f_varchar")
            .addInt32Field("f_int3")
            .build();

    Schema inputType =
        Schema.builder().addInt32Field("f_int").addRowField("f_row", nestedSchema).build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1, Row.withSchema(nestedSchema).addValues(312, "CC", 313).build())
                        .build())
                .withSchema(
                    inputType, SerializableFunctions.identity(), SerializableFunctions.identity()));

    PCollection<Row> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT 1 as `f_int`, ROW(3, 'BB', f_int + 1) as `f_row1` FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(Row.withSchema(resultSchema).addValues(1, 3, "BB", 2).build());

    pipeline.run();
  }

  @Test
  public void testRowConstructorBraces() {

    Schema nestedSchema =
        Schema.builder()
            .addInt32Field("f_nestedInt")
            .addStringField("f_nestedString")
            .addInt32Field("f_nestedIntPlusOne")
            .build();

    Schema resultSchema =
        Schema.builder()
            .addInt32Field("f_int")
            .addInt32Field("f_int2")
            .addStringField("f_varchar")
            .addInt32Field("f_int3")
            .build();

    Schema inputType =
        Schema.builder().addInt32Field("f_int").addRowField("f_row", nestedSchema).build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1, Row.withSchema(nestedSchema).addValues(312, "CC", 313).build())
                        .build())
                .withSchema(
                    inputType, SerializableFunctions.identity(), SerializableFunctions.identity()));

    PCollection<Row> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT 1 as `f_int`, (3, 'BB', f_int + 1) as `f_row1` FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(Row.withSchema(resultSchema).addValues(1, 3, "BB", 2).build());

    pipeline.run();
  }

  @Test
  public void testNestedRowFieldAccess() {

    Schema nestedSchema =
        Schema.builder()
            .addInt32Field("f_nestedInt")
            .addStringField("f_nestedString")
            .addInt32Field("f_nestedIntPlusOne")
            .build();

    Schema resultSchema = Schema.builder().addStringField("f_nestedString").build();

    Schema inputType =
        Schema.builder().addInt32Field("f_int").addRowField("f_nestedRow", nestedSchema).build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1, Row.withSchema(nestedSchema).addValues(312, "CC", 313).build())
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2, Row.withSchema(nestedSchema).addValues(412, "DD", 413).build())
                        .build())
                .withSchema(
                    inputType, SerializableFunctions.identity(), SerializableFunctions.identity()));

    PCollection<Row> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT `PCOLLECTION`.`f_nestedRow`.`f_nestedString` FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema).addValues("CC").build(),
            Row.withSchema(resultSchema).addValues("DD").build());

    pipeline.run();
  }

  @Test
  public void testNestedRowArrayFieldAccess() {

    Schema resultSchema =
        Schema.builder().addArrayField("f_nestedArray", Schema.FieldType.STRING).build();

    Schema nestedSchema =
        Schema.builder()
            .addInt32Field("f_nestedInt")
            .addStringField("f_nestedString")
            .addInt32Field("f_nestedIntPlusOne")
            .addArrayField("f_nestedArray", Schema.FieldType.STRING)
            .build();

    Schema inputType =
        Schema.builder().addInt32Field("f_int").addRowField("f_nestedRow", nestedSchema).build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1,
                            Row.withSchema(nestedSchema)
                                .addValues(312, "CC", 313, Arrays.asList("one", "two"))
                                .build())
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2,
                            Row.withSchema(nestedSchema)
                                .addValues(412, "DD", 413, Arrays.asList("three", "four"))
                                .build())
                        .build())
                .withSchema(
                    inputType, SerializableFunctions.identity(), SerializableFunctions.identity()));

    PCollection<Row> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT `PCOLLECTION`.`f_nestedRow`.`f_nestedArray` FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema).addArray(Arrays.asList("one", "two")).build(),
            Row.withSchema(resultSchema).addArray(Arrays.asList("three", "four")).build());

    pipeline.run();
  }

  @Test
  public void testNestedRowArrayElementAccess() {

    Schema resultSchema = Schema.builder().addStringField("f_nestedArrayStringField").build();

    Schema nestedSchema =
        Schema.builder()
            .addInt32Field("f_nestedInt")
            .addStringField("f_nestedString")
            .addInt32Field("f_nestedIntPlusOne")
            .addArrayField("f_nestedArray", Schema.FieldType.STRING)
            .build();

    Schema inputType =
        Schema.builder().addInt32Field("f_int").addRowField("f_nestedRow", nestedSchema).build();

    PCollection<Row> input =
        pipeline.apply(
            Create.of(
                    Row.withSchema(inputType)
                        .addValues(
                            1,
                            Row.withSchema(nestedSchema)
                                .addValues(312, "CC", 313, Arrays.asList("one", "two"))
                                .build())
                        .build(),
                    Row.withSchema(inputType)
                        .addValues(
                            2,
                            Row.withSchema(nestedSchema)
                                .addValues(412, "DD", 413, Arrays.asList("three", "four"))
                                .build())
                        .build())
                .withSchema(
                    inputType, SerializableFunctions.identity(), SerializableFunctions.identity()));

    PCollection<Row> result =
        input
            .apply(
                SqlTransform.query(
                    "SELECT `PCOLLECTION`.`f_nestedRow`.`f_nestedArray`[2] FROM PCOLLECTION"))
            .setRowSchema(resultSchema);

    PAssert.that(result)
        .containsInAnyOrder(
            Row.withSchema(resultSchema).addValues("two").build(),
            Row.withSchema(resultSchema).addValues("four").build());

    pipeline.run();
  }
}
