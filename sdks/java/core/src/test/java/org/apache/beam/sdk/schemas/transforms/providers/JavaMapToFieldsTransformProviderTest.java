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
package org.apache.beam.sdk.schemas.transforms.providers;

import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JavaMapToFieldsTransformProvider}. */
@RunWith(JUnit4.class)
public class JavaMapToFieldsTransformProviderTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testRenameFields() {
    Schema inputSchema =
        Schema.of(
            Schema.Field.of("a", Schema.FieldType.STRING),
            Schema.Field.of("b", Schema.FieldType.INT32),
            Schema.Field.of("c", Schema.FieldType.DOUBLE));

    PCollection<Row> input =
        pipeline
            .apply(
                Create.of(
                    Row.withSchema(inputSchema).addValues("foo", 2, 0.5).build(),
                    Row.withSchema(inputSchema).addValues("bar", 4, 0.25).build()))
            .setRowSchema(inputSchema);

    PCollection<Row> renamed =
        PCollectionRowTuple.of(JavaMapToFieldsTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaMapToFieldsTransformProvider()
                    .from(
                        JavaMapToFieldsTransformProvider.Configuration.builder()
                            .setFields(
                                ImmutableMap.of(
                                    "newC",
                                    JavaRowUdf.Configuration.builder().setExpression("c").build(),
                                    "newA",
                                    JavaRowUdf.Configuration.builder().setExpression("a").build()))
                            .build()))
            .get(JavaMapToFieldsTransformProvider.OUTPUT_ROWS_TAG);

    Schema outputSchema = renamed.getSchema();

    PAssert.that(renamed)
        .containsInAnyOrder(
            Row.withSchema(outputSchema)
                .withFieldValue("newC", 0.5)
                .withFieldValue("newA", "foo")
                .build(),
            Row.withSchema(outputSchema)
                .withFieldValue("newC", 0.25)
                .withFieldValue("newA", "bar")
                .build());

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testAppendAndDropFields() {
    Schema inputSchema =
        Schema.of(
            Schema.Field.of("a", Schema.FieldType.INT32),
            Schema.Field.of("b", Schema.FieldType.DOUBLE));

    PCollection<Row> input =
        pipeline
            .apply(
                Create.of(
                    Row.withSchema(inputSchema).addValues(2, 0.5).build(),
                    Row.withSchema(inputSchema).addValues(4, 0.25).build()))
            .setRowSchema(inputSchema);

    PCollection<Row> renamed =
        PCollectionRowTuple.of(JavaMapToFieldsTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaMapToFieldsTransformProvider()
                    .from(
                        JavaMapToFieldsTransformProvider.Configuration.builder()
                            .setLanguage("java")
                            .setAppend(true)
                            .setDrop(Collections.singletonList("b"))
                            .setFields(
                                ImmutableMap.of(
                                    "sum",
                                    JavaRowUdf.Configuration.builder()
                                        .setExpression("a+b")
                                        .build()))
                            .build()))
            .get(JavaMapToFieldsTransformProvider.OUTPUT_ROWS_TAG);

    Schema outputSchema = renamed.getSchema();

    PAssert.that(renamed)
        .containsInAnyOrder(
            Row.withSchema(outputSchema).withFieldValue("a", 2).withFieldValue("sum", 2.5).build(),
            Row.withSchema(outputSchema)
                .withFieldValue("a", 4)
                .withFieldValue("sum", 4.25)
                .build());

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testErrorHandling() {
    Schema inputSchema = Schema.of(Schema.Field.of("x", Schema.FieldType.INT32));

    PCollection<Row> input =
        pipeline
            .apply(
                Create.of(
                    Row.withSchema(inputSchema).addValues(4).build(),
                    Row.withSchema(inputSchema).addValues(-1).build()))
            .setRowSchema(inputSchema);

    PCollectionRowTuple result =
        PCollectionRowTuple.of(JavaMapToFieldsTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaMapToFieldsTransformProvider()
                    .from(
                        JavaMapToFieldsTransformProvider.Configuration.builder()
                            .setLanguage("java")
                            .setFields(
                                ImmutableMap.of(
                                    "sqrt",
                                    JavaRowUdf.Configuration.builder()
                                        .setCallable(
                                            "import java.util.function.Function;"
                                                + "import org.apache.beam.sdk.values.Row;"
                                                + "public class Sqrt implements Function<Row, Double> {"
                                                + "  public Double apply(Row row) {"
                                                + "    int x = row.getInt32(\"x\");"
                                                + "    if (x < 0) {"
                                                + "      throw new ArithmeticException(\"negative value\");"
                                                + "    } else {"
                                                + "      return Math.sqrt(x);"
                                                + "    }"
                                                + "  }"
                                                + "}")
                                        .build()))
                            .setErrorHandling(ErrorHandling.builder().setOutput("errors").build())
                            .build()));

    PCollection<Row> sqrts = result.get(JavaMapToFieldsTransformProvider.OUTPUT_ROWS_TAG);
    Schema outputSchema = sqrts.getSchema();
    PAssert.that(sqrts)
        .containsInAnyOrder(Row.withSchema(outputSchema).withFieldValue("sqrt", 2.0).build());

    PCollection<Row> errors = result.get("errors");
    Schema errorSchema = errors.getSchema();
    PAssert.that(errors)
        .containsInAnyOrder(
            Row.withSchema(errorSchema)
                .withFieldValue("failed_row", Row.withSchema(inputSchema).addValues(-1).build())
                .withFieldValue("error_message", "negative value")
                .build());
    pipeline.run();
  }
}
