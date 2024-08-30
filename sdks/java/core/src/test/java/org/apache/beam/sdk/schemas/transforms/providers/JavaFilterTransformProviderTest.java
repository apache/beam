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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class JavaFilterTransformProviderTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testFilter() {
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
        PCollectionRowTuple.of(JavaFilterTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaFilterTransformProvider()
                    .from(
                        JavaFilterTransformProvider.Configuration.builder()
                            .setKeep(
                                JavaRowUdf.Configuration.builder()
                                    .setExpression("b + c > 3")
                                    .build())
                            .build()))
            .get(JavaFilterTransformProvider.OUTPUT_ROWS_TAG);

    PAssert.that(renamed)
        .containsInAnyOrder(
            Row.withSchema(inputSchema)
                .withFieldValue("a", "bar")
                .withFieldValue("b", 4)
                .withFieldValue("c", 0.25)
                .build());

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testErrorHandling() {
    Schema inputSchema = Schema.of(Schema.Field.of("s", Schema.FieldType.STRING));

    PCollection<Row> input =
        pipeline
            .apply(
                Create.of(
                    Row.withSchema(inputSchema).addValues("short").build(),
                    Row.withSchema(inputSchema).addValues("looooooooooooong").build()))
            .setRowSchema(inputSchema);

    PCollectionRowTuple result =
        PCollectionRowTuple.of(JavaFilterTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaFilterTransformProvider()
                    .from(
                        JavaFilterTransformProvider.Configuration.builder()
                            .setLanguage("java")
                            .setKeep(
                                JavaRowUdf.Configuration.builder()
                                    .setExpression("s.charAt(7) == 'o'")
                                    .build())
                            .setErrorHandling(ErrorHandling.builder().setOutput("errors").build())
                            .build()));

    PCollection<Row> good = result.get(JavaFilterTransformProvider.OUTPUT_ROWS_TAG);
    PAssert.that(good)
        .containsInAnyOrder(
            Row.withSchema(inputSchema).withFieldValue("s", "looooooooooooong").build());

    PCollection<Row> errors = result.get("errors");
    Schema errorSchema = errors.getSchema();
    PAssert.that(errors)
        .containsInAnyOrder(
            Row.withSchema(errorSchema)
                .withFieldValue(
                    "failed_row", Row.withSchema(inputSchema).addValues("short").build())
                .withFieldValue("error_message", "String index out of range: 7")
                .build());
    pipeline.run();
  }
}
