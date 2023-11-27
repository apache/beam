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

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class JavaExplodeTransformProviderTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static final Schema INPUT_SCHEMA =
      Schema.of(
          Schema.Field.of("a", Schema.FieldType.iterable(Schema.FieldType.INT32)),
          Schema.Field.of("b", Schema.FieldType.DOUBLE),
          Schema.Field.of("c", Schema.FieldType.array(Schema.FieldType.STRING)));

  private static final Schema OUTPUT_SCHEMA =
      Schema.of(
          Schema.Field.of("a", Schema.FieldType.INT32),
          Schema.Field.of("b", Schema.FieldType.DOUBLE),
          Schema.Field.of("c", Schema.FieldType.STRING));

  private static final List<Row> INPUT_ROWS =
      ImmutableList.of(
          Row.withSchema(INPUT_SCHEMA)
              .addValues(ImmutableList.of(1, 2), 1.5, ImmutableList.of("x", "y"))
              .build());

  @Test
  @Category(NeedsRunner.class)
  public void testCrossProduct() {
    PCollection<Row> input = pipeline.apply(Create.of(INPUT_ROWS)).setRowSchema(INPUT_SCHEMA);

    PCollection<Row> exploded =
        PCollectionRowTuple.of(JavaExplodeTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaExplodeTransformProvider()
                    .from(
                        JavaExplodeTransformProvider.Configuration.builder()
                            .setFields(ImmutableList.of("a", "c"))
                            .setCrossProduct(true)
                            .build()))
            .get(JavaExplodeTransformProvider.OUTPUT_ROWS_TAG);

    PAssert.that(exploded)
        .containsInAnyOrder(
            Row.withSchema(OUTPUT_SCHEMA).addValues(1, 1.5, "x").build(),
            Row.withSchema(OUTPUT_SCHEMA).addValues(2, 1.5, "x").build(),
            Row.withSchema(OUTPUT_SCHEMA).addValues(1, 1.5, "y").build(),
            Row.withSchema(OUTPUT_SCHEMA).addValues(2, 1.5, "y").build());

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testZipProduct() {
    PCollection<Row> input = pipeline.apply(Create.of(INPUT_ROWS)).setRowSchema(INPUT_SCHEMA);

    PCollection<Row> exploded =
        PCollectionRowTuple.of(JavaExplodeTransformProvider.INPUT_ROWS_TAG, input)
            .apply(
                new JavaExplodeTransformProvider()
                    .from(
                        JavaExplodeTransformProvider.Configuration.builder()
                            .setFields(ImmutableList.of("a", "c"))
                            .setCrossProduct(false)
                            .build()))
            .get(JavaExplodeTransformProvider.OUTPUT_ROWS_TAG);

    PAssert.that(exploded)
        .containsInAnyOrder(
            Row.withSchema(OUTPUT_SCHEMA).addValues(1, 1.5, "x").build(),
            Row.withSchema(OUTPUT_SCHEMA).addValues(2, 1.5, "y").build());

    pipeline.run();
  }
}
