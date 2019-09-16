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
package org.apache.beam.sdk.schemas.transforms;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link org.apache.beam.sdk.schemas.transforms.Unnest}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class UnnestTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  static final Schema SIMPLE_SCHEMA =
      Schema.builder().addInt32Field("field1").addStringField("field2").build();
  static final Schema NESTED_SCHEMA =
      Schema.builder()
          .addRowField("nested1", SIMPLE_SCHEMA)
          .addRowField("nested2", SIMPLE_SCHEMA)
          .build();
  static final Schema UNNESTED_SCHEMA =
      Schema.builder()
          .addInt32Field("nested1_field1")
          .addStringField("nested1_field2")
          .addInt32Field("nested2_field1")
          .addStringField("nested2_field2")
          .build();
  static final Schema NESTED_SCHEMA2 =
      Schema.builder().addRowField("nested", SIMPLE_SCHEMA).build();
  static final Schema DOUBLE_NESTED_SCHEMA =
      Schema.builder().addRowField("nested", NESTED_SCHEMA).build();

  @Test
  @Category(NeedsRunner.class)
  public void testFlatSchema() {
    List<Row> rows =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline.apply(Create.of(rows).withRowSchema(SIMPLE_SCHEMA)).apply(Unnest.create());
    PAssert.that(unnested).containsInAnyOrder(rows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleUnnesting() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA).addValues(r, r).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline.apply(Create.of(rows).withRowSchema(NESTED_SCHEMA)).apply(Unnest.create());
    assertEquals(UNNESTED_SCHEMA, unnested.getSchema());
    List<Row> expected =
        bottomRow.stream()
            .map(
                r ->
                    Row.withSchema(UNNESTED_SCHEMA)
                        .addValues(r.getValue(0), r.getValue(1), r.getValue(0), r.getValue(1))
                        .build())
            .collect(Collectors.toList());
    ;
    PAssert.that(unnested).containsInAnyOrder(expected);
    pipeline.run();
  }

  static final Schema ONE_LEVEL_UNNESTED_SCHEMA =
      Schema.builder()
          .addRowField("nested_nested1", SIMPLE_SCHEMA)
          .addRowField("nested_nested2", SIMPLE_SCHEMA)
          .build();

  static final Schema UNNESTED2_SCHEMA_ALTERNATE =
      Schema.builder().addInt32Field("field1").addStringField("field2").build();

  @Test
  @Category(NeedsRunner.class)
  public void testAlternateNamePolicy() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA2).addValues(r).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline
            .apply(Create.of(rows).withRowSchema(NESTED_SCHEMA2))
            .apply(Unnest.<Row>create().withFieldNameFunction(Unnest.KEEP_NESTED_NAME));
    assertEquals(UNNESTED2_SCHEMA_ALTERNATE, unnested.getSchema());
    List<Row> expected =
        bottomRow.stream()
            .map(
                r ->
                    Row.withSchema(UNNESTED2_SCHEMA_ALTERNATE)
                        .addValues(r.getValue(0), r.getValue(1))
                        .build())
            .collect(Collectors.toList());
    ;
    PAssert.that(unnested).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testClashingNamePolicy() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    thrown.expect(IllegalArgumentException.class);
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA).addValues(r, r).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline
            .apply(Create.of(rows).withRowSchema(NESTED_SCHEMA))
            .apply(Unnest.<Row>create().withFieldNameFunction(Unnest.KEEP_NESTED_NAME));
    pipeline.run();
  }
}
