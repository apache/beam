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

import static junit.framework.TestCase.assertEquals;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

/** Tests for {@link AddFields}. */
public class AddFieldsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void addSimpleFields() {
    Schema schema = Schema.builder().addStringField("field1").build();
    PCollection<Row> added =
        pipeline
            .apply(
                Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("field2", Schema.FieldType.INT32)
                    .field("field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("field4", Schema.FieldType.iterable(Schema.FieldType.STRING)));

    Schema expectedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addNullableField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    assertEquals(expectedSchema, added.getSchema());
    Row expected = Row.withSchema(expectedSchema).addValues("value", null, null, null).build();
    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addSimpleFieldsDefaultValue() {
    Schema schema = Schema.builder().addStringField("field1").build();
    PCollection<Row> added =
        pipeline
            .apply(
                Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
            .apply(AddFields.<Row>create().field("field2", Schema.FieldType.INT32, 42));
    Schema expectedSchema =
        Schema.builder()
            .addStringField("field1")
            .addField("field2", Schema.FieldType.INT32)
            .build();
    assertEquals(expectedSchema, added.getSchema());
    Row expected = Row.withSchema(expectedSchema).addValues("value", 42).build();
    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNestedField() {
    Schema nested = Schema.builder().addStringField("field1").build();
    Schema schema = Schema.builder().addRowField("nested", nested).build();

    Row subRow = Row.withSchema(nested).addValue("value").build();
    Row row = Row.withSchema(schema).addValue(subRow).build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("nested.field2", Schema.FieldType.INT32)
                    .field("nested.field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("nested.field4", Schema.FieldType.iterable(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addNullableField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema = Schema.builder().addRowField("nested", expectedNestedSchema).build();
    assertEquals(expectedSchema, added.getSchema());

    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", null, null, null).build();
    Row expected = Row.withSchema(expectedSchema).addValue(expectedNested).build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNestedFieldDefaultValue() {
    Schema nested = Schema.builder().addStringField("field1").build();
    Schema schema = Schema.builder().addRowField("nested", nested).build();

    Row subRow = Row.withSchema(nested).addValue("value").build();
    Row row = Row.withSchema(schema).addValue(subRow).build();
    List<String> list = ImmutableList.of("one", "two", "three");
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("nested.field2", Schema.FieldType.INT32, 42)
                    .field("nested.field3", Schema.FieldType.array(Schema.FieldType.STRING), list)
                    .field(
                        "nested.field4", Schema.FieldType.iterable(Schema.FieldType.STRING), list));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addField("field2", Schema.FieldType.INT32)
            .addField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema = Schema.builder().addRowField("nested", expectedNestedSchema).build();
    assertEquals(expectedSchema, added.getSchema());
    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", 42, list, list).build();
    Row expected = Row.withSchema(expectedSchema).addValue(expectedNested).build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addSimpleAndNestedField() {
    Schema nested = Schema.builder().addStringField("field1").build();
    Schema schema = Schema.builder().addRowField("nested", nested).build();

    Row subRow = Row.withSchema(nested).addValue("value").build();
    Row row = Row.withSchema(schema).addValue(subRow).build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("field2", Schema.FieldType.INT32)
                    .field("nested.field2", Schema.FieldType.INT32)
                    .field("nested.field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("nested.field4", Schema.FieldType.iterable(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addNullableField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema =
        Schema.builder()
            .addRowField("nested", expectedNestedSchema)
            .addNullableField("field2", Schema.FieldType.INT32)
            .build();
    assertEquals(expectedSchema, added.getSchema());

    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", null, null, null).build();
    Row expected = Row.withSchema(expectedSchema).addValues(expectedNested, null).build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void recursivelyAddNestedFields() {
    Schema schema = Schema.of();

    Row row = Row.withSchema(schema).build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("nested.field1", Schema.FieldType.STRING, "value")
                    .field("nested.field2", Schema.FieldType.INT32)
                    .field("nested.field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("nested.field4", Schema.FieldType.iterable(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addNullableField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema =
        Schema.builder()
            .addNullableField("nested", Schema.FieldType.row(expectedNestedSchema))
            .build();
    assertEquals(expectedSchema, added.getSchema());

    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", null, null, null).build();
    Row expected = Row.withSchema(expectedSchema).addValue(expectedNested).build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNestedCollectionField() {
    Schema nested = Schema.builder().addStringField("field1").build();
    Schema schema =
        Schema.builder()
            .addArrayField("array", Schema.FieldType.row(nested))
            .addIterableField("iter", Schema.FieldType.row(nested))
            .build();

    Row subRow = Row.withSchema(nested).addValue("value").build();
    Row row =
        Row.withSchema(schema)
            .addArray(subRow, subRow)
            .addIterable(ImmutableList.of(subRow, subRow))
            .build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("array.field2", Schema.FieldType.INT32)
                    .field("array.field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("iter.field2", Schema.FieldType.INT32)
                    .field("iter.field3", Schema.FieldType.array(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema =
        Schema.builder()
            .addArrayField("array", Schema.FieldType.row(expectedNestedSchema))
            .addIterableField("iter", Schema.FieldType.row(expectedNestedSchema))
            .build();
    assertEquals(expectedSchema, added.getSchema());

    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", null, null).build();
    Row expected =
        Row.withSchema(expectedSchema)
            .addArray(expectedNested, expectedNested)
            .addIterable(ImmutableList.of(expectedNested, expectedNested))
            .build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void recursivelyAddNestedArrayField() {
    Schema schema = Schema.builder().build();
    Row row = Row.withSchema(schema).build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("array[].field1", FieldType.STRING)
                    .field("array[].field2", Schema.FieldType.INT32)
                    .field("array[].field3", Schema.FieldType.array(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addNullableField("field1", FieldType.STRING)
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema =
        Schema.builder()
            .addNullableField(
                "array",
                Schema.FieldType.array(
                    Schema.FieldType.row(expectedNestedSchema).withNullable(true)))
            .build();
    assertEquals(expectedSchema, added.getSchema());

    Row expected = Row.withSchema(expectedSchema).addValue(Collections.emptyList()).build();
    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNestedMapField() {
    Schema nested = Schema.builder().addStringField("field1").build();
    Schema schema =
        Schema.builder()
            .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.row(nested))
            .build();

    Row subRow = Row.withSchema(nested).addValue("value").build();
    Row row = Row.withSchema(schema).addValue(ImmutableMap.of("key", subRow)).build();
    PCollection<Row> added =
        pipeline
            .apply(Create.of(row).withRowSchema(schema))
            .apply(
                AddFields.<Row>create()
                    .field("map.field2", Schema.FieldType.INT32)
                    .field("map.field3", Schema.FieldType.array(Schema.FieldType.STRING))
                    .field("map.field4", Schema.FieldType.iterable(Schema.FieldType.STRING)));

    Schema expectedNestedSchema =
        Schema.builder()
            .addStringField("field1")
            .addNullableField("field2", Schema.FieldType.INT32)
            .addNullableField("field3", Schema.FieldType.array(Schema.FieldType.STRING))
            .addNullableField("field4", Schema.FieldType.iterable(Schema.FieldType.STRING))
            .build();
    Schema expectedSchema =
        Schema.builder()
            .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.row(expectedNestedSchema))
            .build();
    assertEquals(expectedSchema, added.getSchema());

    Row expectedNested =
        Row.withSchema(expectedNestedSchema).addValues("value", null, null, null).build();
    Row expected =
        Row.withSchema(expectedSchema).addValue(ImmutableMap.of("key", expectedNested)).build();

    PAssert.that(added).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addDuplicateField() {
    Schema schema = Schema.builder().addStringField("field1").build();
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
        .apply(AddFields.<Row>create().field("field1", Schema.FieldType.INT32));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void addNonNullableField() {
    Schema schema = Schema.builder().addStringField("field1").build();
    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(Row.withSchema(schema).addValue("value").build()).withRowSchema(schema))
        .apply(AddFields.<Row>create().field("field2", Schema.FieldType.INT32, null));
    pipeline.run();
  }
}
