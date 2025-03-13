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

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.RenameFields.RenamePair;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for {@link RenameFields}. */
public class RenameFieldsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void renameTopLevelFields() {
    Schema schema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    PCollection<Row> renamed =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(schema).addValues("one", 1).build(),
                        Row.withSchema(schema).addValues("two", 2).build())
                    .withRowSchema(schema))
            .apply(RenameFields.<Row>create().rename("field1", "new1").rename("field2", "new2"));
    Schema expectedSchema = Schema.builder().addStringField("new1").addInt32Field("new2").build();
    assertEquals(expectedSchema, renamed.getSchema());
    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(expectedSchema).addValues("one", 1).build(),
            Row.withSchema(expectedSchema).addValues("two", 2).build());
    PAssert.that(renamed).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void renameNestedFields() {
    Schema nestedSchema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    Schema schema =
        Schema.builder().addStringField("field1").addRowField("nested", nestedSchema).build();

    PCollection<Row> renamed =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(schema)
                            .addValues(
                                "one", Row.withSchema(nestedSchema).addValues("one", 1).build())
                            .build(),
                        Row.withSchema(schema)
                            .addValues(
                                "two", Row.withSchema(nestedSchema).addValues("two", 1).build())
                            .build())
                    .withRowSchema(schema))
            .apply(
                RenameFields.<Row>create()
                    .rename("nested.field1", "new1")
                    .rename("nested.field2", "new2"));

    Schema expectedNestedSchema =
        Schema.builder().addStringField("new1").addInt32Field("new2").build();
    Schema expectedSchema =
        Schema.builder()
            .addStringField("field1")
            .addRowField("nested", expectedNestedSchema)
            .build();
    assertEquals(expectedSchema, renamed.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(expectedSchema)
                .addValues("one", Row.withSchema(expectedNestedSchema).addValues("one", 1).build())
                .build(),
            Row.withSchema(expectedSchema)
                .addValues("two", Row.withSchema(expectedNestedSchema).addValues("two", 1).build())
                .build());

    PAssert.that(renamed).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void renameTopLevelAndNestedFields() {
    Schema nestedSchema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    Schema schema =
        Schema.builder().addStringField("field1").addRowField("nested", nestedSchema).build();

    PCollection<Row> renamed =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(schema)
                            .addValues(
                                "one", Row.withSchema(nestedSchema).addValues("one", 1).build())
                            .build(),
                        Row.withSchema(schema)
                            .addValues(
                                "two", Row.withSchema(nestedSchema).addValues("two", 1).build())
                            .build())
                    .withRowSchema(schema))
            .apply(
                RenameFields.<Row>create()
                    .rename("field1", "top1")
                    .rename("nested", "newnested")
                    .rename("nested.field1", "new1")
                    .rename("nested.field2", "new2"));

    Schema expectedNestedSchema =
        Schema.builder().addStringField("new1").addInt32Field("new2").build();
    Schema expectedSchema =
        Schema.builder()
            .addStringField("top1")
            .addRowField("newnested", expectedNestedSchema)
            .build();
    assertEquals(expectedSchema, renamed.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(expectedSchema)
                .addValues("one", Row.withSchema(expectedNestedSchema).addValues("one", 1).build())
                .build(),
            Row.withSchema(expectedSchema)
                .addValues("two", Row.withSchema(expectedNestedSchema).addValues("two", 1).build())
                .build());

    PAssert.that(renamed).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void renameNestedInArrayFields() {
    Schema nestedSchema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    Schema schema =
        Schema.builder().addArrayField("array", Schema.FieldType.row(nestedSchema)).build();

    PCollection<Row> renamed =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(schema)
                            .addValue(
                                ImmutableList.of(
                                    Row.withSchema(nestedSchema).addValues("one", 1).build()))
                            .build(),
                        Row.withSchema(schema)
                            .addValue(
                                ImmutableList.of(
                                    Row.withSchema(nestedSchema).addValues("two", 1).build()))
                            .build())
                    .withRowSchema(schema))
            .apply(
                RenameFields.<Row>create()
                    .rename("array.field1", "new1")
                    .rename("array.field2", "new2"));

    Schema expectedNestedSchema =
        Schema.builder().addStringField("new1").addInt32Field("new2").build();
    Schema expectedSchema =
        Schema.builder().addArrayField("array", Schema.FieldType.row(expectedNestedSchema)).build();
    assertEquals(expectedSchema, renamed.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(expectedSchema)
                .addValue(
                    ImmutableList.of(
                        Row.withSchema(expectedNestedSchema).addValues("one", 1).build()))
                .build(),
            Row.withSchema(expectedSchema)
                .addValue(
                    ImmutableList.of(
                        Row.withSchema(expectedNestedSchema).addValues("two", 1).build()))
                .build());

    PAssert.that(renamed).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void renameNestedInMapFields() {
    Schema nestedSchema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    Schema schema =
        Schema.builder()
            .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.row(nestedSchema))
            .build();

    PCollection<Row> renamed =
        pipeline
            .apply(
                Create.of(
                        Row.withSchema(schema)
                            .addValue(
                                ImmutableMap.of(
                                    "k1", Row.withSchema(nestedSchema).addValues("one", 1).build()))
                            .build(),
                        Row.withSchema(schema)
                            .addValue(
                                ImmutableMap.of(
                                    "k2", Row.withSchema(nestedSchema).addValues("two", 1).build()))
                            .build())
                    .withRowSchema(schema))
            .apply(
                RenameFields.<Row>create()
                    .rename("map.field1", "new1")
                    .rename("map.field2", "new2"));

    Schema expectedNestedSchema =
        Schema.builder().addStringField("new1").addInt32Field("new2").build();
    Schema expectedSchema =
        Schema.builder()
            .addMapField("map", Schema.FieldType.STRING, Schema.FieldType.row(expectedNestedSchema))
            .build();
    assertEquals(expectedSchema, renamed.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(expectedSchema)
                .addValue(
                    ImmutableMap.of(
                        "k1", Row.withSchema(expectedNestedSchema).addValues("one", 1).build()))
                .build(),
            Row.withSchema(expectedSchema)
                .addValue(
                    ImmutableMap.of(
                        "k2", Row.withSchema(expectedNestedSchema).addValues("two", 1).build()))
                .build());

    PAssert.that(renamed).containsInAnyOrder(expectedRows);
    pipeline.run();
  }

  @Test
  public void testRenameRow() {
    Schema nestedSchema = Schema.builder().addStringField("field1").addInt32Field("field2").build();
    Schema schema =
        Schema.builder().addStringField("field1").addRowField("nested", nestedSchema).build();

    Schema expectedNestedSchema =
        Schema.builder().addStringField("bottom1").addInt32Field("bottom2").build();
    Schema expectedSchema =
        Schema.builder()
            .addStringField("top1")
            .addRowField("top_nested", expectedNestedSchema)
            .build();

    List<RenamePair> renames =
        ImmutableList.of(
                RenamePair.of(FieldAccessDescriptor.withFieldNames("field1"), "top1"),
                RenamePair.of(FieldAccessDescriptor.withFieldNames("nested"), "top_nested"),
                RenamePair.of(FieldAccessDescriptor.withFieldNames("nested.field1"), "bottom1"),
                RenamePair.of(FieldAccessDescriptor.withFieldNames("nested.field2"), "bottom2"))
            .stream()
            .map(r -> r.resolve(schema))
            .collect(Collectors.toList());

    final Map<UUID, Schema> renamedSchemasMap = Maps.newHashMap();
    final Map<UUID, BitSet> nestedFieldRenamedMap = Maps.newHashMap();
    RenameFields.renameSchema(schema, renames, renamedSchemasMap, nestedFieldRenamedMap);

    assertEquals(expectedSchema, renamedSchemasMap.get(schema.getUUID()));

    Row row =
        Row.withSchema(schema)
            .withFieldValue("field1", "one")
            .withFieldValue(
                "nested",
                Row.withSchema(nestedSchema)
                    .withFieldValue("field1", "one")
                    .withFieldValue("field2", 1)
                    .build())
            .build();
    Row expectedRow =
        Row.withSchema(expectedSchema)
            .withFieldValue("top1", "one")
            .withFieldValue(
                "top_nested",
                Row.withSchema(expectedNestedSchema)
                    .withFieldValue("bottom1", "one")
                    .withFieldValue("bottom2", 1)
                    .build())
            .build();

    Row renamedRow =
        RenameFields.renameRow(
            row,
            renamedSchemasMap.get(schema.getUUID()),
            nestedFieldRenamedMap.get(schema.getUUID()),
            renamedSchemasMap,
            nestedFieldRenamedMap);
    assertEquals(expectedRow, renamedRow);
  }
}
