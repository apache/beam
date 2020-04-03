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

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link Select}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class SelectTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  /** flat schema to select from. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Schema1 {
    abstract String getField1();

    abstract Integer getField2();

    abstract Double getField3();

    static Schema1 create() {
      return new AutoValue_SelectTest_Schema1("field1", 42, 3.14);
    }
  };

  /** A class matching the schema resulting from selection field1, field3. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Schema1Selected {
    abstract String getField1();

    abstract Double getField3();

    static Schema1Selected create() {
      return new AutoValue_SelectTest_Schema1Selected("field1", 3.14);
    }
  }

  /**
   * A class matching the schema resulting from selection field1, field3 with the fields renamed.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Schema1SelectedRenamed {
    abstract String getFieldOne();

    abstract Double getFieldThree();

    static Schema1SelectedRenamed create() {
      return new AutoValue_SelectTest_Schema1SelectedRenamed("field1", 3.14);
    }
  }

  /** A nested schema class. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Schema2 {
    abstract String getField1();

    abstract Schema1 getField2();

    static Schema2 create() {
      return new AutoValue_SelectTest_Schema2("field1", Schema1.create());
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectMissingFieldName() {
    thrown.expect(IllegalArgumentException.class);
    pipeline.apply(Create.of(Schema1.create())).apply(Select.fieldNames("missing"));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectMissingFieldIndex() {
    thrown.expect(IllegalArgumentException.class);
    pipeline.apply(Create.of(Schema1.create())).apply(Select.fieldIds(42));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectAll() {
    PCollection<Schema1> rows =
        pipeline
            .apply(Create.of(Schema1.create()))
            .apply(Select.fieldNames("*"))
            .apply(Convert.to(Schema1.class));
    PAssert.that(rows).containsInAnyOrder(Schema1.create());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleSelect() {
    PCollection<Schema1Selected> rows =
        pipeline
            .apply(Create.of(Schema1.create()))
            .apply(Select.fieldNames("field1", "field3"))
            .apply(Convert.to(Schema1Selected.class));
    PAssert.that(rows).containsInAnyOrder(Schema1Selected.create());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleSelectRename() {
    PCollection<Schema1SelectedRenamed> rows =
        pipeline
            .apply(Create.of(Schema1.create()))
            .apply(
                Select.<Schema1>create()
                    .withFieldNameAs("field1", "fieldOne")
                    .withFieldNameAs("field3", "fieldThree"))
            .apply(Convert.to(Schema1SelectedRenamed.class));
    PAssert.that(rows).containsInAnyOrder(Schema1SelectedRenamed.create());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectWithOutputSchema() {
    Schema outputSchema =
        Schema.builder()
            .addStringField("stringField")
            .addStringField("nestedStringField")
            .addDoubleField("nestedDoubleField")
            .build();

    Schema2 input = Schema2.create();
    PCollection<Row> rows =
        pipeline
            .apply(Create.of(input))
            .apply(
                Select.<Schema2>fieldNames("field1", "field2.field1", "field2.field3")
                    .withOutputSchema(outputSchema));
    Row expectedOutput =
        Row.withSchema(outputSchema)
            .addValues(
                input.getField1(), input.getField2().getField1(), input.getField2().getField3())
            .build();
    PAssert.that(rows).containsInAnyOrder(expectedOutput);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectWithOutputSchemaIncorrectSchema() {
    Schema outputSchema =
        Schema.builder()
            .addStringField("stringField")
            .addStringField("nestedStringField")
            .addStringField("nestedDoubleField")
            .build();

    thrown.expect(IllegalArgumentException.class);
    pipeline
        .apply(Create.of(Schema2.create()))
        .apply(
            Select.<Schema2>fieldNames("field1", "field2.field1", "field2.field3")
                .withOutputSchema(outputSchema));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedAll() {
    PCollection<Schema1> rows =
        pipeline
            .apply(Create.of(Schema2.create()))
            .apply(Select.fieldNames("field2"))
            .apply(Convert.to(Schema1.class));
    PAssert.that(rows).containsInAnyOrder(Schema1.create());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedAllWildcard() {
    PCollection<Schema1> rows =
        pipeline
            .apply(Create.of(Schema2.create()))
            .apply(Select.fieldNames("field2.*"))
            .apply(Convert.to(Schema1.class));
    PAssert.that(rows).containsInAnyOrder(Schema1.create());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectNestedPartial() {
    PCollection<Schema1Selected> rows =
        pipeline
            .apply(Create.of(Schema2.create()))
            .apply(Select.fieldNames("field2.field1", "field2.field3"))
            .apply(Convert.to(Schema1Selected.class));
    PAssert.that(rows).containsInAnyOrder(Schema1Selected.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PrimitiveArray {
    abstract List<Double> getField1();

    static PrimitiveArray create() {
      return new AutoValue_SelectTest_PrimitiveArray(ImmutableList.of(1.0, 2.1, 3.2));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectPrimitiveArray() {
    PCollection<PrimitiveArray> selected =
        pipeline
            .apply(Create.of(PrimitiveArray.create()))
            .apply(Select.fieldNames("field1"))
            .apply(Convert.to(PrimitiveArray.class));
    PAssert.that(selected).containsInAnyOrder(PrimitiveArray.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class RowSingleArray {
    abstract List<Schema1> getField1();

    static RowSingleArray create() {
      return new AutoValue_SelectTest_RowSingleArray(
          ImmutableList.of(Schema1.create(), Schema1.create(), Schema1.create()));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PartialRowSingleArray {
    abstract List<String> getField1();

    abstract List<Double> getField3();

    static PartialRowSingleArray create() {
      return new AutoValue_SelectTest_PartialRowSingleArray(
          ImmutableList.of("field1", "field1", "field1"), ImmutableList.of(3.14, 3.14, 3.14));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowArray() {
    PCollection<PartialRowSingleArray> selected =
        pipeline
            .apply(Create.of(RowSingleArray.create()))
            .apply(Select.fieldNames("field1.field1", "field1.field3"))
            .apply(Convert.to(PartialRowSingleArray.class));
    PAssert.that(selected).containsInAnyOrder(PartialRowSingleArray.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class RowSingleMap {
    abstract Map<String, Schema1> getField1();

    static RowSingleMap create() {
      return new AutoValue_SelectTest_RowSingleMap(
          ImmutableMap.of(
              "key1", Schema1.create(),
              "key2", Schema1.create(),
              "key3", Schema1.create()));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PartialRowSingleMap {
    abstract Map<String, String> getField1();

    abstract Map<String, Double> getField3();

    static PartialRowSingleMap create() {
      return new AutoValue_SelectTest_PartialRowSingleMap(
          ImmutableMap.of(
              "key1", "field1",
              "key2", "field1",
              "key3", "field1"),
          ImmutableMap.of(
              "key1", 3.14,
              "key2", 3.14,
              "key3", 3.14));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowMap() {
    PCollection<PartialRowSingleMap> selected =
        pipeline
            .apply(Create.of(RowSingleMap.create()))
            .apply(Select.fieldNames("field1.field1", "field1.field3"))
            .apply(Convert.to(PartialRowSingleMap.class));
    PAssert.that(selected).containsInAnyOrder(PartialRowSingleMap.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class RowMultipleArray {
    private static final List<Schema1> ROW_LIST =
        ImmutableList.of(Schema1.create(), Schema1.create(), Schema1.create());
    private static final List<List<Schema1>> ROW_LIST_LIST =
        ImmutableList.of(ROW_LIST, ROW_LIST, ROW_LIST);

    abstract List<List<List<Schema1>>> getField1();

    static RowMultipleArray create() {
      return new AutoValue_SelectTest_RowMultipleArray(
          ImmutableList.of(ROW_LIST_LIST, ROW_LIST_LIST, ROW_LIST_LIST));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PartialRowMultipleArray {
    private static final List<Schema1Selected> ROW_LIST =
        ImmutableList.of(
            Schema1Selected.create(), Schema1Selected.create(), Schema1Selected.create());
    private static final List<List<Schema1Selected>> ROW_LIST_LIST =
        ImmutableList.of(ROW_LIST, ROW_LIST, ROW_LIST);
    private static final List<String> STRING_LIST = ImmutableList.of("field1", "field1", "field1");
    private static final List<List<String>> STRING_LISTLIST =
        ImmutableList.of(STRING_LIST, STRING_LIST, STRING_LIST);
    private static final List<Double> DOUBLE_LIST = ImmutableList.of(3.14, 3.14, 3.14);
    private static final List<List<Double>> DOUBLE_LISTLIST =
        ImmutableList.of(DOUBLE_LIST, DOUBLE_LIST, DOUBLE_LIST);

    abstract List<List<List<String>>> getField1();

    abstract List<List<List<Double>>> getField3();

    static PartialRowMultipleArray create() {
      return new AutoValue_SelectTest_PartialRowMultipleArray(
          ImmutableList.of(STRING_LISTLIST, STRING_LISTLIST, STRING_LISTLIST),
          ImmutableList.of(DOUBLE_LISTLIST, DOUBLE_LISTLIST, DOUBLE_LISTLIST));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectedNestedArrays() {
    PCollection<RowMultipleArray> input = pipeline.apply(Create.of(RowMultipleArray.create()));

    PCollection<PartialRowMultipleArray> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowMultipleArray.class));
    PAssert.that(selected).containsInAnyOrder(PartialRowMultipleArray.create());

    PCollection<PartialRowMultipleArray> selected2 =
        input
            .apply("select2", Select.fieldNames("field1[][][].field1", "field1[][][].field3"))
            .apply("convert2", Convert.to(PartialRowMultipleArray.class));

    PAssert.that(selected).containsInAnyOrder(PartialRowMultipleArray.create());
    PAssert.that(selected2).containsInAnyOrder(PartialRowMultipleArray.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class RowMultipleMaps {
    static final Map<String, Schema1> ROW_MAP =
        ImmutableMap.of(
            "key1", Schema1.create(),
            "key2", Schema1.create(),
            "key3", Schema1.create());
    static final Map<String, Map<String, Schema1>> ROW_MAP_MAP =
        ImmutableMap.of(
            "key1", ROW_MAP,
            "key2", ROW_MAP,
            "key3", ROW_MAP);

    abstract Map<String, Map<String, Map<String, Schema1>>> getField1();

    static RowMultipleMaps create() {
      return new AutoValue_SelectTest_RowMultipleMaps(
          ImmutableMap.of(
              "key1", ROW_MAP_MAP,
              "key2", ROW_MAP_MAP,
              "key3", ROW_MAP_MAP));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PartialRowMultipleMaps {
    static final Map<String, String> STRING_MAP =
        ImmutableMap.of(
            "key1", "field1",
            "key2", "field1",
            "key3", "field1");
    static final Map<String, Map<String, String>> STRING_MAPMAP =
        ImmutableMap.of(
            "key1", STRING_MAP,
            "key2", STRING_MAP,
            "key3", STRING_MAP);
    static final Map<String, Double> DOUBLE_MAP =
        ImmutableMap.of(
            "key1", 3.14,
            "key2", 3.14,
            "key3", 3.14);
    static final Map<String, Map<String, Double>> DOUBLE_MAPMAP =
        ImmutableMap.of(
            "key1", DOUBLE_MAP,
            "key2", DOUBLE_MAP,
            "key3", DOUBLE_MAP);

    abstract Map<String, Map<String, Map<String, String>>> getField1();

    abstract Map<String, Map<String, Map<String, Double>>> getField3();

    static PartialRowMultipleMaps create() {
      return new AutoValue_SelectTest_PartialRowMultipleMaps(
          ImmutableMap.of(
              "key1", STRING_MAPMAP,
              "key2", STRING_MAPMAP,
              "key3", STRING_MAPMAP),
          ImmutableMap.of(
              "key1", DOUBLE_MAPMAP,
              "key2", DOUBLE_MAPMAP,
              "key3", DOUBLE_MAPMAP));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowNestedMaps() {
    PCollection<RowMultipleMaps> input = pipeline.apply(Create.of(RowMultipleMaps.create()));

    PCollection<PartialRowMultipleMaps> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowMultipleMaps.class));

    PCollection<PartialRowMultipleMaps> selected2 =
        input
            .apply("select2", Select.fieldNames("field1{}{}{}.field1", "field1{}{}{}.field3"))
            .apply("convert2", Convert.to(PartialRowMultipleMaps.class));

    PAssert.that(selected).containsInAnyOrder(PartialRowMultipleMaps.create());
    PAssert.that(selected2).containsInAnyOrder(PartialRowMultipleMaps.create());
    pipeline.run();
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class RowNestedArraysAndMaps {
    static final List<Schema1> ROW_LIST =
        ImmutableList.of(Schema1.create(), Schema1.create(), Schema1.create());
    static final Map<String, List<Schema1>> ROW_MAP_LIST =
        ImmutableMap.of(
            "key1", ROW_LIST,
            "key2", ROW_LIST,
            "key3", ROW_LIST);

    abstract List<Map<String, List<Schema1>>> getField1();

    static RowNestedArraysAndMaps create() {
      return new AutoValue_SelectTest_RowNestedArraysAndMaps(
          ImmutableList.of(ROW_MAP_LIST, ROW_MAP_LIST, ROW_MAP_LIST));
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class PartialRowNestedArraysAndMaps {
    static final Map<String, List<String>> STRING_MAP =
        ImmutableMap.of(
            "key1", ImmutableList.of("field1", "field1", "field1"),
            "key2", ImmutableList.of("field1", "field1", "field1"),
            "key3", ImmutableList.of("field1", "field1", "field1"));
    static final Map<String, List<Double>> DOUBLE_MAP =
        ImmutableMap.of(
            "key1", ImmutableList.of(3.14, 3.14, 3.14),
            "key2", ImmutableList.of(3.14, 3.14, 3.14),
            "key3", ImmutableList.of(3.14, 3.14, 3.14));

    abstract List<Map<String, List<String>>> getField1();

    abstract List<Map<String, List<Double>>> getField3();

    static PartialRowNestedArraysAndMaps create() {
      return new AutoValue_SelectTest_PartialRowNestedArraysAndMaps(
          ImmutableList.of(STRING_MAP, STRING_MAP, STRING_MAP),
          ImmutableList.of(DOUBLE_MAP, DOUBLE_MAP, DOUBLE_MAP));
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSelectRowNestedListsAndMaps() {
    PCollection<RowNestedArraysAndMaps> input =
        pipeline.apply(Create.of(RowNestedArraysAndMaps.create()));

    PCollection<PartialRowNestedArraysAndMaps> selected =
        input
            .apply("select1", Select.fieldNames("field1.field1", "field1.field3"))
            .apply("convert1", Convert.to(PartialRowNestedArraysAndMaps.class));

    PCollection<PartialRowNestedArraysAndMaps> selected2 =
        input
            .apply("select2", Select.fieldNames("field1[]{}[].field1", "field1[]{}[].field3"))
            .apply("convert2", Convert.to(PartialRowNestedArraysAndMaps.class));

    PAssert.that(selected).containsInAnyOrder(PartialRowNestedArraysAndMaps.create());
    PAssert.that(selected2).containsInAnyOrder(PartialRowNestedArraysAndMaps.create());
    pipeline.run();
  }

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
        pipeline
            .apply(Create.of(rows).withRowSchema(SIMPLE_SCHEMA))
            .apply(Select.flattenedSchema());
    PAssert.that(unnested).containsInAnyOrder(rows);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testSimpleFlattening() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA).addValues(r, r).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline
            .apply(Create.of(rows).withRowSchema(NESTED_SCHEMA))
            .apply(Select.flattenedSchema());
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
  public void testAlternateNamePolicyFlatten() {
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
            .apply(Select.<Row>flattenedSchema().keepMostNestedFieldName());
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
  public void testClashingNamePolicyFlatten() {
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
            .apply(Select.<Row>flattenedSchema().keepMostNestedFieldName());
    pipeline.run();
  }

  static final Schema CLASHING_NAME_UNNESTED_SCHEMA =
      Schema.builder()
          .addInt32Field("field1")
          .addStringField("field2")
          .addInt32Field("n2field1")
          .addStringField("n2field2")
          .build();

  @Test
  @Category(NeedsRunner.class)
  public void testClashingNameWithRenameFlatten() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA).addValues(r, r).build())
            .collect(Collectors.toList());
    PCollection<Row> unnested =
        pipeline
            .apply(Create.of(rows).withRowSchema(NESTED_SCHEMA))
            .apply(
                Select.<Row>flattenedSchema()
                    .keepMostNestedFieldName()
                    .withFieldNameAs("nested2.field1", "n2field1")
                    .withFieldNameAs("nested2.field2", "n2field2"));
    assertEquals(CLASHING_NAME_UNNESTED_SCHEMA, unnested.getSchema());
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFlattenWithOutputSchema() {
    List<Row> bottomRow =
        IntStream.rangeClosed(0, 2)
            .mapToObj(i -> Row.withSchema(SIMPLE_SCHEMA).addValues(i, Integer.toString(i)).build())
            .collect(Collectors.toList());
    List<Row> rows =
        bottomRow.stream()
            .map(r -> Row.withSchema(NESTED_SCHEMA).addValues(r, r).build())
            .collect(Collectors.toList());

    PCollection<Row> unnested =
        pipeline
            .apply(Create.of(rows).withRowSchema(NESTED_SCHEMA))
            .apply(Select.<Row>flattenedSchema().withOutputSchema(CLASHING_NAME_UNNESTED_SCHEMA));
    assertEquals(CLASHING_NAME_UNNESTED_SCHEMA, unnested.getSchema());
    pipeline.run();
  }
}
