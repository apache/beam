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
package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link SelectHelpers}. */
@RunWith(Parameterized.class)
public class SelectHelpersTest {
  @Parameterized.Parameter public boolean useOptimizedSelect;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  static final Schema FLAT_SCHEMA =
      Schema.builder()
          .addStringField("field1")
          .addInt32Field("field2")
          .addDoubleField("field3")
          .addStringField("field_extra")
          .build();
  static final Row FLAT_ROW =
      Row.withSchema(FLAT_SCHEMA).addValues("first", 42, 3.14, "extra").build();

  static final Schema NESTED_SCHEMA =
      Schema.builder().addRowField("nested", FLAT_SCHEMA).addStringField("foo").build();
  static final Row NESTED_ROW = Row.withSchema(NESTED_SCHEMA).addValues(FLAT_ROW, "").build();

  static final Schema DOUBLE_NESTED_SCHEMA =
      Schema.builder().addRowField("nested2", NESTED_SCHEMA).build();
  static final Row DOUBLE_NESTED_ROW =
      Row.withSchema(DOUBLE_NESTED_SCHEMA).addValue(NESTED_ROW).build();

  static final Schema ARRAY_SCHEMA =
      Schema.builder()
          .addArrayField("primitiveArray", FieldType.INT32)
          .addArrayField("rowArray", FieldType.row(FLAT_SCHEMA))
          .addArrayField("arrayOfRowArray", FieldType.array(FieldType.row(FLAT_SCHEMA)))
          .addArrayField("nestedRowArray", FieldType.row(NESTED_SCHEMA))
          .build();
  static final Row ARRAY_ROW =
      Row.withSchema(ARRAY_SCHEMA)
          .addArray(1, 2)
          .addArray(FLAT_ROW, FLAT_ROW)
          .addArray(ImmutableList.of(FLAT_ROW), ImmutableList.of(FLAT_ROW))
          .addArray(NESTED_ROW, NESTED_ROW)
          .build();

  static final Schema ITERABLE_SCHEMA =
      Schema.builder()
          .addIterableField("primitiveIter", FieldType.INT32)
          .addIterableField("rowIter", FieldType.row(FLAT_SCHEMA))
          .addIterableField("iterOfRowIter", FieldType.iterable(FieldType.row(FLAT_SCHEMA)))
          .addIterableField("nestedRowIter", FieldType.row(NESTED_SCHEMA))
          .build();
  static final Row ITERABLE_ROW =
      Row.withSchema(ARRAY_SCHEMA)
          .addIterable(ImmutableList.of(1, 2))
          .addIterable(ImmutableList.of(FLAT_ROW, FLAT_ROW))
          .addIterable(ImmutableList.of(ImmutableList.of(FLAT_ROW), ImmutableList.of(FLAT_ROW)))
          .addIterable(ImmutableList.of(NESTED_ROW, NESTED_ROW))
          .build();
  static final Schema MAP_SCHEMA =
      Schema.builder().addMapField("map", FieldType.INT32, FieldType.row(FLAT_SCHEMA)).build();
  static final Row MAP_ROW =
      Row.withSchema(MAP_SCHEMA).addValue(ImmutableMap.of(1, FLAT_ROW)).build();

  static final Schema MAP_ARRAY_SCHEMA =
      Schema.builder()
          .addMapField("map", FieldType.INT32, FieldType.array(FieldType.row(FLAT_SCHEMA)))
          .build();
  static final Row MAP_ARRAY_ROW =
      Row.withSchema(MAP_ARRAY_SCHEMA)
          .addValue(ImmutableMap.of(1, ImmutableList.of(FLAT_ROW)))
          .build();

  static final Schema MAP_ITERABLE_SCHEMA =
      Schema.builder()
          .addMapField("map", FieldType.INT32, FieldType.iterable(FieldType.row(FLAT_SCHEMA)))
          .build();
  static final Row MAP_ITERABLE_ROW =
      Row.withSchema(MAP_ITERABLE_SCHEMA)
          .addValue(ImmutableMap.of(1, ImmutableList.of(FLAT_ROW)))
          .build();

  Row selectRow(Schema inputScema, FieldAccessDescriptor fieldAccessDescriptor, Row row) {
    RowSelector rowSelector =
        useOptimizedSelect
            ? SelectHelpers.getRowSelectorOptimized(inputScema, fieldAccessDescriptor)
            : SelectHelpers.getRowSelector(inputScema, fieldAccessDescriptor);
    return rowSelector.select(row);
  }

  @Test
  public void testSelectAll() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("*").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    assertEquals(FLAT_SCHEMA, outputSchema);

    Row row = selectRow(FLAT_SCHEMA, fieldAccessDescriptor, FLAT_ROW);
    assertEquals(FLAT_ROW, row);
  }

  @Test
  public void testsSimpleSelectSingle() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addStringField("field1").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(FLAT_SCHEMA, fieldAccessDescriptor, FLAT_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testsSimpleSelectSingleWithUnderscore() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field_extra").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addStringField("field_extra").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(FLAT_SCHEMA, fieldAccessDescriptor, FLAT_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("extra").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testsSimpleSelectMultiple() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1", "field3").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addStringField("field1").addDoubleField("field3").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(FLAT_SCHEMA, fieldAccessDescriptor, FLAT_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValues("first", 3.14).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectedNested() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nested").resolve(NESTED_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(NESTED_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addRowField("nested", FLAT_SCHEMA).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(NESTED_SCHEMA, fieldAccessDescriptor, NESTED_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue(FLAT_ROW).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectedNestedSingle() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nested.field1").resolve(NESTED_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(NESTED_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addStringField("field1").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(NESTED_SCHEMA, fieldAccessDescriptor, NESTED_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectedNestedWildcard() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nested.*").resolve(NESTED_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(NESTED_SCHEMA, fieldAccessDescriptor);
    assertEquals(FLAT_SCHEMA, outputSchema);

    Row row = selectRow(NESTED_SCHEMA, fieldAccessDescriptor, NESTED_ROW);
    assertEquals(FLAT_ROW, row);
  }

  @Test
  public void testSelectDoubleNested() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nested2.nested.field1").resolve(DOUBLE_NESTED_SCHEMA);
    Schema outputSchema =
        SelectHelpers.getOutputSchema(DOUBLE_NESTED_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addStringField("field1").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(DOUBLE_NESTED_SCHEMA, fieldAccessDescriptor, DOUBLE_NESTED_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfPrimitive() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("primitiveArray").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addArrayField("primitiveArray", FieldType.INT32).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ARRAY_SCHEMA, fieldAccessDescriptor, ARRAY_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addArray(1, 2).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectIterableOfPrimitive() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("primitiveIter").resolve(ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ITERABLE_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addIterableField("primitiveIter", FieldType.INT32).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ITERABLE_SCHEMA, fieldAccessDescriptor, ITERABLE_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addIterable(ImmutableList.of(1, 2)).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfRow() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("rowArray").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addArrayField("rowArray", FieldType.row(FLAT_SCHEMA)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ARRAY_SCHEMA, fieldAccessDescriptor, ARRAY_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addArray(FLAT_ROW, FLAT_ROW).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectIterableOfRow() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("rowIter").resolve(ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ITERABLE_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addIterableField("rowIter", FieldType.row(FLAT_SCHEMA)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ITERABLE_SCHEMA, fieldAccessDescriptor, ITERABLE_ROW);
    Row expectedRow =
        Row.withSchema(expectedSchema).addIterable(ImmutableList.of(FLAT_ROW, FLAT_ROW)).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfRowPartial() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("rowArray[].field1").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema = Schema.builder().addArrayField("field1", FieldType.STRING).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ARRAY_SCHEMA, fieldAccessDescriptor, ARRAY_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addArray("first", "first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectIterableOfRowPartial() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("rowIter[].field1").resolve(ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ITERABLE_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema = Schema.builder().addIterableField("field1", FieldType.STRING).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ITERABLE_SCHEMA, fieldAccessDescriptor, ITERABLE_ROW);
    Row expectedRow =
        Row.withSchema(expectedSchema).addIterable(ImmutableList.of("first", "first")).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfRowArray() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("arrayOfRowArray[][].field1").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema =
        Schema.builder().addArrayField("field1", FieldType.array(FieldType.STRING)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ARRAY_SCHEMA, fieldAccessDescriptor, ARRAY_ROW);

    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addArray(ImmutableList.of("first"), ImmutableList.of("first"))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectIterableOfRowIterable() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("iterOfRowIter[][].field1").resolve(ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ITERABLE_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema =
        Schema.builder().addIterableField("field1", FieldType.iterable(FieldType.STRING)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ITERABLE_SCHEMA, fieldAccessDescriptor, ITERABLE_ROW);

    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addIterable(ImmutableList.of(ImmutableList.of("first"), ImmutableList.of("first")))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfNestedRow() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nestedRowArray[].nested.field1")
            .resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedElementSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema = Schema.builder().addArrayField("field1", FieldType.STRING).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ARRAY_SCHEMA, fieldAccessDescriptor, ARRAY_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addArray("first", "first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectIterableOfNestedRow() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nestedRowIter[].nested.field1")
            .resolve(ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ITERABLE_SCHEMA, fieldAccessDescriptor);

    Schema expectedElementSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema = Schema.builder().addIterableField("field1", FieldType.STRING).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(ITERABLE_SCHEMA, fieldAccessDescriptor, ITERABLE_ROW);
    Row expectedRow =
        Row.withSchema(expectedSchema).addIterable(ImmutableList.of("first", "first")).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfRowSelectSingle() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map{}.field1").resolve(MAP_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_SCHEMA, fieldAccessDescriptor);

    Schema expectedValueSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema =
        Schema.builder().addMapField("field1", FieldType.INT32, FieldType.STRING).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(MAP_SCHEMA, fieldAccessDescriptor, MAP_ROW);
    Row expectedRow = Row.withSchema(expectedSchema).addValue(ImmutableMap.of(1, "first")).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfRowSelectAll() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map{}.*").resolve(MAP_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder()
            .addMapField("field1", FieldType.INT32, FieldType.STRING)
            .addMapField("field2", FieldType.INT32, FieldType.INT32)
            .addMapField("field3", FieldType.INT32, FieldType.DOUBLE)
            .addMapField("field_extra", FieldType.INT32, FieldType.STRING)
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(MAP_SCHEMA, fieldAccessDescriptor, MAP_ROW);
    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addValue(ImmutableMap.of(1, FLAT_ROW.getValue(0)))
            .addValue(ImmutableMap.of(1, FLAT_ROW.getValue(1)))
            .addValue(ImmutableMap.of(1, FLAT_ROW.getValue(2)))
            .addValue(ImmutableMap.of(1, FLAT_ROW.getValue(3)))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfArray() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map.field1").resolve(MAP_ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema =
        Schema.builder()
            .addMapField("field1", FieldType.INT32, FieldType.array(FieldType.STRING))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(MAP_ARRAY_SCHEMA, fieldAccessDescriptor, MAP_ARRAY_ROW);

    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addValue(ImmutableMap.of(1, ImmutableList.of("first")))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfIterable() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map.field1").resolve(MAP_ITERABLE_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_ITERABLE_SCHEMA, fieldAccessDescriptor);

    Schema expectedSchema =
        Schema.builder()
            .addMapField("field1", FieldType.INT32, FieldType.iterable(FieldType.STRING))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = selectRow(MAP_ITERABLE_SCHEMA, fieldAccessDescriptor, MAP_ITERABLE_ROW);

    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addValue(ImmutableMap.of(1, ImmutableList.of("first")))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectFieldOfRecord() {
    Schema f1 = Schema.builder().addInt64Field("f0").build();
    Schema f2 = Schema.builder().addRowField("f1", f1).build();
    Schema f3 = Schema.builder().addRowField("f2", f2).build();

    Row r1 = Row.withSchema(f1).addValue(42L).build(); // {"f0": 42}
    Row r2 = Row.withSchema(f2).addValue(r1).build(); // {"f1": {"f0": 42}}
    Row r3 = Row.withSchema(f3).addValue(r2).build(); // {"f2": {"f1": {"f0": 42}}}

    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("f2.f1").resolve(f3);

    Schema outputSchema = SelectHelpers.getOutputSchema(f3, fieldAccessDescriptor);

    Row out = selectRow(f3, fieldAccessDescriptor, r3);

    assertEquals(f2, outputSchema);
    assertEquals(r2, out);
  }

  @Test
  public void testSelectFieldOfRecordOrRecord() {
    Schema f1 = Schema.builder().addInt64Field("f0").build();
    Schema f2 = Schema.builder().addRowField("f1", f1).build();
    Schema f3 = Schema.builder().addRowField("f2", f2).build();
    Schema f4 = Schema.builder().addRowField("f3", f3).build();

    Row r1 = Row.withSchema(f1).addValue(42L).build(); // {"f0": 42}
    Row r2 = Row.withSchema(f2).addValue(r1).build(); // {"f1": {"f0": 42}}
    Row r3 = Row.withSchema(f3).addValue(r2).build(); // {"f2": {"f1": {"f0": 42}}}
    Row r4 = Row.withSchema(f4).addValue(r3).build(); // {"f3": {"f2": {"f1": {"f0": 42}}}}

    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("f3.f2").resolve(f4);

    Schema outputSchema = SelectHelpers.getOutputSchema(f4, fieldAccessDescriptor);

    Row out = selectRow(f4, fieldAccessDescriptor, r4);

    assertEquals(f3, outputSchema);
    assertEquals(r3, out);
  }

  @Test
  public void testArrayRowArray() {
    Schema f1 = Schema.builder().addStringField("f0").build();
    Schema f2 = Schema.builder().addArrayField("f1", FieldType.row(f1)).build();
    Schema f3 = Schema.builder().addRowField("f2", f2).build();
    Schema f4 = Schema.builder().addArrayField("f3", FieldType.row(f3)).build();

    Row r1 = Row.withSchema(f1).addValue("first").build();
    Row r2 = Row.withSchema(f2).addArray(r1, r1).build();
    Row r3 = Row.withSchema(f3).addValue(r2).build();
    Row r4 = Row.withSchema(f4).addArray(r3, r3).build();

    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("f3.f2.f1.f0").resolve(f4);
    Schema outputSchema = SelectHelpers.getOutputSchema(f4, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addArrayField("f0", FieldType.array(FieldType.STRING)).build();
    assertEquals(expectedSchema, outputSchema);
    Row out = selectRow(f4, fieldAccessDescriptor, r4);
    Row expected =
        Row.withSchema(outputSchema)
            .addArray(Lists.newArrayList("first", "first"), Lists.newArrayList("first", "first"))
            .build();
    assertEquals(expected, out);
  }

  @Test
  public void testIterableRowIterable() {
    Schema f1 = Schema.builder().addStringField("f0").build();
    Schema f2 = Schema.builder().addIterableField("f1", FieldType.row(f1)).build();
    Schema f3 = Schema.builder().addRowField("f2", f2).build();
    Schema f4 = Schema.builder().addIterableField("f3", FieldType.row(f3)).build();

    Row r1 = Row.withSchema(f1).addValue("first").build();
    Row r2 = Row.withSchema(f2).addIterable(ImmutableList.of(r1, r1)).build();
    Row r3 = Row.withSchema(f3).addValue(r2).build();
    Row r4 = Row.withSchema(f4).addIterable(ImmutableList.of(r3, r3)).build();

    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("f3.f2.f1.f0").resolve(f4);
    Schema outputSchema = SelectHelpers.getOutputSchema(f4, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addIterableField("f0", FieldType.iterable(FieldType.STRING)).build();
    assertEquals(expectedSchema, outputSchema);
    Row out = selectRow(f4, fieldAccessDescriptor, r4);
    Row expected =
        Row.withSchema(outputSchema)
            .addIterable(
                ImmutableList.of(
                    ImmutableList.of("first", "first"), ImmutableList.of("first", "first")))
            .build();
    assertEquals(expected, out);
  }

  static final Schema NESTED_NULLABLE_SCHEMA =
      Schema.builder()
          .addNullableField("nested", FieldType.row(FLAT_SCHEMA))
          .addNullableField("nestedArray", FieldType.array(FieldType.row(FLAT_SCHEMA)))
          .addNullableField(
              "nestedMap", FieldType.map(FieldType.STRING, FieldType.row(FLAT_SCHEMA)))
          .build();
  static final Row NESTED_NULLABLE_ROW = Row.nullRow(NESTED_NULLABLE_SCHEMA);

  @Test
  public void testNullableSchema() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nested.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema1 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1);
    Schema expectedSchema1 = Schema.builder().addNullableField("field1", FieldType.STRING).build();
    assertEquals(expectedSchema1, schema1);

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nested.*").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema2 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2);
    Schema expectedSchema2 =
        Schema.builder()
            .addNullableField("field1", FieldType.STRING)
            .addNullableField("field2", FieldType.INT32)
            .addNullableField("field3", FieldType.DOUBLE)
            .addNullableField("field_extra", FieldType.STRING)
            .build();
    assertEquals(expectedSchema2, schema2);
  }

  @Test
  public void testNullableSchemaArray() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nestedArray.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema1 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1);
    Schema expectedSchema1 =
        Schema.builder().addNullableField("field1", FieldType.array(FieldType.STRING)).build();
    assertEquals(expectedSchema1, schema1);

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nestedArray.*").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema2 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2);
    Schema expectedSchema2 =
        Schema.builder()
            .addNullableField("field1", FieldType.array(FieldType.STRING))
            .addNullableField("field2", FieldType.array(FieldType.INT32))
            .addNullableField("field3", FieldType.array(FieldType.DOUBLE))
            .addNullableField("field_extra", FieldType.array(FieldType.STRING))
            .build();
    assertEquals(expectedSchema2, schema2);
  }

  @Test
  public void testNullableSchemaMap() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nestedMap.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema1 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1);
    Schema expectedSchema1 =
        Schema.builder()
            .addNullableField("field1", FieldType.map(FieldType.STRING, FieldType.STRING))
            .build();
    assertEquals(expectedSchema1, schema1);

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nestedMap.*").resolve(NESTED_NULLABLE_SCHEMA);
    Schema schema2 = SelectHelpers.getOutputSchema(NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2);
    Schema expectedSchema2 =
        Schema.builder()
            .addNullableField("field1", FieldType.map(FieldType.STRING, FieldType.STRING))
            .addNullableField("field2", FieldType.map(FieldType.STRING, FieldType.INT32))
            .addNullableField("field3", FieldType.map(FieldType.STRING, FieldType.DOUBLE))
            .addNullableField("field_extra", FieldType.map(FieldType.STRING, FieldType.STRING))
            .build();
    assertEquals(expectedSchema2, schema2);
  }

  @Test
  public void testSelectNullableNestedRow() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nested.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Row out1 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertNull(out1.getValue(0));

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nested.*").resolve(NESTED_NULLABLE_SCHEMA);
    Row out2 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertEquals(Collections.nCopies(4, null), out2.getValues());
  }

  @Test
  public void testSelectNullableNestedRowArray() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nestedArray.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Row out1 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertNull(out1.getValue(0));

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nestedArray.*").resolve(NESTED_NULLABLE_SCHEMA);
    Row out2 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertEquals(Collections.nCopies(4, null), out2.getValues());
  }

  @Test
  public void testSelectNullableNestedRowMap() {
    FieldAccessDescriptor fieldAccessDescriptor1 =
        FieldAccessDescriptor.withFieldNames("nestedMap.field1").resolve(NESTED_NULLABLE_SCHEMA);
    Row out1 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor1, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertNull(out1.getValue(0));

    FieldAccessDescriptor fieldAccessDescriptor2 =
        FieldAccessDescriptor.withFieldNames("nestedMap.*").resolve(NESTED_NULLABLE_SCHEMA);
    Row out2 =
        selectRow(
            NESTED_NULLABLE_SCHEMA, fieldAccessDescriptor2, Row.nullRow(NESTED_NULLABLE_SCHEMA));
    assertEquals(Collections.nCopies(4, null), out2.getValues());
  }
}
