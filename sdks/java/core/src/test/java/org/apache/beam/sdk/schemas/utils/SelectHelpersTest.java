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

import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Tests for {@link SelectHelpers}. */
public class SelectHelpersTest {
  static final Schema FLAT_SCHEMA =
      Schema.builder()
          .addStringField("field1")
          .addInt32Field("field2")
          .addDoubleField("field3")
          .build();
  static final Row FLAT_ROW = Row.withSchema(FLAT_SCHEMA).addValues("first", 42, 3.14).build();

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

  @Test
  public void testSelectAll() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("*").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    assertEquals(FLAT_SCHEMA, outputSchema);

    Row row = SelectHelpers.selectRow(FLAT_ROW, fieldAccessDescriptor, FLAT_SCHEMA, outputSchema);
    assertEquals(FLAT_ROW, row);
  }

  @Test
  public void testsSimpleSelectSingle() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1").resolve(FLAT_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(FLAT_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema = Schema.builder().addStringField("field1").build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(FLAT_ROW, fieldAccessDescriptor, FLAT_SCHEMA, outputSchema);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("first").build();
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

    Row row = SelectHelpers.selectRow(FLAT_ROW, fieldAccessDescriptor, FLAT_SCHEMA, outputSchema);
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

    Row row =
        SelectHelpers.selectRow(NESTED_ROW, fieldAccessDescriptor, NESTED_SCHEMA, outputSchema);
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

    Row row =
        SelectHelpers.selectRow(NESTED_ROW, fieldAccessDescriptor, NESTED_SCHEMA, outputSchema);
    Row expectedRow = Row.withSchema(expectedSchema).addValue("first").build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectedNestedWildcard() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("nested.*").resolve(NESTED_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(NESTED_SCHEMA, fieldAccessDescriptor);
    assertEquals(FLAT_SCHEMA, outputSchema);

    Row row =
        SelectHelpers.selectRow(NESTED_ROW, fieldAccessDescriptor, NESTED_SCHEMA, outputSchema);
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

    Row row =
        SelectHelpers.selectRow(
            DOUBLE_NESTED_ROW, fieldAccessDescriptor, DOUBLE_NESTED_SCHEMA, outputSchema);
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

    Row row = SelectHelpers.selectRow(ARRAY_ROW, fieldAccessDescriptor, ARRAY_SCHEMA, outputSchema);
    Row expectedRow = Row.withSchema(expectedSchema).addArray(1, 2).build();
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

    Row row = SelectHelpers.selectRow(ARRAY_ROW, fieldAccessDescriptor, ARRAY_SCHEMA, outputSchema);
    Row expectedRow = Row.withSchema(expectedSchema).addArray(FLAT_ROW, FLAT_ROW).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfRowPartial() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("rowArray[].field1").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedElementSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema =
        Schema.builder().addArrayField("rowArray", FieldType.row(expectedElementSchema)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(ARRAY_ROW, fieldAccessDescriptor, ARRAY_SCHEMA, outputSchema);

    Row expectedElement = Row.withSchema(expectedElementSchema).addValue("first").build();
    Row expectedRow =
        Row.withSchema(expectedSchema).addArray(expectedElement, expectedElement).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectArrayOfRowArray() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("arrayOfRowArray[][].field1").resolve(ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedElementSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema =
        Schema.builder()
            .addArrayField("arrayOfRowArray", FieldType.array(FieldType.row(expectedElementSchema)))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(ARRAY_ROW, fieldAccessDescriptor, ARRAY_SCHEMA, outputSchema);

    Row expectedElement = Row.withSchema(expectedElementSchema).addValue("first").build();
    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addArray(ImmutableList.of(expectedElement), ImmutableList.of(expectedElement))
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
    Schema expectedSchema =
        Schema.builder()
            .addArrayField("nestedRowArray", FieldType.row(expectedElementSchema))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(ARRAY_ROW, fieldAccessDescriptor, ARRAY_SCHEMA, outputSchema);

    Row expectedElement = Row.withSchema(expectedElementSchema).addValue("first").build();
    Row expectedRow =
        Row.withSchema(expectedSchema).addArray(expectedElement, expectedElement).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfRowSelectSingle() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map{}.field1").resolve(MAP_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_SCHEMA, fieldAccessDescriptor);

    Schema expectedValueSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema =
        Schema.builder()
            .addMapField("map", FieldType.INT32, FieldType.row(expectedValueSchema))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(MAP_ROW, fieldAccessDescriptor, MAP_SCHEMA, outputSchema);

    Row expectedValueRow = Row.withSchema(expectedValueSchema).addValue("first").build();
    Row expectedRow =
        Row.withSchema(expectedSchema).addValue(ImmutableMap.of(1, expectedValueRow)).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfRowSelectAll() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map{}.*").resolve(MAP_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_SCHEMA, fieldAccessDescriptor);
    Schema expectedSchema =
        Schema.builder().addMapField("map", FieldType.INT32, FieldType.row(FLAT_SCHEMA)).build();
    assertEquals(expectedSchema, outputSchema);

    Row row = SelectHelpers.selectRow(MAP_ROW, fieldAccessDescriptor, MAP_SCHEMA, outputSchema);
    Row expectedRow = Row.withSchema(expectedSchema).addValue(ImmutableMap.of(1, FLAT_ROW)).build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testSelectMapOfArray() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("map.field1").resolve(MAP_ARRAY_SCHEMA);
    Schema outputSchema = SelectHelpers.getOutputSchema(MAP_ARRAY_SCHEMA, fieldAccessDescriptor);

    Schema expectedValueSchema = Schema.builder().addStringField("field1").build();
    Schema expectedSchema =
        Schema.builder()
            .addMapField(
                "map", FieldType.INT32, FieldType.array(FieldType.row(expectedValueSchema)))
            .build();
    assertEquals(expectedSchema, outputSchema);

    Row row =
        SelectHelpers.selectRow(
            MAP_ARRAY_ROW, fieldAccessDescriptor, MAP_ARRAY_SCHEMA, outputSchema);
    Row expectedElement = Row.withSchema(expectedValueSchema).addValue("first").build();

    Row expectedRow =
        Row.withSchema(expectedSchema)
            .addValue(ImmutableMap.of(1, ImmutableList.of(expectedElement)))
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

    Row out = SelectHelpers.selectRow(r3, fieldAccessDescriptor, r3.getSchema(), outputSchema);

    assertEquals(outputSchema, f2);
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

    Row out = SelectHelpers.selectRow(r4, fieldAccessDescriptor, r4.getSchema(), outputSchema);

    assertEquals(f3, outputSchema);
    assertEquals(r3, out);
  }
}
