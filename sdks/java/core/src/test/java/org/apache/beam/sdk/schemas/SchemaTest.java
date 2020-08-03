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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link Schema}. */
public class SchemaTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreate() {
    Schema schema =
        Schema.builder()
            .addByteField("f_byte")
            .addInt16Field("f_int16")
            .addInt32Field("f_int32")
            .addInt64Field("f_int64")
            .addDecimalField("f_decimal")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_datetime")
            .addBooleanField("f_boolean")
            .build();
    assertEquals(10, schema.getFieldCount());

    assertEquals(0, schema.indexOf("f_byte"));
    assertEquals("f_byte", schema.getField(0).getName());
    assertEquals(FieldType.BYTE, schema.getField(0).getType());

    assertEquals(1, schema.indexOf("f_int16"));
    assertEquals("f_int16", schema.getField(1).getName());
    assertEquals(FieldType.INT16, schema.getField(1).getType());

    assertEquals(2, schema.indexOf("f_int32"));
    assertEquals("f_int32", schema.getField(2).getName());
    assertEquals(FieldType.INT32, schema.getField(2).getType());

    assertEquals(3, schema.indexOf("f_int64"));
    assertEquals("f_int64", schema.getField(3).getName());
    assertEquals(FieldType.INT64, schema.getField(3).getType());

    assertEquals(4, schema.indexOf("f_decimal"));
    assertEquals("f_decimal", schema.getField(4).getName());
    assertEquals(FieldType.DECIMAL, schema.getField(4).getType());

    assertEquals(5, schema.indexOf("f_float"));
    assertEquals("f_float", schema.getField(5).getName());
    assertEquals(FieldType.FLOAT, schema.getField(5).getType());

    assertEquals(6, schema.indexOf("f_double"));
    assertEquals("f_double", schema.getField(6).getName());
    assertEquals(FieldType.DOUBLE, schema.getField(6).getType());

    assertEquals(7, schema.indexOf("f_string"));
    assertEquals("f_string", schema.getField(7).getName());
    assertEquals(FieldType.STRING, schema.getField(7).getType());

    assertEquals(8, schema.indexOf("f_datetime"));
    assertEquals("f_datetime", schema.getField(8).getName());
    assertEquals(FieldType.DATETIME, schema.getField(8).getType());

    assertEquals(9, schema.indexOf("f_boolean"));
    assertEquals("f_boolean", schema.getField(9).getName());
    assertEquals(FieldType.BOOLEAN, schema.getField(9).getType());
  }

  @Test
  public void testNestedSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    Schema schema = Schema.of(Field.of("nested", FieldType.row(nestedSchema)));
    Field inner = schema.getField("nested").getType().getRowSchema().getField("f1_str");
    assertEquals("f1_str", inner.getName());
    assertEquals(FieldType.STRING, inner.getType());
  }

  @Test
  public void testArraySchema() {
    FieldType arrayType = FieldType.array(FieldType.STRING);
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testArrayOfRowSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    FieldType arrayType = FieldType.array(FieldType.row(nestedSchema));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testNestedArraySchema() {
    FieldType arrayType = FieldType.array(FieldType.array(FieldType.STRING));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testIterableSchema() {
    FieldType iterableType = FieldType.iterable(FieldType.STRING);
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testIterableOfRowSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    FieldType iterableType = FieldType.iterable(FieldType.row(nestedSchema));
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testNestedIterableSchema() {
    FieldType iterableType = FieldType.iterable(FieldType.iterable(FieldType.STRING));
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testWrongName() {
    Schema schema = Schema.of(Field.of("f_byte", FieldType.BYTE));
    thrown.expect(IllegalArgumentException.class);
    schema.getField("f_string");
  }

  @Test
  public void testWrongIndex() {
    Schema schema = Schema.of(Field.of("f_byte", FieldType.BYTE));
    thrown.expect(IndexOutOfBoundsException.class);
    schema.getField(1);
  }

  @Test
  public void testCollector() {
    Schema schema =
        Stream.of(
                Schema.Field.of("f_int", FieldType.INT32),
                Schema.Field.of("f_string", FieldType.STRING))
            .collect(toSchema());

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getField(0).getName());
    assertEquals(FieldType.INT32, schema.getField(0).getType());
    assertEquals("f_string", schema.getField(1).getName());
    assertEquals(FieldType.STRING, schema.getField(1).getType());
  }

  @Test
  public void testEquivalent() {
    final Schema expectedNested1 =
        Schema.builder().addStringField("yard1").addInt64Field("yard2").build();

    final Schema expectedSchema1 =
        Schema.builder()
            .addStringField("field1")
            .addInt64Field("field2")
            .addRowField("field3", expectedNested1)
            .addArrayField("field4", FieldType.row(expectedNested1))
            .addMapField("field5", FieldType.STRING, FieldType.row(expectedNested1))
            .build();

    final Schema expectedNested2 =
        Schema.builder().addInt64Field("yard2").addStringField("yard1").build();

    final Schema expectedSchema2 =
        Schema.builder()
            .addMapField("field5", FieldType.STRING, FieldType.row(expectedNested2))
            .addArrayField("field4", FieldType.row(expectedNested2))
            .addRowField("field3", expectedNested2)
            .addInt64Field("field2")
            .addStringField("field1")
            .build();

    assertNotEquals(expectedSchema1, expectedSchema2);
    assertTrue(expectedSchema1.equivalent(expectedSchema2));
  }

  @Test
  public void testPrimitiveNotEquivalent() {
    Schema schema1 = Schema.builder().addInt64Field("foo").build();
    Schema schema2 = Schema.builder().addStringField("foo").build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));

    schema1 = Schema.builder().addInt64Field("foo").build();
    schema2 = Schema.builder().addInt64Field("bar").build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));

    schema1 = Schema.builder().addInt64Field("foo").build();
    schema2 = Schema.builder().addNullableField("foo", FieldType.INT64).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testFieldsWithDifferentMetadataAreEquivalent() {
    Field foo = Field.of("foo", FieldType.STRING);
    Field fooWithMetadata = Field.of("foo", FieldType.STRING.withMetadata("key", "value"));

    Schema schema1 = Schema.builder().addField(foo).build();
    Schema schema2 = Schema.builder().addField(foo).build();
    assertEquals(schema1, schema2);
    assertTrue(schema1.equivalent(schema2));

    schema1 = Schema.builder().addField(foo).build();
    schema2 = Schema.builder().addField(fooWithMetadata).build();
    assertNotEquals(schema1, schema2);
    assertTrue(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 = Schema.builder().addRowField("foo", nestedSchema1).build();
    Schema schema2 = Schema.builder().addRowField("foo", nestedSchema2).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testArrayNotEquivalent() {
    Schema schema1 = Schema.builder().addArrayField("foo", FieldType.BOOLEAN).build();
    Schema schema2 = Schema.builder().addArrayField("foo", FieldType.DATETIME).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedArraysNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 = Schema.builder().addArrayField("foo", FieldType.row(nestedSchema1)).build();
    Schema schema2 = Schema.builder().addArrayField("foo", FieldType.row(nestedSchema2)).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testMapNotEquivalent() {
    Schema schema1 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.BOOLEAN).build();
    Schema schema2 =
        Schema.builder().addMapField("foo", FieldType.DATETIME, FieldType.BOOLEAN).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));

    schema1 = Schema.builder().addMapField("foo", FieldType.STRING, FieldType.BOOLEAN).build();
    schema2 = Schema.builder().addMapField("foo", FieldType.STRING, FieldType.STRING).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedMapsNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.row(nestedSchema1)).build();
    Schema schema2 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.row(nestedSchema2)).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  static class TestType extends PassThroughLogicalType<Long> {
    TestType(String id, String arg) {
      super(id, FieldType.STRING, arg, FieldType.INT64);
    }
  }

  @Test
  public void testLogicalType() {
    Schema schema1 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg")).build();
    Schema schema2 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg")).build();
    assertEquals(schema1, schema2); // Logical types are the same.

    Schema schema3 =
        Schema.builder()
            .addNullableField("logical", Schema.FieldType.logicalType(new TestType("id", "arg")))
            .build();
    assertNotEquals(schema1, schema3); // schema1 and schema3 differ in Nullability

    Schema schema4 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id2", "arg")).build();
    assertNotEquals(schema1, schema4); // Logical type id is different.

    Schema schema5 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg2")).build();
    assertNotEquals(schema1, schema5); // Logical type arg is different.
  }

  @Test
  public void testTypesEquality() {
    Schema schema1 = Schema.builder().addStringField("foo").build();
    Schema schema2 = Schema.builder().addStringField("bar").build();
    assertTrue(schema1.typesEqual(schema2)); // schema1 and schema2 only differ by names

    Schema schema3 = Schema.builder().addNullableField("foo", FieldType.STRING).build();
    assertFalse(schema1.typesEqual(schema3)); // schema1 and schema3 differ in Nullability

    Schema schema4 = Schema.builder().addInt32Field("foo").build();
    assertFalse(schema1.typesEqual(schema4)); // schema1 and schema4 differ by types
  }

  @Test
  public void testIllegalIndexOf() {
    Schema schema = Schema.builder().addStringField("foo").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot find field bar in schema " + schema);

    schema.indexOf("bar");
  }

  @Test
  public void testIllegalNameOf() {
    Schema schema = Schema.builder().addStringField("foo").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot find field 1");

    schema.nameOf(1);
  }
}
