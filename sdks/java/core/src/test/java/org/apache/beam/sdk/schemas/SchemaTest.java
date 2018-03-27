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

import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link Schema}.
 */
public class SchemaTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreate() {
    Schema schema = Schema.of(
        Field.of("f_byte", FieldTypeDescriptor.of(FieldType.BYTE)),
        Field.of("f_int16", FieldTypeDescriptor.of(FieldType.INT16)),
        Field.of("f_int32", FieldTypeDescriptor.of(FieldType.INT32)),
        Field.of("f_int64", FieldTypeDescriptor.of(FieldType.INT64)),
        Field.of("f_decimal", FieldTypeDescriptor.of(FieldType.DECIMAL)),
        Field.of("f_float", FieldTypeDescriptor.of(FieldType.FLOAT)),
        Field.of("f_double", FieldTypeDescriptor.of(FieldType.DOUBLE)),
        Field.of("f_string", FieldTypeDescriptor.of(FieldType.STRING)),
        Field.of("f_datetime", FieldTypeDescriptor.of(FieldType.DATETIME)),
        Field.of("f_boolean", FieldTypeDescriptor.of(FieldType.BOOLEAN)));
    assertEquals(10, schema.getFieldCount());

    assertEquals(0, schema.indexOf("f_byte"));
    assertEquals("f_byte", schema.getField(0).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.BYTE), schema.getField(0).getTypeDescriptor());

    assertEquals(1, schema.indexOf("f_int16"));
    assertEquals("f_int16", schema.getField(1).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.INT16), schema.getField(1).getTypeDescriptor());

    assertEquals(2, schema.indexOf("f_int32"));
    assertEquals("f_int32", schema.getField(2).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.INT32), schema.getField(2).getTypeDescriptor());

    assertEquals(3, schema.indexOf("f_int64"));
    assertEquals("f_int64", schema.getField(3).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.INT64), schema.getField(3).getTypeDescriptor());

    assertEquals(4, schema.indexOf("f_decimal"));
    assertEquals("f_decimal", schema.getField(4).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.DECIMAL),
        schema.getField(4).getTypeDescriptor());

    assertEquals(5, schema.indexOf("f_float"));
    assertEquals("f_float", schema.getField(5).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.FLOAT), schema.getField(5).getTypeDescriptor());

    assertEquals(6, schema.indexOf("f_double"));
    assertEquals("f_double", schema.getField(6).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.DOUBLE), schema.getField(6).getTypeDescriptor());

    assertEquals(7, schema.indexOf("f_string"));
    assertEquals("f_string", schema.getField(7).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.STRING), schema.getField(7).getTypeDescriptor());

    assertEquals(8, schema.indexOf("f_datetime"));
    assertEquals("f_datetime", schema.getField(8).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.DATETIME),
        schema.getField(8).getTypeDescriptor());

    assertEquals(9, schema.indexOf("f_boolean"));
    assertEquals("f_boolean", schema.getField(9).getName());
    assertEquals(FieldTypeDescriptor.of(FieldType.BOOLEAN), schema.getField(9).getTypeDescriptor());
  }

  @Test
  public void testNestedSchema() {
    Schema nestedSchema = Schema.of(
        Field.of("f1_str", FieldTypeDescriptor.of(FieldType.STRING)));
    Schema schema = Schema.of(
        Field.of("nested", FieldTypeDescriptor.of(FieldType.ROW).withRowSchema(nestedSchema)));
    Field inner = schema.getField("nested").getTypeDescriptor().getRowSchema().getField("f1_str");
    assertEquals("f1_str", inner.getName());
    assertEquals(FieldType.STRING, inner.getTypeDescriptor().getType());
  }

  @Test
  public void testArraySchema() {
    FieldTypeDescriptor arrayType = FieldTypeDescriptor.of(FieldType.ARRAY)
        .withComponentType(FieldTypeDescriptor.of(FieldType.STRING));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getTypeDescriptor());
  }

  @Test
  public void testArrayOfRowSchema() {
    Schema nestedSchema = Schema.of(
        Field.of("f1_str", FieldTypeDescriptor.of(FieldType.STRING)));
    FieldTypeDescriptor arrayType = FieldTypeDescriptor.of(FieldType.ARRAY)
        .withComponentType(FieldTypeDescriptor.of(FieldType.ROW)
            .withRowSchema(nestedSchema));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getTypeDescriptor());
  }

  @Test
  public void testNestedArraySchema() {
    FieldTypeDescriptor arrayType = FieldTypeDescriptor.of(FieldType.ARRAY)
        .withComponentType(FieldTypeDescriptor.of(FieldType.ARRAY)
            .withComponentType(FieldTypeDescriptor.of(FieldType.STRING)));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getTypeDescriptor());
  }

  @Test
  public void testWrongName() {
    Schema schema = Schema.of(Field.of("f_byte", FieldTypeDescriptor.of(FieldType.BYTE)));
    thrown.expect(IllegalArgumentException.class);
    schema.getField("f_string");
  }

  @Test
  public void testWrongIndex() {
    Schema schema = Schema.of(
        Field.of("f_byte", FieldTypeDescriptor.of(FieldType.BYTE)));
    thrown.expect(IndexOutOfBoundsException.class);
    schema.getField(1);
  }



  @Test
  public void testCollector() {
    Schema schema =
        Stream
            .of(
                Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32)),
                Schema.Field.of("f_string", FieldTypeDescriptor.of(FieldType.STRING)))
            .collect(toSchema());

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getField(0).getName());
    assertEquals(FieldType.INT32, schema.getField(0).getTypeDescriptor().getType());
    assertEquals("f_string", schema.getField(1).getName());
    assertEquals(FieldType.STRING, schema.getField(1).getTypeDescriptor().getType());
  }
}
