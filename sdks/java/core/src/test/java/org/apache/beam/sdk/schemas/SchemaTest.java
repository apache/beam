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
import org.apache.beam.sdk.schemas.Schema.TypeName;
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
    Schema schema = Schema.builder()
        .addByteField("f_byte", false)
        .addInt16Field("f_int16", false)
        .addInt32Field("f_int32", false)
        .addInt64Field("f_int64", false)
        .addDecimalField("f_decimal", false)
        .addFloatField("f_float", false)
        .addDoubleField("f_double", false)
        .addStringField("f_string", false)
        .addDateTimeField("f_datetime", false)
        .addBooleanField("f_boolean", false).build();
    assertEquals(10, schema.getFieldCount());

    assertEquals(0, schema.indexOf("f_byte"));
    assertEquals("f_byte", schema.getField(0).getName());
    assertEquals(TypeName.BYTE.type(), schema.getField(0).getType());

    assertEquals(1, schema.indexOf("f_int16"));
    assertEquals("f_int16", schema.getField(1).getName());
    assertEquals(TypeName.INT16.type(), schema.getField(1).getType());

    assertEquals(2, schema.indexOf("f_int32"));
    assertEquals("f_int32", schema.getField(2).getName());
    assertEquals(TypeName.INT32.type(), schema.getField(2).getType());

    assertEquals(3, schema.indexOf("f_int64"));
    assertEquals("f_int64", schema.getField(3).getName());
    assertEquals(TypeName.INT64.type(), schema.getField(3).getType());

    assertEquals(4, schema.indexOf("f_decimal"));
    assertEquals("f_decimal", schema.getField(4).getName());
    assertEquals(TypeName.DECIMAL.type(),
        schema.getField(4).getType());

    assertEquals(5, schema.indexOf("f_float"));
    assertEquals("f_float", schema.getField(5).getName());
    assertEquals(TypeName.FLOAT.type(), schema.getField(5).getType());

    assertEquals(6, schema.indexOf("f_double"));
    assertEquals("f_double", schema.getField(6).getName());
    assertEquals(TypeName.DOUBLE.type(), schema.getField(6).getType());

    assertEquals(7, schema.indexOf("f_string"));
    assertEquals("f_string", schema.getField(7).getName());
    assertEquals(TypeName.STRING.type(), schema.getField(7).getType());

    assertEquals(8, schema.indexOf("f_datetime"));
    assertEquals("f_datetime", schema.getField(8).getName());
    assertEquals(TypeName.DATETIME.type(),
        schema.getField(8).getType());

    assertEquals(9, schema.indexOf("f_boolean"));
    assertEquals("f_boolean", schema.getField(9).getName());
    assertEquals(TypeName.BOOLEAN.type(), schema.getField(9).getType());
  }

  @Test
  public void testNestedSchema() {
    Schema nestedSchema = Schema.of(
        Field.of("f1_str", TypeName.STRING.type()));
    Schema schema = Schema.of(
        Field.of("nested", TypeName.ROW.type().withRowSchema(nestedSchema)));
    Field inner = schema.getField("nested").getType().getRowSchema().getField("f1_str");
    assertEquals("f1_str", inner.getName());
    assertEquals(TypeName.STRING, inner.getType().getTypeName());
  }

  @Test
  public void testArraySchema() {
    FieldType arrayType = TypeName.ARRAY.type()
        .withCollectionType(TypeName.STRING.type());
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testArrayOfRowSchema() {
    Schema nestedSchema = Schema.of(
        Field.of("f1_str", TypeName.STRING.type()));
    FieldType arrayType = TypeName.ARRAY.type()
        .withCollectionType(TypeName.ROW.type()
            .withRowSchema(nestedSchema));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testNestedArraySchema() {
    FieldType arrayType = TypeName.ARRAY.type()
        .withCollectionType(TypeName.ARRAY.type()
            .withCollectionType(TypeName.STRING.type()));
    Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
  }

  @Test
  public void testWrongName() {
    Schema schema = Schema.of(Field.of("f_byte", TypeName.BYTE.type()));
    thrown.expect(IllegalArgumentException.class);
    schema.getField("f_string");
  }

  @Test
  public void testWrongIndex() {
    Schema schema = Schema.of(
        Field.of("f_byte", TypeName.BYTE.type()));
    thrown.expect(IndexOutOfBoundsException.class);
    schema.getField(1);
  }



  @Test
  public void testCollector() {
    Schema schema =
        Stream
            .of(
                Schema.Field.of("f_int", TypeName.INT32.type()),
                Schema.Field.of("f_string", TypeName.STRING.type()))
            .collect(toSchema());

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getField(0).getName());
    assertEquals(TypeName.INT32, schema.getField(0).getType().getTypeName());
    assertEquals("f_string", schema.getField(1).getName());
    assertEquals(TypeName.STRING, schema.getField(1).getType().getTypeName());
  }
}
