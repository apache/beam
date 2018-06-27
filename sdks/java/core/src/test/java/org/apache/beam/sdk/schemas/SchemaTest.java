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
}
