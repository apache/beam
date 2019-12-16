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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** test for {@link FieldTypeDescriptors}. */
public class FieldTypeDescriptorsTest {
  @Test
  public void testPrimitiveTypeToJavaType() {
    assertEquals(
        TypeDescriptors.bytes(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.BYTE));
    assertEquals(
        TypeDescriptors.shorts(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.INT16));
    assertEquals(
        TypeDescriptors.integers(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.INT32));
    assertEquals(
        TypeDescriptors.longs(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.INT64));
    assertEquals(
        TypeDescriptors.bigdecimals(),
        FieldTypeDescriptors.javaTypeForFieldType(FieldType.DECIMAL));
    assertEquals(
        TypeDescriptors.floats(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.FLOAT));
    assertEquals(
        TypeDescriptors.doubles(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.DOUBLE));
    assertEquals(
        TypeDescriptors.strings(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.STRING));
    assertEquals(
        TypeDescriptor.of(Instant.class),
        FieldTypeDescriptors.javaTypeForFieldType(FieldType.DATETIME));
    assertEquals(
        TypeDescriptors.booleans(), FieldTypeDescriptors.javaTypeForFieldType(FieldType.BOOLEAN));
    assertEquals(
        TypeDescriptor.of(byte[].class),
        FieldTypeDescriptors.javaTypeForFieldType(FieldType.BYTES));
  }

  @Test
  public void testRowTypeToJavaType() {
    assertEquals(
        TypeDescriptors.lists(TypeDescriptors.rows()),
        FieldTypeDescriptors.javaTypeForFieldType(
            FieldType.array(FieldType.row(Schema.builder().build()))));
  }

  @Test
  public void testArrayTypeToJavaType() {
    assertEquals(
        TypeDescriptors.lists(TypeDescriptors.longs()),
        FieldTypeDescriptors.javaTypeForFieldType(FieldType.array(FieldType.INT64)));
    assertEquals(
        TypeDescriptors.lists(TypeDescriptors.lists(TypeDescriptors.longs())),
        FieldTypeDescriptors.javaTypeForFieldType(
            FieldType.array(FieldType.array(FieldType.INT64))));
  }

  @Test
  public void testIterableTypeToJavaType() {
    assertEquals(
        TypeDescriptors.iterables(TypeDescriptors.longs()),
        FieldTypeDescriptors.javaTypeForFieldType(FieldType.iterable(FieldType.INT64)));
    assertEquals(
        TypeDescriptors.iterables(TypeDescriptors.iterables(TypeDescriptors.longs())),
        FieldTypeDescriptors.javaTypeForFieldType(
            FieldType.iterable(FieldType.iterable(FieldType.INT64))));
  }

  @Test
  public void testMapTypeToJavaType() {
    assertEquals(
        TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.longs()),
        FieldTypeDescriptors.javaTypeForFieldType(
            FieldType.map(FieldType.STRING, FieldType.INT64)));
    assertEquals(
        TypeDescriptors.maps(
            TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.longs())),
        FieldTypeDescriptors.javaTypeForFieldType(
            FieldType.map(FieldType.STRING, FieldType.array(FieldType.INT64))));
  }

  @Test
  public void testPrimitiveTypeToFieldType() {
    assertEquals(
        FieldType.BYTE, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.bytes()));
    assertEquals(
        FieldType.INT16, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.shorts()));
    assertEquals(
        FieldType.INT32, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.integers()));
    assertEquals(
        FieldType.INT64, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.longs()));
    assertEquals(
        FieldType.DECIMAL,
        FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.bigdecimals()));
    assertEquals(
        FieldType.FLOAT, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.floats()));
    assertEquals(
        FieldType.DOUBLE, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.doubles()));
    assertEquals(
        FieldType.STRING, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.strings()));
    assertEquals(
        FieldType.DATETIME,
        FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptor.of(Instant.class)));
    assertEquals(
        FieldType.BOOLEAN, FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.booleans()));
    assertEquals(
        FieldType.BYTES,
        FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptor.of(byte[].class)));
  }

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRowTypeToFieldType() {
    thrown.expect(IllegalArgumentException.class);
    FieldTypeDescriptors.fieldTypeForJavaType(TypeDescriptors.rows());
  }

  @Test
  public void testArrayTypeToFieldType() {
    assertEquals(
        FieldType.array(FieldType.STRING),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.lists(TypeDescriptors.strings())));
    assertEquals(
        FieldType.array(FieldType.array(FieldType.STRING)),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.lists(TypeDescriptors.lists(TypeDescriptors.strings()))));
    assertEquals(
        FieldType.array(FieldType.STRING),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptor.of(new ArrayList<String>() {}.getClass())));
  }

  @Test
  public void testIterableTypeToFieldType() {
    assertEquals(
        FieldType.iterable(FieldType.STRING),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.iterables(TypeDescriptors.strings())));
    assertEquals(
        FieldType.iterable(FieldType.iterable(FieldType.STRING)),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.iterables(TypeDescriptors.iterables(TypeDescriptors.strings()))));
  }

  @Test
  public void testMapTypeToFieldType() {
    assertEquals(
        FieldType.map(FieldType.STRING, FieldType.INT64),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.maps(TypeDescriptors.strings(), TypeDescriptors.longs())));
    assertEquals(
        FieldType.map(FieldType.STRING, FieldType.array(FieldType.INT64)),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.maps(
                TypeDescriptors.strings(), TypeDescriptors.lists(TypeDescriptors.longs()))));
    assertEquals(
        FieldType.map(FieldType.STRING, FieldType.iterable(FieldType.INT64)),
        FieldTypeDescriptors.fieldTypeForJavaType(
            TypeDescriptors.maps(
                TypeDescriptors.strings(), TypeDescriptors.iterables(TypeDescriptors.longs()))));
  }
}
