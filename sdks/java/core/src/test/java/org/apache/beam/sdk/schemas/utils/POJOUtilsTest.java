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

import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_ARRAY_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_COLLECTION_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_MAP_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.NESTED_POJO_WITH_SIMPLE_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.POJO_WITH_BOXED_FIELDS_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.POJO_WITH_BYTE_ARRAY_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.PRIMITIVE_ARRAY_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.PRIMITIVE_MAP_POJO_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestPOJOs.SIMPLE_POJO_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;
import org.apache.beam.sdk.schemas.JavaFieldSchema.JavaFieldTypeSupplier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedCollectionPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedMapPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.NestedPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.POJOWithBoxedFields;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.POJOWithByteArray;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.POJOWithNullables;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.PrimitiveArrayPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.PrimitiveMapPOJO;
import org.apache.beam.sdk.schemas.utils.TestPOJOs.SimplePOJO;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for the {@link POJOUtils} class. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class POJOUtilsTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final Instant INSTANT = DateTime.parse("1979-03-15").toInstant();
  static final byte[] BYTE_ARRAY = "byteArray".getBytes(StandardCharsets.UTF_8);
  static final ByteBuffer BYTE_BUFFER =
      ByteBuffer.wrap("byteBuffer".getBytes(StandardCharsets.UTF_8));

  @Test
  public void testNullables() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<POJOWithNullables>() {}, JavaFieldTypeSupplier.INSTANCE);
    assertTrue(schema.getField("str").getType().getNullable());
    assertFalse(schema.getField("anInt").getType().getNullable());
  }

  @Test
  public void testSimplePOJO() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<SimplePOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    assertEquals(SIMPLE_POJO_SCHEMA, schema);
  }

  @Test
  public void testNestedPOJO() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<NestedPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_POJO_SCHEMA, schema);
  }

  @Test
  public void testNestedPOJOWithSimplePOJO() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<TestPOJOs.NestedPOJOWithSimplePOJO>() {},
            JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_POJO_WITH_SIMPLE_POJO_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveArray() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<PrimitiveArrayPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_ARRAY_POJO_SCHEMA, schema);
  }

  @Test
  public void testNestedArray() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<NestedArrayPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_ARRAY_POJO_SCHEMA, schema);
  }

  @Test
  public void testNestedCollection() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<NestedCollectionPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_COLLECTION_POJO_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveMap() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<PrimitiveMapPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_MAP_POJO_SCHEMA, schema);
  }

  @Test
  public void testNestedMap() {
    Schema schema =
        POJOUtils.schemaFromPojoClass(
            new TypeDescriptor<NestedMapPOJO>() {}, JavaFieldTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_MAP_POJO_SCHEMA, schema);
  }

  @Test
  public void testGeneratedSimpleGetters() {
    SimplePOJO simplePojo =
        new SimplePOJO(
            "field1",
            (byte) 41,
            (short) 42,
            43,
            44L,
            true,
            DATE,
            INSTANT,
            BYTE_ARRAY,
            BYTE_BUFFER,
            new BigDecimal(42),
            new StringBuilder("stringBuilder"));

    List<FieldValueGetter> getters =
        POJOUtils.getGetters(
            new TypeDescriptor<SimplePOJO>() {},
            SIMPLE_POJO_SCHEMA,
            JavaFieldTypeSupplier.INSTANCE,
            new DefaultTypeConversionsFactory());
    assertEquals(12, getters.size());
    assertEquals("str", getters.get(0).name());
    assertEquals("field1", getters.get(0).get(simplePojo));
    assertEquals((byte) 41, getters.get(1).get(simplePojo));
    assertEquals((short) 42, getters.get(2).get(simplePojo));
    assertEquals((int) 43, getters.get(3).get(simplePojo));
    assertEquals((long) 44, getters.get(4).get(simplePojo));
    assertTrue((Boolean) getters.get(5).get(simplePojo));
    assertEquals(DATE.toInstant(), getters.get(6).get(simplePojo));
    assertEquals(INSTANT, getters.get(7).get(simplePojo));
    assertArrayEquals("Unexpected bytes", BYTE_ARRAY, (byte[]) getters.get(8).get(simplePojo));
    assertArrayEquals(
        "Unexpected bytes", BYTE_BUFFER.array(), (byte[]) getters.get(9).get(simplePojo));
    assertEquals(new BigDecimal(42), getters.get(10).get(simplePojo));
    assertEquals("stringBuilder", getters.get(11).get(simplePojo));
  }

  @Test
  public void testGeneratedSimpleSetters() {
    SimplePOJO simplePojo = new SimplePOJO();
    List<FieldValueSetter> setters =
        POJOUtils.getSetters(
            new TypeDescriptor<SimplePOJO>() {},
            SIMPLE_POJO_SCHEMA,
            JavaFieldTypeSupplier.INSTANCE,
            new DefaultTypeConversionsFactory());
    assertEquals(12, setters.size());

    setters.get(0).set(simplePojo, "field1");
    setters.get(1).set(simplePojo, (byte) 41);
    setters.get(2).set(simplePojo, (short) 42);
    setters.get(3).set(simplePojo, (int) 43);
    setters.get(4).set(simplePojo, (long) 44);
    setters.get(5).set(simplePojo, true);
    setters.get(6).set(simplePojo, DATE.toInstant());
    setters.get(7).set(simplePojo, INSTANT);
    setters.get(8).set(simplePojo, BYTE_ARRAY);
    setters.get(9).set(simplePojo, BYTE_BUFFER.array());
    setters.get(10).set(simplePojo, new BigDecimal(42));
    setters.get(11).set(simplePojo, "stringBuilder");

    assertEquals("field1", simplePojo.str);
    assertEquals((byte) 41, simplePojo.aByte);
    assertEquals((short) 42, simplePojo.aShort);
    assertEquals((int) 43, simplePojo.anInt);
    assertEquals((long) 44, simplePojo.aLong);
    assertTrue(simplePojo.aBoolean);
    assertEquals(DATE, simplePojo.dateTime);
    assertEquals(INSTANT, simplePojo.instant);
    assertArrayEquals("Unexpected bytes", BYTE_ARRAY, simplePojo.bytes);
    assertEquals(BYTE_BUFFER, simplePojo.byteBuffer);
    assertEquals(new BigDecimal(42), simplePojo.bigDecimal);
    assertEquals("stringBuilder", simplePojo.stringBuilder.toString());
  }

  @Test
  public void testGeneratedSimpleBoxedGetters() {
    POJOWithBoxedFields pojo = new POJOWithBoxedFields((byte) 41, (short) 42, 43, 44L, true);

    List<FieldValueGetter> getters =
        POJOUtils.getGetters(
            new TypeDescriptor<POJOWithBoxedFields>() {},
            POJO_WITH_BOXED_FIELDS_SCHEMA,
            JavaFieldTypeSupplier.INSTANCE,
            new DefaultTypeConversionsFactory());
    assertEquals((byte) 41, getters.get(0).get(pojo));
    assertEquals((short) 42, getters.get(1).get(pojo));
    assertEquals((int) 43, getters.get(2).get(pojo));
    assertEquals((long) 44, getters.get(3).get(pojo));
    assertTrue((Boolean) getters.get(4).get(pojo));
  }

  @Test
  public void testGeneratedSimpleBoxedSetters() {
    POJOWithBoxedFields pojo = new POJOWithBoxedFields();
    List<FieldValueSetter> setters =
        POJOUtils.getSetters(
            new TypeDescriptor<POJOWithBoxedFields>() {},
            POJO_WITH_BOXED_FIELDS_SCHEMA,
            JavaFieldTypeSupplier.INSTANCE,
            new DefaultTypeConversionsFactory());

    setters.get(0).set(pojo, (byte) 41);
    setters.get(1).set(pojo, (short) 42);
    setters.get(2).set(pojo, (int) 43);
    setters.get(3).set(pojo, (long) 44);
    setters.get(4).set(pojo, true);

    assertEquals((byte) 41, pojo.aByte.byteValue());
    assertEquals((short) 42, pojo.aShort.shortValue());
    assertEquals((int) 43, pojo.anInt.intValue());
    assertEquals((long) 44, pojo.aLong.longValue());
    assertTrue(pojo.aBoolean.booleanValue());
  }

  @Test
  public void testGeneratedByteBufferSetters() {
    POJOWithByteArray pojo = new POJOWithByteArray();
    List<FieldValueSetter> setters =
        POJOUtils.getSetters(
            new TypeDescriptor<POJOWithByteArray>() {},
            POJO_WITH_BYTE_ARRAY_SCHEMA,
            JavaFieldTypeSupplier.INSTANCE,
            new DefaultTypeConversionsFactory());
    setters.get(0).set(pojo, BYTE_ARRAY);
    setters.get(1).set(pojo, BYTE_BUFFER.array());

    assertArrayEquals("not equal", BYTE_ARRAY, pojo.bytes1);
    assertEquals(BYTE_BUFFER, pojo.bytes2);
  }
}
