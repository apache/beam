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

import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.ITERABLE_BEAM_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_ARRAYS_BEAM_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_MAP_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.PRIMITIVE_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.SIMPLE_BEAN_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.ArrayOfByteArray;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.IterableBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.MismatchingNullableBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedArraysBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedMapBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.PrimitiveArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.SimpleBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.SimpleBeanWithAnnotations;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for the {@link JavaBeanSchema} schema provider. */
public class JavaBeanSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());

  private SimpleBean createSimple(String name) {
    return new SimpleBean(
        name,
        (byte) 1,
        (short) 2,
        3,
        4L,
        true,
        DATE,
        DATE.toInstant(),
        BYTE_ARRAY,
        BigDecimal.ONE,
        new StringBuilder(name).append("builder"));
  }

  private SimpleBeanWithAnnotations createAnnotated(String name) {
    return new SimpleBeanWithAnnotations(
        name,
        (byte) 1,
        (short) 2,
        3,
        4L,
        true,
        DATE,
        DATE.toInstant(),
        BYTE_ARRAY,
        BigDecimal.ONE,
        new StringBuilder(name).append("builder"));
  }

  private Row createSimpleRow(String name) {
    return Row.withSchema(SIMPLE_BEAN_SCHEMA)
        .addValues(
            name,
            (byte) 1,
            (short) 2,
            3,
            4L,
            true,
            DATE,
            DATE,
            BYTE_ARRAY,
            BYTE_ARRAY,
            BigDecimal.ONE,
            new StringBuilder(name).append("builder").toString())
        .build();
  }

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimpleBean.class);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_BEAN_SCHEMA, schema);
  }

  @Test
  public void testToRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleBean bean = createSimple("string");
    Row row = registry.getToRowFunction(SimpleBean.class).apply(bean);

    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertTrue(row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), row.getDateTime("instant"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));
  }

  @Test
  public void testFromRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row = createSimpleRow("string");

    SimpleBean bean = registry.getFromRowFunction(SimpleBean.class).apply(row);
    assertEquals("string", bean.getStr());
    assertEquals((byte) 1, bean.getaByte());
    assertEquals((short) 2, bean.getaShort());
    assertEquals((int) 3, bean.getAnInt());
    assertEquals((long) 4, bean.getaLong());
    assertTrue(bean.isaBoolean());
    assertEquals(DATE, bean.getDateTime());
    assertEquals(DATE.toInstant(), bean.getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getByteBuffer().array());
    assertEquals(BigDecimal.ONE, bean.getBigDecimal());
    assertEquals("stringbuilder", bean.getStringBuilder().toString());
  }

  @Test
  public void testToRowSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(registry.getToRowFunction(SimpleBean.class));
  }

  @Test
  public void testFromRowSerializable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SerializableUtils.ensureSerializableRoundTrip(registry.getFromRowFunction(SimpleBean.class));
  }

  @Test
  public void testFromRowWithGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleBean bean = createSimple("string");
    Row row = registry.getToRowFunction(SimpleBean.class).apply(bean);
    // Test that the fromRowFunction simply returns the original object back.
    SimpleBean extracted = registry.getFromRowFunction(SimpleBean.class).apply(row);
    assertSame(bean, extracted);
  }

  @Test
  public void testRecursiveGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_BEAN_SCHEMA, registry.getSchema(NestedBean.class));

    NestedBean bean = new NestedBean(createSimple("string"));
    Row row = registry.getToRowFunction(NestedBean.class).apply(bean);

    Row nestedRow = row.getRow("nested");
    assertEquals("string", nestedRow.getString("str"));
    assertEquals((byte) 1, (Object) nestedRow.getByte("aByte"));
    assertEquals((short) 2, (Object) nestedRow.getInt16("aShort"));
    assertEquals((int) 3, (Object) nestedRow.getInt32("anInt"));
    assertEquals((long) 4, (Object) nestedRow.getInt64("aLong"));
    assertTrue(nestedRow.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), nestedRow.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), nestedRow.getDateTime("instant"));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes("bytes"));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, nestedRow.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", nestedRow.getString("stringBuilder"));
  }

  @Test
  public void testRecursiveSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row nestedRow = createSimpleRow("string");

    Row row = Row.withSchema(NESTED_BEAN_SCHEMA).addValue(nestedRow).build();
    NestedBean bean = registry.getFromRowFunction(NestedBean.class).apply(row);
    assertEquals("string", bean.getNested().getStr());
    assertEquals((byte) 1, bean.getNested().getaByte());
    assertEquals((short) 2, bean.getNested().getaShort());
    assertEquals((int) 3, bean.getNested().getAnInt());
    assertEquals((long) 4, bean.getNested().getaLong());
    assertTrue(bean.getNested().isaBoolean());
    assertEquals(DATE, bean.getNested().getDateTime());
    assertEquals(DATE.toInstant(), bean.getNested().getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getNested().getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getNested().getByteBuffer().array());
    assertEquals(BigDecimal.ONE, bean.getNested().getBigDecimal());
    assertEquals("stringbuilder", bean.getNested().getStringBuilder().toString());
  }

  @Test
  public void testPrimitiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        PRIMITIVE_ARRAY_BEAN_SCHEMA, registry.getSchema(PrimitiveArrayBean.class));

    List<String> strList = ImmutableList.of("a", "b", "c");
    int[] intArray = {1, 2, 3, 4};
    Long[] longArray = {42L, 43L, 44L};
    PrimitiveArrayBean bean = new PrimitiveArrayBean(strList, intArray, longArray);
    Row row = registry.getToRowFunction(PrimitiveArrayBean.class).apply(bean);
    assertEquals(strList, row.getArray("strings"));
    assertEquals(Ints.asList(intArray), row.getArray("integers"));
    assertEquals(Arrays.asList(longArray), row.getArray("longs"));

    // Ensure that list caching works.
    assertSame(row.getArray("strings"), row.getArray("strings"));
    assertSame(row.getArray("integers"), row.getArray("integers"));
    assertSame(row.getArray("longs"), row.getArray("longs"));
  }

  @Test
  public void testPrimitiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Row row =
        Row.withSchema(PRIMITIVE_ARRAY_BEAN_SCHEMA)
            .addArray("a", "b", "c", "d")
            .addArray(1, 2, 3, 4)
            .addArray(42L, 43L, 44L, 45L)
            .build();
    PrimitiveArrayBean bean = registry.getFromRowFunction(PrimitiveArrayBean.class).apply(row);
    assertEquals(row.getArray("strings"), bean.getStrings());
    assertEquals(row.getArray("integers"), Ints.asList(bean.getIntegers()));
    assertEquals(row.getArray("longs"), Arrays.asList(bean.getLongs()));
  }

  @Test
  public void testRecursiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_ARRAY_BEAN_SCHEMA, registry.getSchema(NestedArrayBean.class));

    SimpleBean simple1 = createSimple("string1");
    SimpleBean simple2 = createSimple("string2");
    SimpleBean simple3 = createSimple("string3");

    NestedArrayBean bean = new NestedArrayBean(simple1, simple2, simple3);
    Row row = registry.getToRowFunction(NestedArrayBean.class).apply(bean);
    List<Row> rows = (List) row.getArray("beans");
    assertSame(simple1, registry.getFromRowFunction(SimpleBean.class).apply(rows.get(0)));
    assertSame(simple2, registry.getFromRowFunction(SimpleBean.class).apply(rows.get(1)));
    assertSame(simple3, registry.getFromRowFunction(SimpleBean.class).apply(rows.get(2)));
  }

  @Test
  public void testRecursiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");

    Row row = Row.withSchema(NESTED_ARRAY_BEAN_SCHEMA).addArray(row1, row2, row3).build();
    NestedArrayBean bean = registry.getFromRowFunction(NestedArrayBean.class).apply(row);
    assertEquals(3, bean.getBeans().length);
    assertEquals("string1", bean.getBeans()[0].getStr());
    assertEquals("string2", bean.getBeans()[1].getStr());
    assertEquals("string3", bean.getBeans()[2].getStr());
  }

  @Test
  public void testNestedArraysGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_ARRAYS_BEAM_SCHEMA, registry.getSchema(NestedArraysBean.class));

    List<List<String>> listOfLists =
        Lists.newArrayList(
            Lists.newArrayList("a", "b", "c"),
            Lists.newArrayList("d", "e", "f"),
            Lists.newArrayList("g", "h", "i"));
    NestedArraysBean bean = new NestedArraysBean(listOfLists);
    Row row = registry.getToRowFunction(NestedArraysBean.class).apply(bean);
    assertEquals(listOfLists, row.getArray("lists"));
  }

  @Test
  public void testNestedArraysSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    List<List<String>> listOfLists =
        Lists.newArrayList(
            Lists.newArrayList("a", "b", "c"),
            Lists.newArrayList("d", "e", "f"),
            Lists.newArrayList("g", "h", "i"));
    Row row = Row.withSchema(NESTED_ARRAYS_BEAM_SCHEMA).addArray(listOfLists).build();
    NestedArraysBean bean = registry.getFromRowFunction(NestedArraysBean.class).apply(row);
    assertEquals(listOfLists, bean.getLists());
  }

  @Test
  public void testMapFieldGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SchemaTestUtils.assertSchemaEquivalent(
        NESTED_MAP_BEAN_SCHEMA, registry.getSchema(NestedMapBean.class));

    SimpleBean simple1 = createSimple("string1");
    SimpleBean simple2 = createSimple("string2");
    SimpleBean simple3 = createSimple("string3");

    NestedMapBean bean =
        new NestedMapBean(
            ImmutableMap.of(
                "simple1", simple1,
                "simple2", simple2,
                "simple3", simple3));
    Row row = registry.getToRowFunction(NestedMapBean.class).apply(bean);
    Map<String, Row> extractedMap = row.getMap("map");
    assertEquals(3, extractedMap.size());
    assertEquals("string1", extractedMap.get("simple1").getString("str"));
    assertEquals("string2", extractedMap.get("simple2").getString("str"));
    assertEquals("string3", extractedMap.get("simple3").getString("str"));
  }

  @Test
  public void testMapFieldSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");
    Row row =
        Row.withSchema(NESTED_MAP_BEAN_SCHEMA)
            .addValue(
                ImmutableMap.of(
                    "simple1", row1,
                    "simple2", row2,
                    "simple3", row3))
            .build();
    NestedMapBean bean = registry.getFromRowFunction(NestedMapBean.class).apply(row);
    assertEquals(3, bean.getMap().size());
    assertEquals("string1", bean.getMap().get("simple1").getStr());
    assertEquals("string2", bean.getMap().get("simple2").getStr());
    assertEquals("string3", bean.getMap().get("simple3").getStr());
  }

  @Test
  public void testAnnotations() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimpleBeanWithAnnotations.class);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_BEAN_SCHEMA, schema);

    SimpleBeanWithAnnotations pojo = createAnnotated("string");
    Row row = registry.getToRowFunction(SimpleBeanWithAnnotations.class).apply(pojo);
    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString("str"));
    assertEquals((byte) 1, (Object) row.getByte("aByte"));
    assertEquals((short) 2, (Object) row.getInt16("aShort"));
    assertEquals((int) 3, (Object) row.getInt32("anInt"));
    assertEquals((long) 4, (Object) row.getInt64("aLong"));
    assertTrue(row.getBoolean("aBoolean"));
    assertEquals(DATE.toInstant(), row.getDateTime("dateTime"));
    assertEquals(DATE.toInstant(), row.getDateTime("instant"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("bytes"));
    assertArrayEquals(BYTE_ARRAY, row.getBytes("byteBuffer"));
    assertEquals(BigDecimal.ONE, row.getDecimal("bigDecimal"));
    assertEquals("stringbuilder", row.getString("stringBuilder"));

    SimpleBeanWithAnnotations pojo2 =
        registry
            .getFromRowFunction(SimpleBeanWithAnnotations.class)
            .apply(createSimpleRow("string"));
    assertEquals(pojo, pojo2);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testMismatchingNullable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    thrown.expect(RuntimeException.class);
    Schema schema = registry.getSchema(MismatchingNullableBean.class);
  }

  @Test
  public void testFromRowIterable() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(IterableBean.class);
    SchemaTestUtils.assertSchemaEquivalent(ITERABLE_BEAM_SCHEMA, schema);

    List<String> list = Lists.newArrayList("one", "two");
    Row iterableRow = Row.withSchema(ITERABLE_BEAM_SCHEMA).addIterable(list).build();
    IterableBean converted = registry.getFromRowFunction(IterableBean.class).apply(iterableRow);
    assertEquals(list, Lists.newArrayList(converted.getStrings()));

    // Make sure that the captured Iterable is backed by the previous one.
    list.add("three");
    assertEquals(list, Lists.newArrayList(converted.getStrings()));
  }

  @Test
  public void testToRowArrayOfBytes() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(ArrayOfByteArray.class);
    SchemaTestUtils.assertSchemaEquivalent(ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA, schema);

    ArrayOfByteArray arrayOfByteArray =
        new ArrayOfByteArray(
            ImmutableList.of(ByteBuffer.wrap(BYTE_ARRAY), ByteBuffer.wrap(BYTE_ARRAY)));
    Row expectedRow =
        Row.withSchema(ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA)
            .addArray(ImmutableList.of(BYTE_ARRAY, BYTE_ARRAY))
            .build();
    Row converted = registry.getToRowFunction(ArrayOfByteArray.class).apply(arrayOfByteArray);
    assertEquals(expectedRow, converted);
  }

  @Test
  public void testFromRowArrayOfBytes() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(ArrayOfByteArray.class);
    SchemaTestUtils.assertSchemaEquivalent(ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA, schema);

    ArrayOfByteArray expectedArrayOfByteArray =
        new ArrayOfByteArray(
            ImmutableList.of(ByteBuffer.wrap(BYTE_ARRAY), ByteBuffer.wrap(BYTE_ARRAY)));
    Row row =
        Row.withSchema(ARRAY_OF_BYTE_ARRAY_BEAM_SCHEMA)
            .addArray(ImmutableList.of(BYTE_ARRAY, BYTE_ARRAY))
            .build();
    ArrayOfByteArray converted = registry.getFromRowFunction(ArrayOfByteArray.class).apply(row);
    assertEquals(expectedArrayOfByteArray, converted);
  }
}
