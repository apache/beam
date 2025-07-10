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

import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.BEAN_WITH_BOXED_FIELDS_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.BEAN_WITH_BYTE_ARRAY_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_COLLECTION_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.NESTED_MAP_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.PRIMITIVE_ARRAY_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.PRIMITIVE_MAP_BEAN_SCHEMA;
import static org.apache.beam.sdk.schemas.utils.TestJavaBeans.SIMPLE_BEAN_SCHEMA;
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
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema.GetterTypeSupplier;
import org.apache.beam.sdk.schemas.JavaBeanSchema.SetterTypeSupplier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.BeanWithBoxedFields;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.BeanWithByteArray;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedCollectionBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NestedMapBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.NullableBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.PrimitiveArrayBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.PrimitiveMapBean;
import org.apache.beam.sdk.schemas.utils.TestJavaBeans.SimpleBean;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.DateTime;
import org.junit.Test;

/** Tests for the {@link JavaBeanUtils} class. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class JavaBeanUtilsTest {
  @Test
  public void testNullable() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<NullableBean>() {}, GetterTypeSupplier.INSTANCE);
    assertTrue(schema.getField("str").getType().getNullable());
    assertFalse(schema.getField("anInt").getType().getNullable());
  }

  @Test
  public void testSimpleBean() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<SimpleBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(SIMPLE_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedBean() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<NestedBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_BEAN_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveArray() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<PrimitiveArrayBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_ARRAY_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedArray() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<NestedArrayBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_ARRAY_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedCollection() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<NestedCollectionBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_COLLECTION_BEAN_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveMap() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<PrimitiveMapBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(PRIMITIVE_MAP_BEAN_SCHEMA, schema);
  }

  @Test
  public void testNestedMap() {
    Schema schema =
        JavaBeanUtils.schemaFromJavaBeanClass(
            new TypeDescriptor<NestedMapBean>() {}, GetterTypeSupplier.INSTANCE);
    SchemaTestUtils.assertSchemaEquivalent(NESTED_MAP_BEAN_SCHEMA, schema);
  }

  @Test
  public void testGeneratedSimpleGetters() {
    SimpleBean simpleBean = new SimpleBean();
    simpleBean.setStr("field1");
    simpleBean.setaByte((byte) 41);
    simpleBean.setaShort((short) 42);
    simpleBean.setAnInt(43);
    simpleBean.setaLong(44);
    simpleBean.setaBoolean(true);
    simpleBean.setDateTime(DateTime.parse("1979-03-14"));
    simpleBean.setInstant(DateTime.parse("1979-03-15").toInstant());
    simpleBean.setBytes("bytes1".getBytes(StandardCharsets.UTF_8));
    simpleBean.setByteBuffer(ByteBuffer.wrap("bytes2".getBytes(StandardCharsets.UTF_8)));
    simpleBean.setBigDecimal(new BigDecimal(42));
    simpleBean.setStringBuilder(new StringBuilder("stringBuilder"));

    List<FieldValueGetter<@NonNull SimpleBean, Object>> getters =
        JavaBeanUtils.getGetters(
            new TypeDescriptor<SimpleBean>() {},
            SIMPLE_BEAN_SCHEMA,
            new GetterTypeSupplier(),
            new DefaultTypeConversionsFactory());
    assertEquals(12, getters.size());
    assertEquals("str", getters.get(0).name());

    assertEquals("field1", getters.get(0).get(simpleBean));
    assertEquals((byte) 41, getters.get(1).get(simpleBean));
    assertEquals((short) 42, getters.get(2).get(simpleBean));
    assertEquals((int) 43, getters.get(3).get(simpleBean));
    assertEquals((long) 44, getters.get(4).get(simpleBean));
    assertTrue((Boolean) getters.get(5).get(simpleBean));
    assertEquals(DateTime.parse("1979-03-14").toInstant(), getters.get(6).get(simpleBean));
    assertEquals(DateTime.parse("1979-03-15").toInstant(), getters.get(7).get(simpleBean));
    assertArrayEquals(
        "Unexpected bytes",
        "bytes1".getBytes(StandardCharsets.UTF_8),
        (byte[]) getters.get(8).get(simpleBean));
    assertArrayEquals(
        "Unexpected bytes",
        "bytes2".getBytes(StandardCharsets.UTF_8),
        (byte[]) getters.get(9).get(simpleBean));
    assertEquals(new BigDecimal(42), getters.get(10).get(simpleBean));
    assertEquals("stringBuilder", getters.get(11).get(simpleBean).toString());
  }

  @Test
  public void testGeneratedSimpleSetters() {
    SimpleBean simpleBean = new SimpleBean();
    List<FieldValueSetter> setters =
        JavaBeanUtils.getSetters(
            new TypeDescriptor<SimpleBean>() {},
            SIMPLE_BEAN_SCHEMA,
            new SetterTypeSupplier(),
            new DefaultTypeConversionsFactory());
    assertEquals(12, setters.size());

    setters.get(0).set(simpleBean, "field1");
    setters.get(1).set(simpleBean, (byte) 41);
    setters.get(2).set(simpleBean, (short) 42);
    setters.get(3).set(simpleBean, (int) 43);
    setters.get(4).set(simpleBean, (long) 44);
    setters.get(5).set(simpleBean, true);
    setters.get(6).set(simpleBean, DateTime.parse("1979-03-14").toInstant());
    setters.get(7).set(simpleBean, DateTime.parse("1979-03-15").toInstant());
    setters.get(8).set(simpleBean, "bytes1".getBytes(StandardCharsets.UTF_8));
    setters.get(9).set(simpleBean, "bytes2".getBytes(StandardCharsets.UTF_8));
    setters.get(10).set(simpleBean, new BigDecimal(42));
    setters.get(11).set(simpleBean, "stringBuilder");

    assertEquals("field1", simpleBean.getStr());
    assertEquals((byte) 41, simpleBean.getaByte());
    assertEquals((short) 42, simpleBean.getaShort());
    assertEquals((int) 43, simpleBean.getAnInt());
    assertEquals((long) 44, simpleBean.getaLong());
    assertTrue(simpleBean.isaBoolean());
    assertEquals(DateTime.parse("1979-03-14"), simpleBean.getDateTime());
    assertEquals(DateTime.parse("1979-03-15").toInstant(), simpleBean.getInstant());
    assertArrayEquals(
        "Unexpected bytes", "bytes1".getBytes(StandardCharsets.UTF_8), simpleBean.getBytes());
    assertEquals(
        ByteBuffer.wrap("bytes2".getBytes(StandardCharsets.UTF_8)), simpleBean.getByteBuffer());
    assertEquals(new BigDecimal(42), simpleBean.getBigDecimal());
    assertEquals("stringBuilder", simpleBean.getStringBuilder().toString());
  }

  @Test
  public void testGeneratedSimpleBoxedGetters() {
    BeanWithBoxedFields bean = new BeanWithBoxedFields();
    bean.setaByte((byte) 41);
    bean.setaShort((short) 42);
    bean.setAnInt(43);
    bean.setaLong(44L);
    bean.setaBoolean(true);

    List<FieldValueGetter<@NonNull BeanWithBoxedFields, Object>> getters =
        JavaBeanUtils.getGetters(
            new TypeDescriptor<BeanWithBoxedFields>() {},
            BEAN_WITH_BOXED_FIELDS_SCHEMA,
            new JavaBeanSchema.GetterTypeSupplier(),
            new DefaultTypeConversionsFactory());
    assertEquals((byte) 41, getters.get(0).get(bean));
    assertEquals((short) 42, getters.get(1).get(bean));
    assertEquals((int) 43, getters.get(2).get(bean));
    assertEquals((long) 44, getters.get(3).get(bean));
    assertTrue((Boolean) getters.get(4).get(bean));
  }

  @Test
  public void testGeneratedSimpleBoxedSetters() {
    BeanWithBoxedFields bean = new BeanWithBoxedFields();
    List<FieldValueSetter> setters =
        JavaBeanUtils.getSetters(
            new TypeDescriptor<BeanWithBoxedFields>() {},
            BEAN_WITH_BOXED_FIELDS_SCHEMA,
            new SetterTypeSupplier(),
            new DefaultTypeConversionsFactory());

    setters.get(0).set(bean, (byte) 41);
    setters.get(1).set(bean, (short) 42);
    setters.get(2).set(bean, (int) 43);
    setters.get(3).set(bean, (long) 44);
    setters.get(4).set(bean, true);

    assertEquals((byte) 41, bean.getaByte().byteValue());
    assertEquals((short) 42, bean.getaShort().shortValue());
    assertEquals((int) 43, bean.getAnInt().intValue());
    assertEquals((long) 44, bean.getaLong().longValue());
    assertTrue(bean.getaBoolean().booleanValue());
  }

  @Test
  public void testGeneratedByteBufferSetters() {
    BeanWithByteArray bean = new BeanWithByteArray();
    List<FieldValueSetter> setters =
        JavaBeanUtils.getSetters(
            new TypeDescriptor<BeanWithByteArray>() {},
            BEAN_WITH_BYTE_ARRAY_SCHEMA,
            new SetterTypeSupplier(),
            new DefaultTypeConversionsFactory());
    setters.get(0).set(bean, "field1".getBytes(StandardCharsets.UTF_8));
    setters.get(1).set(bean, "field2".getBytes(StandardCharsets.UTF_8));

    assertArrayEquals("not equal", "field1".getBytes(StandardCharsets.UTF_8), bean.getBytes1());
    assertEquals(ByteBuffer.wrap("field2".getBytes(StandardCharsets.UTF_8)), bean.getBytes2());
  }
}
