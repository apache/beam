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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;
import org.joda.time.DateTime;
import org.junit.Test;

public class JavaBeanUtilsTest {
  public static class SimpleBean {
    private String str;
    private byte aByte;
    private short aShort;
    private int anInt;
    private long aLong;
    private boolean aBoolean;
    private DateTime dateTime;
    private byte[] bytes1;
    private ByteBuffer bytes2;

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public byte getaByte() {
      return aByte;
    }

    public void setaByte(byte aByte) {
      this.aByte = aByte;
    }

    public short getaShort() {
      return aShort;
    }

    public void setaShort(short aShort) {
      this.aShort = aShort;
    }

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public long getaLong() {
      return aLong;
    }

    public void setaLong(long aLong) {
      this.aLong = aLong;
    }

    public boolean isaBoolean() {
      return aBoolean;
    }

    public void setaBoolean(boolean aBoolean) {
      this.aBoolean = aBoolean;
    }

    public DateTime getDateTime() {
      return dateTime;
    }

    public void setDateTime(DateTime dateTime) {
      this.dateTime = dateTime;
    }

    public byte[] getBytes1() {
      return bytes1;
    }

    public void setBytes1(byte[] bytes1) {
      this.bytes1 = bytes1;
    }

    public ByteBuffer getBytes2() {
      return bytes2;
    }

    public void setBytes2(ByteBuffer bytes2) {
      this.bytes2 = bytes2;
    }
  }

  static final Schema SIMPLE_SCHEMA = Schema.builder()
      .addStringField("str")
      .addByteField("aByte")
      .addInt16Field("aShort")
      .addInt32Field("anInt")
      .addInt64Field("aLong")
      .addBooleanField("aBoolean")
      .addDateTimeField("dateTime")
      .addByteArrayField("bytes1")
      .addByteArrayField("bytes2")
      .build();

  @Test
  public void testSimpleBean() {
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(SimpleBean.class);
    assertEquals(SIMPLE_SCHEMA, schema);
  }

  static class NestedBean {
    private int anInt;
    private SimpleBean nested;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public SimpleBean getNested() {
      return nested;
    }

    public void setNested(SimpleBean nested) {
      this.nested = nested;
    }
  }

  @Test
  public void testNestedBean() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addRowField("nested", SIMPLE_SCHEMA)
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(NestedBean.class);
    assertEquals(expected, schema);
  }

  static class PrimitiveArrayBean {
    private int anInt;
    private String[] strings;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public String[] getStrings() {
      return strings;
    }

    public void setStrings(String[] strings) {
      this.strings = strings;
    }
  }

  @Test
  public void testPrimitiveArray() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("strings", FieldType.STRING)
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(PrimitiveArrayBean.class);
    assertEquals(expected, schema);
  }

  static class NestedArrayBean {
    private int anInt;
    private SimpleBean[] simples;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public SimpleBean[] getSimples() {
      return simples;
    }

    public void setSimples(SimpleBean[] simples) {
      this.simples = simples;
    }
  }

  @Test
  public void testNestedArray() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("simples", FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(NestedArrayBean.class);
    assertEquals(expected, schema);
  }

  static class NestedCollectionBean {
    private int anInt;
    private List<SimpleBean> simples;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public List<SimpleBean> getSimples() {
      return simples;
    }

    public void setSimples(List<SimpleBean> simples) {
      this.simples = simples;
    }
  }

  @Test
  public void testNestedCollection() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addArrayField("simples", FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(NestedCollectionBean.class);
    assertEquals(expected, schema);
  }

  static class PrimitiveMapBean {
    private int anInt;
    private Map<String, Integer> map;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public Map<String, Integer> getMap() {
      return map;
    }

    public void setMap(Map<String, Integer> map) {
      this.map = map;
    }
  }

  @Test
  public void testPrimitiveMap() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addMapField("map", FieldType.STRING, FieldType.INT32)
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(PrimitiveMapBean.class);
    assertEquals(expected, schema);
  }

  static class NestedMapBean {
    private int anInt;
    private Map<String, SimpleBean> map;

    public int getAnInt() {
      return anInt;
    }

    public void setAnInt(int anInt) {
      this.anInt = anInt;
    }

    public Map<String, SimpleBean> getMap() {
      return map;
    }

    public void setMap(Map<String, SimpleBean> map) {
      this.map = map;
    }
  }

  @Test
  public void testNestedMap() {
    Schema expected = Schema.builder()
        .addInt32Field("anInt")
        .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_SCHEMA))
        .build();
    Schema schema = JavaBeanUtils.schemaFromJavaBeanClass(NestedMapBean.class);
    assertEquals(expected, schema);
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
    simpleBean.setBytes1("bytes1".getBytes(Charset.defaultCharset()));
    simpleBean.setBytes2(ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())));

    List<FieldValueGetter> getters = JavaBeanUtils.getGetters(SimpleBean.class);
    assertEquals(9, getters.size());
    assertEquals("str", getters.get(0).name());

    assertEquals("field1", getters.get(0).get(simpleBean));
    assertEquals((byte) 41, getters.get(1).get(simpleBean));
    assertEquals((short) 42, getters.get(2).get(simpleBean));
    assertEquals((int) 43, getters.get(3).get(simpleBean));
    assertEquals((long) 44, getters.get(4).get(simpleBean));
    assertEquals(true, getters.get(5).get(simpleBean));
    assertEquals(DateTime.parse("1979-03-14"), getters.get(6).get(simpleBean));
    assertArrayEquals("Unexpected bytes",
        "bytes1".getBytes(Charset.defaultCharset()),
        (byte[]) getters.get(7).get(simpleBean));
    assertArrayEquals("Unexpected bytes",
        "bytes2".getBytes(Charset.defaultCharset()),
        (byte[]) getters.get(8).get(simpleBean));

  }

  @Test
  public void testGeneratedSimpleSetters() {
    SimpleBean simpleBean = new SimpleBean();
    List<FieldValueSetter> setters = JavaBeanUtils.getSetters(SimpleBean.class);
    assertEquals(9, setters.size());

    setters.get(0).set(simpleBean, "field1");
    setters.get(1).set(simpleBean, (byte) 41);
    setters.get(2).set(simpleBean, (short) 42);
    setters.get(3).set(simpleBean, (int) 43);
    setters.get(4).set(simpleBean, (long) 44);
    setters.get(5).set(simpleBean, true);
    setters.get(6).set(simpleBean, DateTime.parse("1979-03-14"));
    setters.get(7).set(simpleBean, "bytes1".getBytes(Charset.defaultCharset()));
    setters.get(8).set(simpleBean, "bytes2".getBytes(Charset.defaultCharset()));

    assertEquals("field1", simpleBean.getStr());
    assertEquals((byte) 41, simpleBean.getaByte());
    assertEquals((short) 42, simpleBean.getaShort());
    assertEquals((int) 43, simpleBean.getAnInt());
    assertEquals((long) 44, simpleBean.getaLong());
    assertEquals(true, simpleBean.isaBoolean());
    assertEquals(DateTime.parse("1979-03-14"), simpleBean.getDateTime());
    assertArrayEquals("Unexpected bytes",
        "bytes1".getBytes(Charset.defaultCharset()),
        simpleBean.getBytes1());
    assertEquals(ByteBuffer.wrap("bytes2".getBytes(Charset.defaultCharset())),
        simpleBean.getBytes2());
  }

  public static class BeanWithBoxedFields {
    private Byte aByte;
    private Short aShort;
    private Integer anInt;
    private Long aLong;
    private Boolean aBoolean;

    public Byte getaByte() {
      return aByte;
    }

    public void setaByte(Byte aByte) {
      this.aByte = aByte;
    }

    public Short getaShort() {
      return aShort;
    }

    public void setaShort(Short aShort) {
      this.aShort = aShort;
    }

    public Integer getAnInt() {
      return anInt;
    }

    public void setAnInt(Integer anInt) {
      this.anInt = anInt;
    }

    public Long getaLong() {
      return aLong;
    }

    public void setaLong(Long aLong) {
      this.aLong = aLong;
    }

    public Boolean getaBoolean() {
      return aBoolean;
    }

    public void setaBoolean(Boolean aBoolean) {
      this.aBoolean = aBoolean;
    }
  }

  @Test
  public void testGeneratedSimpleBoxedGetters() {
    BeanWithBoxedFields bean = new BeanWithBoxedFields();
    bean.setaByte((byte) 41);
    bean.setaShort((short) 42);
    bean.setAnInt(43);
    bean.setaLong(44L);
    bean.setaBoolean(true);

    List<FieldValueGetter> getters = JavaBeanUtils.getGetters(BeanWithBoxedFields.class);
    assertEquals((byte) 41, getters.get(0).get(bean));
    assertEquals((short) 42, getters.get(1).get(bean));
    assertEquals((int) 43, getters.get(2).get(bean));
    assertEquals((long) 44, getters.get(3).get(bean));
    assertEquals(true, getters.get(4).get(bean));
  }

  @Test
  public void testGeneratedSimpleBoxedSetters() {
    BeanWithBoxedFields bean = new BeanWithBoxedFields();
    List<FieldValueSetter> setters = JavaBeanUtils.getSetters(BeanWithBoxedFields.class);

    setters.get(0).set(bean, (byte) 41);
    setters.get(1).set(bean, (short) 42);
    setters.get(2).set(bean, (int) 43);
    setters.get(3).set(bean, (long) 44);
    setters.get(4).set(bean, true);

    assertEquals((byte) 41, bean.getaByte().byteValue());
    assertEquals((short) 42, bean.getaShort().shortValue());
    assertEquals((int) 43, bean.getAnInt().intValue());
    assertEquals((long) 44, bean.getaLong().longValue());
    assertEquals(true, bean.getaBoolean().booleanValue());
  }

  public static class BeanWithByteArray {
    private byte[] bytes1;
    private ByteBuffer bytes2;

    public byte[] getBytes1() {
      return bytes1;
    }

    public void setBytes1(byte[] bytes1) {
      this.bytes1 = bytes1;
    }

    public ByteBuffer getBytes2() {
      return bytes2;
    }

    public void setBytes2(ByteBuffer bytes2) {
      this.bytes2 = bytes2;
    }
  }

  @Test
  public void testGeneratedByteBufferSetters() {
    BeanWithByteArray bean = new BeanWithByteArray();
    List<FieldValueSetter> setters = JavaBeanUtils.getSetters(BeanWithByteArray.class);
    setters.get(0).set(bean, "field1".getBytes(Charset.defaultCharset()));
    setters.get(1).set(bean, "field2".getBytes(Charset.defaultCharset()));

    assertArrayEquals("not equal",
        "field1".getBytes(Charset.defaultCharset()), bean.getBytes1());
    assertEquals(ByteBuffer.wrap("field2".getBytes(Charset.defaultCharset())), bean.getBytes2());
  }
}
