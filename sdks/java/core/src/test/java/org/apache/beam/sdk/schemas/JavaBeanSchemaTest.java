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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Test;

public class JavaBeanSchemaTest {
  static final DateTime DATE = DateTime.parse("1979-03-14");
  static final byte[] BYTE_ARRAY = "bytearray".getBytes(Charset.defaultCharset());

  @DefaultSchema(JavaBeanSchema.class)
  public static class SimpleBean {
    private String str;
    private byte aByte;
    private short aShort;
    private int anInt;
    private long aLong;
    private boolean aBoolean;
    private DateTime dateTime;
    private Instant instant;
    private byte[] bytes;
    private ByteBuffer byteBuffer;
    private BigDecimal bigDecimal;
    private StringBuilder stringBuilder;

    public SimpleBean() { }

    public SimpleBean(String str, byte aByte, short aShort, int anInt, long aLong, boolean aBoolean,
                      DateTime dateTime, Instant instant, byte[] bytes, BigDecimal bigDecimal,
                      StringBuilder stringBuilder) {
      this.str = str;
      this.aByte = aByte;
      this.aShort = aShort;
      this.anInt = anInt;
      this.aLong = aLong;
      this.aBoolean = aBoolean;
      this.dateTime = dateTime;
      this.instant = instant;
      this.bytes = bytes;
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.bigDecimal = bigDecimal;
      this.stringBuilder = stringBuilder;
    }

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

    public Instant getInstant() {
      return instant;
    }

    public void setInstant(Instant instant) {
      this.instant = instant;
    }

    public byte[] getBytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      this.bytes = bytes;
    }

    public ByteBuffer getByteBuffer() {
      return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    public BigDecimal getBigDecimal() {
      return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
      this.bigDecimal = bigDecimal;
    }

    public StringBuilder getStringBuilder() {
      return stringBuilder;
    }

    public void setStringBuilder(StringBuilder stringBuilder) {
      this.stringBuilder = stringBuilder;
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
      .addDateTimeField("instant")
      .addByteArrayField("bytes")
      .addByteArrayField("byteBuffer")
      .addDecimalField("bigDecimal")
      .addStringField("stringBuilder")
      .build();

  private SimpleBean createSimple(String name) {
    return new SimpleBean(name, (byte) 1, (short) 2, 3, 4L,true,
        DATE, DATE.toInstant(), BYTE_ARRAY, BigDecimal.ONE,
        new StringBuilder(name).append("builder"));
  }

  private Row createSimpleRow(String name) {
    return Row.withSchema(SIMPLE_SCHEMA)
        .addValues(name, (byte) 1, (short) 2, 3, 4L, true, DATE, DATE, BYTE_ARRAY, BYTE_ARRAY,
            BigDecimal.ONE, new StringBuilder(name).append("builder").toString()).build();
  }

  @Test
  public void testSchema() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(SimpleBean.class);
    assertEquals(SIMPLE_SCHEMA, schema);
  }

  @Test
  public void testToRow() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    SimpleBean bean = createSimple("string");
    Row row = registry.getToRowFunction(SimpleBean.class).apply(bean);

    assertEquals(12, row.getFieldCount());
    assertEquals("string", row.getString(0));
    assertEquals((byte) 1, row.getByte(1).byteValue());
    assertEquals((short) 2, row.getInt16(2).shortValue());
    assertEquals((int) 3, row.getInt32(3).intValue());
    assertEquals((long) 4, row.getInt64(4).longValue());
    assertEquals(true, row.getBoolean(5));
    assertEquals(DATE, row.getDateTime(6));
    assertEquals(DATE, row.getDateTime(7));
    assertArrayEquals(BYTE_ARRAY, row.getBytes(8));
    assertArrayEquals(BYTE_ARRAY, row.getBytes(9));
    assertEquals(BigDecimal.ONE, row.getDecimal(10));
    assertEquals("stringbuilder", row.getString(11));
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
    assertEquals(true, bean.isaBoolean());
    assertEquals(DATE, bean.getDateTime());
    assertEquals(DATE.toInstant(), bean.getInstant());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getBytes());
    assertArrayEquals("not equal", BYTE_ARRAY, bean.getByteBuffer().array());
    assertEquals(BigDecimal.ONE, bean.getBigDecimal());
    assertEquals("stringbuilder", bean.getStringBuilder().toString());
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

  @DefaultSchema(JavaBeanSchema.class)
  public static class RecursiveBrean {
    private String str;
    private SimpleBean nested;

    public RecursiveBrean(String str, SimpleBean nested) {
      this.str = str;
      this.nested = nested;
    }

    public RecursiveBrean() {
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public SimpleBean getNested() {
      return nested;
    }

    public void setNested(SimpleBean nested) {
      this.nested = nested;
    }
  }
  static final Schema RECURSIVE_SCHEMA = Schema.builder()
      .addStringField("str")
      .addRowField("nested", SIMPLE_SCHEMA)
      .build();

  @Test
  public void testRecursiveGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(RECURSIVE_SCHEMA, registry.getSchema(RecursiveBrean.class));

    RecursiveBrean bean = new RecursiveBrean("str", createSimple("string"));
    Row row = registry.getToRowFunction(RecursiveBrean.class)
        .apply(bean);

    assertEquals("str", row.getString("str"));
    Row nestedRow = row.getRow("nested");
    assertEquals("string", nestedRow.getString(0));
    assertEquals((byte) 1, nestedRow.getByte(1).byteValue());
    assertEquals((short) 2, nestedRow.getInt16(2).shortValue());
    assertEquals((int) 3, nestedRow.getInt32(3).intValue());
    assertEquals((long) 4, nestedRow.getInt64(4).longValue());
    assertEquals(true, nestedRow.getBoolean(5));
    assertEquals(DATE, nestedRow.getDateTime(6));
    assertEquals(DATE, nestedRow.getDateTime(7));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes(8));
    assertArrayEquals("not equal", BYTE_ARRAY, nestedRow.getBytes(9));
    assertEquals(BigDecimal.ONE, nestedRow.getDecimal(10));
    assertEquals("stringbuilder", nestedRow.getString(11));
  }

  @Test
  public void testRecursiveSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row nestedRow = createSimpleRow("string");

    Row row = Row.withSchema(RECURSIVE_SCHEMA)
        .addValue("str")
        .addValue(nestedRow)
        .build();
    RecursiveBrean bean = registry.getFromRowFunction(RecursiveBrean.class).apply(row);
    assertEquals("str", bean.str);
    assertEquals("string", bean.nested.str);
    assertEquals((byte) 1, bean.nested.aByte);
    assertEquals((short) 2, bean.nested.aShort);
    assertEquals((int) 3, bean.nested.anInt);
    assertEquals((long) 4, bean.nested.aLong);
    assertEquals(true, bean.nested.aBoolean);
    assertEquals(DATE, bean.nested.dateTime);
    assertEquals(DATE.toInstant(), bean.nested.instant);
    assertArrayEquals("not equal", BYTE_ARRAY, bean.nested.bytes);
    assertArrayEquals("not equal", BYTE_ARRAY, bean.nested.byteBuffer.array());
    assertEquals(BigDecimal.ONE, bean.nested.bigDecimal);
    assertEquals("stringbuilder", bean.nested.stringBuilder.toString());
  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class PrimitiveArrayBean {
    // Test every type of array parameter supported.
    private List<String> strings;
    private int[] integers;
    private Long[] longs;

    public PrimitiveArrayBean(List<String> strings, int[] integers, Long[] longs) {
      this.strings = strings;
      this.integers = integers;
      this.longs = longs;
    }

    public PrimitiveArrayBean() {
    }

    public List<String> getStrings() {
      return strings;
    }

    public void setStrings(List<String> strings) {
      this.strings = strings;
    }

    public int[] getIntegers() {
      return integers;
    }

    public void setIntegers(int[] integers) {
      this.integers = integers;
    }

    public Long[] getLongs() {
      return longs;
    }

    public void setLongs(Long[] longs) {
      this.longs = longs;
    }
  }
  static final Schema PRIMITIVE_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("strings", FieldType.STRING)
      .addArrayField("integers", FieldType.INT32)
      .addArrayField("longs", FieldType.INT64)
      .build();

  @Test
  public void testPrimitiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(PRIMITIVE_ARRAY_SCHEMA, registry.getSchema(PrimitiveArrayBean.class));

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
    Row row = Row.withSchema(PRIMITIVE_ARRAY_SCHEMA)
        .addArray("a", "b", "c", "d")
        .addArray(1, 2, 3, 4)
        .addArray(42L, 43L, 44L, 45L)
        .build();
    PrimitiveArrayBean bean = registry.getFromRowFunction(PrimitiveArrayBean.class).apply(row);
    assertEquals(row.getArray(0), bean.strings);
    assertEquals(row.getArray(1), Ints.asList(bean.integers));
    assertEquals(row.getArray(2), Arrays.asList(bean.longs));
  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class RecursiveArrayBean {
    private List<SimpleBean> beans;

    public RecursiveArrayBean(List<SimpleBean> beans) {
      this.beans = beans;
    }

    public RecursiveArrayBean() {
    }

    public List<SimpleBean> getBeans() {
      return beans;
    }

    public void setBeans(List<SimpleBean> beans) {
      this.beans = beans;
    }
  }
  static final Schema RECURSIVE_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("beans", FieldType.row(SIMPLE_SCHEMA))
      .build();

  @Test
  public void testRecursiveArrayGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(RECURSIVE_ARRAY_SCHEMA, registry.getSchema(RecursiveArrayBean.class));

    SimpleBean simple1 = createSimple("string1");
    SimpleBean simple2 = createSimple("string2");
    SimpleBean simple3 = createSimple("string3");

    RecursiveArrayBean bean = new RecursiveArrayBean(Lists.newArrayList(simple1, simple2, simple3));
    Row row = registry.getToRowFunction(RecursiveArrayBean.class).apply(bean);
    List<Row> rows = row.getArray("beans");
    assertSame(simple1, registry.getFromRowFunction(RecursiveArrayBean.class).apply(rows.get(0)));
    assertSame(simple2, registry.getFromRowFunction(RecursiveArrayBean.class).apply(rows.get(1)));
    assertSame(simple3, registry.getFromRowFunction(RecursiveArrayBean.class).apply(rows.get(2)));
  }

  @Test
  public void testRecursiveArraySetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");;

    Row row = Row.withSchema(RECURSIVE_ARRAY_SCHEMA)
        .addArray(row1, row2, row3)
        .build();
    RecursiveArrayBean bean = registry.getFromRowFunction(RecursiveArrayBean.class).apply(row);
    assertEquals(3, bean.beans.size());
    assertEquals("string1", bean.beans.get(0).str);
    assertEquals("string2", bean.beans.get(1).str);
    assertEquals("string3", bean.beans.get(2).str);

  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class NestedArraysBean {
    private List<List<String>> lists;

    public NestedArraysBean(List<List<String>> lists) {
      this.lists = lists;
    }

    public NestedArraysBean() {
    }

    public List<List<String>> getLists() {
      return lists;
    }

    public void setLists(List<List<String>> lists) {
      this.lists = lists;
    }
  }
  static final Schema NESTED_ARRAY_SCHEMA = Schema.builder()
      .addArrayField("lists", FieldType.array(FieldType.STRING))
      .build();

  @Test
  public void testNestedArraysGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(NESTED_ARRAY_SCHEMA, registry.getSchema(NestedArraysBean.class));

    List<List<String>> listOfLists = Lists.newArrayList(
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
    List<List<String>> listOfLists = Lists.newArrayList(
        Lists.newArrayList("a", "b", "c"),
        Lists.newArrayList("d", "e", "f"),
        Lists.newArrayList("g", "h", "i"));
    Row row = Row.withSchema(NESTED_ARRAY_SCHEMA)
        .addArray(listOfLists)
        .build();
    NestedArraysBean bean = registry.getFromRowFunction(NestedArraysBean.class).apply(row);
    assertEquals(listOfLists, bean.lists);
  }

  @DefaultSchema(JavaBeanSchema.class)
  public static class MapBean {
    private Map<String, SimpleBean> map;

    public MapBean(Map<String, SimpleBean> map) {
      this.map = map;
    }

    public MapBean() {
    }

    public Map<String, SimpleBean> getMap() {
      return map;
    }

    public void setMap(Map<String, SimpleBean> map) {
      this.map = map;
    }
  }
  static final Schema MAP_SCHEMA = Schema.builder()
      .addMapField("map", FieldType.STRING, FieldType.row(SIMPLE_SCHEMA))
      .build();

  @Test
  public void testMapFieldGetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    assertEquals(MAP_SCHEMA, registry.getSchema(MapBean.class));

    SimpleBean simple1 = createSimple("string1");
    SimpleBean simple2 = createSimple("string2");
    SimpleBean simple3 = createSimple("string3");

    MapBean bean = new MapBean(ImmutableMap.of(
        "simple1",simple1 ,
        "simple2", simple2,
        "simple3", simple3));
    Row row = registry.getToRowFunction(MapBean.class).apply(bean);
    Map<String, Row> extractedMap = row.getMap("map");
    assertEquals(3, extractedMap.size());
    assertEquals("string1", extractedMap.get("simple1").getString(0));
    assertEquals("string2", extractedMap.get("simple2").getString(0));
    assertEquals("string3", extractedMap.get("simple3").getString(0));
  }

  @Test
  public void testMapFieldSetters() throws NoSuchSchemaException {
    SchemaRegistry registry = SchemaRegistry.createDefault();

    Row row1 = createSimpleRow("string1");
    Row row2 = createSimpleRow("string2");
    Row row3 = createSimpleRow("string3");
    Row row = Row.withSchema(MAP_SCHEMA)
        .addValue(ImmutableMap.of(
            "simple1", row1,
            "simple2", row2,
            "simple3", row3))
        .build();
    MapBean bean = registry.getFromRowFunction(MapBean.class).apply(row);
    assertEquals(3, bean.map.size());
    assertEquals("string1", bean.map.get("simple1").str);
    assertEquals("string2", bean.map.get("simple2").str);
    assertEquals("string3", bean.map.get("simple3").str);
  }
}
